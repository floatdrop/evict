package tinylfu

import (
	"hash/maphash"
	"math"
	"math/rand/v2"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/evict/internal/cachetest"
	"github.com/floatdrop/evict/internal/sketch"
)

func TestShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		capacity int
		opts     []Option
		want     int
	}{
		{"small caches stay in one piece", 511, nil, 1},
		{"no capacity", 0, []Option{WithShards(8)}, 1},
		{"as requested", 1000, []Option{WithShards(4)}, 4},
		{"rounded up to a power of two", 1000, []Option{WithShards(5)}, 8},
		{"limited to the capacity", 6, []Option{WithShards(64)}, 4},
		{"zero means the default", 100, []Option{WithShards(0)}, 1},
		{"the last option wins", 1000, []Option{WithShards(2), WithShards(16)}, 16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := New[int, int](tt.capacity, tt.opts...)

			if got := len(c.shards); got != tt.want {
				t.Errorf("%d shards, want %d", got, tt.want)
			}
			if got := int(c.mask) + 1; got != len(c.shards) {
				t.Errorf("mask selects among %d shards, there are %d", got, len(c.shards))
			}

			total := 0
			for i := range c.shards {
				total += c.shards[i].capacity
				if got, want := c.shards[i].slab.Cap(), c.shards[i].capacity; got != want {
					t.Errorf("shard %d has a slab for %d entries, want %d", i, got, want)
				}
			}
			if want := max(tt.capacity, 0); total != want || c.Cap() != want {
				t.Errorf("shards hold %d entries, Cap() = %d, want %d", total, c.Cap(), want)
			}
		})
	}
}

func TestDefaultShards(t *testing.T) {
	t.Parallel()

	c := New[int, int](1 << 20)

	if runtime.GOMAXPROCS(0) > 1 && len(c.shards) == 1 {
		t.Error("a large cache has a single shard by default")
	}

	// Every shard gets its share of the keys. With twice as many keys as
	// there is room for, each of them is as good as certain to fill up.
	for key := range 2 * c.Cap() {
		c.Set(key, key)
	}
	for i := range c.shards {
		s := &c.shards[i]
		if got, want := s.slab.Len(), s.capacity; got < want*9/10 {
			t.Errorf("shard %d holds %d entries of %d after %d insertions", i, got, want, 2*c.Cap())
		}
	}
}

// TestShardsShareTheWindow checks that the shards of a cache have one size
// for their small queues, in proportion to their capacities, and that what
// one of them is asked for moves it for all.
func TestShardsShareTheWindow(t *testing.T) {
	t.Parallel()

	// Eight shards: four of 129 entries and four of 128. They hand on their
	// counts every 16 misses.
	c := New[int, int](1028, WithShards(8))
	// The sketch of a shard: for its entries, with the counters halved
	// after ten requests for each.
	for i := range c.shards {
		s := &c.shards[i]
		want := 129 - i/4
		var seen sketch.Sketch
		seen.Init(want, 10*want)
		if s.capacity != want || s.batch != 16 || s.win != &c.window || !reflect.DeepEqual(&s.seen, &seen) {
			t.Errorf("shard %d holds %d entries and hands on its counts every %d misses; want %d and 16, and a sketch for as many entries",
				i, s.capacity, s.batch, want)
		}
	}
	if c.window.base != 129 || c.window.climber.period != 40*1028 {
		t.Fatalf("the window is for shards of %d entries and moves every %v requests, want 129 and %d",
			c.window.base, c.window.climber.period, 40*1028)
	}

	sizes := func() (all [8]int) {
		for i := range c.shards {
			all[i] = c.window.sizeFor(c.shards[i].capacity)
		}
		return all
	}
	// A hundredth of 129 is less than one, and one is the least.
	if got := sizes(); got != [8]int{1, 1, 1, 1, 1, 1, 1, 1} {
		t.Fatalf("the small queues are to have %v entries, want one each", got)
	}
	c.Resize(100)
	if got, want := sizes(), [8]int{100, 100, 100, 100, 99, 99, 99, 99}; got != want {
		t.Fatalf("after Resize(100) the small queues are to have %v entries, want %v", got, want)
	}
	c.Resize(1)

	// Keys that all belong to the first shard move the size for all, once
	// the misses that they bring reach a period's worth. The shard hands
	// them on in batches, so that is up to a batch later.
	sets := 0
	for key := 0; c.Window() == 1; key++ {
		if h := maphash.Comparable(c.seed, key); uint32(h>>32)&c.mask == 0 {
			c.Set(key, key)
			sets++
		}
		if sets > 40*1028+16 {
			t.Fatalf("the size has not moved after %d misses, want it to after %d", sets, 40*1028)
		}
	}
	if sets < 40*1028 || sets%16 != 0 {
		t.Errorf("the size moved after %d misses, want %d or the next multiple of 16", sets, 40*1028)
	}
	// Nothing was read: a share of 0 answered, measured for the first time.
	// The size has gone up by the first step, and moves next after a
	// period's worth of misses.
	// The first step is 129/16 entries: 9.0625 for the large shards, and
	// in proportion 8.99 for the others.
	if got := sizes(); got[0] != 9 || got[7] != 8 {
		t.Errorf("after the first move the small queues are to have %v entries", got)
	}
	if got, want := c.window.due.Load(), uint64(sets)+40*1028; got != want {
		t.Errorf("the next move is due at %d misses, want %d", got, want)
	}
	for i := 1; i < len(c.shards); i++ {
		if s := &c.shards[i]; s.slab.Len() != 0 {
			t.Errorf("shard %d holds %d entries, want none", i, s.slab.Len())
		}
	}
}

// TestBatches checks how many misses a shard keeps to itself before it hands
// them on: none in a cache in one piece, and in one in several shards one in
// eight of its entries, up to 32.
func TestBatches(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		capacity, shards int
		batch            uint64
	}{
		{100, 1, 1},
		{100_000, 1, 1},
		{4096, 8, 32},
		{1024, 4, 32},
		{1028, 8, 16},
		{1024, 64, 2},
		{64, 64, 1},
	} {
		c := New[int, int](tt.capacity, WithShards(tt.shards))
		for i := range c.shards {
			if got := c.shards[i].batch; got != tt.batch {
				t.Errorf("capacity %d in %d shards: shard %d hands on its counts every %d misses, want %d",
					tt.capacity, tt.shards, i, got, tt.batch)
			}
		}
	}
}

// TestClearKeepsBacklog checks that Clear leaves a shard that hands on its
// misses in batches with the ones that it has yet to hand on. The window's
// next measurement starts without them, as all of them start without some,
// and ends without some of its own.
func TestClearKeepsBacklog(t *testing.T) {
	t.Parallel()

	c := New[int, int](1024, WithShards(4)) // Batches of 32.
	s := &c.shards[0]
	sets := 0
	for key := 0; sets < 5; key++ {
		if h := maphash.Comparable(c.seed, key); uint32(h>>32)&c.mask == 0 {
			c.Set(key, key)
			sets++
		}
	}
	if s.misses != 5 || c.window.misses.Load() != 0 {
		t.Fatalf("after 5 misses the shard holds back %d and the window has %d, want 5 and 0", s.misses, c.window.misses.Load())
	}
	c.Clear()
	if s.misses != 5 || c.window.missesThen != 0 {
		t.Errorf("after Clear the shard holds back %d misses and the window measures from %d, want 5 and 0", s.misses, c.window.missesThen)
	}
}

// check fails the test if the window is not as it should be between moves:
// counted from no later than now, with the next move due a period after the
// last one, and the size within its bounds and as the shards see it.
func (w *window) check(t *testing.T) {
	t.Helper()

	w.mu.Lock()
	defer w.mu.Unlock()

	hits, misses, due := w.readers.Hits()+w.hits.Load(), w.misses.Load(), w.due.Load()
	if w.hitsThen > hits || w.missesThen > misses {
		t.Errorf("the window measures from %d hits and %d misses, of which there have been %d and %d", w.hitsThen, w.missesThen, hits, misses)
	}
	if due-w.missesThen != w.climber.due() {
		t.Errorf("the next move is due at %d misses, %d after the last one, want %d after", due, due-w.missesThen, w.climber.due())
	}
	if c := &w.climber; c.target < c.least || c.target > c.most {
		t.Errorf("the target %v is outside [%v, %v]", c.target, c.least, c.most)
	}
	if got := math.Float64frombits(w.target.Load()); got != w.climber.target {
		t.Errorf("the shards see a target of %v, the climber has %v", got, w.climber.target)
	}
}

// TestMovesOnce checks that a move that is due happens once, however many
// writers find it due before it has happened.
func TestMovesOnce(t *testing.T) {
	t.Parallel()

	c := New[int, int](100, WithShards(1))
	w := &c.window
	w.misses.Store(w.due.Load()) // Due, as two writers find it.

	w.mu.Lock()
	defer w.mu.Unlock()
	w.move() // The first of them moves it,
	target, step, due := w.climber.target, w.climber.step, w.due.Load()
	if !w.climber.measured || target == 1 {
		t.Fatalf("the size has not moved: %+v", w.climber)
	}
	w.move() // and the second finds it moved.
	if w.climber.target != target || w.climber.step != step || w.due.Load() != due {
		t.Errorf("the size moved twice: to %v and then %v, with the next move due at %d and then %d",
			target, w.climber.target, due, w.due.Load())
	}
}

// TestWindowUnderLoad has goroutines read and write a cache, and clear it
// every so often, while the window is checked. Then a move that is due has
// to happen at the next batch of misses.
func TestWindowUnderLoad(t *testing.T) {
	t.Parallel()

	for _, n := range []int{1, 8, 64} {
		c := New[int, int](256, WithShards(n))
		w := &c.window
		var (
			stop atomic.Bool
			wg   sync.WaitGroup
		)
		for g := range 8 {
			wg.Go(func() {
				rng := rand.New(rand.NewPCG(uint64(g), 9))
				for !stop.Load() {
					// Sometimes a few hot keys, sometimes many cold ones.
					key := rng.IntN(300 + rng.IntN(3)*1000)
					if _, ok := c.Get(key); !ok {
						c.Set(key, key)
					}
					if rng.IntN(50) == 0 {
						c.Set(key, 1)
					}
				}
			})
		}
		wg.Go(func() {
			for !stop.Load() {
				c.Clear()
				time.Sleep(3 * time.Millisecond)
			}
		})
		for start := time.Now(); time.Since(start) < 300*time.Millisecond; time.Sleep(50 * time.Microsecond) {
			w.check(t)
		}
		stop.Store(true)
		wg.Wait()
		w.check(t)

		// Nobody else is about now, so the fields are this goroutine's to
		// read without the lock.
		last, due := w.missesThen, w.due.Load()
		for key := -1; w.missesThen == last; key-- {
			c.Set(key, key)
			if w.misses.Load() > due+uint64(n)*batchSize {
				t.Fatalf("%d shards: no move at %d misses, though one was due at %d", n, w.misses.Load(), due)
			}
		}
		// The move took everything that had been handed on into account.
		w.check(t)
		if w.missesThen != w.misses.Load() || w.missesThen < due {
			t.Errorf("%d shards: the move was at %d misses, with %d handed on and the move due at %d", n, w.missesThen, w.misses.Load(), due)
		}
	}
}

// TestWindowCounts checks what the window goes by: the hits that readers
// count in their slots, those counted under the lock of a shard, and the
// misses, all since the size last moved.
func TestWindowCounts(t *testing.T) {
	t.Parallel()

	c := New[int, int](100, WithShards(1))
	w := &c.window
	misses := func(n int) {
		for range n {
			c.Set(-1-int(w.misses.Load()), 0)
		}
	}
	// Keys -1, -2 and so on. The newest of them is sure to be in the cache,
	// in the small queue; others may be or not.
	misses(3999)
	if w.climber.measured {
		t.Fatal("the size moved after 3999 misses, want 4000")
	}
	for range 7 {
		c.Get(-3999) // Hits that readers count in their slots.
	}
	c.Set(-3999, 1) // A write to an entry that is there counts as a hit.
	release := c.OccupyReaders(0)
	c.Get(-3999)  // As does a read that takes the lock.
	c.Get(-10000) // Never there: no hit.
	c.Peek(-3999)
	release()
	c.Peek(-3999) // Peeking counts for nothing.
	misses(1)
	if !w.climber.measured || !near(w.climber.rate, 9.0/4009) {
		t.Fatalf("after 9 hits and 4000 misses the rate is %v (measured: %t), want %v", w.climber.rate, w.climber.measured, 9.0/4009)
	}

	// The next period counts from there.
	due := w.due.Load()
	if want := uint64(4000 + 3991); due != want { // 4000 times 4000/4009, rounded down.
		t.Fatalf("the next move is due at %d misses, want %d", due, want)
	}
	for range 5 {
		c.Get(-4000)
	}
	misses(int(due - w.misses.Load()))
	if !near(w.climber.rate, 5.0/float64(5+due-4000)) {
		t.Errorf("after the next period the rate is %v, want %v", w.climber.rate, 5.0/float64(5+due-4000))
	}

	// Clear starts the measurements over, and keeps the size and what the
	// last measurement says of the misses to wait for.
	size, rate := c.Window(), w.climber.rate
	for range 100 {
		c.Get(-int(due)) // Hits before Clear, which do not count after it,
	}
	c.Set(-int(due), 2) // nor does one that the shard counted itself.
	c.Clear()
	if w.climber.measured || c.Window() != size {
		t.Fatalf("after Clear the size is %d and something is measured (%t), want %d and nothing", c.Window(), w.climber.measured, size)
	}
	from, next := w.misses.Load(), w.due.Load()
	if want := from + uint64(4000*(1-rate)); next != want {
		t.Fatalf("after Clear at %d misses the next move is due at %d, want %d", from, next, want)
	}
	misses(1)
	for range 3 {
		c.Get(-int(from + 1))
	}
	misses(int(next - w.misses.Load()))
	if want := 3.0 / float64(3+next-from); !w.climber.measured || !near(w.climber.rate, want) {
		t.Errorf("the first period after Clear: rate %v (measured: %t), want %v", w.climber.rate, w.climber.measured, want)
	}
}

// TestBookkeeping checks through a long run of operations that the counters
// of a shard agree with its lists, that every entry is tagged with the queue
// it is in and with what the sketch knows its key by, and that the queues
// and the counts stay within their bounds.
func TestBookkeeping(t *testing.T) {
	t.Parallel()

	c := New[int, int](40, WithShards(1))
	s := &c.shards[0]

	count := func(head uint32) int {
		n := 0
		for idx := s.slab.Links[head].Next; idx != head; idx = s.slab.Links[idx].Next {
			tag := s.slab.Links[idx].Tag
			if got := tag & 1; got != head {
				t.Fatalf("node %d is in queue %d but tagged for queue %d", idx, head, got)
			}
			if reads := s.slab.Nodes[idx].Meta.Load(); reads > maxCount {
				t.Fatalf("node %d has counted %d reads, want at most %d", idx, reads, maxCount)
			}
			// What the main queue leaves an entry with is one less than
			// what it came by, and the small queue leaves nothing.
			if left := kept(tag); left >= maxCount || head == small && left != 0 {
				t.Fatalf("node %d in queue %d was left with a count of %d", idx, head, left)
			}
			if got, want := printIn(s.slab.Hash(idx), tag), printOf(maphash.Comparable(c.seed, s.slab.Nodes[idx].Key)); got != want {
				t.Fatalf("node %d is tagged for the sketch as %#x, want %#x", idx, got, want)
			}
			n++
		}
		return n
	}

	rng := rand.New(rand.NewPCG(5, 6))
	deleted, promoted, over := [queues]int{}, 0, 0
	sizes := map[int]bool{}
	for step := range 200_000 {
		key := rng.IntN(120)
		mains, evicted := s.mains, false
		switch op := rng.IntN(100); {
		case step%10_000 == 9_999:
			c.Clear()
		case op < 30:
			old, _, displaced := c.Set(key, step)
			evicted = displaced && old != key
		case op < 90:
			c.Get(rng.IntN(60)) // Some keys are read much more than others.
		default:
			if idx := s.slab.Find(uint32(maphash.Comparable(c.seed, key)), key); idx != 0 {
				deleted[s.slab.Links[idx].Tag&1]++
			}
			c.Delete(key)
		}
		if s.mains > mains {
			promoted++
		}

		smalls, mains := count(small), count(main)
		if smalls != s.smalls || mains != s.mains {
			t.Fatalf("step %d: the queues hold %d and %d entries, the shard counts %d and %d", step, smalls, mains, s.smalls, s.mains)
		}
		if smalls+mains > s.capacity {
			t.Fatalf("step %d: %d entries, want at most %d", step, smalls+mains, s.capacity)
		}
		if got := c.Len(); got != smalls+mains {
			t.Fatalf("step %d: Len() = %d, want %d", step, got, smalls+mains)
		}
		size := s.win.sizeFor(s.capacity)
		if size < 1 || size > 32 {
			t.Fatalf("step %d: the small queue is to have %d entries, want 1 to 32", step, size)
		}
		sizes[size] = true
		// An eviction leaves the small queue within its share. Without
		// one it may be over: after deletions there is room that is
		// nobody's.
		if smalls > size {
			if evicted {
				t.Fatalf("step %d: the small queue holds %d entries after an eviction, want at most %d", step, smalls, size)
			}
			over++
		}
	}

	// The run is only worth something if it went down every road.
	if deleted[small] == 0 || deleted[main] == 0 || promoted == 0 || over == 0 || len(sizes) < 5 {
		t.Errorf("%d and %d entries were deleted from the queues, %d moved to the main queue, the small queue was over its share %d times and had %d different shares; want some of each",
			deleted[small], deleted[main], promoted, over, len(sizes))
	}
}

// TestHashSplit checks that a shard does not index its keys by the same bits
// of the hash that put them into the shard. If it did, the keys of a shard
// would all ask for the same few slots of its index, and every lookup would
// have to wade through the others.
func TestHashSplit(t *testing.T) {
	t.Parallel()

	// Many small shards: the index of a shard has fewer slots than there
	// are shards, so bits that were shared would leave no choice at all.
	const shardCount, perShard = 1024, 256

	c := New[uint64, struct{}](shardCount*perShard, WithShards(shardCount))
	rng := rand.New(rand.NewPCG(1, 2))
	for range 2 * shardCount * perShard {
		c.Set(rng.Uint64(), struct{}{})
	}

	worst := 0
	for i := range c.shards {
		worst = max(worst, c.shards[i].slab.MaxDisplacement())
	}
	// With hashes that are independent, an entry is rarely more than a
	// dozen slots from where it belongs. With shared bits it is hundreds.
	if worst > 64 {
		t.Errorf("an entry is %d slots away from the slot its hash asks for", worst)
	}
}

// TestTags checks what a tag holds: the queue, a count, and enough of the
// hash to tell the sketch which key an entry has, whatever the other two
// are.
func TestTags(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(3, 4))
	for range 10_000 {
		h := rng.Uint64()

		// The sketch knows a key by its hash, less the three bits that a
		// tag has no room for.
		known := printOf(h)
		if want := h>>35<<32 | h&(1<<32-1); known != want {
			t.Fatalf("the sketch knows %#x by %#x, want %#x", h, known, want)
		}
		for bit := range 64 {
			if other := printOf(h ^ 1<<bit); (other == known) != (bit >= 32 && bit < 35) {
				t.Fatalf("with bit %d the other way, the sketch knows %#x by %#x and by %#x", bit, h, known, other)
			}
		}

		for _, queue := range []uint32{small, main} {
			tag := tagOf(h, queue)
			if tag&1 != queue || kept(tag) != 0 {
				t.Fatalf("the tag of %#x for queue %d says queue %d and a count of %d", h, queue, tag&1, kept(tag))
			}
			for count := range uint32(maxCount) {
				// Whatever the count was before.
				for before := range uint32(maxCount) {
					with := keep(keep(tag, before), count)
					if with&1 != queue || kept(with) != count || printIn(uint32(h), with) != known {
						t.Fatalf("the tag of %#x for queue %d with a count of %d says queue %d, a count of %d and %#x for the sketch, want %#x",
							h, queue, count, with&1, kept(with), printIn(uint32(h), with), known)
					}
				}
			}
		}
		// Moving up is a matter of one bit.
		if tagOf(h, small)|main != tagOf(h, main) {
			t.Fatalf("the tags of %#x for the two queues differ in more than the queue", h)
		}
	}
}

// TestShardPadding checks that the shards of a cache keep out of each other's
// cache lines.
func TestShardPadding(t *testing.T) {
	t.Parallel()

	cachetest.Padded(t, shard[int, int]{})
	cachetest.Padded(t, shard[string, []byte]{})
}
