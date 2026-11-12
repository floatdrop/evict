package s3fifo

import (
	"hash/maphash"
	"math"
	"math/rand/v2"
	"runtime"
	"testing"

	"github.com/floatdrop/evict/internal/cachetest"
	"github.com/floatdrop/evict/internal/ghost"
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

func TestQueueSizes(t *testing.T) {
	t.Parallel()

	// A tenth of a shard is for the small queue, rounded down, and there
	// are nine tenths as many ghosts as entries, rounded down as well.
	for capacity, want := range map[int]struct{ mainCap, ghosts int }{
		0: {0, 0}, 1: {1, 0}, 9: {9, 8}, 10: {9, 9}, 19: {18, 17}, 100: {90, 90}, 1005: {905, 904},
	} {
		c := New[int, int](capacity, WithShards(1))
		s := &c.shards[0]
		if s.capacity != capacity || s.mainCap != want.mainCap || s.ghosts.Cap() != want.ghosts {
			t.Errorf("capacity %d: the shard holds %d entries, %d of them in the main queue, and %d ghosts; want %d, %d and %d",
				capacity, s.capacity, s.mainCap, s.ghosts.Cap(), capacity, want.mainCap, want.ghosts)
		}
	}

	c := New[int, int](1000, WithShards(8))
	for i := range c.shards {
		if s := &c.shards[i]; s.capacity != 125 || s.mainCap != 113 {
			t.Errorf("shard %d holds %d entries, %d of them in the main queue; want 125 and 113", i, s.capacity, s.mainCap)
		}
	}
}

// TestShare checks the arithmetic of the ratios: a share is rounded down, and
// the defaults come out as the tenth and the nine tenths that they stand for,
// whatever the capacity.
func TestShare(t *testing.T) {
	t.Parallel()

	for capacity := range 1 << 21 {
		if small, ghosts := share(0.1, capacity), share(0.9, capacity); small != capacity/10 || ghosts != capacity*9/10 {
			t.Fatalf("capacity %d: a share of 0.1 is %d and one of 0.9 is %d, want %d and %d",
				capacity, small, ghosts, capacity/10, capacity*9/10)
		}
	}
	for _, tt := range []struct {
		ratio float64
		n     int
		want  int
	}{
		{0, 99, 0},
		{1, 12345, 12345},
		{0.5, 7, 3},
		{0.3, 1000, 300},
		{6, 100, 600},
		{2.85, 500, 1425},
		// In binary, 0.57 times 100 is a hair less than 57, and 0.7 times 10
		// a hair less than 7.
		{0.57, 100, 57},
		{0.7, 10, 7},
		{0.29, 100, 29},
		{0.999, 1000, 999},
		{0.9999, 1000, 999},
		{1.0 / 3, 3, 1},
		{2.0 / 3, 3, 2},
		// The hair is no more than that: a thousandth of an entry short of
		// one is none, and so is a hundred-thousandth.
		{0.09995, 10, 0},
		{0.099999, 10, 0},
		{0.999995, 2, 1},
	} {
		if got := share(tt.ratio, tt.n); got != tt.want {
			t.Errorf("share(%v, %d) = %d, want %d", tt.ratio, tt.n, got, tt.want)
		}
	}
}

// TestOptions checks what the options that tune a cache make of its shards,
// and that they keep to their ranges.
func TestOptions(t *testing.T) {
	t.Parallel()

	nan := math.NaN()
	for _, tt := range []struct {
		name             string
		options          []Option
		mainCap, ghosts  int
		promote, returns int
	}{
		{"defaults", nil, 90, 90, 2, 1},
		{"small queue", []Option{WithSmallRatio(0.3)}, 70, 90, 2, 1},
		{"no small queue", []Option{WithSmallRatio(0)}, 100, 90, 2, 1},
		{"nothing but a small queue", []Option{WithSmallRatio(1)}, 0, 90, 2, 1},
		{"small queue below the range", []Option{WithSmallRatio(-0.5)}, 100, 90, 2, 1},
		{"small queue above the range", []Option{WithSmallRatio(1.5)}, 0, 90, 2, 1},
		{"small queue that is not a number", []Option{WithSmallRatio(nan)}, 100, 90, 2, 1},
		{"ghosts", []Option{WithGhostRatio(3)}, 90, 300, 2, 1},
		{"no ghosts", []Option{WithGhostRatio(0)}, 90, 0, 2, 1},
		{"ghosts below the range", []Option{WithGhostRatio(-1)}, 90, 0, 2, 1},
		{"ghosts above the range", []Option{WithGhostRatio(100)}, 90, 800, 2, 1},
		{"ghosts that are not a number", []Option{WithGhostRatio(nan)}, 90, 0, 2, 1},
		{"promotion", []Option{WithPromotionReads(1)}, 90, 90, 1, 1},
		{"promotion below the range", []Option{WithPromotionReads(-3)}, 90, 90, 1, 1},
		{"promotion above the range", []Option{WithPromotionReads(9)}, 90, 90, 3, 1},
		{"returns", []Option{WithGhostReturns(2)}, 90, 90, 2, 2},
		{"returns below the range", []Option{WithGhostReturns(0)}, 90, 90, 2, 1},
		{"returns above the range", []Option{WithGhostReturns(1000)}, 90, 90, 2, 255},
		{"the last one counts", []Option{WithSmallRatio(0.5), WithGhostRatio(2), WithSmallRatio(0.2)}, 80, 200, 2, 1},
		{"all of them", []Option{WithSmallRatio(0.5), WithGhostRatio(6), WithPromotionReads(3), WithGhostReturns(2)}, 50, 600, 3, 2},
	} {
		c := New[int, int](100, append(tt.options, WithShards(1))...)
		s := &c.shards[0]
		if s.mainCap != tt.mainCap || s.ghosts.Cap() != tt.ghosts || int(s.promote) != tt.promote || s.returns != tt.returns {
			t.Errorf("%s: the main queue has room for %d, there are %d ghosts, promotion takes %d reads and a ghost %d returns; want %d, %d, %d and %d",
				tt.name, s.mainCap, s.ghosts.Cap(), s.promote, s.returns, tt.mainCap, tt.ghosts, tt.promote, tt.returns)
		}
		// The ghosts count how often they are asked for if they have to
		// come back more than once, and only then.
		if counts := counting(&s.ghosts); tt.ghosts > 0 && counts != (tt.returns > 1) {
			t.Errorf("%s: that the ghosts count is %t, with %d returns", tt.name, counts, tt.returns)
		}
	}

	// The shares are those of each shard, and what goes for one shard goes
	// for all.
	c := New[int, int](1000, WithShards(8), WithSmallRatio(0.2), WithGhostRatio(2), WithPromotionReads(1), WithGhostReturns(3))
	for i := range c.shards {
		s := &c.shards[i]
		if s.capacity != 125 || s.mainCap != 100 || s.ghosts.Cap() != 250 {
			t.Errorf("shard %d holds %d entries, %d of them in the main queue, and %d ghosts; want 125, 100 and 250",
				i, s.capacity, s.mainCap, s.ghosts.Cap())
		}
		if counts := counting(&s.ghosts); s.promote != 1 || s.returns != 3 || !counts {
			t.Errorf("shard %d: promotion takes %d reads and a ghost %d returns, and that the ghosts count is %t; want 1, 3 and true",
				i, s.promote, s.returns, counts)
		}
	}
}

// counting reports whether a set of ghosts counts how often each one is asked
// for: one that does keeps a ghost that is to come back twice the first time.
// The set is used up by asking.
func counting(ghosts *ghost.Set) bool {
	const hash = 12345
	ghosts.Add(hash)
	return !ghosts.Recall(hash, 2)
}

// TestBookkeeping checks through a long run of operations that the counters
// of a shard agree with its lists, that every entry is tagged with the queue
// it is in, and that the queues stay within their bounds.
func TestBookkeeping(t *testing.T) {
	t.Parallel()

	c := New[int, int](40, WithShards(1))
	s := &c.shards[0]

	count := func(head uint32) int {
		n := 0
		for idx := s.slab.Links[head].Next; idx != head; idx = s.slab.Links[idx].Next {
			if got := s.slab.Links[idx].Tag & 1; got != head {
				t.Fatalf("node %d is in queue %d but tagged for queue %d", idx, head, got)
			}
			if f := s.slab.Nodes[idx].Meta(); f > maxFrequency {
				t.Fatalf("node %d has counted %d reads, want at most %d", idx, f, maxFrequency)
			}
			n++
		}
		return n
	}

	rng := rand.New(rand.NewPCG(5, 6))
	deleted, promoted, over := [queues]int{}, 0, 0
	for step := range 100_000 {
		key := rng.IntN(120)
		mains := s.mains
		switch op := rng.IntN(100); {
		case op < 30:
			c.Set(key, step)
		case op < 90:
			c.Get(rng.IntN(60)) // Some keys are read much more than others.
		case op < 99:
			if idx := s.slab.Find(uint32(maphash.Comparable(c.seed, key)), key); idx != 0 {
				deleted[s.slab.Links[idx].Tag&1]++
			}
			c.Delete(key)
		default:
			c.Clear()
		}
		if s.mains > mains {
			promoted++
		}

		smalls, mains := count(small), count(main)
		if smalls != s.smalls || mains != s.mains {
			t.Fatalf("step %d: the queues hold %d and %d entries, the shard counts %d and %d", step, smalls, mains, s.smalls, s.mains)
		}
		if smalls+mains > s.capacity || s.ghosts.Len() > s.ghosts.Cap() {
			t.Fatalf("step %d: %d entries and %d ghosts, want at most %d and %d", step, smalls+mains, s.ghosts.Len(), s.capacity, s.ghosts.Cap())
		}
		// The main queue may be over its share: an entry that moves up or
		// comes back as a ghost joins it whether it has room or not. It is
		// then the one to evict next.
		if mains > s.mainCap {
			over++
		}
		if got := c.Len(); got != smalls+mains {
			t.Fatalf("step %d: Len() = %d, want %d", step, got, smalls+mains)
		}
	}

	// The run is only worth something if it went down every road.
	if deleted[small] == 0 || deleted[main] == 0 || promoted == 0 || over == 0 {
		t.Errorf("%d and %d entries were deleted from the queues, %d moved to the main queue, and it was over its share %d times; want some of each",
			deleted[small], deleted[main], promoted, over)
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

// TestGhostOf checks that an entry leaves the ghost that its key is asked
// for by when it comes back.
func TestGhostOf(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(3, 4))
	for range 10_000 {
		h := rng.Uint64()
		want := ghostOf(uint32(h), tagOf(h, small))
		if got := ghostOf(uint32(h), tagOf(h, main)); got != want {
			t.Fatalf("the ghost of %#x is %#x in the small queue and %#x in the main queue", h, want, got)
		}
		// It is the hash, less the one bit that the ghosts keep to
		// themselves.
		if want != (h>>33)<<32|h&(1<<32-1) || want>>63 != 0 {
			t.Fatalf("the ghost of %#x is %#x", h, want)
		}
		if tagOf(h, main)&1 != main || tagOf(h, small)&1 != small {
			t.Fatalf("the tags of %#x do not say which queue they are for", h)
		}
	}
}

// TestShardPadding checks that the shards of a cache keep out of each other's
// cache lines and take up whole lines, whatever the keys and values.
func TestShardPadding(t *testing.T) {
	t.Parallel()

	cachetest.Padded(t, Cache[int, int]{}.shards)
	cachetest.Padded(t, Cache[string, []byte]{}.shards)
}
