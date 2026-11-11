package sieve_test

import (
	"fmt"
	"iter"
	"math/rand/v2"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	"github.com/floatdrop/evict"
	"github.com/floatdrop/evict/internal/cachetest"
	"github.com/floatdrop/evict/sieve"
)

var factories = cachetest.Factories{
	Int: func(capacity, shards int) evict.Cache[int, int] {
		return sieve.New[int, int](capacity, sieve.WithShards(shards))
	},
	String: func(capacity, shards int) evict.Cache[string, string] {
		return sieve.New[string, string](capacity, sieve.WithShards(shards))
	},
	Float: func(capacity, shards int) evict.Cache[float64, int] {
		return sieve.New[float64, int](capacity, sieve.WithShards(shards))
	},
	Blob: func(capacity, shards int) evict.Cache[int, *cachetest.Blob] {
		return sieve.New[int, *cachetest.Blob](capacity, sieve.WithShards(shards))
	},
	BlobKey: func(capacity, shards int) evict.Cache[*cachetest.Blob, int] {
		return sieve.New[*cachetest.Blob, int](capacity, sieve.WithShards(shards))
	},
	Wide: func(capacity, shards int) evict.Cache[string, cachetest.Wide] {
		return sieve.New[string, cachetest.Wide](capacity, sieve.WithShards(shards))
	},
}

func TestCache(t *testing.T) {
	t.Parallel()

	cachetest.Run(t, factories)
}

// TestNoAllocs is not parallel: counting allocations needs the process to
// itself.
func TestNoAllocs(t *testing.T) {
	cachetest.NoAllocs(t, factories)
}

// keys returns the keys of the cache in the order of All.
func keys[K comparable, V any](c *sieve.Cache[K, V]) []K {
	var all []K
	for k := range c.All() {
		all = append(all, k)
	}
	return all
}

func TestEviction(t *testing.T) {
	t.Parallel()

	c := sieve.New[string, int](4, sieve.WithShards(1))
	expect := func(want ...string) {
		t.Helper()
		if got := keys(c); !slices.Equal(got, want) {
			t.Fatalf("cache holds %v, want %v", got, want)
		}
	}
	evicts := func(key string, value int, wantKey string, wantValue int) {
		t.Helper()
		if k, v, displaced := c.Set(key, value); !displaced || k != wantKey || v != wantValue {
			t.Fatalf("Set(%q) = %q, %d, %t, want %q, %d evicted", key, k, v, displaced, wantKey, wantValue)
		}
	}

	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)
	c.Set("d", 4)
	expect("d", "c", "b", "a")

	// Using an entry does not move it.
	c.Get("a")
	c.Get("b")
	expect("d", "c", "b", "a")

	// The hand starts at the oldest entry. It passes "a" and "b", which have
	// been used, and evicts "c".
	evicts("e", 5, "c", 3)
	expect("e", "d", "b", "a")

	// It carries on from where it stopped, so "d" is next, although "a" and
	// "b" are older and no longer marked.
	evicts("f", 6, "d", 4)
	expect("f", "e", "b", "a")

	// Peeking at an entry does not protect it, and neither does iterating
	// over it. The hand is at "e".
	c.Peek("e")
	evicts("g", 7, "e", 5)
	expect("g", "f", "b", "a")

	// Writing to an entry protects it like reading it does. The hand passes
	// "f" and "g", runs off the newest entry, and starts over at the oldest.
	c.Set("f", 60)
	c.Get("g")
	evicts("h", 8, "a", 1)
	expect("h", "g", "f", "b")

	// The hand is at "b" now, and "f" has lost its mark.
	evicts("i", 9, "b", 2)
	evicts("j", 10, "f", 60)
	expect("j", "i", "h", "g")
}

func TestDeleteAtHand(t *testing.T) {
	t.Parallel()

	c := sieve.New[string, int](4, sieve.WithShards(1))
	for i, key := range []string{"a", "b", "c", "d"} {
		c.Set(key, i)
	}

	// Leave the hand at "c": "a" and "b" are spared, "c" is not, and with
	// "c" gone the hand is at "d".
	c.Get("a")
	c.Get("b")
	c.Set("e", 4)
	if got, want := keys(c), []string{"e", "d", "b", "a"}; !slices.Equal(got, want) {
		t.Fatalf("cache holds %v, want %v", got, want)
	}

	// Deleting the entry under the hand moves the hand on to the next newer
	// one, which is "e", rather than leaving it on a node that will be
	// reused for the newest entry.
	c.Delete("d")
	c.Set("f", 5)
	if k, _, displaced := c.Set("g", 6); !displaced || k != "e" {
		t.Fatalf(`Set("g") evicted %q, %t, want "e"`, k, displaced)
	}

	// With the hand on the newest entry, deleting that entry sends the hand
	// back to the oldest one.
	c.Clear()
	for i, key := range []string{"a", "b", "c", "d"} {
		c.Set(key, i)
	}
	c.Get("a")
	c.Get("b")
	c.Set("e", 4) // Evicts "c" and leaves the hand at "d".
	c.Delete("e") // Now "d" is the newest entry.
	c.Delete("d")
	c.Set("f", 5)
	c.Set("g", 6)
	if k, _, displaced := c.Set("h", 7); !displaced || k != "a" {
		t.Fatalf(`Set("h") evicted %q, %t, want "a"`, k, displaced)
	}
}

// model is SIEVE at its simplest: a slice of entries from the newest to the
// oldest, and a hand that points at one of them.
type model struct {
	capacity int
	entries  []*entry
	hand     *entry // nil: start at the oldest entry
}

type entry struct {
	key, value int
	visited    bool
}

func (m *model) find(key int) int {
	return slices.IndexFunc(m.entries, func(e *entry) bool { return e.key == key })
}

// remove drops entry i. If the hand is on it, the hand moves to the next newer
// entry, or off the end if there is none.
func (m *model) remove(i int) {
	if m.hand == m.entries[i] {
		m.hand = nil
		if i > 0 {
			m.hand = m.entries[i-1]
		}
	}
	m.entries = slices.Delete(m.entries, i, i+1)
}

func (m *model) Get(key int) (int, bool) {
	i := m.find(key)
	if i < 0 {
		return 0, false
	}
	m.entries[i].visited = true
	return m.entries[i].value, true
}

func (m *model) Peek(key int) (int, bool) {
	i := m.find(key)
	if i < 0 {
		return 0, false
	}
	return m.entries[i].value, true
}

func (m *model) Set(key, value int) (oldKey, oldValue int, displaced bool) {
	if i := m.find(key); i >= 0 {
		e := m.entries[i]
		oldValue, e.value, e.visited = e.value, value, true
		return key, oldValue, true
	}
	if m.capacity == 0 {
		return key, value, true
	}

	if len(m.entries) == m.capacity {
		i := len(m.entries) - 1
		if m.hand != nil {
			i = slices.Index(m.entries, m.hand)
		}
		for m.entries[i].visited {
			m.entries[i].visited = false
			i--
			if i < 0 {
				i = len(m.entries) - 1
			}
		}
		oldKey, oldValue, displaced = m.entries[i].key, m.entries[i].value, true
		m.hand = m.entries[i]
		m.remove(i)
	}

	m.entries = slices.Insert(m.entries, 0, &entry{key: key, value: value})
	return oldKey, oldValue, displaced
}

func (m *model) Delete(key int) (int, bool) {
	i := m.find(key)
	if i < 0 {
		return 0, false
	}
	value := m.entries[i].value
	m.remove(i)
	return value, true
}

func (m *model) Len() int { return len(m.entries) }
func (m *model) Cap() int { return m.capacity }

func (m *model) Clear() {
	m.entries = nil
	m.hand = nil
}

func (m *model) All() iter.Seq2[int, int] {
	return func(yield func(int, int) bool) {
		for _, e := range m.entries {
			if !yield(e.key, e.value) {
				return
			}
		}
	}
}

func TestAgainstModel(t *testing.T) {
	t.Parallel()

	for i, tt := range []struct{ capacity, keys, steps int }{
		{0, 3, 100},
		{1, 3, 2_000},
		{2, 4, 5_000},
		{3, 6, 5_000},
		{8, 12, 20_000},
		{8, 100, 20_000},
		{50, 40, 20_000}, // Never full.
		{50, 80, 50_000},
		{300, 1000, 50_000},
	} {
		// Once the way it is meant to be, and once with readers that have
		// to take the lock.
		for _, crowded := range []bool{false, true} {
			c := sieve.New[int, int](tt.capacity, sieve.WithShards(1))
			if crowded {
				c.OccupyReaders(0)
			}
			rng := rand.New(rand.NewPCG(uint64(i), 1))
			cachetest.Compare(t, c, &model{capacity: tt.capacity}, cachetest.RandomOps(rng, tt.keys, tt.steps))
		}
	}
}

// TestKeysWithOneHash checks that a read of one key does not count for another
// key that happens to have the same hash. A reader finds its entry by the
// hash and has to look at the other one to see that it is not the one it
// wants; what the cache keeps must not depend on that, or it would depend on
// the seed of the hash.
func TestKeysWithOneHash(t *testing.T) {
	t.Parallel()

	c := sieve.New[int, int](4, sieve.WithShards(1))
	stored, other := sieve.Twins(c)

	c.Set(stored, 0)
	for range 3 {
		if _, ok := c.Get(other); ok {
			t.Fatalf("Get found key %d, which was never set", other)
		}
		if _, ok := c.Peek(other); ok {
			t.Fatalf("Peek found key %d, which was never set", other)
		}
	}
	for key := -1; key >= -3; key-- {
		c.Set(key, 0)
	}
	// The cache is full, and its oldest entry is one that nobody has read.
	if evicted, _, displaced := c.Set(-4, 0); !displaced || evicted != stored {
		t.Errorf("Set evicted %d, %t, want %d: the reads of %d, which has the same hash, were counted for it",
			evicted, displaced, stored, other)
	}
}

// TestHeldUpReader checks what a reader costs the others that stops in the
// middle of reading an entry, as one does whose goroutine has to make way: the
// value it is looking at stays alive, and that is all. Nobody waits for the
// reader, except for Clear, which promises to leave nothing behind.
func TestHeldUpReader(t *testing.T) {
	t.Parallel()

	for _, shards := range []int{1, 4} {
		c := sieve.New[int, *cachetest.Blob](64, sieve.WithShards(shards))

		ref := func() weak.Pointer[cachetest.Blob] {
			blob := new(cachetest.Blob)
			c.Set(0, blob)
			return weak.Make(blob)
		}()

		release, ok := c.Hold(0)
		if !ok {
			t.Fatal("Hold found no entry for a key that was just set")
		}

		// The entry is replaced and deleted, and so is everything else, many
		// times over. None of it waits.
		for i := range 10_000 {
			c.Set(i%200, new(cachetest.Blob))
			if i%3 == 0 {
				c.Delete((i + 1) % 200)
			}
		}
		runtime.GC()
		runtime.GC()
		if ref.Value() == nil {
			t.Fatalf("%d shards: the value that a reader is looking at was let go of", shards)
		}

		// Clear does wait, and keeps the cache usable while it does.
		done := make(chan struct{})
		go func() {
			defer close(done)
			c.Clear()
		}()
		select {
		case <-done:
			t.Fatalf("%d shards: Clear returned while a reader was looking at an entry", shards)
		case <-time.After(20 * time.Millisecond):
		}
		for i := range 1_000 {
			c.Set(i%200, nil)
			c.Get(i % 200)
			c.Delete(i % 200)
		}

		release()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("%d shards: Clear is still waiting although the reader has left", shards)
		}
		if cachetest.Alive([]weak.Pointer[cachetest.Blob]{ref}, 0) != 0 {
			t.Errorf("%d shards: the value is alive after Clear", shards)
		}

		// The same for an entry that is in the cache when it is cleared,
		// rather than one that has left it before. And Clear waits for
		// that reader only: one that looks at an entry which came and
		// went after Clear had begun is none of its business.
		c.Set(1, new(cachetest.Blob))
		release, ok = c.Hold(1)
		if !ok {
			t.Fatal("Hold found no entry for a key that was just set")
		}
		done = make(chan struct{})
		go func() {
			defer close(done)
			c.Clear()
		}()
		select {
		case <-done:
			t.Fatalf("%d shards: Clear returned while a reader was looking at an entry it removed", shards)
		case <-time.After(20 * time.Millisecond):
		}
		// Clear has begun once the cache is empty. An entry that comes and
		// goes before that is one that Clear is right to wait for, and a
		// goroutine may be that late to start when the machine is busy.
		for deadline := time.Now().Add(10 * time.Second); c.Len() != 0; runtime.Gosched() {
			if time.Now().After(deadline) {
				t.Fatalf("%d shards: Clear has not removed the entry yet", shards)
			}
		}
		c.Set(1, new(cachetest.Blob))
		later, ok := c.Hold(1)
		if !ok {
			t.Fatal("Hold found no entry for a key that was set while Clear was waiting")
		}
		c.Delete(1)

		release()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("%d shards: Clear is waiting for a reader of an entry that came after it", shards)
		}
		later()
		if waits := c.Waits(); waits != 0 {
			t.Errorf("%d shards: writers had to wait for a reader %d times", shards, waits)
		}
		runtime.KeepAlive(c)
	}
}

// TestKeysThatTakeACallToCompare checks that readers do not hold up writers
// when comparing two keys is not a matter of a few instructions.
//
// Go takes the CPU away from a goroutine that has had it for a while at the
// next function it calls. Were a reader to call one while it has an entry
// pinned, and comparing keys of some types is a call, readers would keep
// getting held up at just that point whenever there are more of them than
// CPUs. Enough of them, and a writer finds every spare node of its shard
// pinned and has to wait.
func TestKeysThatTakeACallToCompare(t *testing.T) {
	t.Parallel()

	type wide struct {
		a, b, c, d, e string
		n             int
	}
	keys := make([]any, 64)
	for i := range keys {
		if i%2 == 0 {
			keys[i] = i
		} else {
			keys[i] = wide{a: "a", b: "b", c: "c", d: "d", e: strconv.Itoa(i), n: i}
		}
	}

	c := sieve.New[any, int](len(keys), sieve.WithShards(1))
	for i, key := range keys {
		c.Set(key, i)
	}

	var (
		wg   sync.WaitGroup
		stop atomic.Bool
	)
	for r := range 16 * runtime.GOMAXPROCS(0) {
		wg.Go(func() {
			for i := r; !stop.Load(); i++ {
				if v, ok := c.Get(keys[i%len(keys)]); ok && v < 0 {
					t.Errorf("Get(%v) = %d", keys[i%len(keys)], v)
					return
				}
			}
		})
	}
	for start, i := time.Now(), 0; time.Since(start) < 300*time.Millisecond; i++ {
		c.Set(keys[i%len(keys)], i)
	}
	stop.Store(true)
	wg.Wait()

	// The race detector turns what a reader does while it has an entry
	// pinned into a series of calls, so with it readers are held up there
	// after all. That says nothing about a program.
	if waits := c.Waits(); waits != 0 && !cachetest.Race {
		t.Errorf("the writer had to wait for readers %d times", waits)
	}
}

// TestReadersHelp checks that what a shard could not let go of, because
// readers were looking at it, does not stay around for want of a write: once
// those readers are gone, the ones that come after them see to it.
func TestReadersHelp(t *testing.T) {
	t.Parallel()

	// Whichever way the readers read, they help.
	t.Run("Get", func(t *testing.T) {
		t.Parallel()
		testReadersHelp(t, func(c *sieve.Cache[int, *cachetest.Blob], key int) { c.Get(key) })
	})
	t.Run("Peek", func(t *testing.T) {
		t.Parallel()
		testReadersHelp(t, func(c *sieve.Cache[int, *cachetest.Blob], key int) { c.Peek(key) })
	})
}

func testReadersHelp(t *testing.T, read func(c *sieve.Cache[int, *cachetest.Blob], key int)) {
	t.Helper()

	const (
		capacity = 16 // The shard has patience for two entries that are gone.
		held     = 4
	)
	c := sieve.New[int, *cachetest.Blob](capacity, sieve.WithShards(1))

	refs := make([]weak.Pointer[cachetest.Blob], capacity)
	for key := range capacity {
		blob := new(cachetest.Blob)
		refs[key] = weak.Make(blob)
		c.Set(key, blob)
	}

	// Four readers stop in the middle of reading an entry each. Every one
	// of them is a goroutine of its own, as readers are, and stays one for
	// as long as it looks: a goroutine can only look at so many entries at
	// once, and four of them find a place to say so however few there are.
	var (
		looking, readers sync.WaitGroup
		moveOn           = make(chan struct{})
	)
	for key := range held {
		looking.Add(1)
		readers.Go(func() {
			release, ok := c.Hold(key)
			if !ok {
				t.Errorf("Hold(%d) found no entry", key)
			}
			looking.Done()
			<-moveOn
			release()
		})
	}
	looking.Wait()

	alive := func(limit int) int { return cachetest.Alive(refs, limit) }

	// Everything is deleted. The shard lets go of what it can.
	for key := range capacity {
		c.Delete(key)
	}
	if got := alive(held + 2); got < held || got > held+2 {
		t.Fatalf("%d values are alive after all were deleted, want the %d that readers look at and up to 2 more", got, held)
	}

	// The readers that are not held up try to help every so often, to no
	// avail for now. They must not get in the way of a writer when they do.
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 20_000 {
			c.Set(capacity+i%capacity, nil)
			c.Delete(capacity + i%capacity)
		}
	})
	for range 4 {
		wg.Go(func() {
			for i := range 100_000 {
				read(c, i%(2*capacity))
			}
		})
	}
	wg.Wait()
	if got := alive(len(refs)); got < held {
		t.Fatalf("%d values are alive, fewer than the %d that readers are looking at", got, held)
	}

	// The readers move on, and nobody writes to the cache again.
	close(moveOn)
	readers.Wait()
	for i := range 400_000 {
		read(c, i%capacity)
	}
	if got := alive(0); got != 0 {
		t.Errorf("%d values are alive after plenty of reads, want none: the readers did not help", got)
	}
	runtime.KeepAlive(c)
}

// TestCrowdedReaders runs the tests of reading and writing at the same time
// once more, with caches whose readers find few of the slots free in which
// they say what they are looking at, or none. They then read under the lock
// of the shard, as they do when the index changes under them, and that must
// not make a difference to anyone.
func TestCrowdedReaders(t *testing.T) {
	t.Parallel()

	for _, leave := range []int{0, 3} {
		t.Run(fmt.Sprintf("leave=%d", leave), func(t *testing.T) {
			t.Parallel()

			cachetest.Versions(t, func(capacity, shards int) evict.Cache[string, cachetest.Wide] {
				c := sieve.New[string, cachetest.Wide](capacity, sieve.WithShards(shards))
				c.OccupyReaders(leave)
				return c
			})
			cachetest.Present(t, func(capacity, shards int) evict.Cache[int, int] {
				c := sieve.New[int, int](capacity, sieve.WithShards(shards))
				c.OccupyReaders(leave)
				return c
			})
		})
	}
}

func FuzzAgainstModel(f *testing.F) {
	f.Add(3, []byte{})
	f.Add(1, []byte{128, 129, 0, 130, 1})
	f.Add(3, []byte{128, 129, 130, 0, 131, 192, 132, 133})
	f.Add(5, []byte("an arbitrary sequence of operations \xff on a small cache"))

	f.Fuzz(func(t *testing.T, capacity int, ops []byte) {
		capacity %= 40
		cachetest.Compare(t,
			sieve.New[int, int](capacity, sieve.WithShards(1)),
			&model{capacity: max(capacity, 0)},
			cachetest.BytesOps(ops),
		)
	})
}
