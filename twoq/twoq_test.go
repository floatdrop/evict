package twoq_test

import (
	"iter"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/floatdrop/evict"
	"github.com/floatdrop/evict/internal/cachetest"
	"github.com/floatdrop/evict/twoq"
)

var factories = cachetest.Factories{
	Int: func(capacity, shards int) evict.Cache[int, int] {
		return twoq.New[int, int](capacity, twoq.WithShards(shards))
	},
	String: func(capacity, shards int) evict.Cache[string, string] {
		return twoq.New[string, string](capacity, twoq.WithShards(shards))
	},
	Float: func(capacity, shards int) evict.Cache[float64, int] {
		return twoq.New[float64, int](capacity, twoq.WithShards(shards))
	},
	Blob: func(capacity, shards int) evict.Cache[int, *cachetest.Blob] {
		return twoq.New[int, *cachetest.Blob](capacity, twoq.WithShards(shards))
	},
	BlobKey: func(capacity, shards int) evict.Cache[*cachetest.Blob, int] {
		return twoq.New[*cachetest.Blob, int](capacity, twoq.WithShards(shards))
	},
	Wide: func(capacity, shards int) evict.Cache[string, cachetest.Wide] {
		return twoq.New[string, cachetest.Wide](capacity, twoq.WithShards(shards))
	},
	Ghosts: 0.5,
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
func keys[K comparable, V any](c *twoq.Cache[K, V]) []K {
	var all []K
	for k := range c.All() {
		all = append(all, k)
	}
	return all
}

func TestEviction(t *testing.T) {
	t.Parallel()

	// Four entries, of which the recent queue may hold two before it has to
	// give way, and a memory of two keys.
	c := twoq.New[string, int](4, twoq.WithShards(1), twoq.WithRecentRatio(0.5), twoq.WithGhostRatio(0.5))

	// expect checks the frequent entries, most recently used first, followed
	// by the recent entries, newest first.
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

	// Reading a recent entry does nothing for it: "a" is the oldest, and
	// out it goes.
	c.Get("a")
	evicts("e", 5, "a", 1)
	expect("e", "d", "c", "b")

	// But its key is remembered. Setting it again makes it a frequent entry,
	// at the expense of the oldest recent one.
	evicts("a", 10, "b", 2)
	expect("a", "e", "d", "c")

	evicts("f", 6, "c", 3)
	expect("a", "f", "e", "d")

	// The same happens to "b", whose key was remembered as well.
	evicts("b", 20, "d", 4)
	expect("b", "a", "f", "e")

	// The recent queue is down to its share of two entries now, so the next
	// one to go is the least recently used frequent entry.
	evicts("g", 7, "a", 10)
	expect("b", "g", "f", "e")

	// Reading a frequent entry does count.
	c.Get("b")
	evicts("h", 8, "e", 5)
	expect("b", "h", "g", "f")

	// The cache remembers two keys, and "c" is no longer one of them: it
	// starts over as a recent entry. So does "a", which was evicted from
	// the frequent queue and not remembered in the first place.
	evicts("c", 30, "f", 6)
	expect("b", "c", "h", "g")
	evicts("a", 11, "g", 7)
	expect("b", "a", "c", "h")

	// "f" is remembered and joins "b". Among the frequent entries, the one
	// that was used last stays longest.
	evicts("f", 60, "h", 8)
	expect("f", "b", "a", "c")
	c.Get("b")
	expect("b", "f", "a", "c")
	evicts("i", 9, "f", 60)
	expect("b", "i", "a", "c")

	// Writing to a recent entry changes its value and nothing else.
	evicts("c", 31, "c", 30)
	expect("b", "i", "a", "c")

	// A deleted entry leaves room, so nothing is evicted for the next one.
	c.Delete("b")
	if _, _, displaced := c.Set("j", 10); displaced {
		t.Fatal(`Set("j") displaced an entry while there was room`)
	}
	expect("j", "i", "a", "c")
}

// TestClearForgets checks that Clear does away with the ghosts, also when
// they are all that is left.
func TestClearForgets(t *testing.T) {
	t.Parallel()

	c := twoq.New[int, int](2, twoq.WithShards(1))
	c.Set(1, 1)
	c.Set(2, 2)
	c.Set(3, 3) // Evicts 1, which becomes a ghost.
	c.Delete(2)
	c.Delete(3)
	c.Clear()

	// Had 1 been remembered, it would be a frequent entry now and come
	// first.
	c.Set(1, 1)
	c.Set(4, 4)
	if got, want := keys(c), []int{4, 1}; !slices.Equal(got, want) {
		t.Errorf("cache holds %v, want %v", got, want)
	}
}

// TestRatios checks the queues of caches with the shares at their limits.
func TestRatios(t *testing.T) {
	t.Parallel()

	t.Run("no memory", func(t *testing.T) {
		t.Parallel()

		// Without remembered keys nothing becomes frequent, and the cache
		// evicts in the order of arrival, whatever is read in between.
		c := twoq.New[int, int](3, twoq.WithShards(1), twoq.WithGhostRatio(0))
		for key := range 10 {
			c.Get(key - 1)
			c.Get(key - 2)
			if k, _, displaced := c.Set(key, key); displaced != (key >= 3) || k != max(key-3, 0) {
				t.Fatalf("Set(%d) evicted %d, %t", key, k, displaced)
			}
		}
		// Not even a key that returns right after it was evicted.
		c.Set(6, 6)
		if got, want := keys(c), []int{6, 9, 8}; !slices.Equal(got, want) {
			t.Errorf("cache holds %v, want %v", got, want)
		}
	})

	t.Run("no recent share", func(t *testing.T) {
		t.Parallel()

		// The recent queue always gives way, so frequent entries are only
		// evicted when there is no recent one.
		c := twoq.New[int, int](2, twoq.WithShards(1), twoq.WithRecentRatio(0), twoq.WithGhostRatio(1))
		c.Set(1, 1)
		c.Set(2, 2)
		c.Set(3, 3) // Evicts 1, and remembers it.
		c.Set(1, 1) // Evicts 2. Now 1 is frequent.
		c.Set(2, 2) // Evicts 3. Now 2 is frequent as well.
		if got, want := keys(c), []int{2, 1}; !slices.Equal(got, want) {
			t.Fatalf("cache holds %v, want %v", got, want)
		}
		if k, _, displaced := c.Set(4, 4); !displaced || k != 1 {
			t.Errorf("Set(4) evicted %d, %t, want the least recently used frequent entry 1", k, displaced)
		}
		if k, _, displaced := c.Set(5, 5); !displaced || k != 4 {
			t.Errorf("Set(5) evicted %d, %t, want the recent entry 4", k, displaced)
		}
	})

	t.Run("all recent", func(t *testing.T) {
		t.Parallel()

		// The recent queue never has to give way while there is a frequent
		// entry to evict instead.
		c := twoq.New[int, int](3, twoq.WithShards(1), twoq.WithRecentRatio(1), twoq.WithGhostRatio(1))
		for key := range 4 {
			c.Set(key, key) // The last one evicts 0, and remembers it.
		}
		c.Set(0, 0) // Evicts 1. Now 0 is frequent.
		if got, want := keys(c), []int{0, 3, 2}; !slices.Equal(got, want) {
			t.Fatalf("cache holds %v, want %v", got, want)
		}
		if k, _, displaced := c.Set(4, 4); !displaced || k != 0 {
			t.Errorf("Set(4) evicted %d, %t, want the frequent entry 0", k, displaced)
		}
	})

	t.Run("out of range", func(t *testing.T) {
		t.Parallel()

		// Ratios are brought into range rather than rejected.
		nan := func() float64 { var zero float64; return zero / zero }()
		for _, ratio := range []float64{-1, 2, nan} {
			c := twoq.New[int, int](8, twoq.WithRecentRatio(ratio), twoq.WithGhostRatio(ratio))
			for key := range 100 {
				c.Set(key%13, key)
				c.Get(key % 7)
			}
			if got := c.Len(); got != 8 {
				t.Errorf("ratio %v: Len() = %d, want 8", ratio, got)
			}
		}
	})
}

// model is 2Q at its simplest: a slice for each queue and one for the keys
// that are remembered, all of them with the newest element first.
type model struct {
	capacity, recentCap, ghostCap int

	recent, frequent []entry
	ghosts           []int
}

type entry struct{ key, value int }

func find(queue []entry, key int) int {
	return slices.IndexFunc(queue, func(e entry) bool { return e.key == key })
}

func (m *model) Get(key int) (int, bool) {
	if i := find(m.frequent, key); i >= 0 {
		e := m.frequent[i]
		m.frequent = slices.Insert(slices.Delete(m.frequent, i, i+1), 0, e)
		return e.value, true
	}
	return m.Peek(key)
}

func (m *model) Peek(key int) (int, bool) {
	for _, queue := range [][]entry{m.frequent, m.recent} {
		if i := find(queue, key); i >= 0 {
			return queue[i].value, true
		}
	}
	return 0, false
}

func (m *model) Set(key, value int) (oldKey, oldValue int, displaced bool) {
	if old, ok := m.Get(key); ok {
		for _, queue := range [][]entry{m.frequent, m.recent} {
			if i := find(queue, key); i >= 0 {
				queue[i].value = value
			}
		}
		return key, old, true
	}
	if m.capacity == 0 {
		return key, value, true
	}

	// A key that is remembered gives up its place in the memory first, so
	// that the key of the entry evicted for it can take that place.
	remembered := slices.Index(m.ghosts, key)
	if remembered >= 0 {
		m.ghosts = slices.Delete(m.ghosts, remembered, remembered+1)
	}

	// This is the procedure "reclaimfor" of the paper.
	if len(m.recent)+len(m.frequent) == m.capacity {
		if len(m.recent) > m.recentCap || len(m.frequent) == 0 {
			last := len(m.recent) - 1
			oldKey, oldValue, displaced = m.recent[last].key, m.recent[last].value, true
			m.recent = m.recent[:last]
			m.ghosts = slices.Insert(m.ghosts, 0, oldKey)
			if len(m.ghosts) > m.ghostCap {
				m.ghosts = m.ghosts[:m.ghostCap]
			}
		} else {
			last := len(m.frequent) - 1
			oldKey, oldValue, displaced = m.frequent[last].key, m.frequent[last].value, true
			m.frequent = m.frequent[:last]
		}
	}

	if remembered >= 0 {
		m.frequent = slices.Insert(m.frequent, 0, entry{key, value})
	} else {
		m.recent = slices.Insert(m.recent, 0, entry{key, value})
	}
	return oldKey, oldValue, displaced
}

func (m *model) Delete(key int) (int, bool) {
	value, ok := m.Peek(key)
	m.frequent = slices.DeleteFunc(m.frequent, func(e entry) bool { return e.key == key })
	m.recent = slices.DeleteFunc(m.recent, func(e entry) bool { return e.key == key })
	return value, ok
}

func (m *model) Len() int { return len(m.recent) + len(m.frequent) }
func (m *model) Cap() int { return m.capacity }

func (m *model) Clear() {
	m.recent, m.frequent, m.ghosts = nil, nil, nil
}

func (m *model) All() iter.Seq2[int, int] {
	return func(yield func(int, int) bool) {
		for _, e := range slices.Concat(m.frequent, m.recent) {
			if !yield(e.key, e.value) {
				return
			}
		}
	}
}

// newModel returns a model with the queue sizes that New derives from the
// capacity and the ratios.
func newModel(capacity int, recentRatio, ghostRatio float64) *model {
	round := func(x float64) int { return int(x + 0.5) }
	return &model{
		capacity:  capacity,
		recentCap: round(recentRatio * float64(capacity)),
		ghostCap:  round(ghostRatio * float64(capacity)),
	}
}

func TestAgainstModel(t *testing.T) {
	t.Parallel()

	for i, tt := range []struct {
		capacity, keys, steps int
		recent, ghost         float64
	}{
		{0, 3, 100, 0.25, 0.5},
		{1, 3, 2_000, 0.25, 0.5},
		{2, 4, 5_000, 0.25, 0.5},
		{3, 6, 5_000, 0.25, 0.5},
		{4, 9, 20_000, 0.5, 0.5},
		{8, 12, 20_000, 0.25, 0.5},
		{8, 100, 20_000, 0.25, 0.5},
		{8, 30, 20_000, 0, 1},
		{8, 30, 20_000, 1, 1},
		{8, 30, 20_000, 0.25, 0},
		{50, 40, 20_000, 0.25, 0.5}, // Never full.
		{50, 80, 50_000, 0.25, 0.5},
		{50, 120, 50_000, 0.1, 0.9},
		{300, 1000, 50_000, 0.25, 0.5},
	} {
		rng := rand.New(rand.NewPCG(uint64(i), 1))
		cachetest.Compare(t,
			twoq.New[int, int](tt.capacity,
				twoq.WithShards(1), twoq.WithRecentRatio(tt.recent), twoq.WithGhostRatio(tt.ghost)),
			newModel(tt.capacity, tt.recent, tt.ghost),
			cachetest.RandomOps(rng, tt.keys, tt.steps),
		)
	}
}

func FuzzAgainstModel(f *testing.F) {
	f.Add(3, uint8(64), uint8(128), []byte{})
	f.Add(1, uint8(64), uint8(128), []byte{128, 129, 0, 130, 1, 128})
	f.Add(4, uint8(128), uint8(128), []byte{128, 129, 130, 131, 132, 128, 133, 129, 134})
	f.Add(5, uint8(0), uint8(255), []byte("an arbitrary sequence of operations \xff on a small cache"))

	f.Fuzz(func(t *testing.T, capacity int, recent, ghost uint8, ops []byte) {
		capacity %= 40
		recentRatio, ghostRatio := float64(recent)/255, float64(ghost)/255
		cachetest.Compare(t,
			twoq.New[int, int](capacity,
				twoq.WithShards(1), twoq.WithRecentRatio(recentRatio), twoq.WithGhostRatio(ghostRatio)),
			newModel(max(capacity, 0), recentRatio, ghostRatio),
			cachetest.BytesOps(ops),
		)
	})
}
