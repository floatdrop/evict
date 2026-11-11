package lru_test

import (
	"iter"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/floatdrop/evict"
	"github.com/floatdrop/evict/internal/cachetest"
	"github.com/floatdrop/evict/lru"
)

var factories = cachetest.Factories{
	Int: func(capacity, shards int) evict.Cache[int, int] {
		return lru.New[int, int](capacity, lru.WithShards(shards))
	},
	String: func(capacity, shards int) evict.Cache[string, string] {
		return lru.New[string, string](capacity, lru.WithShards(shards))
	},
	Float: func(capacity, shards int) evict.Cache[float64, int] {
		return lru.New[float64, int](capacity, lru.WithShards(shards))
	},
	Blob: func(capacity, shards int) evict.Cache[int, *cachetest.Blob] {
		return lru.New[int, *cachetest.Blob](capacity, lru.WithShards(shards))
	},
	BlobKey: func(capacity, shards int) evict.Cache[*cachetest.Blob, int] {
		return lru.New[*cachetest.Blob, int](capacity, lru.WithShards(shards))
	},
	Wide: func(capacity, shards int) evict.Cache[string, cachetest.Wide] {
		return lru.New[string, cachetest.Wide](capacity, lru.WithShards(shards))
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
func keys[K comparable, V any](c *lru.Cache[K, V]) []K {
	var all []K
	for k := range c.All() {
		all = append(all, k)
	}
	return all
}

func TestEviction(t *testing.T) {
	t.Parallel()

	c := lru.New[string, int](3, lru.WithShards(1))
	expect := func(want ...string) {
		t.Helper()
		if got := keys(c); !slices.Equal(got, want) {
			t.Fatalf("cache holds %v, want %v", got, want)
		}
	}

	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)
	expect("c", "b", "a")

	// Reading an entry makes it the most recently used.
	c.Get("a")
	expect("a", "c", "b")

	// So does writing to it.
	c.Set("c", 30)
	expect("c", "a", "b")

	// Peeking at it and iterating over it do not, and neither does asking
	// for something that is not there.
	c.Peek("b")
	c.Get("x")
	expect("c", "a", "b")

	// A new entry pushes out the least recently used one.
	if k, v, displaced := c.Set("d", 4); !displaced || k != "b" || v != 2 {
		t.Fatalf(`Set("d") = %q, %d, %t, want "b", 2 evicted`, k, v, displaced)
	}
	expect("d", "c", "a")

	// A deleted entry leaves room for a new one.
	c.Delete("c")
	expect("d", "a")
	if _, _, displaced := c.Set("e", 5); displaced {
		t.Fatal(`Set("e") displaced an entry while there was room`)
	}
	expect("e", "d", "a")

	if k, v, displaced := c.Set("f", 6); !displaced || k != "a" || v != 1 {
		t.Fatalf(`Set("f") = %q, %d, %t, want "a", 1 evicted`, k, v, displaced)
	}
	expect("f", "e", "d")
}

// model is an LRU cache at its simplest: a slice of entries ordered from the
// most to the least recently used.
type model struct {
	capacity int
	entries  []entry
}

type entry struct{ key, value int }

func (m *model) find(key int) int {
	return slices.IndexFunc(m.entries, func(e entry) bool { return e.key == key })
}

func (m *model) use(i int) {
	e := m.entries[i]
	m.entries = slices.Insert(slices.Delete(m.entries, i, i+1), 0, e)
}

func (m *model) Get(key int) (int, bool) {
	i := m.find(key)
	if i < 0 {
		return 0, false
	}
	m.use(i)
	return m.entries[0].value, true
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
		old := m.entries[i].value
		m.entries[i].value = value
		m.use(i)
		return key, old, true
	}
	if m.capacity == 0 {
		return key, value, true
	}

	var evicted entry
	full := len(m.entries) == m.capacity
	if full {
		evicted = m.entries[len(m.entries)-1]
		m.entries = m.entries[:len(m.entries)-1]
	}
	m.entries = slices.Insert(m.entries, 0, entry{key, value})
	return evicted.key, evicted.value, full
}

func (m *model) Delete(key int) (int, bool) {
	i := m.find(key)
	if i < 0 {
		return 0, false
	}
	value := m.entries[i].value
	m.entries = slices.Delete(m.entries, i, i+1)
	return value, true
}

func (m *model) Len() int { return len(m.entries) }
func (m *model) Cap() int { return m.capacity }
func (m *model) Clear()   { m.entries = nil }

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
		rng := rand.New(rand.NewPCG(uint64(i), 1))
		cachetest.Compare(t,
			lru.New[int, int](tt.capacity, lru.WithShards(1)),
			&model{capacity: tt.capacity},
			cachetest.RandomOps(rng, tt.keys, tt.steps),
		)
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
			lru.New[int, int](capacity, lru.WithShards(1)),
			&model{capacity: max(capacity, 0)},
			cachetest.BytesOps(ops),
		)
	})
}
