package stats_test

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/floatdrop/evict"
	"github.com/floatdrop/evict/lru"
	"github.com/floatdrop/evict/s3fifo"
	"github.com/floatdrop/evict/stats"
)

// batcher is a cache with a GetMany of its own, which says how often it was
// called.
type batcher struct {
	evict.Cache[int, int]
	calls int
}

func (b *batcher) GetMany(keys, values []int, found []bool) {
	b.calls++
	for i, k := range keys {
		values[i], found[i] = b.Get(k)
	}
}

// GetMany has to find what Peek finds, and count a hit or a miss for every
// key, whether the cache has a GetMany of its own or not.
func TestGetMany(t *testing.T) {
	t.Parallel()

	const capacity = 500
	for name, cache := range map[string]evict.Cache[int, int]{
		"lru":    lru.New[int, int](capacity),
		"s3fifo": s3fifo.New[int, int](capacity),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			c := stats.Wrap(cache)
			for k := range capacity / 2 {
				c.Set(k, -k)
			}

			rng := rand.New(rand.NewPCG(1, 2))
			var hits, misses uint64
			for _, n := range []int{0, 1, 3, 4, 31, 32, 33, 100} {
				keys := make([]int, n)
				for i := range keys {
					keys[i] = rng.IntN(capacity)
				}
				values, found := make([]int, n), make([]bool, n)
				c.GetMany(keys, values, found)
				for i, k := range keys {
					v, ok := c.Peek(k)
					if values[i] != v || found[i] != ok {
						t.Fatalf("GetMany of %d keys: key %d gave %d, %t; Peek gives %d, %t", n, k, values[i], found[i], v, ok)
					}
					if ok {
						hits++
					} else {
						misses++
					}
				}
			}

			if s := c.Stats(); s.Hits != hits || s.Misses != misses {
				t.Errorf("%d hits and %d misses counted, want %d and %d", s.Hits, s.Misses, hits, misses)
			}
		})
	}
}

// A cache that has a GetMany of its own is asked with it.
func TestGetManyForwards(t *testing.T) {
	t.Parallel()

	b := &batcher{Cache: lru.New[int, int](10)}
	c := stats.Wrap[int, int](b)
	c.Set(1, 1)
	values, found := make([]int, 2), make([]bool, 2)
	c.GetMany([]int{1, 2}, values, found)

	if b.calls != 1 {
		t.Errorf("the cache's GetMany was called %d times, want once", b.calls)
	}
	if s := c.Stats(); s.Hits != 1 || s.Misses != 1 {
		t.Errorf("%d hits and %d misses counted, want 1 and 1", s.Hits, s.Misses)
	}
}

// Slices that are too short for the keys make GetMany panic before it looks
// anything up or counts it.
func TestGetManyShortSlices(t *testing.T) {
	t.Parallel()

	c := stats.Wrap(lru.New[int, int](10))
	c.Set(1, 1)
	values, found := make([]int, 8), make([]bool, 8)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("GetMany with values and found shorter than keys did not panic")
			}
		}()
		c.GetMany([]int{1, 2, 3}, values[:2], found[:2])
	}()

	if slices.ContainsFunc(values, func(v int) bool { return v != 0 }) || slices.Contains(found, true) {
		t.Errorf("GetMany wrote %v and %v before it panicked", values, found)
	}
	if s := c.Stats(); s.Hits != 0 || s.Misses != 0 {
		t.Errorf("%d hits and %d misses counted before the panic", s.Hits, s.Misses)
	}
}

// TestGetManyNoAllocs is not parallel: counting allocations needs the process
// to itself.
func TestGetManyNoAllocs(t *testing.T) {
	for name, cache := range map[string]evict.Cache[int, int]{
		"lru":    lru.New[int, int](100),
		"s3fifo": s3fifo.New[int, int](100),
	} {
		t.Run(name, func(t *testing.T) {
			c := stats.Wrap(cache)
			keys := make([]int, 40)
			for i := range keys {
				keys[i] = i
				c.Set(i, i)
			}
			values, found := make([]int, len(keys)), make([]bool, len(keys))
			if allocs := testing.AllocsPerRun(100, func() { c.GetMany(keys, values, found) }); allocs != 0 {
				t.Errorf("GetMany made %v allocations, want 0", allocs)
			}
		})
	}
}
