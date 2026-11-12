package stats_test

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/floatdrop/evict"
	"github.com/floatdrop/evict/internal/cachetest"
	"github.com/floatdrop/evict/lru"
	"github.com/floatdrop/evict/s3fifo"
	"github.com/floatdrop/evict/sieve"
	"github.com/floatdrop/evict/stats"
	"github.com/floatdrop/evict/tinylfu"
	"github.com/floatdrop/evict/twoq"
)

// A wrapped cache must still be a cache. One with a lock and one without get
// the suite.
var (
	lruFactories = cachetest.Factories{
		Int: func(capacity, shards int) evict.Cache[int, int] {
			return stats.Wrap(lru.New[int, int](capacity, lru.WithShards(shards)))
		},
		String: func(capacity, shards int) evict.Cache[string, string] {
			return stats.Wrap(lru.New[string, string](capacity, lru.WithShards(shards)))
		},
		Float: func(capacity, shards int) evict.Cache[float64, int] {
			return stats.Wrap(lru.New[float64, int](capacity, lru.WithShards(shards)))
		},
		Blob: func(capacity, shards int) evict.Cache[int, *cachetest.Blob] {
			return stats.Wrap(lru.New[int, *cachetest.Blob](capacity, lru.WithShards(shards)))
		},
		BlobKey: func(capacity, shards int) evict.Cache[*cachetest.Blob, int] {
			return stats.Wrap(lru.New[*cachetest.Blob, int](capacity, lru.WithShards(shards)))
		},
		Wide: func(capacity, shards int) evict.Cache[string, cachetest.Wide] {
			return stats.Wrap(lru.New[string, cachetest.Wide](capacity, lru.WithShards(shards)))
		},
	}

	s3fifoFactories = cachetest.Factories{
		Int: func(capacity, shards int) evict.Cache[int, int] {
			return stats.Wrap(s3fifo.New[int, int](capacity, s3fifo.WithShards(shards)))
		},
		String: func(capacity, shards int) evict.Cache[string, string] {
			return stats.Wrap(s3fifo.New[string, string](capacity, s3fifo.WithShards(shards)))
		},
		Float: func(capacity, shards int) evict.Cache[float64, int] {
			return stats.Wrap(s3fifo.New[float64, int](capacity, s3fifo.WithShards(shards)))
		},
		Blob: func(capacity, shards int) evict.Cache[int, *cachetest.Blob] {
			return stats.Wrap(s3fifo.New[int, *cachetest.Blob](capacity, s3fifo.WithShards(shards)))
		},
		BlobKey: func(capacity, shards int) evict.Cache[*cachetest.Blob, int] {
			return stats.Wrap(s3fifo.New[*cachetest.Blob, int](capacity, s3fifo.WithShards(shards)))
		},
		Wide: func(capacity, shards int) evict.Cache[string, cachetest.Wide] {
			return stats.Wrap(s3fifo.New[string, cachetest.Wide](capacity, s3fifo.WithShards(shards)))
		},
	}
)

func TestCache(t *testing.T) {
	t.Parallel()

	t.Run("lru", func(t *testing.T) { t.Parallel(); cachetest.Run(t, lruFactories) })
	t.Run("s3fifo", func(t *testing.T) { t.Parallel(); cachetest.Run(t, s3fifoFactories) })
}

// TestNoAllocs is not parallel: counting allocations needs the process to
// itself.
func TestNoAllocs(t *testing.T) {
	t.Run("lru", func(t *testing.T) { cachetest.NoAllocs(t, lruFactories) })
	t.Run("s3fifo", func(t *testing.T) { cachetest.NoAllocs(t, s3fifoFactories) })
}

func TestCounts(t *testing.T) {
	t.Parallel()

	c := stats.Wrap(lru.New[string, int](2))
	want := func(step string, hits, misses, evictions uint64, n int) {
		t.Helper()
		got := c.Stats()
		w := stats.Snapshot{Hits: hits, Misses: misses, Evictions: evictions, Len: n, Cap: 2}
		if got != w {
			t.Errorf("after %s: Stats() = %+v, want %+v", step, got, w)
		}
	}

	want("nothing", 0, 0, 0, 0)

	c.Set("a", 1)
	c.Set("b", 2)
	want("two new entries", 0, 0, 0, 2)

	c.Get("a")
	c.Get("a")
	c.Get("x")
	want("two hits and a miss", 2, 1, 0, 2)

	if k, _, displaced := c.Set("a", 10); !displaced || k != "a" {
		t.Fatalf("Set(a) displaced %q, %t; want a replacement", k, displaced)
	}
	want("a replacement", 2, 1, 0, 2)

	if k, _, displaced := c.Set("c", 3); !displaced || k != "b" {
		t.Fatalf("Set(c) displaced %q, %t; want b to be evicted", k, displaced)
	}
	want("an eviction", 2, 1, 1, 2)

	c.Peek("a")
	c.Peek("x")
	c.Delete("a")
	c.Delete("x")
	for range c.All() {
	}
	want("Peek, Delete and All", 2, 1, 1, 1)

	c.Clear()
	want("Clear", 2, 1, 1, 0)
}

// A cache without capacity hands every new entry straight back, with the key
// it was given. That is not a replacement.
func TestZeroCapacity(t *testing.T) {
	t.Parallel()

	c := stats.Wrap(lru.New[int, int](0))
	c.Set(1, 1)
	c.Set(1, 1)
	c.Get(1)

	want := stats.Snapshot{Misses: 1, Evictions: 2}
	if got := c.Stats(); got != want {
		t.Errorf("Stats() = %+v, want %+v", got, want)
	}
}

// A key that is not equal to itself is never found, so an entry that it
// pushes out is not its own, though it has the same key.
func TestNaN(t *testing.T) {
	t.Parallel()

	c := stats.Wrap(lru.New[float64, int](1, lru.WithShards(1)))
	c.Set(math.NaN(), 1)
	c.Set(math.NaN(), 2)

	if got := c.Stats().Evictions; got != 1 {
		t.Errorf("Evictions = %d, want 1", got)
	}
}

// Every policy reports what it evicts through Set. Each of those has to be
// counted, and no replacement: not even of +0 by -0, which are equal but
// tell apart.
func TestEveryPolicy(t *testing.T) {
	t.Parallel()

	const capacity = 100
	policies := map[string]func() evict.Cache[float64, int]{
		"lru":     func() evict.Cache[float64, int] { return lru.New[float64, int](capacity, lru.WithShards(4)) },
		"twoq":    func() evict.Cache[float64, int] { return twoq.New[float64, int](capacity, twoq.WithShards(4)) },
		"sieve":   func() evict.Cache[float64, int] { return sieve.New[float64, int](capacity, sieve.WithShards(4)) },
		"s3fifo":  func() evict.Cache[float64, int] { return s3fifo.New[float64, int](capacity, s3fifo.WithShards(4)) },
		"tinylfu": func() evict.Cache[float64, int] { return tinylfu.New[float64, int](capacity, tinylfu.WithShards(4)) },
	}
	for name, newCache := range policies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			c := stats.Wrap(newCache())
			c.Set(0, 0)
			c.Set(math.Copysign(0, -1), 0)
			if s := c.Stats(); s.Evictions != 0 || s.Len != 1 {
				t.Fatalf("-0 replacing +0: %d evictions and %d entries, want 0 and 1", s.Evictions, s.Len)
			}

			// Some keys are read, so that the policies that promote entries
			// have some to promote, and twoq some to remember.
			added := uint64(1)
			for i := 1; i < 20*capacity; i++ {
				key := float64(i % (3 * capacity))
				if _, ok := c.Get(key); !ok {
					added++
				}
				c.Set(key, i)
				c.Get(float64(i % 50))
			}

			s := c.Stats()
			if s.Evictions != added-uint64(s.Len) {
				t.Errorf("%d entries added, %d evicted and %d left", added, s.Evictions, s.Len)
			}
		})
	}
}

// Goroutines that run at the same time should count in different stripes,
// most of them: two that share one take its cache line from each other on
// every count. A count that lands in the wrong stripe is still counted, so
// only this test notices.
func TestStripes(t *testing.T) {
	t.Parallel()

	const goroutines = 16
	c := stats.Wrap(lru.New[int, int](1))

	stripes := make(chan any, goroutines)
	hold := make(chan struct{})
	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			stripes <- stats.StripeOf(c)
			// A goroutine that has ended may hand its stack on to the
			// next one, which would then pick the same stripe.
			<-hold
		})
	}
	distinct := map[any]bool{}
	for range goroutines {
		distinct[<-stripes] = true
	}
	close(hold)
	wg.Wait()

	if len(distinct) < goroutines/4 {
		t.Errorf("%d goroutines counted in %d stripes, want %d or more", goroutines, len(distinct), goroutines/4)
	}
}

// Every request has to be counted once, however many goroutines make them
// and whichever stripes they share.
func TestConcurrent(t *testing.T) {
	t.Parallel()

	const (
		goroutines = 16
		requests   = 2000
		capacity   = 64
	)
	c := stats.Wrap(s3fifo.New[int, int](capacity))

	// A miss is followed by a Set, which replaces an entry if another
	// goroutine has stored one for the key in between, and otherwise adds
	// one. What was added and is no longer there was evicted.
	var replaced atomic.Uint64
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range requests {
				key := (g*requests + i*7) % (4 * capacity)
				if _, ok := c.Get(key); ok {
					continue
				}
				if k, _, displaced := c.Set(key, i); displaced && k == key {
					replaced.Add(1)
				}
			}
		})
	}
	wg.Wait()

	s := c.Stats()
	if s.Hits+s.Misses != goroutines*requests {
		t.Errorf("%d hits and %d misses, want %d requests", s.Hits, s.Misses, goroutines*requests)
	}
	if s.Hits == 0 || s.Evictions == 0 {
		t.Errorf("%d hits and %d evictions; the test wants both", s.Hits, s.Evictions)
	}
	if added := s.Misses - replaced.Load(); s.Evictions != added-uint64(s.Len) {
		t.Errorf("%d entries added, %d evicted and %d left", added, s.Evictions, s.Len)
	}
}

func TestSnapshot(t *testing.T) {
	t.Parallel()

	if got := (stats.Snapshot{}).HitRatio(); got != 0 {
		t.Errorf("HitRatio() = %v with no requests, want 0", got)
	}
	if got := (stats.Snapshot{Hits: 3, Misses: 1}).HitRatio(); got != 0.75 {
		t.Errorf("HitRatio() = %v, want 0.75", got)
	}

	before := stats.Snapshot{Hits: 10, Misses: 5, Evictions: 2, Len: 7, Cap: 10}
	after := stats.Snapshot{Hits: 13, Misses: 6, Evictions: 3, Len: 9, Cap: 10}
	want := stats.Snapshot{Hits: 3, Misses: 1, Evictions: 1, Len: 9, Cap: 10}
	if got := after.Sub(before); got != want {
		t.Errorf("Sub() = %+v, want %+v", got, want)
	}
}

// Stats are counted in an array of stripes, which must each fill whole cache
// lines, or goroutines that count in neighbouring stripes would keep taking
// a line away from each other.
func TestStripeSize(t *testing.T) {
	t.Parallel()

	if size := stats.StripeSize; size%128 != 0 {
		t.Errorf("a stripe takes %d bytes, want a multiple of 128", size)
	}
}
