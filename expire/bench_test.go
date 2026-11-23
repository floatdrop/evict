package expire_test

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/evict"
	"github.com/floatdrop/evict/expire"
	"github.com/floatdrop/evict/internal/cachetest"
	"github.com/floatdrop/evict/s3fifo"
	"github.com/floatdrop/evict/tinylfu"
)

const benchCapacity = 1 << 14

// fill sets the keys 0 to benchCapacity-1, through set.
func fill(set func(key, value int)) {
	for key := range benchCapacity {
		set(key, key)
	}
}

// benchGet reads entries that are all there, on every CPU at once. The loop is
// written out for each type, so that it makes the call it is about and no
// other.
func BenchmarkGet(b *testing.B) {
	b.Run("direct", func(b *testing.B) {
		c := tinylfu.New[int, int](benchCapacity)
		fill(func(k, v int) { c.Set(k, v) })
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Int()
			for pb.Next() {
				c.Get(i & (benchCapacity - 1))
				i++
			}
		})
	})

	b.Run("interface", func(b *testing.B) {
		var c evict.Cache[int, int] = tinylfu.New[int, int](benchCapacity)
		fill(func(k, v int) { c.Set(k, v) })
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Int()
			for pb.Next() {
				c.Get(i & (benchCapacity - 1))
				i++
			}
		})
	})

	b.Run("expire", func(b *testing.B) {
		c := expire.Wrap(tinylfu.New[int, expire.Entry[int]](benchCapacity), time.Hour)
		fill(func(k, v int) { c.Set(k, v) })
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Int()
			for pb.Next() {
				c.Get(i & (benchCapacity - 1))
				i++
			}
		})
	})

	// Every entry is due within 50 ms of the copy of the time, so every
	// read reads the time itself.
	b.Run("due", func(b *testing.B) {
		clock := expire.NewRealClock()
		c := expire.WrapWith(tinylfu.New[int, expire.Entry[int]](benchCapacity), time.Hour, clock)
		fill(func(k, v int) { c.Set(k, v) })
		clock.SetCopy(clock.Read() + time.Hour - 30*time.Millisecond)
		// Filling the cache takes about a millisecond. Had it taken 30, the
		// first entries would have expired by the copy of the time.
		for _, key := range []int{0, benchCapacity - 1} {
			if _, ok := c.Get(key); !ok {
				b.Fatalf("Get(%d) misses; the benchmark is for entries that are due", key)
			}
		}
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Int()
			for pb.Next() {
				c.Get(i & (benchCapacity - 1))
				i++
			}
		})
	})
}

// BenchmarkSet replaces entries, on one CPU at a time: Set takes the lock of a
// shard either way, and reads the time through the wrapper.
func BenchmarkSet(b *testing.B) {
	b.Run("direct", func(b *testing.B) {
		c := tinylfu.New[int, int](benchCapacity)
		fill(func(k, v int) { c.Set(k, v) })
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			c.Set(i&(benchCapacity-1), i)
			i++
		}
	})

	b.Run("expire", func(b *testing.B) {
		c := expire.Wrap(tinylfu.New[int, expire.Entry[int]](benchCapacity), time.Hour)
		fill(func(k, v int) { c.Set(k, v) })
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			c.Set(i&(benchCapacity-1), i)
			i++
		}
	})
}

// BenchmarkGetMany looks up keys of a cache too large for the CPU's caches, on
// every CPU at once: with the GetMany of s3fifo, and with that of the wrapper,
// which looks them up in batches of its own. ns/key is the time per key.
func BenchmarkGetMany(b *testing.B) {
	const entries = 1 << 20
	keys, seq := cachetest.UniformInts(entries)

	run := func(b *testing.B, n int, getMany func(keys, values []int, found []bool)) {
		b.Helper()
		mask := len(seq) - 1
		var (
			mu   sync.Mutex
			hits int
		)
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			values, found := make([]int, n), make([]bool, n)
			i, h := rand.Int()&^(n-1), 0
			for pb.Next() {
				getMany(seq[i&mask:i&mask+n], values, found)
				for _, ok := range found {
					if ok {
						h++
					}
				}
				i += n
			}
			mu.Lock()
			hits += h
			mu.Unlock()
		})
		total := float64(b.N * n)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/total, "ns/key")
		b.ReportMetric(float64(hits)/total, "hits")
	}

	direct := s3fifo.New[int, int](entries)
	wrapped := expire.Wrap(s3fifo.New[int, expire.Entry[int]](entries), time.Hour)
	for _, k := range keys {
		direct.Set(k, k)
		wrapped.Set(k, k)
	}
	for _, n := range []int{8, 32, 128} {
		b.Run(fmt.Sprintf("keys=%d/direct", n), func(b *testing.B) { run(b, n, direct.GetMany) })
		b.Run(fmt.Sprintf("keys=%d/expire", n), func(b *testing.B) { run(b, n, wrapped.GetMany) })
	}
}
