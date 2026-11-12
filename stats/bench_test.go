package stats_test

import (
	"math/rand/v2"
	"testing"

	"github.com/floatdrop/evict"
	"github.com/floatdrop/evict/stats"
	"github.com/floatdrop/evict/tinylfu"
)

const benchCapacity = 1 << 14

func benchCache() *tinylfu.Cache[int, int] {
	c := tinylfu.New[int, int](benchCapacity)
	for key := range benchCapacity {
		c.Set(key, key)
	}
	return c
}

// BenchmarkGet reads entries that are all there, on every CPU at once: from
// the cache itself, from the same cache behind the interface, and through the
// counter. The loops are written out, so that each makes the call it is
// about and no other.
func BenchmarkGet(b *testing.B) {
	b.Run("direct", func(b *testing.B) {
		c := benchCache()
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
		var c evict.Cache[int, int] = benchCache()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Int()
			for pb.Next() {
				c.Get(i & (benchCapacity - 1))
				i++
			}
		})
	})

	b.Run("stats", func(b *testing.B) {
		c := stats.Wrap(benchCache())
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

// BenchmarkStats takes a snapshot of a cache that nobody else is using.
func BenchmarkStats(b *testing.B) {
	c := stats.Wrap(benchCache())
	b.ReportAllocs()
	for b.Loop() {
		c.Stats()
	}
}
