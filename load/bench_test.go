package load_test

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/floatdrop/evict"
	"github.com/floatdrop/evict/load"
	"github.com/floatdrop/evict/tinylfu"
)

const benchCapacity = 1 << 14

// fill sets the keys 0 to benchCapacity-1, through set.
func fill(set func(key, value int)) {
	for key := range benchCapacity {
		set(key, key)
	}
}

// BenchmarkFetch reads entries that are all there, on every CPU at once: with
// the Get of tinylfu, through an interface, and with the Fetch of the
// wrapper, which calls Get through one. The loop is written out for each, so
// that it makes the call it is about and no other.
func BenchmarkFetch(b *testing.B) {
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

	b.Run("load", func(b *testing.B) {
		c := load.Wrap(tinylfu.New[int, int](benchCapacity), never[int, int])
		fill(func(k, v int) { c.Set(k, v) })
		ctx := b.Context()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Int()
			for pb.Next() {
				_, _ = c.Fetch(ctx, i&(benchCapacity-1))
				i++
			}
		})
	})
}

// BenchmarkSet replaces entries, on every CPU at once: directly, and through
// the wrapper, which hashes the key once more and locks a stripe around the
// Set of tinylfu.
func BenchmarkSet(b *testing.B) {
	b.Run("direct", func(b *testing.B) {
		c := tinylfu.New[int, int](benchCapacity)
		fill(func(k, v int) { c.Set(k, v) })
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Int()
			for pb.Next() {
				c.Set(i&(benchCapacity-1), i)
				i++
			}
		})
	})

	b.Run("load", func(b *testing.B) {
		c := load.Wrap(tinylfu.New[int, int](benchCapacity), never[int, int])
		fill(func(k, v int) { c.Set(k, v) })
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := rand.Int()
			for pb.Next() {
				c.Set(i&(benchCapacity-1), i)
				i++
			}
		})
	})
}

// BenchmarkLoad is a Fetch that loads, on one CPU: a miss, a goroutine for
// the load, and the Set that stores its value. The loader returns at once, as
// no source does.
func BenchmarkLoad(b *testing.B) {
	c := load.Wrap(tinylfu.New[int, int](benchCapacity), func(_ context.Context, key int) (int, error) {
		return key, nil
	})
	ctx := context.Background()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_, _ = c.Fetch(ctx, i)
		i++
	}
}
