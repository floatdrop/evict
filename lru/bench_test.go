package lru_test

import (
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/floatdrop/evict/lru"
)

// These benchmarks measure single operations on a cache that fits in the CPU
// caches. For comparisons with other libraries and for larger workloads see
// the bench module.
//
// The cache is in one piece wherever a benchmark depends on what is cached:
// a cache with several shards evicts before it is full, and a hit would not
// always be one.

const benchCapacity = 1 << 14

// benchKeys returns a power-of-two number of keys in random order, drawn from
// [0, benchCapacity).
func benchKeys() []int {
	rng := rand.New(rand.NewPCG(1, 2))
	keys := make([]int, 1<<16)
	for i := range keys {
		keys[i] = rng.IntN(benchCapacity)
	}
	return keys
}

func BenchmarkGet(b *testing.B) {
	b.Run("hit", func(b *testing.B) {
		c := lru.New[int, int](benchCapacity, lru.WithShards(1))
		for key := range benchCapacity {
			c.Set(key, key)
		}
		keys := benchKeys()

		b.ReportAllocs()
		i := 0
		for b.Loop() {
			c.Get(keys[i&(len(keys)-1)])
			i++
		}
	})

	b.Run("miss", func(b *testing.B) {
		c := lru.New[int, int](benchCapacity, lru.WithShards(1))
		for key := range benchCapacity {
			c.Set(-key-1, key)
		}
		keys := benchKeys()

		b.ReportAllocs()
		i := 0
		for b.Loop() {
			c.Get(keys[i&(len(keys)-1)])
			i++
		}
	})

	b.Run("hit/string", func(b *testing.B) {
		c := lru.New[string, int](benchCapacity, lru.WithShards(1))
		names := make([]string, benchCapacity)
		for key := range benchCapacity {
			names[key] = "a key of ordinary length " + strconv.Itoa(key)
			c.Set(names[key], key)
		}
		keys := benchKeys()

		b.ReportAllocs()
		i := 0
		for b.Loop() {
			c.Get(names[keys[i&(len(keys)-1)]])
			i++
		}
	})
}

func BenchmarkSet(b *testing.B) {
	b.Run("update", func(b *testing.B) {
		c := lru.New[int, int](benchCapacity, lru.WithShards(1))
		for key := range benchCapacity {
			c.Set(key, key)
		}
		keys := benchKeys()

		b.ReportAllocs()
		i := 0
		for b.Loop() {
			c.Set(keys[i&(len(keys)-1)], i)
			i++
		}
	})

	b.Run("evict", func(b *testing.B) {
		c := lru.New[int, int](benchCapacity, lru.WithShards(1))
		for key := range benchCapacity {
			c.Set(-key-1, key)
		}

		b.ReportAllocs()
		i := 0
		for b.Loop() {
			c.Set(i, i) // Always a new key, so always an eviction.
			i++
		}
	})
}

// BenchmarkParallel has every CPU working on the same cache: three reads for
// every write, on keys of which a few are much more popular than the rest and
// a quarter does not fit.
func BenchmarkParallel(b *testing.B) {
	const keySpace = 4 * benchCapacity

	c := lru.New[int, int](benchCapacity)
	zipf := rand.NewZipf(rand.New(rand.NewPCG(1, 2)), 1.01, 1, keySpace-1)
	keys := make([]int, 1<<16)
	for i := range keys {
		keys[i] = int(zipf.Uint64())
		c.Set(keys[i], i)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := rand.Int()
		for pb.Next() {
			key := keys[i&(len(keys)-1)]
			if i&3 == 0 {
				c.Set(key, i)
			} else {
				c.Get(key)
			}
			i++
		}
	})
}
