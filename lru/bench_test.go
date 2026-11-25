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

// BenchmarkSetIfAbsent is BenchmarkSet with SetIfAbsent: for keys that the
// cache holds, which it reads, and for new ones, which it stores.
func BenchmarkSetIfAbsent(b *testing.B) {
	b.Run("present", func(b *testing.B) {
		c := lru.New[int, int](benchCapacity, lru.WithShards(1))
		for key := range benchCapacity {
			c.Set(key, key)
		}
		keys := benchKeys()

		b.ReportAllocs()
		i := 0
		for b.Loop() {
			c.SetIfAbsent(keys[i&(len(keys)-1)], i)
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
			c.SetIfAbsent(i, i) // Always a new key, so always an eviction.
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

// BenchmarkGetMany looks up 32 keys at a time, picked at random from 16,384
// entries that are all there, on every CPU at once: with GetMany, and with
// Get for each key. ns/key is the time per key.
func BenchmarkGetMany(b *testing.B) {
	const (
		entries = 1 << 14
		n       = 32
	)
	for _, shards := range []int{1, 0} {
		c := lru.New[int, int](entries, lru.WithShards(shards))
		for key := range entries {
			c.Set(key, key)
		}
		seq := rand.New(rand.NewPCG(1, 2)).Perm(entries)
		run := func(b *testing.B, many bool) {
			b.Helper()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				values, found := make([]int, n), make([]bool, n)
				i := rand.Int() &^ (n - 1)
				for pb.Next() {
					keys := seq[i&(entries-1) : i&(entries-1)+n]
					if many {
						c.GetMany(keys, values, found)
					} else {
						for j, key := range keys {
							values[j], found[j] = c.Get(key)
						}
					}
					i += n
				}
			})
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/key")
		}
		name := "shards=" + strconv.Itoa(shards)
		b.Run(name+"/GetMany", func(b *testing.B) { run(b, true) })
		b.Run(name+"/Get", func(b *testing.B) { run(b, false) })
	}
}
