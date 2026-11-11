package twoq_test

import (
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/floatdrop/evict/twoq"
)

// These benchmarks measure single operations on a cache that fits in the CPU
// caches. For comparisons with other libraries and for larger workloads see
// the bench module.

const benchCapacity = 1 << 14

// The keys of a cache made by steady, by what has become of them.
const (
	frequentFrom, frequentTo = benchCapacity / 2, benchCapacity
	ghostFrom, ghostTo       = benchCapacity, benchCapacity * 3 / 2
	recentFrom, recentTo     = benchCapacity * 3 / 2, benchCapacity * 2
	unknownFrom              = benchCapacity * 2
)

// steady returns a cache in the state that a 2Q cache settles into: half of
// its entries are frequent, half are recent, and it remembers as many keys as
// it may. A cache that has merely been filled would have nothing but recent
// entries, and reads would have nothing to do. The cache is in one piece, so
// that the state is exactly this one.
func steady[K comparable](key func(int) K) *twoq.Cache[K, int] {
	c := twoq.New[K, int](benchCapacity, twoq.WithShards(1))
	for i := range 2 * benchCapacity {
		c.Set(key(i), i) // The first half is evicted again; the later ones become ghosts.
	}
	for i := frequentFrom; i < frequentTo; i++ {
		c.Set(key(i), i) // The ghosts return.
	}
	return c
}

// benchKeys returns a power-of-two number of keys in random order, drawn from
// [from, to).
func benchKeys(from, to int) []int {
	rng := rand.New(rand.NewPCG(1, 2))
	keys := make([]int, 1<<16)
	for i := range keys {
		keys[i] = from + rng.IntN(to-from)
	}
	return keys
}

func BenchmarkGet(b *testing.B) {
	number := func(i int) int { return i }
	for _, tt := range []struct {
		name     string
		from, to int
	}{
		{"frequent", frequentFrom, frequentTo}, // A hit that moves the entry to the front.
		{"recent", recentFrom, recentTo},       // A hit that changes nothing.
		{"ghost", ghostFrom, ghostTo},          // A miss on a key that is remembered.
		{"miss", unknownFrom, 2 * unknownFrom}, // A miss on a key that is not.
	} {
		b.Run(tt.name, func(b *testing.B) {
			c := steady(number)
			keys := benchKeys(tt.from, tt.to)
			if _, ok := c.Get(keys[0]); ok != (tt.name == "frequent" || tt.name == "recent") {
				b.Fatalf("Get(%d) = %t: the cache is not in the state this benchmark is for", keys[0], ok)
			}

			b.ReportAllocs()
			i := 0
			for b.Loop() {
				c.Get(keys[i&(len(keys)-1)])
				i++
			}
		})
	}

	b.Run("frequent/string", func(b *testing.B) {
		names := make([]string, 2*benchCapacity)
		for i := range names {
			names[i] = "a key of ordinary length " + strconv.Itoa(i)
		}
		c := steady(func(i int) string { return names[i] })
		keys := benchKeys(frequentFrom, frequentTo)

		b.ReportAllocs()
		i := 0
		for b.Loop() {
			c.Get(names[keys[i&(len(keys)-1)]])
			i++
		}
	})
}

func BenchmarkSet(b *testing.B) {
	number := func(i int) int { return i }

	b.Run("update", func(b *testing.B) {
		c := steady(number)
		keys := benchKeys(frequentFrom, frequentTo)

		b.ReportAllocs()
		i := 0
		for b.Loop() {
			c.Set(keys[i&(len(keys)-1)], i)
			i++
		}
	})

	b.Run("evict", func(b *testing.B) {
		c := steady(number)

		b.ReportAllocs()
		i := 0
		for b.Loop() {
			c.Set(-i-1, i) // Always a new key, so always an eviction.
			i++
		}
	})
}

// BenchmarkParallel has every CPU working on the same cache: three reads for
// every write, on keys of which a few are much more popular than the rest and
// a quarter does not fit.
func BenchmarkParallel(b *testing.B) {
	const keySpace = 4 * benchCapacity

	c := twoq.New[int, int](benchCapacity)
	zipf := rand.NewZipf(rand.New(rand.NewPCG(1, 2)), 1.01, 1, keySpace-1)
	keys := make([]int, 1<<16)
	for i := range keys {
		keys[i] = int(zipf.Uint64())
	}
	// Twice through the keys, so that the popular ones have had the time to
	// be evicted and to come back.
	for range 2 {
		for i, key := range keys {
			c.Set(key, i)
		}
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
