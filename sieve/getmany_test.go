package sieve_test

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/floatdrop/evict/internal/cachetest"
	"github.com/floatdrop/evict/internal/cslab"
	"github.com/floatdrop/evict/sieve"
)

// GetMany has to find what Peek finds, key by key, for any number of keys
// around that of a batch, also when a key comes up more than once.
func TestGetMany(t *testing.T) {
	t.Parallel()

	const capacity = 1000
	c := sieve.New[int, int](capacity)
	rng := rand.New(rand.NewPCG(1, 2))
	for range 3 * capacity {
		k := rng.IntN(2 * capacity)
		c.Set(k, -k)
	}

	// Lengths that leave every number of keys from 0 to MinBatch at the end
	// of a call, where they go through Get.
	for _, n := range []int{
		0, 1, 2, 3, cslab.MinBatch, cslab.Batch - 1, cslab.Batch, cslab.Batch + 1,
		cslab.Batch + cslab.MinBatch - 1, 2*cslab.Batch + 3, 3*cslab.Batch + 5, capacity,
	} {
		keys := make([]int, n)
		for i := range keys {
			keys[i] = rng.IntN(2 * capacity)
		}
		values, found := make([]int, n), make([]bool, n)
		c.GetMany(keys, values, found)
		for i, k := range keys {
			if v, ok := c.Peek(k); values[i] != v || found[i] != ok {
				t.Fatalf("GetMany of %d keys: key %d gave %d, %t; Peek gives %d, %t", n, k, values[i], found[i], v, ok)
			}
		}
	}
}

// values may be keys itself: each key is looked up before its value is
// written over it.
func TestGetManyInPlace(t *testing.T) {
	t.Parallel()

	c := sieve.New[int, int](1000)
	for k := range 500 {
		c.Set(k, -k)
	}
	rng := rand.New(rand.NewPCG(5, 6))
	keys := make([]int, 2*cslab.Batch+3)
	for i := range keys {
		keys[i] = rng.IntN(1000)
	}
	asked := slices.Clone(keys)
	found := make([]bool, len(keys))
	c.GetMany(keys, keys, found)
	for i, k := range asked {
		if v, ok := c.Peek(k); keys[i] != v || found[i] != ok {
			t.Fatalf("key %d gave %d, %t; Peek gives %d, %t", k, keys[i], found[i], v, ok)
		}
	}
}

// GetMany has to count reads as Get does, also those of a key that comes up
// several times, past the limit, and also when it has to take the lock: what
// the cache evicts next depends on them.
func TestGetManyCountsReads(t *testing.T) {
	t.Parallel()

	for _, locked := range []bool{false, true} {
		t.Run("locked="+strconv.FormatBool(locked), func(t *testing.T) {
			t.Parallel()

			const capacity = 200
			viaGet := sieve.New[int, int](capacity, sieve.WithShards(1))
			viaMany := sieve.New[int, int](capacity, sieve.WithShards(1))
			for k := range capacity {
				viaGet.Set(k, k)
				viaMany.Set(k, k)
			}

			// Half of the keys are not there, and most come up more than
			// once. The last few are too few to load ahead, and go through
			// Get.
			rng := rand.New(rand.NewPCG(3, 4))
			keys := make([]int, 5*cslab.Batch+cslab.MinBatch-1)
			for i := range keys {
				keys[i] = rng.IntN(2 * capacity)
			}
			values, found := make([]int, len(keys)), make([]bool, len(keys))

			// With every slot of the readers taken, a lookup cannot pin
			// anything and has to take the lock.
			releaseGet, releaseMany := func() {}, func() {}
			if locked {
				releaseGet, releaseMany = viaGet.OccupyReaders(0), viaMany.OccupyReaders(0)
			}
			viaMany.GetMany(keys, values, found)
			for i, k := range keys {
				if v, ok := viaGet.Get(k); values[i] != v || found[i] != ok {
					t.Fatalf("key %d: GetMany gave %d, %t; Get gives %d, %t", k, values[i], found[i], v, ok)
				}
			}
			releaseGet()
			releaseMany()

			for k := range 2 * capacity {
				a, aok := viaGet.Reads(k)
				b, bok := viaMany.Reads(k)
				if a != b || aok != bok {
					t.Errorf("key %d: %d reads, %t after Get; %d, %t after GetMany", k, a, aok, b, bok)
				}
			}

			for k := 2 * capacity; k < 3*capacity; k++ {
				viaGet.Set(k, k)
				viaMany.Set(k, k)
			}
			if a, b := slices.Sorted(maps.Keys(maps.Collect(viaGet.All()))), slices.Sorted(maps.Keys(maps.Collect(viaMany.All()))); !slices.Equal(a, b) {
				t.Errorf("after evictions, the caches hold different keys:\nGet:     %v\nGetMany: %v", a, b)
			}
		})
	}
}

// GetMany reads while others write. Keys that nobody touches have to be found
// with their values, others with a value that was stored under them if at
// all, and the race detector has to find nothing.
func TestGetManyConcurrent(t *testing.T) {
	t.Parallel()

	// The writers never hold so many keys that the cache has to evict.
	const (
		capacity = 1024
		stable   = 64
		changing = 512
	)
	c := sieve.New[int, int](capacity, sieve.WithShards(1))
	for k := range stable {
		c.Set(k, -k)
	}

	var writers, readers sync.WaitGroup
	done := make(chan struct{})
	for w := range 2 {
		writers.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(w), 9))
			for {
				select {
				case <-done:
					return
				default:
				}
				k := stable + rng.IntN(changing)
				if rng.IntN(4) == 0 {
					c.Delete(k)
				} else {
					c.Set(k, -k)
				}
			}
		})
	}
	for r := range 4 {
		readers.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(r), 7))
			n := cslab.Batch + 5
			keys, values, found := make([]int, n), make([]int, n), make([]bool, n)
			for range 2000 {
				for i := range keys {
					keys[i] = rng.IntN(stable + changing)
				}
				c.GetMany(keys, values, found)
				for i, k := range keys {
					if k < stable && !found[i] || found[i] && values[i] != -k {
						t.Errorf("key %d gave %d, %t", k, values[i], found[i])
						return
					}
				}
			}
		})
	}
	readers.Wait()
	close(done)
	writers.Wait()
}

// Slices that are too short for the keys must make GetMany panic before it
// does anything: before it writes past their ends, which there is room for in
// the arrays behind them, and before it counts any read.
func TestGetManyShortSlices(t *testing.T) {
	t.Parallel()

	c := sieve.New[int, int](16)
	keys := []int{1, 2, 3}
	for _, k := range keys {
		c.Set(k, k)
	}
	values, found := make([]int, 8), make([]bool, 8)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("GetMany with values and found shorter than keys did not panic")
			}
		}()
		c.GetMany(keys, values[:2], found[:2])
	}()

	if slices.ContainsFunc(values, func(v int) bool { return v != 0 }) || slices.Contains(found, true) {
		t.Errorf("GetMany wrote %v and %v before it panicked", values, found)
	}
	for _, k := range keys {
		if reads, _ := c.Reads(k); reads != 0 {
			t.Errorf("key %d has %d reads after GetMany panicked", k, reads)
		}
	}
}

// TestGetManyNoAllocs is not parallel: counting allocations needs the process
// to itself.
func TestGetManyNoAllocs(t *testing.T) {
	c := sieve.New[string, int](1000)
	keys := make([]string, 3*cslab.Batch)
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
		if i%2 == 0 {
			c.Set(keys[i], i)
		}
	}
	values, found := make([]int, len(keys)), make([]bool, len(keys))
	if allocs := testing.AllocsPerRun(100, func() { c.GetMany(keys, values, found) }); allocs != 0 {
		t.Errorf("GetMany made %v allocations, want 0", allocs)
	}
}

// The caches and keys of BenchmarkGetMany, made once. Every key has been read
// often enough beforehand that reads no longer change the entries, so that
// the first variant to run does not pay for that.
var (
	benchSmall = sync.OnceValues(func() (*sieve.Cache[int, int], []int) { return filled(cachetest.UniformInts(1 << 14)) })
	benchLarge = sync.OnceValues(func() (*sieve.Cache[int, int], []int) { return filled(cachetest.UniformInts(1 << 20)) })
	benchNames = sync.OnceValues(func() (*sieve.Cache[string, int], []string) {
		return filled(cachetest.UniformStrings(1 << 20))
	})
	benchZipf = sync.OnceValues(func() (*sieve.Cache[string, int], []string) {
		const capacity = 1 << 20
		warm, seq := cachetest.ZipfStrings(capacity)
		c := sieve.New[string, int](capacity)
		for i, k := range warm {
			if _, ok := c.Get(k); !ok {
				c.Set(k, i)
			}
		}
		for _, k := range seq {
			c.Get(k)
		}
		return c, seq
	})
)

// filled returns a cache that holds keys, with room for twice as many so that
// no shard has to evict any.
func filled[K comparable](keys, seq []K) (c *sieve.Cache[K, int], lookups []K) {
	c = sieve.New[K, int](2 * len(keys))
	for i, k := range keys {
		c.Set(k, i)
	}
	for range 4 {
		for _, k := range keys {
			c.Get(k)
		}
	}
	return c, seq
}

// BenchmarkGetMany looks keys up, on every CPU at once: with Get one after the
// other, and with GetMany, in calls of the given number of keys. The keys are
// all there, but for those of zipf, which are requested as in the bench
// module's ReadThrough. ns/key is the time per key, and hits the share of keys
// found.
func BenchmarkGetMany(b *testing.B) {
	b.Run("int/entries=16K/keys=32", func(b *testing.B) { c, seq := benchSmall(); benchGetMany(b, c, seq, 32) })
	for _, n := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("int/entries=1M/keys=%d", n), func(b *testing.B) { c, seq := benchLarge(); benchGetMany(b, c, seq, n) })
	}
	b.Run("string/entries=1M/keys=32", func(b *testing.B) { c, seq := benchNames(); benchGetMany(b, c, seq, 32) })
	b.Run("zipf/capacity=1M/keys=32", func(b *testing.B) { c, seq := benchZipf(); benchGetMany(b, c, seq, 32) })
}

// benchGetMany looks up seq in calls of n keys, a power of two.
func benchGetMany[K comparable](b *testing.B, c *sieve.Cache[K, int], seq []K, n int) {
	b.Helper()
	mask := len(seq) - 1

	run := func(b *testing.B, many bool) {
		b.Helper()
		var (
			mu   sync.Mutex
			hits int
		)
		b.RunParallel(func(pb *testing.PB) {
			values, found := make([]int, n), make([]bool, n)
			i, h := rand.Int()&^(n-1), 0
			for pb.Next() {
				keys := seq[i&mask : i&mask+n]
				if many {
					c.GetMany(keys, values, found)
				} else {
					for j, key := range keys {
						values[j], found[j] = c.Get(key)
					}
				}
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
		keys := float64(b.N * n)
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/keys, "ns/key")
		b.ReportMetric(float64(hits)/keys, "hits")
	}
	b.Run("get", func(b *testing.B) { run(b, false) })
	b.Run("many", func(b *testing.B) { run(b, true) })
}
