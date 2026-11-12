// Package stats counts the hits, misses and evictions of a cache, so that it
// can be monitored.
//
// [Wrap] puts a counter in front of any [evict.Cache]:
//
//	cache := stats.Wrap(tinylfu.New[string, int](10_000))
//	// ... use cache as before ...
//	s := cache.Stats()
//	fmt.Println(s.HitRatio(), s.Evictions, s.Len, s.Cap)
//
// What is counted:
//
//   - Get: a hit if it finds the key, a miss if it does not.
//   - GetMany: the same for each key.
//   - Set: an eviction if it pushed out the entry of another key to make
//     room, or if the cache has no capacity and handed the new entry back.
//     Replacing the entry of a key is not an eviction.
//   - Peek, Delete, Clear and All: nothing.
//
// The counts only go up, and Clear does not reset them. For rates, take a
// [Snapshot] at intervals and subtract the one before with [Snapshot.Sub].
//
// # Cost
//
// The counts are kept in stripes of a cache line each, sixteen for every CPU
// that Go may use when the cache is wrapped. A goroutine picks a stripe by the
// address of its stack, so goroutines on different CPUs rarely count in the
// same line, and the reads of the lock-free caches keep scaling with the CPUs.
// The stripes take 2 to 4 KiB per CPU for every wrapped cache, whatever its
// capacity. If GOMAXPROCS goes up later, goroutines share stripes more often
// and counting slows down.
//
// On an Apple M3 Max, a Get of tinylfu that finds its key took 10 to 14%
// longer through the wrapper, on one CPU and on eight.
//
// The wrapper reaches the cache through an interface, which the compiler
// cannot see through: a key that would otherwise stay where it is may have to
// move to the heap, as one converted from a []byte on the spot does.
package stats

import (
	"iter"
	"math/bits"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/floatdrop/evict"
)

// cacheLine is a size that no two CPUs' cache lines straddle on the
// platforms Go runs on.
const cacheLine = 128

// Snapshot is what a cache had counted at one moment, and how full it was.
//
// Each count is read on its own while the cache may be in use, so the counts
// of one snapshot may be from slightly different moments. A count is never
// less than that of a snapshot taken before it.
type Snapshot struct {
	// Hits and Misses count the keys that Get and GetMany found, and those
	// that they did not.
	Hits, Misses uint64

	// Evictions counts the entries that Set pushed out to make room, and
	// those that a cache without capacity handed straight back.
	Evictions uint64

	// Len is the number of entries in the cache, and Cap the number it can
	// hold.
	Len, Cap int
}

// HitRatio returns the share of the keys looked up with Get and GetMany that
// were found, from 0 to 1, or 0 if there were none.
func (s Snapshot) HitRatio() float64 {
	if s.Hits+s.Misses == 0 {
		return 0
	}
	return float64(s.Hits) / float64(s.Hits+s.Misses)
}

// Sub returns what was counted from prev to s, with the Len and Cap of s.
// Both must be snapshots of the same cache, prev the earlier one.
func (s Snapshot) Sub(prev Snapshot) Snapshot {
	return Snapshot{
		Hits:      s.Hits - prev.Hits,
		Misses:    s.Misses - prev.Misses,
		Evictions: s.Evictions - prev.Evictions,
		Len:       s.Len,
		Cap:       s.Cap,
	}
}

// Cache is an [evict.Cache] that counts what is done with it; see the package
// documentation. Its methods are safe for concurrent use as long as those of
// the cache it wraps are, as they are for every cache in this module.
type Cache[K comparable, V any] struct {
	cache   evict.Cache[K, V]
	many    manyGetter[K, V] // cache, if it has a GetMany
	stripes []stripe
	shift   uint8 // 64 minus log2 of the number of stripes
	zero    bool  // the cache has no capacity, and hands every entry back
}

// manyGetter is a cache that looks up several keys at once, as sieve, s3fifo
// and tinylfu do.
type manyGetter[K comparable, V any] interface {
	GetMany(keys []K, values []V, found []bool)
}

// A stripe holds the counts of the goroutines that pick it, in a cache line
// of its own.
//
// The counts are plain integers that are only ever accessed with the
// functions of sync/atomic. The compiler builds those in wherever it compiles
// the methods of a Cache, which is in the package that uses them. The methods
// of the atomic types, on the other hand, it inlines there only if a package
// that is imported there has inlined them itself. This one does, by way of
// the assertion at the end of the file, but a program that gets to a Cache
// through a generic type of its own package which instantiates nothing would
// import only that package, and make a call more for every count. The
// statswrap probe of internal/inlinecheck checks for that.
type stripe struct {
	hits      uint64 //nolint:modernize // See above.
	misses    uint64 //nolint:modernize // See above.
	evictions uint64 //nolint:modernize // See above.

	_ [cacheLine - 24]byte
}

// Wrap returns a Cache that counts what is done with cache, which it uses from
// then on. Using cache directly as well is fine, but goes uncounted.
func Wrap[K comparable, V any](cache evict.Cache[K, V]) *Cache[K, V] {
	// Sixteen stripes per CPU make it unlikely that two goroutines that run
	// at the same time pick the same one.
	n := 1 << bits.Len(uint(16*runtime.GOMAXPROCS(0)-1))
	many, _ := cache.(manyGetter[K, V])
	return &Cache[K, V]{
		cache:   cache,
		many:    many,
		stripes: make([]stripe, n),
		shift:   uint8(64 - bits.TrailingZeros(uint(n))),
		zero:    cache.Cap() == 0,
	}
}

// stripe returns the stripe that the calling goroutine counts in.
//
// There is no way to ask which goroutine or CPU this is, but the whereabouts
// of the stack tell one goroutine from another: stacks are at least 2 KiB in
// size. The bits of the address below that say how deep into its stack a
// goroutine is, and are dropped. A goroutine that counts from about the same
// depth thus picks the same stripe, which stays in the cache of its CPU, until
// its stack grows and moves; from much deeper, it picks another.
func (c *Cache[K, V]) stripe() *stripe {
	var onStack byte
	//nolint:gosec // The address is only hashed, never turned back into a pointer.
	stack := uint64(uintptr(unsafe.Pointer(&onStack))) >> 11
	return &c.stripes[(stack*0x9e3779b97f4a7c15)>>c.shift]
}

// Get returns the value stored under key, and counts a hit or a miss.
func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	value, ok = c.cache.Get(key)
	s := c.stripe()
	if ok {
		atomic.AddUint64(&s.hits, 1)
	} else {
		atomic.AddUint64(&s.misses, 1)
	}
	return value, ok
}

// GetMany looks up each of keys, stores the value it finds for keys[i] in
// values[i] and whether it found one in found[i], and counts a hit or a miss
// for each key. It panics if values or found is shorter than keys.
//
// If the cache has a GetMany of its own, as sieve, s3fifo and tinylfu do,
// GetMany uses it: for a cache that is too large for the CPU's caches it is
// faster than Get for each key. Otherwise it calls Get for each key.
func (c *Cache[K, V]) GetMany(keys []K, values []V, found []bool) {
	if len(values) < len(keys) || len(found) < len(keys) {
		panic("stats: GetMany: values and found must be at least as long as keys")
	}
	if c.many != nil {
		c.many.GetMany(keys, values, found)
	} else {
		for i, key := range keys {
			values[i], found[i] = c.cache.Get(key)
		}
	}

	hits := 0
	for _, ok := range found[:len(keys)] {
		if ok {
			hits++
		}
	}
	s := c.stripe()
	if hits > 0 {
		atomic.AddUint64(&s.hits, uint64(hits))
	}
	if misses := len(keys) - hits; misses > 0 {
		atomic.AddUint64(&s.misses, uint64(misses))
	}
}

// Peek returns the value stored under key, and counts nothing.
func (c *Cache[K, V]) Peek(key K) (value V, ok bool) { return c.cache.Peek(key) }

// Set stores value under key, and counts an eviction if the entry it reports
// is not the one that key had.
func (c *Cache[K, V]) Set(key K, value V) (oldKey K, oldValue V, displaced bool) {
	oldKey, oldValue, displaced = c.cache.Set(key, value)
	// A key that is replaced comes back equal to the new one, and so does
	// the new entry itself from a cache without capacity, which counts.
	if displaced && (oldKey != key || c.zero) {
		atomic.AddUint64(&c.stripe().evictions, 1)
	}
	return oldKey, oldValue, displaced
}

// Delete removes key and returns the value it had, and counts nothing.
func (c *Cache[K, V]) Delete(key K) (value V, ok bool) { return c.cache.Delete(key) }

// Len returns the number of entries in the cache.
func (c *Cache[K, V]) Len() int { return c.cache.Len() }

// Cap returns the number of entries the cache can hold.
func (c *Cache[K, V]) Cap() int { return c.cache.Cap() }

// Clear removes every entry. It leaves the counts as they are.
func (c *Cache[K, V]) Clear() { c.cache.Clear() }

// All iterates over the entries of the cache, and counts nothing.
func (c *Cache[K, V]) All() iter.Seq2[K, V] { return c.cache.All() }

// Stats returns the counts so far, and the number of entries in the cache and
// its capacity. It reads every stripe and calls Len, which locks the shards of
// some caches one after the other: call it when a monitor asks, not for every
// request.
func (c *Cache[K, V]) Stats() Snapshot {
	var s Snapshot
	for i := range c.stripes {
		st := &c.stripes[i]
		s.Hits += atomic.LoadUint64(&st.hits)
		s.Misses += atomic.LoadUint64(&st.misses)
		s.Evictions += atomic.LoadUint64(&st.evictions)
	}
	s.Len, s.Cap = c.cache.Len(), c.cache.Cap()
	return s
}

var _ evict.Cache[int, int] = (*Cache[int, int])(nil)
