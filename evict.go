// Package evict is a family of fixed-capacity in-memory caches that share one
// API and differ in how they choose what to evict:
//
//   - [github.com/floatdrop/evict/lru] evicts the least recently used entry.
//   - [github.com/floatdrop/evict/sieve] implements SIEVE, a queue with lazy
//     promotion, and reads from it without taking a lock.
//   - [github.com/floatdrop/evict/s3fifo] implements S3-FIFO, which lets new
//     entries prove themselves in a small queue before they get into the
//     main one, and reads without taking a lock as well.
//   - [github.com/floatdrop/evict/tinylfu] admits with TinyLFU: an entry gets
//     past the small queue only if its key has been asked for more often of
//     late than the entry it would replace, by a record that the cache keeps
//     of keys whether it holds them or not. It finds the size of the small
//     queue by itself, and reads without taking a lock as well.
//
// Every cache is safe for concurrent use and keeps its entries in arrays that
// it allocates when it is created and addresses by 32-bit indices. Getting,
// setting and deleting entries therefore allocates nothing, and the garbage
// collector has nothing to trace beyond the keys and values themselves.
//
// There is one thing the caches cannot help: to hash a key that is a pointer
// or an interface, or that contains one, Go moves the key to the heap if it
// is not there already. Strings do not count. A lookup with such a key that
// was made up on the spot, like a number put into an interface, costs an
// allocation that a map would have avoided.
//
// Keys are compared with ==, as in a map. A key that is not equal to itself,
// which is to say one with a NaN in it, can be set but not found again: its
// entry stays until it is evicted, removed by DeleteFunc, or the cache is
// cleared.
//
// # Capacity and shards
//
// A cache spreads its entries over shards by the hash of their keys, so that
// goroutines working on different shards do not wait for each other. Every
// shard has its share of the capacity and evicts on its own: a shard that is
// full evicts although the cache as a whole may have room. The capacity is
// therefore an upper bound that a cache with several shards rarely reaches
// exactly. Small caches have a single shard, for which it is exact, and the
// WithShards option of each package sets the number for the others.
//
// This package holds what the caches have in common: the [Cache] interface.
// [github.com/floatdrop/evict/stats] counts the hits, misses and evictions of
// any of them, [github.com/floatdrop/evict/expire] gives their entries a time
// to live, and [github.com/floatdrop/evict/load] fills them from a source,
// with one load for the goroutines that ask for a key at the same time.
// [github.com/floatdrop/evict/ref] counts the users of values that have to be
// released, so that the caches can share them out.
package evict

import "iter"

// Cache is the behaviour common to every cache in this module. It is
// implemented by *lru.Cache, *sieve.Cache, *s3fifo.Cache and *tinylfu.Cache,
// and by the wrappers *stats.Cache, *expire.Cache and *load.Cache.
//
// All methods are safe for concurrent use.
type Cache[K comparable, V any] interface {
	// Get returns the value stored under key and records the access with
	// the eviction policy.
	Get(key K) (value V, ok bool)

	// GetMany does what Get does for each of keys, one after the other,
	// and stores the value it finds for keys[i] in values[i] and whether
	// it found one in found[i]. It panics if values or found is shorter
	// than keys. values and found may each be keys itself, but must not
	// otherwise overlap it.
	//
	// sieve, s3fifo and tinylfu load what a batch of keys needs from memory
	// at once: for a cache too large for the CPU's caches, a key took about
	// half the time it takes with Get, with 32 keys per call. For one key or
	// a few, Get is faster. lru takes the lock of a shard once for the keys
	// in a row that fall in it.
	GetMany(keys []K, values []V, found []bool)

	// Peek returns the value stored under key without telling the eviction
	// policy about it.
	Peek(key K) (value V, ok bool)

	// Set stores value under key and reports the entry that left the cache
	// as a result, if there is one. That entry is either the previous entry
	// for key, in which case oldKey == key, or an entry that was evicted to
	// make room. A cache with no capacity keeps nothing and hands the new
	// entry straight back.
	//
	// Whatever an entry holds on to can thus be released when Set returns
	// it, whichever way it left, once no goroutine that got it from the
	// cache still uses it; package ref keeps count of those. That goes for
	// the key as well: when an entry is replaced, the cache keeps the key it
	// was just given and returns the one it had.
	Set(key K, value V) (oldKey K, oldValue V, displaced bool)

	// SetIfAbsent stores value under key unless the cache holds key
	// already. If it does, SetIfAbsent changes nothing but records the
	// access with the eviction policy, as Get does, and returns the value
	// that key has, with present true. Otherwise it stores value as Set
	// does, returns it with present false, and reports the entry that left
	// the cache for it, if there is one: one evicted to make room, or, in an
	// expire.Cache, the expired entry of key, with oldKey == key. A cache
	// with no capacity keeps nothing and hands the new entry straight back
	// as evicted.
	//
	// Looking for key and storing value happen at once: of the goroutines
	// that call SetIfAbsent for one key at the same time, one stores its
	// value and the others get it back as actual, unless it has left the
	// cache in between.
	SetIfAbsent(key K, value V) (actual V, present bool, oldKey K, oldValue V, evicted bool)

	// Delete removes key and returns the value it had.
	Delete(key K) (value V, ok bool)

	// DeleteFunc removes the entries for which del returns true and reports
	// how many it removed. It visits one shard at a time, without recording
	// accesses, and tests and removes each entry at once with respect to
	// writes to its key; the pass as a whole is not a snapshot.
	//
	// del runs with the shard locked: it must not call back into the cache,
	// and should return promptly. If it panics, the entries removed before
	// stay removed.
	DeleteFunc(del func(K, V) bool) int

	// Len returns the number of entries in the cache. While the cache is
	// being written to, the shards are counted one after the other and
	// the sum may be one that the cache never held at any one moment.
	Len() int

	// Cap returns the number of entries the cache can hold.
	Cap() int

	// Clear removes every entry, without reporting any of them. If the
	// entries hold on to something that has to be released, do that by
	// going over them with All before Clear, at a time when nothing else
	// is using the cache: an entry that is added in between would be
	// cleared unseen, and one that is replaced would be seen twice, by
	// the loop and by whoever replaced it.
	Clear()

	// All iterates over the entries in no particular order, without
	// recording accesses. It copies the entries of one shard at a time,
	// which is the one operation on a cache that allocates.
	//
	// The cache may be used, also by the loop body, while the iteration is
	// in progress. Entries that are added or removed in the meantime may
	// or may not be seen; no entry is seen twice unless it is removed and
	// added again.
	All() iter.Seq2[K, V]
}
