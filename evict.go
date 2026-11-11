// Package evict is a family of fixed-capacity in-memory caches that share one
// API and differ in how they choose what to evict:
//
//   - [github.com/floatdrop/evict/lru] evicts the least recently used entry.
//   - [github.com/floatdrop/evict/twoq] implements 2Q, which keeps new entries
//     apart from the ones that came back after they had been evicted, so that
//     a burst of new entries cannot push out the proven ones.
//   - [github.com/floatdrop/evict/sieve] implements SIEVE, a queue with lazy
//     promotion, and reads from it without taking a lock.
//   - [github.com/floatdrop/evict/s3fifo] implements S3-FIFO, which lets new
//     entries prove themselves in a small queue before they get into the
//     main one, and reads without taking a lock as well.
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
// entry is out of reach until it is evicted or the cache is cleared.
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
package evict

import "iter"

// Cache is the behaviour common to every cache in this module. It is
// implemented by *lru.Cache, *twoq.Cache, *sieve.Cache and *s3fifo.Cache.
//
// All methods are safe for concurrent use.
type Cache[K comparable, V any] interface {
	// Get returns the value stored under key and records the access with
	// the eviction policy.
	Get(key K) (value V, ok bool)

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
	// it, whichever way it left. That goes for the key as well: when an
	// entry is replaced, the cache keeps the key it was just given and
	// returns the one it had.
	Set(key K, value V) (oldKey K, oldValue V, displaced bool)

	// Delete removes key and returns the value it had.
	Delete(key K) (value V, ok bool)

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
