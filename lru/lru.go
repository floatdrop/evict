// Package lru provides a fixed-capacity cache that evicts the least recently
// used entry.
//
// Every access moves an entry to the front of a queue, and when room is needed
// the entry at the back goes. That makes LRU a good fit for workloads in which
// what was used a moment ago is likely to be used again, and a poor one for
// workloads with scans, where a burst of entries that are touched only once
// pushes out everything else. See the twoq and s3fifo packages for policies
// that cope with those.
package lru

import (
	"hash/maphash"
	"iter"
	"slices"
	"sync"

	"github.com/floatdrop/evict/internal/shards"
	"github.com/floatdrop/evict/internal/slab"
)

// Cache is a fixed-capacity cache that evicts the least recently used entry.
// It is safe for concurrent use and does not allocate once it has been
// created, except in [Cache.All]. A Cache must be created with [New]; the zero
// value is of no use.
//
// The entries are spread over shards that are locked independently, and each
// shard evicts on its own. See [WithShards].
type Cache[K comparable, V any] struct {
	seed     maphash.Seed
	mask     uint32 // number of shards minus one
	shards   []shard[K, V]
	capacity int
}

// A shard is an LRU cache of its own, holding a share of the keys.
type shard[K comparable, V any] struct {
	mu sync.Mutex

	// The entries, linked from the most to the least recently used.
	slab slab.Slab[K, V]

	_ shards.Pad
}

// queue is the list head of a shard's entries.
const queue = 0

// Option configures a [Cache].
type Option func(*config)

type config struct {
	shards int
}

// WithShards sets the number of shards, which is rounded up to a power of two
// and limited to the capacity. Zero or less stands for the default.
//
// Goroutines that use the cache at the same time wait for each other only
// when their keys are in the same shard, so more shards mean less waiting. In
// exchange, the order in which entries are evicted is tracked per shard: a
// shard that is full evicts its own least recently used entry, even while
// another shard has room or holds an entry that has been idle for longer.
// With a single shard, the cache holds exactly the most recently used entries.
//
// By default the cache has a single shard if its capacity is less than 512
// and otherwise several per CPU, as long as that leaves each shard with at
// least 256 entries. On the traces that the benchmarks replay, shards of that
// size move the hit ratio by less than a tenth of a percentage point, but for
// one that is a few thousand requests long, on which single runs have strayed
// by up to one and a half points and the median of eleven by more than one at
// one capacity: see the report on shards there, which was made as on a machine
// with eight CPUs.
func WithShards(n int) Option {
	return func(c *config) { c.shards = n }
}

// New returns a cache that holds up to capacity entries. A cache with a
// capacity of zero or less holds nothing.
func New[K comparable, V any](capacity int, opts ...Option) *Cache[K, V] {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}

	capacity = max(capacity, 0)
	n := shards.Count(capacity, cfg.shards, slab.MaxCapacity)

	c := &Cache[K, V]{
		seed:     maphash.MakeSeed(),
		mask:     uint32(n - 1),
		shards:   make([]shard[K, V], n),
		capacity: capacity,
	}
	for i := range c.shards {
		c.shards[i].slab.Init(shards.Capacity(capacity, n, i), 1)
	}
	return c
}

// Each method starts by hashing the key. The upper half of the hash picks the
// shard and the lower half is what the shard indexes the key under, so that
// keys sharing a shard do not share a prefix of their probe sequences.

// Get returns the value stored under key and marks the entry as the most
// recently used.
func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	h := maphash.Comparable(c.seed, key)
	s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)

	s.mu.Lock()
	if idx := s.slab.Find(hash, key); idx != 0 {
		s.slab.MoveToFront(queue, idx)
		value, ok = s.slab.Nodes[idx].Value, true
	}
	s.mu.Unlock()

	return value, ok
}

// Peek returns the value stored under key without marking the entry as used.
func (c *Cache[K, V]) Peek(key K) (value V, ok bool) {
	h := maphash.Comparable(c.seed, key)
	s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)

	s.mu.Lock()
	if idx := s.slab.Find(hash, key); idx != 0 {
		value, ok = s.slab.Nodes[idx].Value, true
	}
	s.mu.Unlock()

	return value, ok
}

// Set stores value under key and marks the entry as the most recently used.
//
// It reports the entry that left the cache as a result, if there is one:
// either the previous value of key, in which case oldKey == key, or the least
// recently used entry of the key's shard, evicted to make room. A cache
// without capacity hands the new entry straight back.
func (c *Cache[K, V]) Set(key K, value V) (oldKey K, oldValue V, displaced bool) {
	h := maphash.Comparable(c.seed, key)
	s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)

	s.mu.Lock()
	oldKey, oldValue, displaced = s.set(hash, key, value)
	s.mu.Unlock()

	return oldKey, oldValue, displaced
}

func (s *shard[K, V]) set(hash uint32, key K, value V) (oldKey K, oldValue V, displaced bool) {
	sl := &s.slab

	if idx := sl.Find(hash, key); idx != 0 {
		n := &sl.Nodes[idx]
		oldKey, oldValue, displaced = n.Key, n.Value, true
		// The new key takes the place of the old one, although they are
		// equal: they may still differ in what they refer to, and the
		// caller is told that the old one has left.
		n.Key, n.Value = key, value
		sl.MoveToFront(queue, idx)
		return oldKey, oldValue, displaced
	}

	if sl.Len() == sl.Cap() {
		if sl.Cap() == 0 {
			return key, value, true
		}
		lru := sl.Nodes[queue].Prev
		n := &sl.Nodes[lru]
		oldKey, oldValue, displaced = n.Key, n.Value, true
		sl.Unlink(lru)
		sl.Remove(lru)
	}

	sl.PushFront(queue, sl.Insert(hash, key, value))
	return oldKey, oldValue, displaced
}

// Delete removes key and returns the value it had.
func (c *Cache[K, V]) Delete(key K) (value V, ok bool) {
	h := maphash.Comparable(c.seed, key)
	s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)

	s.mu.Lock()
	if idx := s.slab.Find(hash, key); idx != 0 {
		value, ok = s.slab.Nodes[idx].Value, true
		s.slab.Unlink(idx)
		s.slab.Remove(idx)
	}
	s.mu.Unlock()

	return value, ok
}

// Len returns the number of entries in the cache.
func (c *Cache[K, V]) Len() int {
	n := 0
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		n += s.slab.Len()
		s.mu.Unlock()
	}
	return n
}

// Cap returns the number of entries the cache can hold.
func (c *Cache[K, V]) Cap() int { return c.capacity }

// Clear removes every entry.
func (c *Cache[K, V]) Clear() {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		s.slab.Reset()
		s.mu.Unlock()
	}
}

// All iterates over the entries without marking them as used. Within a shard
// they come from the most to the least recently used, so a cache with a
// single shard yields them in exactly that order.
//
// The iteration works on a copy of one shard at a time. The cache may be
// used, also by the loop body, while it is in progress; entries that are
// added or removed in the meantime may or may not be seen.
func (c *Cache[K, V]) All() iter.Seq2[K, V] {
	type entry struct {
		key   K
		value V
	}

	return func(yield func(K, V) bool) {
		var entries []entry
		for i := range c.shards {
			s := &c.shards[i]

			s.mu.Lock()
			nodes := s.slab.Nodes
			entries = slices.Grow(entries[:0], s.slab.Len())
			for idx := nodes[queue].Next; idx != queue; idx = nodes[idx].Next {
				entries = append(entries, entry{nodes[idx].Key, nodes[idx].Value})
			}
			s.mu.Unlock()

			for _, e := range entries {
				if !yield(e.key, e.value) {
					return
				}
			}
			clear(entries) // Do not hold on to them while the next shard is at it.
		}
	}
}

// The methods of a generic type are compiled in the package that uses them
// with a particular key and value type, and that is where the calls they make
// get inlined, or not: the compiler has to know the body of a function to
// inline it, and it learns about the functions of the packages that this one
// builds on, such as the locking of a mutex, only if this package has inlined
// them itself. Instantiating the methods here makes sure it has. Without it,
// every operation on a cache would go through several function calls more.
var (
	_ = (*Cache[int, int]).Get
	_ = (*Cache[int, int]).Peek
	_ = (*Cache[int, int]).Set
	_ = (*Cache[int, int]).Delete
	_ = (*Cache[int, int]).Len
	_ = (*Cache[int, int]).Clear
	_ = (*Cache[int, int]).All
)
