// Package sieve provides a fixed-capacity cache that evicts with SIEVE and
// whose reads take no lock.
//
// SIEVE keeps its entries in the order in which they were added and never
// reorders them. Reading an entry only sets a mark on it. To make room, a
// hand sweeps from the oldest entry towards the newest: an entry with a mark
// loses the mark and stays, and the first entry without one is evicted. The
// hand continues from there the next time, so entries that it has passed are
// not looked at again until it comes around.
//
// Entries that are added and never asked for again therefore leave quickly,
// while popular entries stay for as long as they keep being read. And since
// reading an entry changes nothing but its mark, and nothing at all if the
// mark is there already, any number of goroutines can read at the same time
// without getting in each other's way, even when they read the same entry.
//
// SIEVE is described in "SIEVE is Simpler than LRU: an Efficient Turn-Key
// Eviction Algorithm for Web Caches" by Zhang, Yang, Yue, Vigfusson and
// Rashmi (NSDI '24).
//
// # When not to use it
//
// SIEVE was made for the workloads of web caches, in which a few entries are
// asked for again and again and most of the others never. There it holds its
// own against far more elaborate policies. But it can fail outright.
//
// The hand moves on only when it evicts an entry or passes one that was read.
// Once it has caught up with the newest entries, a new entry is evicted to
// make room for one of the next few unless it was read again by then, and
// everything that is older stays where it is, whether anyone asks for it or
// not. A workload in which an entry is read a second time a little later, and
// then never again, keeps the hand there for good. One of the traces that
// the benchmarks replay, the one called Mixed, is like that for long
// stretches: a SIEVE cache in one piece answers less than one request in a
// hundred of it, where an LRU cache of the same size answers between one in
// nine and three in four.
//
// If you do not know which kind of workload yours is, the s3fifo package is
// the safer choice. It reads without a lock as well.
//
// # Memory
//
// So that readers need no lock, an entry that leaves the cache is not taken
// apart right away: a reader may be looking at it. Its key and value stay
// referenced until the cache has made sure that nobody is, which it does for
// many entries at a time. Until then they are not garbage. A shard may hold
// on to as many entries that have left it as an eighth of its capacity,
// though at least one and never more than 1024, and it does so for as long
// as no more of them leave. [Cache.Clear] is the exception; it lets go of
// everything before it returns, and waits for a reader if it has to.
//
// A reader that is held up in the middle of reading an entry keeps that one
// entry alive for the time being, and is in nobody's way but that of Clear.
// It is not held up there easily: while a reader looks at an entry, it does
// nothing that would give Go the occasion to run another goroutine in its
// place.
//
// A cache also has a table in which its readers say what they are looking
// at. It takes between 2 and 4 KiB for every CPU that Go may use when the
// cache is created, whatever the capacity. For a great many small caches,
// the lru package is the better choice.
package sieve

import (
	"hash/maphash"
	"iter"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"unsafe"

	"github.com/floatdrop/evict/internal/cslab"
	"github.com/floatdrop/evict/internal/shards"
)

// Cache is a fixed-capacity cache that evicts with SIEVE. It is safe for
// concurrent use and does not allocate once it has been created, except in
// [Cache.All]. A Cache must be created with [New]; the zero value is of no
// use.
//
// [Cache.Get], [Cache.GetMany] and [Cache.Peek] take no lock, with rare
// exceptions: when a key is not found while an entry is on its way out of the
// same shard, and when more goroutines read at the same moment than the cache
// was made for, the read takes the lock of the shard to be sure. Writes are
// spread over shards that are locked independently, and each shard evicts on
// its own. See [WithShards].
type Cache[K comparable, V any] struct {
	seed     maphash.Seed
	mask     uint32 // number of shards minus one
	shards   []padded[K, V]
	readers  *cslab.Readers
	capacity int
}

// A shard is a SIEVE cache of its own, holding a share of the keys.
type shard[K comparable, V any] struct {
	// The entries, linked from the newest to the oldest. An entry is marked
	// by a non-zero Node.Meta.
	slab cslab.Slab[K, V]

	// mu serializes the writers. Readers do not take it, unless they find
	// the index in motion.
	mu sync.Mutex

	// hand is the entry that is next in line for eviction, or zero if that
	// is the oldest entry.
	hand uint32
}

// padded is a shard followed by a line or more of padding, so that writers to
// it and readers of the next shard never share a line, and rounded up to whole
// lines, so that every shard starts at the same place in one.
type padded[K comparable, V any] struct {
	shard[K, V]
	_ [shards.Line + (shards.Line-unsafe.Sizeof(shard[K, V]{})%shards.Line)%shards.Line]byte
}

// queue is the list head of a shard's entries.
const queue = 0

// visited is the value of Node.Meta for an entry that has been used since the
// hand last passed it.
const visited = 1

// Option configures a [Cache].
type Option func(*config)

type config struct {
	shards int
}

// WithShards sets the number of shards, which is rounded up to a power of two
// and limited to the capacity. Zero or less stands for the default.
//
// Reads do not depend on it. A write waits for the other writes to the same
// shard, so more shards let more goroutines write at the same time. In
// exchange, each shard evicts from its own entries: a shard that is full
// evicts even while another one has room.
//
// By default the cache has a single shard if its capacity is less than 512
// and otherwise several per CPU, as long as that leaves each shard with at
// least 256 entries. In two cases out of three that the benchmarks replay,
// shards of that size move the hit ratio by a tenth of a percentage point at
// the most. Not in all, though: SIEVE is sensitive to the order in which
// entries arrive, and on two of the traces that it does badly on in one
// piece, shards raise its hit ratio: by a few points on one, and on the
// other by anything from two points to forty, depending on the capacity and
// different on every run. They can lower it as well: by a few tenths of a
// point in some cases, and by several points in single runs of a trace of a
// few thousand requests. See the report on shards there, which was made as
// on a machine with eight CPUs.
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
	n := shards.Count(capacity, cfg.shards, cslab.MaxCapacity)

	c := &Cache[K, V]{
		seed:     maphash.MakeSeed(),
		mask:     uint32(n - 1),
		shards:   make([]padded[K, V], n),
		readers:  cslab.NewReaders(),
		capacity: capacity,
	}
	for i := range c.shards {
		c.shards[i].slab.Init(shards.Capacity(capacity, n, i), 1, c.readers)
	}
	return c
}

// Each method starts by hashing the key. The upper half of the hash picks the
// shard and the lower half is what the shard indexes the key under, so that
// keys sharing a shard do not share a prefix of their probe sequences.

// Get returns the value stored under key and marks the entry as used, which
// spares it the next time the hand passes.
func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	h := maphash.Comparable(c.seed, key)
	s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)

	// The lookup marks the entry, unless it is marked already: popular
	// entries are, and not writing to them again keeps their cache line
	// shared between the CPUs that read it.
	value, ok, certain := s.slab.Lookup(hash, key, visited, false)
	if s.slab.Overdue() {
		s.help()
	}
	if certain {
		return value, ok
	}

	// The index was changing under the lookup, or too many goroutines are
	// reading at once. Do it the plain way.
	s.mu.Lock()
	if idx := s.slab.Find(hash, key); idx != 0 {
		n := &s.slab.Nodes[idx]
		n.SetMeta(visited)
		value, ok = n.Value, true
	}
	s.mu.Unlock()

	return value, ok
}

// GetMany does what Get does for each of keys, one after the other, and
// stores the value it finds for keys[i] in values[i] and whether it found one
// in found[i]. It panics if values or found is shorter than keys. values and
// found may each be keys itself, but must not otherwise overlap it. A key that
// cannot be hashed makes GetMany panic, as it makes Get panic, but possibly
// before the keys just ahead of it have been looked up.
//
// For a cache that is too large for the CPU's caches, GetMany is faster than
// calling Get for each key. It takes up to 32 keys at a time, and before it
// looks any of them up, it loads the slot of the index at which each lookup
// starts and the node that the slot points to, so that the CPU can fetch them
// from memory in parallel. When fewer than four keys are left, it looks them
// up with Get, one by one.
//
// In BenchmarkGetMany, with a million entries and keys picked at random, a
// key took 62% less time than with Get with 32 keys per call on one CPU, and
// 53% less on eight; with 8 keys per call, 49% and 27% less. With 16,384
// entries, which fit in the CPU's caches, it was within 2% of Get. For a
// single key, call Get: through GetMany it took 42% and 36% longer. These are
// medians of three runs on an Apple M3 Max, and vary from run to run.
func (c *Cache[K, V]) GetMany(keys []K, values []V, found []bool) {
	// values[:len(keys)] would only be checked against the capacity, and
	// a slice that is too short would be written past its end.
	if len(values) < len(keys) || len(found) < len(keys) {
		panic("sieve: GetMany: values and found must be at least as long as keys")
	}

	var hashes, words [cslab.Batch]uint64
	for len(keys) >= cslab.MinBatch {
		n := min(len(keys), cslab.Batch)

		for i := range n {
			hashes[i] = maphash.Comparable(c.seed, keys[i])
		}
		for i := range n {
			h := hashes[i]
			words[i] = c.shards[uint32(h>>32)&c.mask].slab.Touch(uint32(h))
		}
		for i := range n {
			h := hashes[i]
			c.shards[uint32(h>>32)&c.mask].slab.TouchNode(uint32(h), words[i])
		}

		// From here on, as in Get.
		for i := range n {
			h := hashes[i]
			s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)
			value, ok, certain := s.slab.Lookup(hash, keys[i], visited, false)
			if s.slab.Overdue() {
				s.help()
			}
			if !certain {
				s.mu.Lock()
				if idx := s.slab.Find(hash, keys[i]); idx != 0 {
					node := &s.slab.Nodes[idx]
					node.SetMeta(visited)
					value, ok = node.Value, true
				}
				s.mu.Unlock()
			}
			values[i], found[i] = value, ok
		}

		keys, values, found = keys[n:], values[n:], found[n:]
	}

	// Too few keys are left for loading ahead to pay.
	for i, key := range keys {
		values[i], found[i] = c.Get(key)
	}
}

// Peek returns the value stored under key without marking the entry as used.
func (c *Cache[K, V]) Peek(key K) (value V, ok bool) {
	h := maphash.Comparable(c.seed, key)
	s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)

	value, ok, certain := s.slab.Lookup(hash, key, 0, false)
	if s.slab.Overdue() {
		s.help()
	}
	if certain {
		return value, ok
	}

	s.mu.Lock()
	if idx := s.slab.Find(hash, key); idx != 0 {
		value, ok = s.slab.Nodes[idx].Value, true
	}
	s.mu.Unlock()

	return value, ok
}

// help is what a reader does for a shard that holds on to entries which are
// gone, because another reader was looking at them when the shard last tried
// to let go of them. A shard tries again whenever it is written to, but if
// nobody writes, it is up to the readers. Looking for the other readers takes
// a while, so only one read in about a thousand does it; that is soon enough.
//
//go:noinline
func (s *shard[K, V]) help() {
	if rand.Uint32()&1023 != 0 || !s.mu.TryLock() {
		return
	}
	s.slab.Reclaim()
	s.mu.Unlock()
}

// Set stores value under key. An entry that exists already is marked as used;
// a new one starts out unmarked.
//
// Set reports the entry that left the cache as a result, if there is one:
// either the previous value of key, in which case oldKey == key, or the entry
// that the key's shard evicted to make room. A cache without capacity hands
// the new entry straight back.
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
		oldKey, oldValue = n.Key, n.Value

		// Readers may be looking at the entry, so it gets its new value
		// in another node, which takes over its place in the queue.
		to := sl.Replace(idx, key, value)
		sl.Nodes[to].SetMeta(visited)
		if s.hand == idx {
			s.hand = to
		}
		return oldKey, oldValue, true
	}

	if sl.Len() == sl.Cap() {
		if sl.Cap() == 0 {
			return key, value, true
		}
		victim := s.victim()
		n := &sl.Nodes[victim]
		oldKey, oldValue, displaced = n.Key, n.Value, true
		s.remove(victim)
	}

	sl.PushFront(queue, sl.Insert(hash, key, value))
	return oldKey, oldValue, displaced
}

// victim moves the hand to the first entry that has not been used since the
// hand last passed it, taking the mark off the entries on the way, and returns
// that entry. The shard must not be empty.
func (s *shard[K, V]) victim() uint32 {
	nodes, links := s.slab.Nodes, s.slab.Links

	// The hand moves from the oldest entry towards the newest and starts
	// over when it runs off the end, which is the list head.
	idx := s.hand
	if idx == queue {
		idx = links[queue].Prev
	}
	// Left alone, the hand is back at an entry that it has cleared after one
	// round. But readers keep marking entries while it moves, so it settles
	// for wherever it is after two.
	for range 2 * s.slab.Len() {
		if nodes[idx].Meta() != visited {
			break
		}
		nodes[idx].SetMeta(0)
		idx = links[idx].Prev
		if idx == queue {
			idx = links[queue].Prev
		}
	}

	s.hand = idx
	return idx
}

// remove takes the entry in node idx out of the shard.
func (s *shard[K, V]) remove(idx uint32) {
	// The hand carries on with the next newer entry.
	if s.hand == idx {
		s.hand = s.slab.Links[idx].Prev
	}
	s.slab.Unlink(idx)
	s.slab.Remove(idx)
}

// Delete removes key and returns the value it had.
func (c *Cache[K, V]) Delete(key K) (value V, ok bool) {
	h := maphash.Comparable(c.seed, key)
	s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)

	s.mu.Lock()
	if idx := s.slab.Find(hash, key); idx != 0 {
		value, ok = s.slab.Nodes[idx].Value, true
		s.remove(idx)
	}
	s.mu.Unlock()

	return value, ok
}

// Len returns the number of entries in the cache.
func (c *Cache[K, V]) Len() int {
	n := 0
	for i := range c.shards {
		n += c.shards[i].slab.Len()
	}
	return n
}

// Cap returns the number of entries the cache can hold.
func (c *Cache[K, V]) Cap() int { return c.capacity }

// Clear removes every entry. When it returns, the cache no longer refers to
// any of the keys and values it held.
func (c *Cache[K, V]) Clear() {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		mark := s.slab.Reset()
		s.hand = 0
		// A reader may still be looking at one of the entries. It is done
		// in no time, unless it is held up; either way the wait for it is
		// not spent with the shard locked.
		for !s.slab.Released(mark) {
			s.mu.Unlock()
			runtime.Gosched()
			s.mu.Lock()
		}
		s.mu.Unlock()
	}
}

// All iterates over the entries without marking them as used. Within a shard
// they come from the newest to the oldest, so a cache with a single shard
// yields them in exactly that order.
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
			nodes, links := s.slab.Nodes, s.slab.Links
			entries = slices.Grow(entries[:0], s.slab.Len())
			for idx := links[queue].Next; idx != queue; idx = links[idx].Next {
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
	_ = (*Cache[int, int]).GetMany
	_ = (*Cache[int, int]).Set
	_ = (*Cache[int, int]).Delete
	_ = (*Cache[int, int]).Len
	_ = (*Cache[int, int]).Clear
	_ = (*Cache[int, int]).All
)
