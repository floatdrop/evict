// Package twoq provides a fixed-capacity cache that evicts with 2Q.
//
// 2Q does not take a single request as proof that an entry is worth keeping.
// A new entry goes into a queue of recent entries, where it stays for a while
// whether or not it is read, and from which it is evicted in the order of
// arrival. The cache remembers the keys of the entries it evicted that way a
// little longer; such a key is called a ghost. If a ghost is set again, the
// entry has shown that it is requested time and again rather than in a single
// burst, and it goes into a second queue of frequent entries, which is run
// like an LRU cache.
//
// A scan over many entries that are touched once therefore passes through
// the recent queue and leaves the frequent entries alone, where it would
// empty an LRU cache.
//
// Mind what that takes. An entry becomes frequent by being evicted and
// coming back while its key is remembered; reading it while it is recent does
// nothing for it. A cache that has never been full has no frequent entries
// yet, and a scan that is longer than the recent queue and the memory
// together leaves nothing behind that could become one.
//
// This is the "full version" of the algorithm in "2Q: A Low Overhead High
// Performance Buffer Management Replacement Algorithm" by Johnson and Shasha
// (VLDB '94), with the queues A1in, A1out and Am of the paper being the
// recent entries, the ghosts and the frequent entries. The two queues of
// entries share the capacity: while the recent queue is longer than its share
// (see [WithRecentRatio]), it is the one that evicts, and otherwise the
// frequent queue is.
//
// Where the paper leaves a choice, or leaves something out, the cache does
// this: a ghost that is set again gives up its place among the ghosts before
// an entry is evicted for it, so that the key of that entry can take the
// place; if there is no frequent entry to evict, a recent one goes, whatever
// the share of the recent queue; and deleting an entry does not make the
// cache forget a ghost.
package twoq

import (
	"hash/maphash"
	"iter"
	"math"
	"slices"
	"sync"
	"unsafe"

	"github.com/floatdrop/evict/internal/shards"
	"github.com/floatdrop/evict/internal/slab"
)

// Cache is a fixed-capacity cache that evicts with 2Q. It is safe for
// concurrent use and does not allocate once it has been created, except in
// [Cache.All]. A Cache must be created with [New]; the zero value is of no
// use.
//
// The entries are spread over shards that are locked independently, and each
// shard evicts on its own. See [WithShards].
type Cache[K comparable, V any] struct {
	seed     maphash.Seed
	mask     uint32 // number of shards minus one
	shards   []padded[K, V]
	capacity int
}

// A shard is a 2Q cache of its own, holding a share of the keys.
type shard[K comparable, V any] struct {
	mu sync.Mutex

	// The entries of both queues and the keys that the shard remembers,
	// each in a list of their own. Node.Meta says which list a node is in.
	slab slab.Slab[K, V]

	// The number of nodes in each list.
	recents, frequents, ghosts int

	capacity  int // entries in the recent and the frequent queue together
	recentCap int // length of the recent queue above which it is the one to evict
	ghostCap  int // keys to remember
}

// padded is a shard followed by a line or more of padding, so that writers to
// it and readers of the next shard never share a line, and rounded up to whole
// lines, so that every shard starts at the same place in one.
type padded[K comparable, V any] struct {
	shard[K, V]
	_ [shards.Line + (shards.Line-unsafe.Sizeof(shard[K, V]{})%shards.Line)%shards.Line]byte
}

// The lists of a shard. The numbers are the list heads in the slab as well as
// the values of Node.Meta.
const (
	// recent is the queue of entries that have been set once, newest first.
	// The paper calls it A1in.
	recent = 0
	// frequent is the queue of entries that were set again after they had
	// left the recent queue, most recently used first. The paper calls it
	// Am.
	frequent = 1
	// ghost is the list of keys that have left the recent queue, most
	// recently evicted first. Its nodes hold no value. The paper calls it
	// A1out.
	ghost = 2

	lists = 3
)

// Option configures a [Cache].
type Option func(*config)

type config struct {
	shards      int
	recentRatio float64
	ghostRatio  float64
}

// WithShards sets the number of shards, which is rounded up to a power of two
// and limited to the capacity. Zero or less stands for the default.
//
// Goroutines that use the cache at the same time wait for each other only
// when their keys are in the same shard, so more shards mean less waiting. In
// exchange, each shard runs its queues on its own share of the keys and the
// capacity: a shard that is full evicts even while another one has room.
//
// By default the cache has a single shard if its capacity is less than 512
// and otherwise several per CPU, as long as that leaves each shard with at
// least 256 entries. On the traces that the benchmarks replay, shards of that
// size nearly always move the hit ratio by about a tenth of a percentage
// point at the most. In two cases they moved it further, by about half a
// point up and by about a point down, and single runs on a trace of a few
// thousand requests strayed by several points: see the report on shards
// there, which was made as on a machine with eight CPUs.
func WithShards(n int) Option {
	return func(c *config) { c.shards = n }
}

// WithRecentRatio sets the share of the capacity that the queue of recent
// entries may take up before the cache evicts from it rather than from the
// queue of frequent entries. The default is 0.25, as the paper recommends.
//
// A larger share keeps a new entry around for longer, at the expense of the
// entries that have proven themselves: the frequent queue gets what is left.
// With a share of 1 nothing is left, and an entry that becomes frequent is
// the next one to be evicted, which makes the cache worse than one that
// evicts in the order of arrival.
//
// The ratio is brought into the range from 0 to 1 if it is outside, and is 0
// if it is not a number. The share of a shard is rounded to whole entries.
func WithRecentRatio(ratio float64) Option {
	return func(c *config) { c.recentRatio = clamp(ratio) }
}

// WithGhostRatio sets the number of ghosts, the keys that the cache remembers
// after it has evicted their entries from the queue of recent entries, as a
// share of the capacity. The default is 0.5, as the paper recommends.
//
// A ghost that is set again goes into the queue of frequent entries. The more
// ghosts, the further apart two requests may be and still count as frequent,
// and the longer a scan the cache gets over. With a ratio of 0 nothing ever
// becomes frequent, and the cache evicts in the order of arrival.
//
// A ghost takes up as much room as an entry does, less whatever its value
// referred to.
//
// The ratio is brought into the range from 0 to 1 if it is outside, and is 0
// if it is not a number. The number for a shard is rounded to whole keys.
func WithGhostRatio(ratio float64) Option {
	return func(c *config) { c.ghostRatio = clamp(ratio) }
}

// clamp returns the ratio that is closest to x. That is 0 if x is not a
// number.
func clamp(x float64) float64 {
	if math.IsNaN(x) {
		return 0
	}
	return min(max(x, 0), 1)
}

// New returns a cache that holds up to capacity entries. A cache with a
// capacity of zero or less holds nothing.
func New[K comparable, V any](capacity int, opts ...Option) *Cache[K, V] {
	cfg := config{recentRatio: 0.25, ghostRatio: 0.5}
	for _, opt := range opts {
		opt(&cfg)
	}

	capacity = max(capacity, 0)
	// A shard needs nodes for its entries and for the keys it remembers,
	// which are at most as many again.
	n := shards.Count(capacity, cfg.shards, slab.MaxCapacity/2)

	c := &Cache[K, V]{
		seed:     maphash.MakeSeed(),
		mask:     uint32(n - 1),
		shards:   make([]padded[K, V], n),
		capacity: capacity,
	}
	for i := range c.shards {
		s := &c.shards[i]
		s.capacity = shards.Capacity(capacity, n, i)
		s.recentCap = int(math.Round(cfg.recentRatio * float64(s.capacity)))
		s.ghostCap = int(math.Round(cfg.ghostRatio * float64(s.capacity)))
		s.slab.Init(s.capacity+s.ghostCap, lists)
	}
	return c
}

// Each method starts by hashing the key. The upper half of the hash picks the
// shard and the lower half is what the shard indexes the key under, so that
// keys sharing a shard do not share a prefix of their probe sequences.

// Get returns the value stored under key. If the entry is in the queue of
// frequent entries, it becomes the most recently used one there. An entry in
// the queue of recent entries gains nothing from being read.
func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	h := maphash.Comparable(c.seed, key)
	s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)

	s.mu.Lock()
	if idx := s.slab.Find(hash, key); idx != 0 {
		// A key that is merely remembered has no value to return.
		if n := &s.slab.Nodes[idx]; n.Meta != ghost {
			if n.Meta == frequent {
				s.slab.MoveToFront(frequent, idx)
			}
			value, ok = n.Value, true
		}
	}
	s.mu.Unlock()

	return value, ok
}

// Peek returns the value stored under key and changes nothing.
func (c *Cache[K, V]) Peek(key K) (value V, ok bool) {
	h := maphash.Comparable(c.seed, key)
	s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)

	s.mu.Lock()
	if idx := s.slab.Find(hash, key); idx != 0 {
		if n := &s.slab.Nodes[idx]; n.Meta != ghost {
			value, ok = n.Value, true
		}
	}
	s.mu.Unlock()

	return value, ok
}

// Set stores value under key. A key that the cache does not know goes into
// the queue of recent entries. A key that it remembers having evicted from
// there goes into the queue of frequent entries. An entry that is cached
// already gets the new value and is otherwise treated as if it had been read.
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

	idx := sl.Find(hash, key)
	if idx == 0 {
		if s.capacity == 0 {
			return key, value, true
		}
		oldKey, oldValue, displaced = s.makeRoom()
		sl.PushFront(recent, sl.Insert(hash, key, value)) // Meta is zero, which is recent.
		s.recents++
		return oldKey, oldValue, displaced
	}

	n := &sl.Nodes[idx]
	if n.Meta != ghost {
		oldKey, oldValue = n.Key, n.Value
		// The new key takes the place of the old one, although they are
		// equal: they may still differ in what they refer to, and the
		// caller is told that the old one has left.
		n.Key, n.Value = key, value
		if n.Meta == frequent {
			sl.MoveToFront(frequent, idx)
		}
		return oldKey, oldValue, true
	}

	// The key was evicted from the recent queue not long ago, and here it
	// is again. Its node becomes an entry of the frequent queue. It leaves
	// the memory before room is made, so that the key of the entry evicted
	// for it can take its place there instead of pushing out another one.
	sl.Unlink(idx)
	s.ghosts--
	oldKey, oldValue, displaced = s.makeRoom()
	n.Key, n.Value = key, value
	n.Meta = frequent
	sl.PushFront(frequent, idx)
	s.frequents++
	return oldKey, oldValue, displaced
}

// makeRoom evicts an entry if the shard is full, and returns it.
func (s *shard[K, V]) makeRoom() (oldKey K, oldValue V, evicted bool) {
	if s.recents+s.frequents < s.capacity {
		return oldKey, oldValue, false
	}
	sl := &s.slab

	// The recent queue gives way while it is over its share, and when there
	// is nothing else to evict.
	if s.recents <= s.recentCap && s.frequents > 0 {
		idx := sl.Nodes[frequent].Prev
		n := &sl.Nodes[idx]
		oldKey, oldValue = n.Key, n.Value
		sl.Unlink(idx)
		sl.Remove(idx)
		s.frequents--
		return oldKey, oldValue, true
	}

	idx := sl.Nodes[recent].Prev
	n := &sl.Nodes[idx]
	oldKey, oldValue = n.Key, n.Value
	sl.Unlink(idx)
	s.recents--
	if s.ghostCap == 0 {
		sl.Remove(idx)
		return oldKey, oldValue, true
	}

	// The entry is gone, but its key is remembered for a while, in the same
	// node. The oldest memory makes way for it.
	var zero V
	n.Value = zero
	n.Meta = ghost
	sl.PushFront(ghost, idx)
	s.ghosts++
	if s.ghosts > s.ghostCap {
		oldest := sl.Nodes[ghost].Prev
		sl.Unlink(oldest)
		sl.Remove(oldest)
		s.ghosts--
	}
	return oldKey, oldValue, true
}

// Delete removes key and returns the value it had. If the cache merely
// remembers the key, it goes on doing so.
func (c *Cache[K, V]) Delete(key K) (value V, ok bool) {
	h := maphash.Comparable(c.seed, key)
	s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)

	s.mu.Lock()
	if idx := s.slab.Find(hash, key); idx != 0 {
		if n := &s.slab.Nodes[idx]; n.Meta != ghost {
			if n.Meta == frequent {
				s.frequents--
			} else {
				s.recents--
			}
			value, ok = n.Value, true
			s.slab.Unlink(idx)
			s.slab.Remove(idx)
		}
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
		n += s.recents + s.frequents
		s.mu.Unlock()
	}
	return n
}

// Cap returns the number of entries the cache can hold.
func (c *Cache[K, V]) Cap() int { return c.capacity }

// Clear removes every entry and forgets every key.
func (c *Cache[K, V]) Clear() {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		s.slab.Reset()
		s.recents, s.frequents, s.ghosts = 0, 0, 0
		s.mu.Unlock()
	}
}

// All iterates over the entries without counting them as used. A shard yields
// its frequent entries first, from the most to the least recently used, and
// then its recent entries, from the newest to the oldest. A cache with a
// single shard yields its entries in exactly that order.
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
			entries = slices.Grow(entries[:0], s.recents+s.frequents)
			for _, head := range [...]uint32{frequent, recent} {
				for idx := nodes[head].Next; idx != head; idx = nodes[idx].Next {
					entries = append(entries, entry{nodes[idx].Key, nodes[idx].Value})
				}
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

// The methods of a generic type are compiled in the package that uses them,
// and the calls they make are inlined there only if this package has inlined
// them itself; see the note in package lru. Instantiating the methods here
// makes sure it has.
var (
	_ = (*Cache[int, int]).Get
	_ = (*Cache[int, int]).Peek
	_ = (*Cache[int, int]).Set
	_ = (*Cache[int, int]).Delete
	_ = (*Cache[int, int]).Len
	_ = (*Cache[int, int]).Clear
	_ = (*Cache[int, int]).All
)
