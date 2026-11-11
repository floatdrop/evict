// Package s3fifo provides a fixed-capacity cache that evicts with S3-FIFO and
// whose reads take no lock.
//
// S3-FIFO runs three queues, each in the order of arrival. A new entry goes
// into a small queue that takes up a tenth of the cache. Most entries are
// never asked for again, and those leave from there before they can crowd out
// anything else. An entry that was read more than once while it was in the
// small queue moves on to the main queue instead, which has the rest of the
// room. The third queue holds no entries, only the ghosts of those that left
// from the small queue: if the key of one comes back in time, it goes
// straight into the main queue.
//
// The main queue gives an entry that has been read since it was last looked
// at another round rather than evicting it, up to three times in a row for an
// entry that is read a lot.
//
// Reading an entry does no more than count up to three on it, and nothing at
// all once it is there. So any number of goroutines can read at the same
// time without getting in each other's way, even when they read the same
// entry.
//
// S3-FIFO is described in "FIFO queues are all you need for cache eviction"
// by Yang, Zhang, Qiu, Yue and Rashmi (SOSP '23). Where the paper leaves
// something open, the cache does as the implementation of its authors does,
// the one in libCacheSim: an entry that moves to the main queue starts
// counting from zero, a ghost that comes back is no longer a ghost, and a
// cache that has not been full yet fills its main queue directly once the
// small queue has its share.
//
// In one thing the cache does less than that implementation, to little
// effect: the place of a ghost that came back is not given to another one
// until its turn comes, so the cache remembers a few keys fewer than it
// has room for.
//
// The small queue needs room to do its work. A shard of fewer than ten
// entries has no share for it at all, and then the cache can do a good deal
// worse than an LRU cache of the same size. For caches as small as that,
// the lru package is the better choice; see also [WithShards].
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
// Of a ghost the cache keeps a hash of the key and nothing else: sixteen
// bytes or so, for nine tenths as many ghosts as there are entries. The key
// itself is let go like the rest of the entry.
//
// A cache also has a table in which its readers say what they are looking
// at. It takes between 2 and 4 KiB for every CPU that Go may use when the
// cache is created, whatever the capacity. For a great many small caches,
// the lru package is the better choice.
package s3fifo

import (
	"hash/maphash"
	"iter"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"

	"github.com/floatdrop/evict/internal/cslab"
	"github.com/floatdrop/evict/internal/ghost"
	"github.com/floatdrop/evict/internal/shards"
)

// Cache is a fixed-capacity cache that evicts with S3-FIFO. It is safe for
// concurrent use and does not allocate once it has been created, except in
// [Cache.All]. A Cache must be created with [New]; the zero value is of no
// use.
//
// [Cache.Get] and [Cache.Peek] take no lock, with rare exceptions: when a key
// is not found while an entry is on its way out of the same shard, and when
// more goroutines read at the same moment than the cache was made for, the
// read takes the lock of the shard to be sure. Writes are spread over shards
// that are locked independently, and each shard evicts on its own. See
// [WithShards].
type Cache[K comparable, V any] struct {
	seed     maphash.Seed
	mask     uint32 // number of shards minus one
	shards   []shard[K, V]
	readers  *cslab.Readers
	capacity int
}

// A shard is an S3-FIFO cache of its own, holding a share of the keys.
type shard[K comparable, V any] struct {
	// The entries of both queues, each in a list of its own with the newest
	// entry first. Node.Meta counts the reads of an entry, up to
	// maxFrequency. Link.Tag says in its lowest bit which list a node is
	// in, and holds bits of the hash of the key in the others; see ghostOf.
	slab cslab.Slab[K, V]

	// mu serializes the writers. Readers do not take it, unless they find
	// the index in motion.
	mu sync.Mutex

	// The ghosts: the keys that have left the small queue, by their hashes.
	ghosts ghost.Set

	// The number of entries in each queue.
	smalls, mains int

	capacity int // entries in both queues together
	mainCap  int // entries that the main queue has room for

	// evicted says that the shard has been full since it was made or last
	// cleared. Until then, new entries are not all sent through the small
	// queue; see set.
	evicted bool

	_ shards.Pad
}

// The queues of a shard. The numbers are the list heads in the slab as well
// as the lowest bit of Link.Tag.
const (
	small = 0
	main  = 1

	queues = 2
)

// A ghost is a key by 63 bits of its hash: the half that its entry is indexed
// under, and all but one bit of the other half. The slab keeps the first for
// every entry and the tag of the entry has room for the second, so that the
// key need not be hashed again when the entry becomes a ghost.

// tagOf returns the tag of an entry with the given hash in the given queue.
func tagOf(h uint64, queue uint32) uint32 { return uint32(h>>33)<<1 | queue }

// ghostOf returns the ghost of an entry from the hash it is indexed under and
// its tag.
func ghostOf(hash, tag uint32) uint64 { return uint64(tag>>1)<<32 | uint64(hash) }

// maxFrequency is as far as the reads of an entry are counted.
const maxFrequency = 3

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
// exchange, each shard runs its queues on its own share of the keys and the
// capacity: a shard that is full evicts even while another one has room.
//
// By default the cache has a single shard if its capacity is less than 512
// and otherwise several per CPU, as long as that leaves each shard with at
// least 256 entries. On the traces that the benchmarks replay, shards of that
// size move the hit ratio by about a tenth of a percentage point at the most,
// and a single run on a trace of a few thousand requests by a few tenths of
// a point: see the report on shards there, which was made as on a machine
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
	n := shards.Count(capacity, cfg.shards, cslab.MaxCapacity)

	c := &Cache[K, V]{
		seed:     maphash.MakeSeed(),
		mask:     uint32(n - 1),
		shards:   make([]shard[K, V], n),
		readers:  cslab.NewReaders(),
		capacity: capacity,
	}
	for i := range c.shards {
		s := &c.shards[i]
		s.capacity = shards.Capacity(capacity, n, i)
		// A tenth of the room is for the small queue. There are as many
		// ghosts as the main queue has entries.
		s.mainCap = s.capacity - s.capacity/10
		s.slab.Init(s.capacity, queues, c.readers)
		// Nine tenths as many ghosts as entries, rounded down, which is
		// not always what the main queue has room for.
		s.ghosts.Init(s.capacity * 9 / 10)
	}
	return c
}

// Each method starts by hashing the key. The upper half of the hash picks the
// shard and the lower half is what the shard indexes the key under, so that
// keys sharing a shard do not share a prefix of their probe sequences.

// Get returns the value stored under key and counts the read, which is what
// keeps the entry in the cache when its turn to leave comes.
func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	h := maphash.Comparable(c.seed, key)
	s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)

	// The lookup counts the read. Readers do not wait for each other, so
	// two of them may count as one. That is close enough. An entry that is
	// read all the time has reached the limit, and is not written to
	// again: its cache line stays shared between the CPUs.
	value, ok, certain := s.slab.Lookup(hash, key, maxFrequency)
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
		n.Meta.Store(min(n.Meta.Load()+1, maxFrequency))
		value, ok = n.Value, true
	}
	s.mu.Unlock()

	return value, ok
}

// Peek returns the value stored under key without counting the read.
func (c *Cache[K, V]) Peek(key K) (value V, ok bool) {
	h := maphash.Comparable(c.seed, key)
	s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)

	value, ok, certain := s.slab.Lookup(hash, key, 0)
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

// Set stores value under key. A key that the cache does not know goes into
// the small queue, and one whose ghost it still has into the main queue. For
// an entry that is cached already, the write counts as a read.
//
// A cache that has not been full yet, or not since it was cleared, puts no
// more new entries into the small queue than that has room for by its share;
// the others go into the main queue.
//
// Set reports the entry that left the cache as a result, if there is one:
// either the previous entry for key, in which case oldKey == key, or the
// entry that the key's shard evicted to make room. A cache without capacity
// hands the new entry straight back.
func (c *Cache[K, V]) Set(key K, value V) (oldKey K, oldValue V, displaced bool) {
	h := maphash.Comparable(c.seed, key)
	s := &c.shards[uint32(h>>32)&c.mask]

	s.mu.Lock()
	oldKey, oldValue, displaced = s.set(h, key, value)
	s.mu.Unlock()

	return oldKey, oldValue, displaced
}

func (s *shard[K, V]) set(h uint64, key K, value V) (oldKey K, oldValue V, displaced bool) {
	sl := &s.slab
	hash := uint32(h)

	if idx := sl.Find(hash, key); idx != 0 {
		n := &sl.Nodes[idx]
		oldKey, oldValue = n.Key, n.Value

		// Readers may be looking at the entry, so it gets its new value
		// in another node, which takes over its place in its queue.
		n = &sl.Nodes[sl.Replace(idx, key, value)]
		n.Meta.Store(min(n.Meta.Load()+1, maxFrequency))
		return oldKey, oldValue, true
	}

	if s.capacity == 0 {
		return key, value, true
	}
	// Whether the key has a ghost is settled first: the entry that is about
	// to be evicted may leave a ghost of its own, and that must not push
	// out the one that is being asked for.
	remembered := s.ghosts.Remove(ghostOf(hash, tagOf(h, small)))
	if s.smalls+s.mains == s.capacity {
		oldKey, oldValue = s.evict()
		displaced = true
	}

	// A shard that has yet to fill up for the first time has nothing to
	// evict, and so nothing to learn from the small queue: once that has
	// its share, the entries that follow go straight to where the room is.
	// Otherwise the entries that happen to come first would have to pass
	// through the small queue all at once when the shard does fill up.
	warming := !s.evicted && s.smalls >= s.capacity-s.mainCap

	idx := sl.Insert(hash, key, value)
	if remembered || warming {
		sl.Links[idx].Tag = tagOf(h, main)
		sl.PushFront(main, idx)
		s.mains++
	} else {
		sl.Links[idx].Tag = tagOf(h, small)
		sl.PushFront(small, idx)
		s.smalls++
	}
	return oldKey, oldValue, displaced
}

// evict takes one entry out of the shard, which must not be empty, and
// returns it.
func (s *shard[K, V]) evict() (key K, value V) {
	sl := &s.slab
	nodes, links := sl.Nodes, sl.Links
	s.evicted = true

	// The small queue is the one to evict from if it has at least its
	// share, which is the case exactly when the main queue has no more
	// than its own. Its oldest entries move to the main queue instead if
	// they have been read more than once, however full that gets: it is
	// the main queue's turn the next time. If all of them move, it is its
	// turn now.
	for fromSmall := s.mains <= s.mainCap; fromSmall && s.smalls > 0; {
		idx := links[small].Prev
		n := &nodes[idx]
		sl.Unlink(idx)
		s.smalls--

		if n.Meta.Load() > 1 {
			n.Meta.Store(0)
			links[idx].Tag |= main
			sl.PushFront(main, idx)
			s.mains++
			continue
		}

		key, value = n.Key, n.Value
		s.ghosts.Add(ghostOf(sl.Hash(idx), links[idx].Tag))
		sl.Remove(idx)
		return key, value
	}

	// The main queue gives its oldest entry another round for every read it
	// has counted, and evicts the first one that has none left. Left alone,
	// that takes four rounds at the most. But readers keep counting while
	// it goes on, so after that the oldest entry goes, whatever it says.
	for range maxFrequency * s.mains {
		idx := links[main].Prev
		n := &nodes[idx]
		f := n.Meta.Load()
		if f == 0 {
			break
		}
		n.Meta.Store(f - 1)
		sl.Unlink(idx)
		sl.PushFront(main, idx)
	}

	idx := links[main].Prev
	n := &nodes[idx]
	key, value = n.Key, n.Value
	sl.Unlink(idx)
	sl.Remove(idx)
	s.mains--
	return key, value
}

// Delete removes key and returns the value it had. If the cache has no entry
// for key but a ghost, the ghost goes: the cache forgets the key altogether.
func (c *Cache[K, V]) Delete(key K) (value V, ok bool) {
	h := maphash.Comparable(c.seed, key)
	s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)

	s.mu.Lock()
	if idx := s.slab.Find(hash, key); idx != 0 {
		value, ok = s.slab.Nodes[idx].Value, true
		if s.slab.Links[idx].Tag&1 == main {
			s.mains--
		} else {
			s.smalls--
		}
		s.slab.Unlink(idx)
		s.slab.Remove(idx)
	} else {
		s.ghosts.Remove(ghostOf(hash, tagOf(h, small)))
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

// Clear removes every entry and every ghost. When it returns, the cache no
// longer refers to any of the keys and values it held.
func (c *Cache[K, V]) Clear() {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		mark := s.slab.Reset()
		s.ghosts.Reset()
		s.smalls, s.mains, s.evicted = 0, 0, false
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

// All iterates over the entries without counting them as read. A shard yields
// the entries of its main queue first and then those of its small queue, each
// from the newest to the oldest. A cache with a single shard yields its
// entries in exactly that order.
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
			entries = slices.Grow(entries[:0], s.smalls+s.mains)
			for _, head := range [...]uint32{main, small} {
				for idx := links[head].Next; idx != head; idx = links[idx].Next {
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
