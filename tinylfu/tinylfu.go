// Package tinylfu provides a fixed-capacity cache that admits with TinyLFU and
// whose reads take no lock.
//
// The cache runs two queues, each in the order of arrival, as S3-FIFO does. A
// new entry goes into a small queue. When it has got to the end of that, it
// is up against the entry that the main queue would evict next, and the
// cache keeps the one that has been asked for more often of late; the other
// one goes. The main queue gives an entry that has been read since it was
// last looked at another round rather than putting it up for eviction, up to
// three times in a row for an entry that is read a lot.
//
// How often a key has been asked for is a matter of record, whether the key
// is in the cache or not: the cache keeps a frequency sketch, a table of small
// counters that keys share and that are halved every so often. So an entry
// that keeps coming back at long intervals gets in, where a queue of ghosts
// would long have forgotten it, and a flood of keys that are asked for once
// does not push out what is known to be in demand. This is the admission
// policy of TinyLFU, and the arrangement of a small queue, or window, in
// front of it is W-TinyLFU.
//
// How much of the room the small queue should have depends on the workload:
// a large one suits keys that are asked for again soon or never, a small one
// leaves more for what has proven itself. The cache finds out by trying. It
// starts with a hundredth, moves the share a step, and goes on in the same
// direction for as long as it answers more requests than before, and back
// when it answers fewer. The shards of a cache share what it finds.
//
// The search needs a slope to climb, and a clear one. Where a somewhat larger
// small queue answers about as many requests as the one the cache has, the
// share that it answers moves more by chance than by the size, and the size
// wanders, at some cost. That is the case when every key comes back a time or
// two after much the same while and then never again, and the while is longer
// than a key lasts in the small queue the cache has found so far: only a far
// larger one would do, and the sketch, which counts how often, has nothing to
// say about how recently. On a workload like that, the cache can answer far
// fewer requests than an LRU cache of the same size, and the lru package is
// the better choice. And when the workload changes from one phase to the
// next, the search starts over with long steps each time.
//
// Reading an entry does no more than count on it, up to three and no further,
// and add one to a tally of the requests answered that the reader keeps in a
// place it writes to anyway, which few other readers use at the same time.
// The sketch learns of the reads when a write next looks at the entry. So any
// number of goroutines can read at the same time without getting in each
// other's way, even when they read the same entry.
//
// TinyLFU and W-TinyLFU are described in "TinyLFU: A Highly Efficient Cache
// Admission Policy" by Einziger, Friedman and Manes (ACM Transactions on
// Storage, 2017), and the search for the size of the window in "Adaptive
// Software Cache Management" by Einziger, Eytan, Friedman and Manes
// (Middleware '18). What is described there keeps its entries in the order in
// which they were last used and counts every read in the sketch at once. This
// cache keeps them in the order of arrival and counts later, which is what
// lets it read without a lock; the queues are those of S3-FIFO ("FIFO queues
// are all you need for cache eviction" by Yang, Zhang, Qiu, Yue and Rashmi,
// SOSP '23).
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
// The sketch holds no keys, only counters: eight to sixteen bytes for every
// entry that the cache has room for, and 64 at least for every shard.
//
// A cache also has a table in which its readers say what they are looking
// at. It takes between 2 and 4 KiB for every CPU that Go may use when the
// cache is created, whatever the capacity. For a great many small caches,
// the lru package is the better choice.
package tinylfu

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
	"github.com/floatdrop/evict/internal/sketch"
)

// Cache is a fixed-capacity cache that admits with TinyLFU. It is safe for
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
	shards   []padded[K, V]
	readers  *cslab.Readers
	capacity int

	// window is the size of the small queues, and the search for it.
	window window
}

// A shard is a cache of its own, holding a share of the keys.
type shard[K comparable, V any] struct {
	// The entries of both queues, each in a list of its own with the newest
	// entry first. Node.Meta counts the reads of an entry since a write
	// last looked at it, up to maxCount. Link.Tag says which list a node
	// is in and what the entry's count was left at then, and holds bits of
	// the hash of the key; see tagOf.
	slab cslab.Slab[K, V]

	// mu serializes the writers. Readers do not take it, unless they find
	// the index in motion.
	mu sync.Mutex

	// seen is how often keys have been asked for of late: once for every
	// time that one was put in, and the reads that entries had counted
	// when a write looked at them.
	seen sketch.Sketch

	// The number of entries in each queue.
	smalls, mains int

	capacity int // entries in both queues together

	// full says that the shard has been full since it was made or last
	// cleared. Until then, new entries are not all sent through the small
	// queue; see set.
	full bool

	// win is the window of the cache. The shard tells it of the hits that it
	// counted under its lock and of its misses, whenever it has batch of
	// the latter.
	win          *window
	hits, misses uint64
	batch        uint64
}

// padded is a shard followed by a line or more of padding, so that writers to
// it and readers of the next shard never share a line, and rounded up to whole
// lines, so that every shard starts at the same place in one.
type padded[K comparable, V any] struct {
	shard[K, V]
	_ [shards.Line + (shards.Line-unsafe.Sizeof(shard[K, V]{})%shards.Line)%shards.Line]byte
}

// The queues of a shard. The numbers are the list heads in the slab as well
// as the lowest bit of Link.Tag.
const (
	small = 0
	main  = 1

	queues = 2
)

const (
	// maxCount is as far as the reads of an entry count for its standing in
	// the main queue, and with the sketch each time a write looks at it. It
	// is as far as they are counted at all.
	maxCount = 3

	// The counters of the sketch are halved when it has counted this many
	// requests for every entry that the shard has room for, and after about
	// half as many from then on; see sketch.Sketch.Init.
	samplePerEntry = 10

	// A shard of a cache that has several tells the window of its misses
	// in batches, so that the writers to different shards do not all write
	// to the same place at every miss: of this many at the most, and of one
	// for every batchShare entries of the shard. The smaller the batches,
	// the closer the window's measurements are to what happened.
	batchSize  = 32
	batchShare = 8
)

// The tag of a node has the queue of the entry in its lowest bit. The two
// bits above that hold the count that the entry was left with when the main
// queue last passed it. The rest is the upper 29 bits of the hash of the key.
// With the lower half of the hash, which the slab keeps for every entry, that
// makes 61 bits to tell the sketch which key an entry has, without hashing
// the key again.

// tagOf returns the tag of an entry with the given hash in the given queue,
// with a count of zero.
func tagOf(h uint64, queue uint32) uint32 { return uint32(h>>35)<<3 | queue }

// kept returns the count that a tag holds.
func kept(tag uint32) uint32 { return tag >> 1 & 3 }

// keep returns the tag with the given count in it.
func keep(tag, count uint32) uint32 { return tag&^(3<<1) | count<<1 }

// The sketch knows a key by the 61 bits of its hash that a node holds.

// printOf returns what the sketch knows the key with the given hash by.
func printOf(h uint64) uint64 { return h>>35<<32 | uint64(uint32(h)) }

// printIn returns the same for an entry, from the hash it is indexed under
// and its tag.
func printIn(hash, tag uint32) uint64 { return uint64(tag>>3)<<32 | uint64(hash) }

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
// exchange, each shard runs its queues and its sketch on its own share of the
// keys and the capacity: a shard that is full evicts even while another one
// has room. The size of the small queue is the same share for all of them.
//
// By default the cache has a single shard if its capacity is less than 512
// and otherwise several per CPU, as long as that leaves each shard with at
// least 256 entries. On the traces that the benchmarks replay, shards of that
// size cost the hit ratio half a percentage point at the most, but for about
// a point on one trace and 2.6 points in one case; part of that is how much
// the cache varies from one run to the next anyway. See the report on shards
// there, which was made as on a machine with eight CPUs.
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
	// The first shard is the largest.
	c.window.init(shards.Capacity(capacity, n, 0), capacity, c.readers)
	for i := range c.shards {
		s := &c.shards[i]
		s.capacity = shards.Capacity(capacity, n, i)
		s.slab.Init(s.capacity, queues, c.readers)
		s.seen.Init(s.capacity, max(samplePerEntry*s.capacity, 1))
		s.win, s.batch = &c.window, 1
		if n > 1 {
			s.batch = uint64(min(batchSize, max(s.capacity/batchShare, 1)))
		}
	}
	return c
}

// Each method starts by hashing the key. The upper half of the hash picks the
// shard and the lower half is what the shard indexes the key under, so that
// keys sharing a shard do not share a prefix of their probe sequences.

// Get returns the value stored under key and counts the read, which speaks
// for the entry when the cache has to choose what to keep.
func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	h := maphash.Comparable(c.seed, key)
	s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)

	// The lookup counts the read on the entry, and as a request answered
	// in the reader's slot. Readers do not wait for each other, so two of
	// them may count as one on the entry. That is close enough. An entry
	// that is read all the time has reached the limit, and is not written
	// to again: its cache line stays shared between the CPUs.
	value, ok, certain := s.slab.Lookup(hash, key, maxCount, true)
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
		n.Meta.Store(min(n.Meta.Load()+1, maxCount))
		value, ok = n.Value, true
		s.hits++
	}
	s.mu.Unlock()

	return value, ok
}

// Peek returns the value stored under key without counting the read.
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

// Set stores value under key. A key that the cache does not hold goes into
// the small queue, and the sketch counts a request for it. For an entry that
// is cached already, the write counts as a read.
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
		n.Meta.Store(min(n.Meta.Load()+1, maxCount))
		s.hits++
		return oldKey, oldValue, true
	}

	if s.capacity == 0 {
		return key, value, true
	}
	// The key is asked for, whatever becomes of it, and the shard did not
	// have it. That may be what makes the size of the small queue move.
	s.seen.Add(printOf(h), 1)
	s.misses++
	if s.misses >= s.batch {
		s.win.note(s.hits, s.misses)
		s.hits, s.misses = 0, 0
	}
	if s.smalls+s.mains == s.capacity {
		oldKey, oldValue = s.evict()
		displaced = true
	}

	// A shard that has yet to fill up for the first time has nothing to
	// evict, and so no use for the small queue beyond its share: the
	// entries that follow go straight to where the room is. Otherwise the
	// entries that happen to come first would have to pass through the
	// small queue one after the other later on, for no reason but their
	// having been first.
	warming := !s.full && s.smalls >= s.win.sizeFor(s.capacity)

	idx := sl.Insert(hash, key, value)
	if warming {
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

// look takes stock of the entry in node idx: it tells the sketch of the reads
// that the entry has counted since a write last looked at it, and returns
// what the entry's count comes to with them.
func (s *shard[K, V]) look(idx uint32) (count uint32) {
	n := &s.slab.Nodes[idx]
	link := &s.slab.Links[idx]
	before := kept(link.Tag)
	reads := n.Meta.Load()
	if reads == 0 {
		// Nothing to tell, and nothing to write to the entry either.
		return before
	}
	// Readers do not wait for this. One that counts between the line
	// above and the next is not counted, which is close enough.
	n.Meta.Store(0)

	count = min(before+reads, maxCount)
	s.seen.Add(printIn(s.slab.Hash(idx), link.Tag), int(count-before))
	return count
}

// asked returns how often the key of the entry in node idx has been asked for
// of late, as far as the sketch knows.
func (s *shard[K, V]) asked(idx uint32) int {
	return s.seen.Estimate(printIn(s.slab.Hash(idx), s.slab.Links[idx].Tag))
}

// promote moves the entry in node idx from the small queue to the front of
// the main queue. It arrives there without a count: the small queue keeps
// none, and a write has just looked at the entry.
func (s *shard[K, V]) promote(idx uint32) {
	sl := &s.slab
	sl.Unlink(idx)
	sl.Links[idx].Tag |= main
	sl.PushFront(main, idx)
	s.smalls--
	s.mains++
}

// victim returns the entry that the main queue would evict: its oldest entry
// that has not been read since the queue last came by it, or zero if the
// queue is empty. The entries that it comes by on the way keep their places
// in the cache for another round, with one off their count.
func (s *shard[K, V]) victim() uint32 {
	sl := &s.slab
	links := sl.Links
	// An entry is let off maxCount times in a row at the most, so after as
	// many rounds the oldest one has no reads left, unless readers keep
	// counting while the queue turns. Then it has to do all the same.
	for range maxCount * s.mains {
		idx := links[main].Prev
		count := s.look(idx)
		if count == 0 {
			break
		}
		links[idx].Tag = keep(links[idx].Tag, count-1)
		sl.Unlink(idx)
		sl.PushFront(main, idx)
	}
	if s.mains == 0 {
		return 0
	}
	return links[main].Prev
}

// evict makes room for one entry and returns the one that had to go. The
// shard is full.
func (s *shard[K, V]) evict() (key K, value V) {
	sl := &s.slab
	links := sl.Links
	s.full = true

	// A small queue whose share has shrunk hands its oldest entries on to
	// the main queue, which is then the one to evict from.
	size := s.win.sizeFor(s.capacity)
	for s.smalls > size {
		idx := links[small].Prev
		s.look(idx)
		s.promote(idx)
	}

	// The main queue has entries if the small one is short of its share,
	// since the two of them fill the shard.
	var gone uint32
	if s.smalls < size {
		gone = s.victim()
	} else {
		// The small queue has its share, so its oldest entry is up
		// against the one that the main queue would evict. It gets in
		// only if it has been asked for more often of late: a tie goes
		// to the entry that has been here for longer.
		gone = links[small].Prev
		s.look(gone)
		if against := s.victim(); against != 0 && s.asked(gone) > s.asked(against) {
			s.promote(gone)
			gone = against
		}
	}

	if links[gone].Tag&1 == main {
		s.mains--
	} else {
		s.smalls--
	}
	n := &sl.Nodes[gone]
	key, value = n.Key, n.Value
	sl.Unlink(gone)
	sl.Remove(gone)
	return key, value
}

// Delete removes key and returns the value it had. The sketch keeps what it
// knows of the key, and learns of the reads that the entry has counted: a key
// that is deleted because its value is out of date is often back soon.
func (c *Cache[K, V]) Delete(key K) (value V, ok bool) {
	h := maphash.Comparable(c.seed, key)
	s, hash := &c.shards[uint32(h>>32)&c.mask], uint32(h)

	s.mu.Lock()
	if idx := s.slab.Find(hash, key); idx != 0 {
		value, ok = s.slab.Nodes[idx].Value, true
		s.look(idx)
		if s.slab.Links[idx].Tag&1 == main {
			s.mains--
		} else {
			s.smalls--
		}
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
		n += c.shards[i].slab.Len()
	}
	return n
}

// Cap returns the number of entries the cache can hold.
func (c *Cache[K, V]) Cap() int { return c.capacity }

// Clear removes every entry and empties the sketch. When it returns, the
// cache no longer refers to any of the keys and values it held. The share of
// the small queue stays as the cache has found it to be right, and the
// search for it goes on from there, with the share of requests that the
// cache answers from then on to compare with.
func (c *Cache[K, V]) Clear() {
	// No move of the size comes between the shards, nor between them and
	// the start of the new measurements.
	c.window.mu.Lock()
	defer c.window.mu.Unlock()

	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		mark := s.slab.Reset()
		s.seen.Reset()
		s.smalls, s.mains, s.full = 0, 0, false
		// A shard that tells the window of every miss has nothing to
		// tell but hits since the last one, which belong to the time
		// before. One that tells it in batches keeps what it has yet to
		// tell: when the next measurement ends, it will be holding back
		// about as much again, and a measurement that began without that
		// would count every hit but too few misses.
		if s.batch == 1 {
			s.hits, s.misses = 0, 0
		}
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
	c.window.forget()
}

// All iterates over the entries without counting them as read. A shard yields
// the entries of its main queue first and then those of its small queue, each
// from the newest to the oldest. A cache with a single shard yields its
// entries in exactly that order.
//
// A shard is locked only while its entries are collected, not while they are
// yielded: the loop body may use the cache, and a change it makes to a shard
// that has not been reached yet shows.
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
