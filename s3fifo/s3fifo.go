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
// straight into the main queue. The sizes of the queues and what it takes to
// move from one to the other can be changed; see Tuning below.
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
// All of this is the cache as it is made by default. The options change the
// numbers in it, and [WithGhostReturns] the rule for ghosts.
//
// # Tuning
//
// The numbers above are the ones that the paper recommends, and they serve a
// wide range of workloads well. They are not the best for each of them. The
// eleven traces that this module is measured with were replayed with 84
// settings, and for each trace the setting was picked that answered the most
// requests, averaged over eight cache sizes. On four traces that one gained
// next to nothing, and on two a great deal: eight and nearly sixteen points
// of hit ratio. The small queue of the setting that served a trace best had a
// twentieth of the room for six of them, a tenth for three and a fifth for
// two. Four traces did best with three or six times as many ghosts, and five
// by sending a key to the main queue only when it comes back for the second
// time. No one setting was good for all: the one that did best for the eleven
// together did worse than the standard one for six of them. The numbers are
// in bench/results/tuning.md. They say what there is to gain at the most: a
// setting that is picked for the requests it is then measured with has seen
// them before, which no cache in use has.
//
// So the options [WithSmallRatio], [WithGhostRatio], [WithPromotionReads] and
// [WithGhostReturns] are for a workload that is known, or better, recorded:
// replay it with the settings in question and compare. They are four of the
// five knobs of S4-FIFO, which picks its settings with a model that was
// trained in advance ("Learning-Augmented Heuristics: Simple, yet Smart,
// Robust and Interpretable Cache Eviction" by Xia, Nixon, Marthen, Bhandari
// and Yang, 2026). A cache of this package does not pick: it does as it is
// told.
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
// bytes or so, for nine tenths as many ghosts as there are entries unless
// [WithGhostRatio] says otherwise. The key itself is let go like the rest of
// the entry.
//
// A cache also has a table in which its readers say what they are looking
// at. It takes between 2 and 4 KiB for every CPU that Go may use when the
// cache is created, whatever the capacity. For a great many small caches,
// the lru package is the better choice.
package s3fifo

import (
	"hash/maphash"
	"iter"
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"unsafe"

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
	shards   []padded[K, V]
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

	// promote is how often an entry must have been read in the small queue
	// to move on to the main one, and returns is how often a key must come
	// back while it is a ghost to go there straight away.
	promote uint32
	returns int

	// evicted says that the shard has been full since it was made or last
	// cleared. Until then, new entries are not all sent through the small
	// queue; see set.
	evicted bool
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
	shards     int
	smallRatio float64
	ghostRatio float64
	promote    int
	returns    int
}

// The defaults are the numbers of the paper.
func defaults() config {
	return config{smallRatio: 0.1, ghostRatio: 0.9, promote: 2, returns: 1}
}

// maxGhostRatio is as far as [WithGhostRatio] goes.
const maxGhostRatio = 8

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

// WithSmallRatio sets the share of the capacity that the small queue has; the
// main queue has the rest. The default is 0.1.
//
// New entries wait in the small queue to be read again. A larger one gives
// them longer, which suits a workload whose entries are asked for again soon
// or not at all, and takes the room from the entries that have proven
// themselves. With a share of 0 a new entry has until the next one arrives,
// and with a share of 1 the main queue holds only what has just moved up.
//
// The ratio is brought into the range from 0 to 1 if it is outside, and is 0
// if it is not a number. The share of a shard is rounded down to whole
// entries.
func WithSmallRatio(ratio float64) Option {
	return func(c *config) { c.smallRatio = clamp(ratio, 1) }
}

// WithGhostRatio sets the number of ghosts, the keys that the cache remembers
// after it has evicted their entries from the small queue, as a share of the
// capacity. The default is 0.9: as many as the main queue has entries.
//
// The more ghosts, the longer a key may stay away and still be recognized
// when it comes back, which is what tells an entry that is asked for every
// now and then from one that is new. A ghost takes about sixteen bytes, so a
// ratio of 6 costs about a hundred bytes for every entry. With a ratio of 0
// nothing is remembered, and an entry gets to the main queue only by being
// read in the small one.
//
// The ratio is brought into the range from 0 to 8 if it is outside, and is 0
// if it is not a number. The number for a shard is rounded down to whole
// keys.
func WithGhostRatio(ratio float64) Option {
	return func(c *config) { c.ghostRatio = clamp(ratio, maxGhostRatio) }
}

// WithPromotionReads sets how often an entry has to be read while it is in
// the small queue to move on to the main queue when its turn to leave comes.
// The default is 2. With 1, a single read is enough, which lets more entries
// through; with 3, an entry has to be read as often as the cache counts at
// all. The number is brought into the range from 1 to 3 if it is outside.
func WithPromotionReads(n int) Option {
	return func(c *config) { c.promote = min(max(n, 1), maxFrequency) }
}

// WithGhostReturns sets how often a key has to come back while the cache
// remembers it to go straight into the main queue. The default is 1: a ghost
// that is asked for is as good as an entry that was read twice.
//
// With 2, the key goes through the small queue once more the first time it
// comes back, and its ghost stays: until the key comes back again, until its
// entry gets into the main queue by being read, until the key is deleted, or
// until the ghost's turn to be forgotten comes, as it would have without the
// entry. That keeps out keys that come round at long intervals without ever
// being asked for in between, as in a scan that repeats; it costs a byte for
// every ghost. The number is brought into the range from 1 to 255 if it is
// outside.
func WithGhostReturns(n int) Option {
	return func(c *config) { c.returns = min(max(n, 1), math.MaxUint8) }
}

// clamp returns the ratio in the range from 0 to most that is closest to x.
// That is 0 if x is not a number.
func clamp(x, most float64) float64 {
	if math.IsNaN(x) {
		return 0
	}
	return min(max(x, 0), most)
}

// share returns the given share of n, rounded down. A ratio is a fraction in
// binary, and its product with n may come out a hair below the whole number
// that it stands for; that hair is not allowed to cost an entry.
func share(ratio float64, n int) int {
	return int(ratio*float64(n) + 1e-6)
}

// New returns a cache that holds up to capacity entries. A cache with a
// capacity of zero or less holds nothing.
func New[K comparable, V any](capacity int, opts ...Option) *Cache[K, V] {
	cfg := defaults()
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
		s := &c.shards[i]
		s.capacity = shards.Capacity(capacity, n, i)
		// By default a tenth of the room is for the small queue, and there
		// are nine tenths as many ghosts as entries. Both are rounded
		// down, so that is not always as many ghosts as the main queue
		// has room for entries.
		s.mainCap = s.capacity - share(cfg.smallRatio, s.capacity)
		s.promote, s.returns = uint32(cfg.promote), cfg.returns
		s.slab.Init(s.capacity, queues, c.readers)
		if ghosts := min(share(cfg.ghostRatio, s.capacity), ghost.MaxCapacity); cfg.returns > 1 {
			s.ghosts.InitCounting(ghosts)
		} else {
			s.ghosts.Init(ghosts)
		}
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
	value, ok, certain := s.slab.Lookup(hash, key, maxFrequency, false)
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

// Set stores value under key. A key that the cache does not know goes into
// the small queue, and one whose ghost it still has into the main queue, or
// into the small queue once more if [WithGhostReturns] asks for that. For an
// entry that is cached already, the write counts as a read.
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
	remembered := s.ghosts.Recall(ghostOf(hash, tagOf(h, small)), s.returns)
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
	// they have been read often enough, which is more than once unless
	// [WithPromotionReads] says otherwise, however full that gets: it is
	// the main queue's turn the next time. If all of them move, it is its
	// turn now.
	for fromSmall := s.mains <= s.mainCap; fromSmall && s.smalls > 0; {
		idx := links[small].Prev
		n := &nodes[idx]
		sl.Unlink(idx)
		s.smalls--

		if n.Meta.Load() >= s.promote {
			n.Meta.Store(0)
			links[idx].Tag |= main
			sl.PushFront(main, idx)
			s.mains++
			// An entry may have a ghost if ghosts have to come back
			// more than once: this one may be here on its first return.
			// It has made it now, and that is the end of the ghost. Were
			// it to stay, the key would get in more easily the next time
			// than if it had to come back only once.
			if s.returns > 1 {
				s.ghosts.Remove(ghostOf(sl.Hash(idx), links[idx].Tag))
			}
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

// Delete removes key and returns the value it had. If the cache has a ghost
// for key, with or without an entry, the ghost goes: the cache forgets the
// key altogether.
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
	}
	// An entry has no ghost, unless ghosts have to come back more than
	// once and this one has only come back.
	if !ok || s.returns > 1 {
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
