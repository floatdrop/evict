package tinylfu

import (
	"hash/maphash"
	"slices"
)

// OccupyReaders takes the slots in which the readers of the cache pin what
// they look at, save for one in every leave of them if leave is not zero, and
// returns a function that gives them back. A reader that finds no slot has to
// take the lock of the shard.
func (c *Cache[K, V]) OccupyReaders(leave int) (release func()) {
	return c.readers.Occupy(leave)
}

// Hold finds the entry for key like a reader does and keeps looking at it
// until the function that it returns is called: a reader that is held up at
// the worst moment. It reports whether there was an entry to look at.
func (c *Cache[K, V]) Hold(key K) (release func(), ok bool) {
	h := maphash.Comparable(c.seed, key)
	s := &c.shards[uint32(h>>32)&c.mask]

	// A reader finds the entry and pins it without the lock. With the
	// lock, there is no doubt about what it has found.
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := s.slab.Find(uint32(h), key)
	if idx == 0 {
		return func() {}, false
	}
	pin := s.slab.Pin(idx)
	if pin == nil {
		return func() {}, false
	}
	return pin.Unpin, true
}

// Waits returns how often a writer to the cache has had to wait for a reader
// to let go of an entry.
func (c *Cache[K, V]) Waits() (waits uint64) {
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		waits += s.slab.Waits()
		s.mu.Unlock()
	}
	return waits
}

// Queues returns the keys in the main queue and in the small queue of the
// first shard, each from the newest to the oldest.
func (c *Cache[K, V]) Queues() (inMain, inSmall []K) {
	s := &c.shards[0]
	s.mu.Lock()
	defer s.mu.Unlock()

	nodes, links := s.slab.Nodes, s.slab.Links
	for idx := links[main].Next; idx != main; idx = links[idx].Next {
		inMain = append(inMain, nodes[idx].Key)
	}
	for idx := links[small].Next; idx != small; idx = links[idx].Next {
		inSmall = append(inSmall, nodes[idx].Key)
	}
	return inMain, inSmall
}

// Print returns what the sketch of a shard knows key by.
func (c *Cache[K, V]) Print(key K) uint64 {
	return printOf(maphash.Comparable(c.seed, key))
}

// Asked returns how often key has been asked for of late, by the sketch of
// its shard.
func (c *Cache[K, V]) Asked(key K) int {
	h := maphash.Comparable(c.seed, key)
	s := &c.shards[uint32(h>>32)&c.mask]
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen.Estimate(printOf(h))
}

// Reads returns the reads that the entry for key has counted since a write
// last looked at it.
func (c *Cache[K, V]) Reads(key K) (reads uint32, ok bool) {
	h := maphash.Comparable(c.seed, key)
	s := &c.shards[uint32(h>>32)&c.mask]
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := s.slab.Find(uint32(h), key)
	if idx == 0 {
		return 0, false
	}
	return s.slab.Nodes[idx].Meta(), true
}

// Window returns the number of entries that the small queue of the first
// shard is to have.
func (c *Cache[K, V]) Window() int { return c.window.sizeFor(c.shards[0].capacity) }

// Resize gives the small queue of the first shard a share of n entries, and
// the others theirs in proportion, as the search for the right size might.
// The search goes on from there.
func (c *Cache[K, V]) Resize(n int) { c.window.resize(n) }

// Twins returns two of the keys 0, 1, 2 and so on that the cache indexes
// under the same hash. A reader that looks for one of them comes across the
// other and can only tell them apart by the keys themselves.
func Twins[V any](c *Cache[int, V]) (first, second int) {
	seen := make(map[uint32]int)
	for key := 0; ; key++ {
		hash := uint32(maphash.Comparable(c.seed, key))
		if other, ok := seen[hash]; ok {
			return other, key
		}
		seen[hash] = key
	}
}

// Apart returns n of the keys 0, 1, 2 and so on of which no two share a
// counter in the sketch of the first shard, and none shares one with any of
// the keys given. Of such keys the sketch knows exactly how often each has
// been asked for, as long as no others are in use. The cache must be in one
// piece.
func Apart[V any](c *Cache[int, V], n int, others ...int) []int {
	seen := &c.shards[0].seen
	var prints []uint64
	for _, key := range others {
		prints = append(prints, c.Print(key))
	}

	keys := make([]int, 0, n)
	for key := 0; len(keys) < n; key++ {
		if key == 1<<24 {
			panic("tinylfu: the sketch has no room for that many keys with counters of their own")
		}
		p := c.Print(key)
		if !slices.ContainsFunc(prints, func(q uint64) bool { return seen.Shared(p, q) }) {
			keys, prints = append(keys, key), append(prints, p)
		}
	}
	return keys
}
