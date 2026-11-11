package s3fifo

import "hash/maphash"

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
