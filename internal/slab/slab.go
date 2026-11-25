// Package slab provides the storage of the cache in this module that does all
// of its work under a lock, lru: a fixed array of entries addressed by
// 32-bit indices, an open-addressing hash index over those entries, and an
// intrusive doubly linked list threaded through them.
//
// Nothing in a [Slab] is a Go pointer unless a key or a value is one, so the
// garbage collector has nothing to trace beyond what the caller stores, and no
// operation allocates once [Slab.Init] has returned.
//
// A Slab is not safe for concurrent use. Its owner serializes all access.
package slab

// A note on how this package is written. Its methods are generic, so they are
// compiled into the packages that use the caches, and the compiler inlines a
// function there only if it has seen its body. It has not seen the bodies of
// small helpers that are not generic themselves, which is why the hot paths
// below spell out their arithmetic instead of calling such helpers.

// MaxCapacity is the largest capacity [Slab.Init] accepts. It keeps the index,
// which has three slots for every entry, addressable with 32 bits.
const MaxCapacity = 1 << 30

// Head is the node that heads the list. It never holds an entry: its Next is
// the first node of the list and its Prev the last, and it is linked to itself
// when the list is empty.
const Head = 0

// Node is one entry of a [Slab].
type Node[K comparable, V any] struct {
	// Prev and Next link the node into the list, which is circular through
	// [Head].
	Prev, Next uint32

	// hash is what the node is indexed under.
	hash uint32

	Key   K
	Value V
}

// Slab is a fixed set of nodes that can be found by key.
//
// The first node is [Head]. Because it never holds an entry, index 0 never
// refers to one and serves as "no node".
type Slab[K comparable, V any] struct {
	// Nodes holds Head followed by the entries.
	Nodes []Node[K, V]

	// slots is the hash index: a linear-probing table of size slots. A slot
	// holds the hash of an entry in its high half and the index of the
	// entry's node in its low half. Zero is an empty slot: no entry lives in
	// node 0. An entry's probe starts at the slot that its hash scaled to
	// size points at, which spreads the hashes over a table of any size.
	slots []uint64
	size  uint32

	free  uint32 // most recently removed node, linked through Next; 0 if none
	fresh uint32 // lowest index that was never handed out
	count uint32 // entries in use
}

// indexSize returns the number of slots in the index of a slab with the given
// capacity: three for every entry, and one at least. That keeps the load
// factor at or below a third, whatever the capacity, and guarantees an empty
// slot, which is what ends an unsuccessful probe.
func indexSize(capacity int) uint { return max(3*uint(capacity), 1) }

// Init readies the slab for capacity entries and leaves the list empty. It
// panics unless capacity lies in [0, MaxCapacity].
func (s *Slab[K, V]) Init(capacity int) {
	if capacity < 0 || capacity > MaxCapacity {
		panic("slab: invalid capacity")
	}

	s.Nodes = make([]Node[K, V], 1+capacity)
	s.slots = make([]uint64, indexSize(capacity))
	s.size = uint32(len(s.slots))
	s.resetList()
}

func (s *Slab[K, V]) resetList() {
	s.Nodes[Head].Prev, s.Nodes[Head].Next = Head, Head
	s.free, s.fresh, s.count = 0, Head+1, 0
}

// Len returns the number of entries in the slab.
func (s *Slab[K, V]) Len() int { return int(s.count) }

// Cap returns the number of entries the slab can hold.
func (s *Slab[K, V]) Cap() int { return len(s.Nodes) - 1 }

// Find returns the index of the node that holds key, or 0 if there is none.
// The hash must be the one the key was inserted with.
func (s *Slab[K, V]) Find(hash uint32, key K) uint32 {
	slots, size := s.slots, s.size
	for i := uint32(uint64(hash) * uint64(size) >> 32); ; {
		slot := slots[i]
		if slot == 0 {
			return 0
		}
		if slot>>32 == uint64(hash) {
			if idx := uint32(slot); s.Nodes[idx].Key == key {
				return idx
			}
		}
		i++
		if i == size {
			i = 0
		}
	}
}

// Insert stores an entry in an unused node, indexes it under hash, and returns
// the index of the node. The node is not in the list yet.
//
// The caller must have established that key is absent. Insert panics if the
// slab is full.
func (s *Slab[K, V]) Insert(hash uint32, key K, value V) uint32 {
	idx := s.free
	switch {
	case idx != 0:
		s.free = s.Nodes[idx].Next
	case int(s.fresh) < len(s.Nodes):
		idx = s.fresh
		s.fresh++
	default:
		panic("slab: insert into a full slab")
	}

	n := &s.Nodes[idx]
	n.hash = hash
	n.Key = key
	n.Value = value
	s.count++

	slots, size := s.slots, s.size
	i := uint32(uint64(hash) * uint64(size) >> 32)
	for slots[i] != 0 {
		i++
		if i == size {
			i = 0
		}
	}
	slots[i] = uint64(hash)<<32 | uint64(idx)

	return idx
}

// Remove drops the entry in node idx from the index, releases its key and
// value, and makes the node available to Insert again. The caller must have
// unlinked the node from the list. Remove panics if the node holds no entry.
func (s *Slab[K, V]) Remove(idx uint32) {
	n := &s.Nodes[idx]
	s.unindex(n.hash, idx)

	var (
		zeroKey   K
		zeroValue V
	)
	n.Key, n.Value = zeroKey, zeroValue

	n.Next = s.free
	s.free = idx
	s.count--
}

// unindex empties the slot that refers to node idx, then closes the gap so
// that probe sequences running through it stay unbroken.
func (s *Slab[K, V]) unindex(hash, idx uint32) {
	slots, size := s.slots, s.size

	hole := uint32(uint64(hash) * uint64(size) >> 32)
	for {
		slot := slots[hole]
		if slot == 0 {
			panic("slab: node is not indexed")
		}
		if slot&(1<<32-1) == uint64(idx) {
			break
		}
		hole++
		if hole == size {
			hole = 0
		}
	}

	// Every entry between the hole and the next empty slot moves back into
	// the hole if that keeps it at or after its home slot. Distances are
	// taken around the end of the table.
	for i := hole; ; {
		i++
		if i == size {
			i = 0
		}
		slot := slots[i]
		if slot == 0 {
			break
		}
		home := uint32((slot >> 32) * uint64(size) >> 32)
		fromHome, fromHole := i-home, i-hole
		if i < home {
			fromHome += size
		}
		if i < hole {
			fromHole += size
		}
		if fromHome >= fromHole {
			slots[hole] = slot
			hole = i
		}
	}
	slots[hole] = 0
}

// MaxDisplacement returns the largest distance between the slot an entry is
// indexed in and the slot its hash asks for. It walks the whole index and is
// meant for tests: a large value means that hashes collide more than they
// should, and that lookups pay for it.
func (s *Slab[K, V]) MaxDisplacement() int {
	longest := uint32(0)
	for i, slot := range s.slots {
		if slot == 0 {
			continue
		}
		at, home := uint32(i), uint32((slot>>32)*uint64(s.size)>>32)
		distance := at - home
		if at < home {
			distance += s.size
		}
		longest = max(longest, distance)
	}
	return int(longest)
}

// Reset removes every entry and empties the list.
func (s *Slab[K, V]) Reset() {
	// Nodes past fresh were never written.
	clear(s.Nodes[:s.fresh])
	clear(s.slots)
	s.resetList()
}

// DeleteFunc visits the entries in the list and calls remove on those for
// which del returns true. remove must unlink and remove just that node. The
// caller holds the shard lock.
func (s *Slab[K, V]) DeleteFunc(del func(K, V) bool, remove func(uint32)) int {
	n := 0
	for idx := s.Nodes[Head].Next; idx != Head; {
		next := s.Nodes[idx].Next // remove reuses the link.
		if e := &s.Nodes[idx]; del(e.Key, e.Value) {
			remove(idx)
			n++
		}
		idx = next
	}
	return n
}

// PushFront links node idx in as the first node of the list.
func (s *Slab[K, V]) PushFront(idx uint32) {
	nodes := s.Nodes
	first := nodes[Head].Next
	nodes[idx].Prev, nodes[idx].Next = Head, first
	nodes[first].Prev = idx
	nodes[Head].Next = idx
}

// Unlink takes node idx out of the list.
func (s *Slab[K, V]) Unlink(idx uint32) {
	nodes := s.Nodes
	prev, next := nodes[idx].Prev, nodes[idx].Next
	nodes[prev].Next = next
	nodes[next].Prev = prev
}

// MoveToFront makes node idx, which must be in the list, its first node.
func (s *Slab[K, V]) MoveToFront(idx uint32) {
	// This is Unlink followed by PushFront, written out so that it is
	// small enough to be inlined: it runs on every hit of an LRU cache.
	nodes := s.Nodes
	first := nodes[Head].Next
	if first == idx {
		return
	}
	n := &nodes[idx]
	nodes[n.Prev].Next = n.Next
	nodes[n.Next].Prev = n.Prev
	n.Prev, n.Next = Head, first
	nodes[first].Prev = idx
	nodes[Head].Next = idx
}
