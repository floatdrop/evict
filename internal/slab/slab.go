// Package slab provides the storage shared by the caches in this module that
// do all of their work under a lock: a fixed array of entries addressed by
// 32-bit indices, an open-addressing hash index over those entries, and
// intrusive doubly linked lists threaded through them.
//
// Nothing in a [Slab] is a Go pointer unless a key or a value is one, so the
// garbage collector has nothing to trace beyond what the caller stores, and no
// operation allocates once [Slab.Init] has returned.
//
// A Slab is not safe for concurrent use. Its owner serializes all access.
package slab

import "math/bits"

// A note on how this package is written. Its methods are generic, so they are
// compiled into the packages that use the caches, and the compiler inlines a
// function there only if it has seen its body. It has not seen the bodies of
// small helpers that are not generic themselves, which is why the hot paths
// below spell out their arithmetic instead of calling such helpers.

// MaxCapacity is the largest capacity [Slab.Init] accepts. It keeps the index
// addressable with 32 bits: that has between one and a half times and three
// times as many slots as the slab has entries, and twice as many at this
// capacity.
const MaxCapacity = 1 << 30

// maxHeads is the largest number of list heads [Slab.Init] accepts.
const maxHeads = 1 << 8

// Node is one entry of a [Slab].
type Node[K comparable, V any] struct {
	// Prev and Next link the node into a circular list. For a list head they
	// are the last and the first node of the list, and the head itself when
	// the list is empty.
	Prev, Next uint32

	// hash is what the node is indexed under.
	hash uint32

	// Meta belongs to the owner of the slab. Insert resets it to zero.
	Meta uint32

	Key   K
	Value V
}

// Slab is a fixed set of nodes that can be found by key.
//
// The first nodes are list heads, as many as [Slab.Init] was asked for. They
// never hold an entry; the owner hands their indices to the list methods.
// Because there is always at least one, index 0 never refers to an entry and
// serves as "no node".
type Slab[K comparable, V any] struct {
	// Nodes holds the list heads followed by the entries.
	Nodes []Node[K, V]

	// slots is the hash index: a linear-probing table whose size is a power
	// of two. A slot holds the hash of an entry in its high half and the
	// index of the entry's node in its low half. Zero is an empty slot: no
	// entry lives in node 0.
	slots []uint64
	mask  uint32

	heads uint32 // number of list heads
	free  uint32 // most recently removed node, linked through Next; 0 if none
	fresh uint32 // lowest index that was never handed out
	count uint32 // entries in use
}

// indexSize returns the number of slots in the index of a slab with the given
// capacity: the smallest power of two above 1.5 times the capacity. That
// keeps the load factor at or below 2/3 and guarantees an empty slot, which
// is what ends an unsuccessful probe.
func indexSize(capacity int) uint {
	return 1 << bits.Len(uint(capacity+capacity/2))
}

// Init readies the slab for capacity entries and the given number of list
// heads, and leaves every list empty. It panics unless capacity lies in
// [0, MaxCapacity] and there are between 1 and 256 heads.
func (s *Slab[K, V]) Init(capacity, heads int) {
	if heads < 1 || heads > maxHeads || capacity < 0 || capacity > MaxCapacity {
		panic("slab: invalid capacity or number of list heads")
	}

	s.Nodes = make([]Node[K, V], heads+capacity)
	s.slots = make([]uint64, indexSize(capacity))
	s.mask = uint32(len(s.slots) - 1)
	s.heads = uint32(heads)
	s.resetLists()
}

func (s *Slab[K, V]) resetLists() {
	for i := range s.heads {
		s.Nodes[i].Prev, s.Nodes[i].Next = i, i
	}
	s.free, s.fresh, s.count = 0, s.heads, 0
}

// Len returns the number of entries in the slab.
func (s *Slab[K, V]) Len() int { return int(s.count) }

// Cap returns the number of entries the slab can hold.
func (s *Slab[K, V]) Cap() int { return len(s.Nodes) - int(s.heads) }

// Find returns the index of the node that holds key, or 0 if there is none.
// The hash must be the one the key was inserted with.
func (s *Slab[K, V]) Find(hash uint32, key K) uint32 {
	slots, mask := s.slots, s.mask
	for i := hash & mask; ; i = (i + 1) & mask {
		slot := slots[i]
		if slot == 0 {
			return 0
		}
		if slot>>32 == uint64(hash) {
			if idx := uint32(slot); s.Nodes[idx].Key == key {
				return idx
			}
		}
	}
}

// Insert stores an entry in an unused node, indexes it under hash, and returns
// the index of the node. The node is not part of any list yet, and its Meta
// is zero.
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
	n.Meta = 0
	n.Key = key
	n.Value = value
	s.count++

	slots, mask := s.slots, s.mask
	i := hash & mask
	for slots[i] != 0 {
		i = (i + 1) & mask
	}
	slots[i] = uint64(hash)<<32 | uint64(idx)

	return idx
}

// Remove drops the entry in node idx from the index, releases its key and
// value, and makes the node available to Insert again. The caller must have
// unlinked the node from its list. Remove panics if the node holds no entry.
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
	slots, mask := s.slots, s.mask

	hole := hash & mask
	for {
		slot := slots[hole]
		if slot == 0 {
			panic("slab: node is not indexed")
		}
		if slot&(1<<32-1) == uint64(idx) {
			break
		}
		hole = (hole + 1) & mask
	}

	// Every entry between the hole and the next empty slot moves back into
	// the hole if that keeps it at or after its home slot. Distances are
	// taken modulo the table size.
	for i := (hole + 1) & mask; slots[i] != 0; i = (i + 1) & mask {
		home := uint32(slots[i]>>32) & mask
		if (i-home)&mask >= (i-hole)&mask {
			slots[hole] = slots[i]
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
		if slot != 0 {
			longest = max(longest, (uint32(i)-uint32(slot>>32))&s.mask)
		}
	}
	return int(longest)
}

// Reset removes every entry and empties every list.
func (s *Slab[K, V]) Reset() {
	// Nodes past fresh were never written.
	clear(s.Nodes[:s.fresh])
	clear(s.slots)
	s.resetLists()
}

// PushFront links node idx in as the first node of the list at head.
func (s *Slab[K, V]) PushFront(head, idx uint32) {
	nodes := s.Nodes
	first := nodes[head].Next
	nodes[idx].Prev, nodes[idx].Next = head, first
	nodes[first].Prev = idx
	nodes[head].Next = idx
}

// Unlink takes node idx out of the list it is in.
func (s *Slab[K, V]) Unlink(idx uint32) {
	nodes := s.Nodes
	prev, next := nodes[idx].Prev, nodes[idx].Next
	nodes[prev].Next = next
	nodes[next].Prev = prev
}

// MoveToFront makes node idx, which must be in the list at head, the first
// node of that list.
func (s *Slab[K, V]) MoveToFront(head, idx uint32) {
	// This is Unlink followed by PushFront, written out so that it is
	// small enough to be inlined: it runs on every hit of an LRU cache.
	nodes := s.Nodes
	first := nodes[head].Next
	if first == idx {
		return
	}
	n := &nodes[idx]
	nodes[n.Prev].Next = n.Next
	nodes[n.Next].Prev = n.Prev
	n.Prev, n.Next = head, first
	nodes[first].Prev = idx
	nodes[head].Next = idx
}
