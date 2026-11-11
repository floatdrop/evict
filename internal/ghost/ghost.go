// Package ghost provides the memory of a cache for keys it no longer holds: a
// set that remembers what was added to it for a while and then forgets it.
//
// All it keeps of a key is 63 bits of a hash, in sixteen bytes or so for each.
// Two keys with the same hash are the same key to it, which is rare enough
// not to matter for what a cache does with the answer.
package ghost

import "math/bits"

// Set remembers the hashes that were added to it until as many more have been
// added as it has room for. It does not allocate once it has been
// initialized. A Set is not safe for concurrent use.
type Set struct {
	// ring holds the hashes in the order in which they were added, and
	// starts over when it reaches the end. A hash has its highest bit set,
	// so that zero can stand for a place that is free. The lower bits are
	// the ones that say where a hash belongs in the index.
	ring []uint64
	next uint32 // the place in ring that the next hash goes to

	// index is a linear-probing table, whose size is a power of two, of
	// the places in ring, each plus one. Zero is an empty slot.
	index []uint32
	mask  uint32

	count int
}

// present is the bit that every hash in the ring has set.
const present = 1 << 63

// MaxCapacity is the largest capacity [Set.Init] accepts. It keeps the index
// addressable with 32 bits: that has between one and a half times and three
// times as many slots as the set has room, and twice as many at this
// capacity.
const MaxCapacity = 1 << 30

// Init readies the set to remember up to capacity hashes, and empties it. It
// panics unless capacity lies in [0, MaxCapacity].
func (s *Set) Init(capacity int) {
	if capacity < 0 || capacity > MaxCapacity {
		panic("ghost: invalid capacity")
	}

	// The smallest power of two above 1.5 times the capacity keeps the load
	// factor at or below 2/3 and guarantees an empty slot, which is what
	// ends an unsuccessful probe.
	size := uint(1) << bits.Len(uint(capacity+capacity/2))

	s.ring = make([]uint64, capacity)
	s.index = make([]uint32, size)
	s.mask = uint32(size - 1)
	s.next, s.count = 0, 0
}

// Len returns the number of hashes in the set.
func (s *Set) Len() int { return s.count }

// Cap returns the number of hashes the set has room for.
func (s *Set) Cap() int { return len(s.ring) }

// Add puts a hash into the set, where it stays until as many more have been
// added as the set has room for, or until it is removed. A hash that is in
// the set already stays where it is. The highest bit of the hash is not
// looked at: two hashes that differ in nothing else are the same.
func (s *Set) Add(hash uint64) {
	if len(s.ring) == 0 {
		return
	}
	hash |= present
	if _, ok := s.find(hash); ok {
		return
	}

	// The place that is next in line holds the oldest hash, unless that
	// one has been removed.
	if s.ring[s.next] != 0 {
		slot, _ := s.find(s.ring[s.next])
		s.unindex(slot)
		s.count--
	}

	s.ring[s.next] = hash
	i := uint32(hash) & s.mask
	for s.index[i] != 0 {
		i = (i + 1) & s.mask
	}
	s.index[i] = s.next + 1
	s.count++

	s.next++
	if int(s.next) == len(s.ring) {
		s.next = 0
	}
}

// Remove takes a hash out of the set and reports whether it was there. As
// with Add, the highest bit of the hash does not count.
func (s *Set) Remove(hash uint64) bool {
	slot, ok := s.find(hash | present)
	if !ok {
		return false
	}
	s.ring[s.index[slot]-1] = 0
	s.unindex(slot)
	s.count--
	return true
}

// Reset empties the set.
func (s *Set) Reset() {
	clear(s.ring)
	clear(s.index)
	s.next, s.count = 0, 0
}

// find returns the slot of the index that refers to the hash.
func (s *Set) find(hash uint64) (slot uint32, ok bool) {
	for i := uint32(hash) & s.mask; ; i = (i + 1) & s.mask {
		place := s.index[i]
		if place == 0 {
			return 0, false
		}
		if s.ring[place-1] == hash {
			return i, true
		}
	}
}

// unindex empties a slot of the index, then closes the gap so that probe
// sequences running through it stay unbroken.
func (s *Set) unindex(hole uint32) {
	index, mask := s.index, s.mask

	// Every entry between the hole and the next empty slot moves back into
	// the hole if that keeps it at or after its home slot. Distances are
	// taken modulo the table size.
	for i := (hole + 1) & mask; index[i] != 0; i = (i + 1) & mask {
		home := uint32(s.ring[index[i]-1]) & mask
		if (i-home)&mask >= (i-hole)&mask {
			index[hole] = index[i]
			hole = i
		}
	}
	index[hole] = 0
}
