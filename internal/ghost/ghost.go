// Package ghost provides the memory of a cache for keys it no longer holds: a
// set that remembers what was added to it for a while and then forgets it.
//
// All it keeps of a key is 63 bits of a hash, in twenty bytes or so for each.
// Two keys with the same hash are the same key to it, which is rare enough
// not to matter for what a cache does with the answer.
package ghost

import "math"

// Set remembers the hashes that were added to it until as many more have been
// added as it has room for. It does not allocate once it has been
// initialized. A Set is not safe for concurrent use.
type Set struct {
	// ring holds the hashes in the order in which they were added, and
	// starts over when it reaches the end. A hash has its highest bit set,
	// so that zero can stand for a place that is free. Its lower 32 bits
	// say where it belongs in the index.
	ring []uint64
	next uint32 // the place in ring that the next hash goes to

	// index is a linear-probing table of size slots, of the places in ring,
	// each plus one. Zero is an empty slot. A hash's probe starts at the slot
	// that its lower 32 bits scaled to size point at.
	index []uint32
	size  uint32

	count int

	// asked says, for every place in ring, how often the hash there has
	// been asked for with Recall. It is nil in a set that does not count.
	asked []uint8
}

// present is the bit that every hash in the ring has set.
const present = 1 << 63

// MaxCapacity is the largest capacity [Set.Init] accepts. It keeps the index,
// which has three slots for every hash the set has room for, addressable with
// 32 bits.
const MaxCapacity = 1 << 30

// Init readies the set to remember up to capacity hashes, and empties it. It
// panics unless capacity lies in [0, MaxCapacity].
func (s *Set) Init(capacity int) {
	if capacity < 0 || capacity > MaxCapacity {
		panic("ghost: invalid capacity")
	}

	// Three slots for every hash keep the load factor at or below a third,
	// whatever the capacity, and guarantee an empty slot, which is what ends
	// an unsuccessful probe.
	size := max(3*uint(capacity), 1)

	s.ring = make([]uint64, capacity)
	s.index = make([]uint32, size)
	s.size = uint32(size)
	s.next, s.count = 0, 0
	s.asked = nil
}

// InitCounting is Init for a set that keeps count of how often each hash is
// asked for with [Set.Recall], at the price of a byte for each.
func (s *Set) InitCounting(capacity int) {
	s.Init(capacity)
	s.asked = make([]uint8, capacity)
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
	if s.asked != nil {
		s.asked[s.next] = 0
	}
	i := s.home(hash)
	for s.index[i] != 0 {
		i = s.after(i)
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

// Recall is Remove for a caller who wants to be asked more than once: it takes
// a hash out of the set and reports that it did only when the hash has been
// asked for the given number of times, this one included. Until then the hash
// stays where it is, and is forgotten when its turn comes like any other. A
// set that does not count, or a number below two, makes Recall the same as
// Remove. A set counts in a byte: a number above 256 is taken for 256.
func (s *Set) Recall(hash uint64, times int) bool {
	slot, ok := s.find(hash | present)
	if !ok {
		return false
	}
	place := s.index[slot] - 1
	if int(place) < len(s.asked) && int(s.asked[place])+1 < min(times, math.MaxUint8+1) {
		s.asked[place]++
		return false
	}
	s.ring[place] = 0
	s.unindex(slot)
	s.count--
	return true
}

// Reset empties the set. The counts of a set that counts need no clearing: a
// hash that is added starts with a count of its own.
func (s *Set) Reset() {
	clear(s.ring)
	clear(s.index)
	s.next, s.count = 0, 0
}

// find returns the slot of the index that refers to the hash.
func (s *Set) find(hash uint64) (slot uint32, ok bool) {
	for i := s.home(hash); ; i = s.after(i) {
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
	index := s.index

	// Every entry between the hole and the next empty slot moves back into
	// the hole if that keeps it at or after its home slot. Distances are
	// taken around the end of the table.
	for i := s.after(hole); index[i] != 0; i = s.after(i) {
		home := s.home(s.ring[index[i]-1])
		fromHome, fromHole := i-home, i-hole
		if i < home {
			fromHome += s.size
		}
		if i < hole {
			fromHole += s.size
		}
		if fromHome >= fromHole {
			index[hole] = index[i]
			hole = i
		}
	}
	index[hole] = 0
}

// home returns the slot of the index that the probe for a hash starts at.
func (s *Set) home(hash uint64) uint32 {
	return uint32(uint64(uint32(hash)) * uint64(s.size) >> 32)
}

// after returns the slot that follows slot i, the first one after the last.
func (s *Set) after(i uint32) uint32 {
	i++
	if i == s.size {
		return 0
	}
	return i
}
