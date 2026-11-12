package cslab

import (
	"math/bits"
	"runtime"
	"sync/atomic"
)

// cacheLine is a size that no two CPUs' cache lines straddle on the
// platforms Go runs on.
const cacheLine = 128

// Readers is where the goroutines that read from the slabs of one cache
// without holding a lock say which node they are looking at, so that a slab
// does not reuse a node under a reader's eyes.
//
// A reader pins a node by writing its name into a slot, and clears the slot
// when it is done. Writing to a slot that few other goroutines use is all
// that reading costs; in particular, readers do not write to anything that
// all of them share, which is what lets them scale with the CPUs.
//
// A reader that is held up while it has a node pinned, because its goroutine
// has to make way for another one, keeps that one node from being reused and
// nothing else.
type Readers struct {
	// Nothing in here changes once it is set up. The padding keeps whatever
	// comes to lie next to it in memory, which may change all the time, off
	// the cache line that every reader loads.
	_ [cacheLine]byte

	slots []Slot
	shift uint8  // 64 minus log2 of the number of slots
	slabs uint32 // how many slabs have been given a name

	_ [cacheLine]byte

	// time counts the looks that slabs have taken at the slots. A slab
	// notes it for every node that leaves its index, and so knows which
	// nodes had left before a look began. Readers have no use for it.
	time atomic.Uint64

	_ [cacheLine - 8]byte

	// The latest look, for the slabs that did not take it themselves: the
	// time at which it began, how many pins it found, or -1 if they were
	// too many to write down, and the pins. The version is odd while this
	// is being written.
	version     atomic.Uint64
	sharedAt    atomic.Uint64
	sharedFound atomic.Int32
	shared      [sharedPins]atomic.Uint64

	_ [cacheLine]byte
}

// sharedPins is the number of pins that a look at the slots writes down for
// other slabs. Pins are held for a few nanoseconds at a time, so a look
// rarely finds more than a handful.
const sharedPins = 16

// share leaves what a look that began at the given time has found for the
// other slabs, unless a later one is there already or someone else is just
// writing theirs down, in which case it can be done without.
func (r *Readers) share(at uint64, pins []uint64, tooMany bool) {
	version := r.version.Load()
	if version&1 != 0 || !r.version.CompareAndSwap(version, version+1) {
		return
	}
	if at > r.sharedAt.Load() {
		r.sharedAt.Store(at)
		found := int32(len(pins))
		if tooMany {
			found = -1
		}
		r.sharedFound.Store(found)
		for i, pin := range pins {
			r.shared[i].Store(pin)
		}
	}
	r.version.Store(version + 2)
}

// Slot is where one reader at a time pins a node.
type Slot struct {
	// pin is zero while the slot is free, and otherwise the name of the node
	// that its reader is looking at: the number of the slab in the upper
	// half, which is never zero, and the index of the node in the lower.
	pin uint64 //nolint:modernize // See the package documentation.

	// hits counts the lookups that found their key with this slot and were
	// asked to say so; see [Slab.Lookup]. Whoever has used the slot has
	// added to it. It is in the line that a reader writes to anyway.
	hits uint64 //nolint:modernize // See the package documentation.

	_ [cacheLine - 16]byte
}

// NewReaders returns a Readers for a new cache. Its slots take up a few
// dozen kilobytes: 128 bytes for each of sixteen slots per CPU.
func NewReaders() *Readers {
	// Sixteen slots per CPU make it unlikely that two goroutines that run
	// at the same time prefer the same slot.
	return newReaders(16 * runtime.GOMAXPROCS(0))
}

// newReaders returns a Readers with at least n slots.
func newReaders(n int) *Readers {
	n = 1 << bits.Len(uint(n-1))
	r := &Readers{
		slots: make([]Slot, n),
		shift: uint8(64 - bits.TrailingZeros(uint(n))),
	}
	// Time starts at one, so that zero is before anything has happened.
	r.time.Store(1)
	return r
}

// name returns what the nodes of a new slab are called in the slots, less
// their index.
func (r *Readers) name() uint64 {
	r.slabs++
	return uint64(r.slabs) << 32
}

// Unpin clears the slot. The node it named is no longer safe to look at.
func (s *Slot) Unpin() { atomic.StoreUint64(&s.pin, 0) }

// Hits returns the number of lookups, by any slab of this Readers, that have
// found their key and were asked to tally it. It looks at every slot, which
// takes a while: a slot to every 128 bytes that [NewReaders] speaks of. A
// lookup that is under way may or may not have been counted.
func (r *Readers) Hits() uint64 {
	var hits uint64
	for i := range r.slots {
		hits += atomic.LoadUint64(&r.slots[i].hits)
	}
	return hits
}

// Occupy takes the slots that are free and returns a function that gives them
// back. In between, nobody can pin a node, and whoever wants to read has to
// do it under a lock. If leave is not zero, one slot in every so many is left
// alone, so that some readers find a slot at once, some after a while, and
// some not at all. Occupy is for tests of all that.
func (r *Readers) Occupy(leave int) (release func()) {
	var taken []*Slot
	for i := range r.slots {
		if leave != 0 && i%leave == 0 {
			continue
		}
		// No slab goes by the name of zero, so this pins nothing.
		if slot := &r.slots[i]; atomic.CompareAndSwapUint64(&slot.pin, 0, 1) {
			taken = append(taken, slot)
		}
	}
	return func() {
		for _, slot := range taken {
			slot.Unpin()
		}
	}
}
