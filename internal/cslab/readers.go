package cslab

import (
	"math/bits"
	"runtime"
	"sync/atomic"
	"unsafe"
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
	pin atomic.Uint64
	_   [cacheLine - 8]byte
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

// Pin announces that the caller is about to look at a node, and returns the
// slot that says so. The node is safe to look at only if it is in the index
// after Pin has returned: a slab that took the node out earlier may not have
// seen the pin. The caller clears the slot with [Slot.Unpin] when it is done
// with the node.
//
// Pin returns nil if the slot that the caller prefers is taken, which is
// rare. The caller should then try [Readers.PinAny].
func (r *Readers) Pin(node uint64) *Slot {
	slot := &r.slots[r.preferred()]
	if slot.pin.CompareAndSwap(0, node) {
		return slot
	}
	return nil
}

// preferred returns the slot that the calling goroutine goes for first.
//
// A goroutine picks the same slot every time, which keeps the slot in the
// cache of the CPU it runs on. There is no way to ask which goroutine or CPU
// this is, but the whereabouts of the stack tell one goroutine from another:
// stacks are at least 2 KiB in size. The bits of the address below that say
// how deep into its stack a goroutine is, and are dropped.
func (r *Readers) preferred() uint64 {
	var onStack byte
	//nolint:gosec // The address is only hashed, never turned back into a pointer.
	stack := uint64(uintptr(unsafe.Pointer(&onStack))) >> 11
	return (stack * 0x9e3779b97f4a7c15) >> r.shift
}

// PinAny is Pin for a caller whose preferred slot is taken: it tries the
// slots after that one. It returns nil if those are taken as well, and the
// caller then has to do its reading under the lock of the slab instead.
//
// It is a function of its own so that Pin is small enough to be inlined.
func (r *Readers) PinAny(node uint64) *Slot {
	i, mask := r.preferred(), uint64(len(r.slots)-1)
	for range 3 {
		i = (i + 1) & mask
		if slot := &r.slots[i]; slot.pin.CompareAndSwap(0, node) {
			return slot
		}
	}
	return nil
}

// Unpin clears the slot. The node it named is no longer safe to look at.
func (s *Slot) Unpin() { s.pin.Store(0) }

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
		if slot := &r.slots[i]; slot.pin.CompareAndSwap(0, 1) {
			taken = append(taken, slot)
		}
	}
	return func() {
		for _, slot := range taken {
			slot.Unpin()
		}
	}
}
