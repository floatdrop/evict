// Package cslab is the concurrent sibling of package slab. It stores entries
// the same way, in a fixed array of nodes with a hash index and intrusive
// lists, and adds one ability: goroutines can look entries up and read them
// while another goroutine changes the slab, without taking a lock.
//
// # How reading without a lock is safe
//
// A reader may only touch what cannot change under it. Three rules see to
// that:
//
//   - The index is made of atomic words, and the reader finds nodes only
//     through it. Of a node it looks at nothing but the key, the value and
//     the Meta, which is atomic as well.
//   - The key and the value of a node are written before the node enters the
//     index and are left alone for as long as a reader might hold the node.
//     Replacing a value therefore means moving the entry to another node.
//   - A reader says which node it is going to look at before it does, in a
//     slot of a [Readers], and then makes sure that the node is still in the
//     index. A node that leaves the index is not reused at once: it waits,
//     and when the slab comes for it, it passes over the nodes that the slots
//     name. A node that was in the index after a reader had named it cannot
//     have been passed for reuse before.
//
// A reader that is held up in the middle of all this therefore keeps one node
// waiting and is in nobody's way otherwise. It is not held up easily, either:
// Go takes the CPU away from a goroutine that keeps computing when it calls a
// function, and between pinning a node and letting go of it a reader calls
// none. It does not even compare keys there, which for some types of keys is
// a call: it copies what it finds and compares afterwards.
//
// Changing a slab is not concurrent: writers serialize among themselves. The
// methods say which side they are for.
//
// The words that readers touch are plain integers that everyone accesses with
// the functions of sync/atomic, not with the methods of its types. The code of
// a cache is generic, so it is compiled in the package that instantiates it.
// Where that package gets to the cache through a generic type of another
// package that instantiates nothing, the compiler does not inline those
// methods, only the functions, which it builds in. The 64-bit words are in
// slices, which keeps them aligned on 32-bit platforms.
package cslab

import (
	"math"
	"runtime"
	"sync/atomic"
	"unsafe"
)

// MaxCapacity is the largest capacity [Slab.Init] accepts. It keeps the index,
// which has three slots for every entry, addressable with 32 bits.
const MaxCapacity = 1 << 30

// minSpare is the least number of nodes that a slab has beyond its capacity.
const minSpare = 16

// Node is what a reader gets to see of an entry of a [Slab].
type Node[K comparable, V any] struct {
	// meta belongs to the owner of the slab; see Meta.
	meta uint32 //nolint:modernize // See the package documentation.

	// Key and Value can be read by anyone who found the node through the
	// index, and are never written while that is possible.
	Key   K
	Value V
}

// Meta returns the word of the node that belongs to the owner of the slab.
// Insert resets it to zero and Replace carries it over. Readers may load and
// store it.
func (n *Node[K, V]) Meta() uint32 { return atomic.LoadUint32(&n.meta) }

// SetMeta sets the word that Meta returns.
func (n *Node[K, V]) SetMeta(meta uint32) { atomic.StoreUint32(&n.meta, meta) }

// Link is the part of an entry that is of use to writers only. It is kept
// apart from the node, in memory that readers never look at: a writer that
// moves an entry within a list changes the links of its neighbors, and that
// should not take their nodes away from the CPUs that are reading them.
type Link struct {
	// Prev and Next link the entry into a circular list. For a list head
	// they are the last and the first entry of the list, and the head
	// itself when the list is empty.
	Prev, Next uint32

	// hash is what the node is indexed under.
	hash uint32

	// Tag belongs to the owner of the slab, like Node.Meta, but is out of
	// the readers' sight and need not be atomic. Insert resets it to zero
	// and Replace carries it over.
	Tag uint32
}

// A node that has left the index keeps its key and its value until it is
// reused, but its link is of no further use. Next queues it up with the other
// nodes that are waiting, Prev says that it is waiting, with a value that is
// not the index of any node, and the other two fields hold the time at which
// it left, as the Readers count time.
const (
	isWaiting uint32 = math.MaxUint32 - iota
	isPinned         // Waiting, and a reader was found looking at it.
)

func (l *Link) setLeft(at uint64) {
	l.hash, l.Tag = uint32(at>>32), uint32(at)
}

func (l *Link) left() uint64 {
	return uint64(l.hash)<<32 | uint64(l.Tag)
}

// Slab is a fixed set of nodes that can be found by key.
//
// The first nodes are list heads, as many as [Slab.Init] was asked for. They
// never hold an entry; the owner hands their indices to the list methods.
// Because there is always at least one, index 0 never refers to an entry and
// serves as "no node".
type Slab[K comparable, V any] struct {
	// Nodes holds the list heads followed by the entries. There are more
	// nodes than the capacity asks for, so that there is one to be had
	// while others wait to be reused.
	Nodes []Node[K, V]

	// slots is the hash index: a linear-probing table of size slots. A slot
	// holds the hash of an entry in its high half and the index of the
	// entry's node in its low half. Zero is an empty slot: no entry lives in
	// node 0. An entry's probe starts at the slot that its hash scaled to
	// size points at, which spreads the hashes over a table of any size.
	slots []uint64
	size  uint32

	// readers is where a reader pins the node it looks at, and name is what
	// the nodes of this slab are called there, less their index.
	readers *Readers
	name    uint64

	// seq is odd while entries are being taken out of the index, which
	// moves other entries to different slots. A reader that did not find
	// its key compares seq before and after to know whether the key might
	// have been moved past it. It may wrap around: a reader would have to
	// sleep through 2³¹ removals to be fooled, and would then miss a key.
	seq uint32 //nolint:modernize // See the package documentation.

	// overdue is one while more nodes are waiting than the slab has
	// patience for, which happens when readers were looking at them the
	// last time the slab came for them. See Overdue.
	overdue uint32

	// The fields above are what a reader needs, and they change only when
	// an entry is removed. The ones below are for writers, and most change
	// with every write, which should not cost the readers the cache line
	// they keep coming back to.
	_ [cacheLine]byte

	// Links holds a link for every node, under the same index.
	Links []Link

	capacity uint32
	heads    uint32 // number of list heads

	// count is the number of entries. Only writers change it.
	count atomic.Int32

	free  uint32 // most recently reclaimed node, linked through Next; 0 if none
	fresh uint32 // lowest index that was never handed out

	// The nodes that have left the index and wait to be reused, oldest
	// first, linked through Next.
	limbo, limboTail uint32
	waiting          uint32 // how many there are
	patience         uint32 // how many there may be before the slab comes for them
	retryAt          uint32 // how many there have to be before it comes again, after it had to leave some
	late             bool   // what overdue was last set to
	waits            uint64 // how often a writer found every spare node pinned
}

// Init readies the slab for capacity entries and the given number of list
// heads, and leaves every list empty. Readers of the slab pin their nodes in
// r, which the slabs of one cache share; they must be readied one after the
// other. Init panics unless there is at least one head and capacity lies in
// [0, MaxCapacity].
func (s *Slab[K, V]) Init(capacity, heads int, r *Readers) {
	if heads < 1 || capacity < 0 || capacity > MaxCapacity {
		panic("cslab: invalid capacity or number of list heads")
	}

	// A node that is taken out of use may have a reader looking at it, and
	// finding out means looking at every slot of Readers. Spare nodes let
	// that happen once for many nodes.
	//
	// The waiting nodes hold on to their keys and values, though, so there
	// may not be many of them in comparison to the entries. A small slab
	// has more spare nodes than it lets wait: it is stuck when readers that
	// are held up have every one of them pinned.
	spare, patience := 0, 0
	if capacity > 0 {
		patience = min(max(capacity/8, 1), 1024)
		spare = max(patience, minSpare)
	}

	// Three slots for every entry that can be indexed keep the load factor
	// at or below a third, whatever the capacity, and guarantee an empty
	// slot, which is what ends an unsuccessful probe.
	size := max(3*uint(capacity), 1)

	s.Nodes = make([]Node[K, V], heads+capacity+spare)
	s.Links = make([]Link, len(s.Nodes))
	s.slots = make([]uint64, size)
	s.size = uint32(size)
	s.readers = r
	s.name = r.name()
	s.capacity = uint32(capacity)
	s.heads = uint32(heads)
	s.patience = uint32(patience)
	s.fresh = s.heads
	for i := range s.heads {
		s.Links[i].Prev, s.Links[i].Next = i, i
	}
}

// Len returns the number of entries in the slab. Anyone may call it.
func (s *Slab[K, V]) Len() int { return int(s.count.Load()) }

// Cap returns the number of entries the slab can hold. Anyone may call it.
func (s *Slab[K, V]) Cap() int { return int(s.capacity) }

// Hash returns the hash that the entry in node idx is indexed under. It is
// for writers.
func (s *Slab[K, V]) Hash(idx uint32) uint32 { return s.Links[idx].hash }

// Lookup returns the value that is stored under key. The hash must be the one
// the key was inserted with. Before it returns the value, Lookup counts the
// read in the Meta of the node, unless the Meta has reached limit already: a
// limit of zero leaves it alone. The Meta of other nodes is as it was. The
// exception is a node whose key has the same hash while someone else changes
// its Meta at that very moment: that one may come out one too high or one
// too low, though never above the limit.
//
// If tally is set, a lookup that finds its key also adds one to the hits of
// the slot it pinned the node with, which is a count that, unlike the Meta of
// a node, knows no limit; see [Readers.Hits]. A lookup that finds nothing, or
// does not get to pin anything, adds nothing.
//
// Lookup is for readers. If it finds no value, certain says whether the key
// is known to be absent. It is not if entries were on the move in the
// meantime, or if no slot was to be had for pinning a node. The caller should
// then ask again as a writer, with [Slab.Find].
func (s *Slab[K, V]) Lookup(hash uint32, key K, limit uint32, tally bool) (value V, found, certain bool) {
	seq := atomic.LoadUint32(&s.seq)

	slots, size := s.slots, s.size
	i := uint32(uint64(hash) * uint64(size) >> 32)
	// The index always has an empty slot, but with entries moving around a
	// reader is not certain to see one. Give up after one round.
	for range slots {
		word := atomic.LoadUint64(&slots[i])
		if word == 0 {
			return value, false, seq&1 == 0 && atomic.LoadUint32(&s.seq) == seq
		}
		if word>>32 != uint64(hash) {
			i++
			if i == size {
				i = 0
			}
			continue
		}

		node := s.name | word&math.MaxUint32
		slot := s.pin(node)
		if slot == nil {
			if slot = s.pinAny(node); slot == nil {
				return value, false, false
			}
		}
		// The pin counts only if the node was still in the index when it
		// was in place. If the slot of the index has changed, look at it
		// again; this does not go on forever, see above.
		if atomic.LoadUint64(&slots[i]) != word {
			atomic.StoreUint64(&slot.pin, 0)
			continue
		}

		// While the node is pinned, nothing here calls a function: that
		// is where a goroutine may have to make way for another, and the
		// node would stay pinned until its turn came again. So the key is
		// not compared here but copied, like the value. The read has to
		// be counted here all the same, where the node is sure to be the
		// one that was found, and so it is counted before the key is known
		// to be the right one.
		n := &s.Nodes[uint32(word)]
		f := n.Meta()
		if f < limit {
			n.SetMeta(f + 1)
		}
		k := n.Key
		value = n.Value
		atomic.StoreUint64(&slot.pin, 0)

		if k == key {
			// The slot is no longer this reader's, but the count is
			// atomic, and its line is still in this CPU's cache.
			if tally {
				atomic.AddUint64(&slot.hits, 1)
			}
			return value, true, true
		}
		// The node belongs to another key with the same hash, which is
		// rare, and the read that was counted was not a read of that key.
		// It is taken back, unless the count has changed since: without
		// that, which entries a cache keeps would depend on which keys
		// happen to share a hash, and so on the seed of the hash.
		//
		// The node is no longer pinned here, and nothing but a count is
		// written to it. If another reader counted a read of its own in
		// between and found the count at the limit already, that read is
		// lost; and if the node has gone to another entry by now, with a
		// count of just f+1, it is that entry which loses a read. Both
		// take a shared hash and a race on top, and cost one entry one
		// read.
		if f < limit {
			atomic.CompareAndSwapUint32(&n.meta, f+1, f)
		}
		var zero V
		value = zero
		i++
		if i == size {
			i = 0
		}
	}
	return value, false, false
}

// Pin pins node idx the way a reader does that has found it, and returns the
// slot to clear, or nil if there is none to be had. It is for writers, who
// know that the node is in the index, and for tests of what a slab does while
// a reader is held up.
func (s *Slab[K, V]) Pin(idx uint32) *Slot {
	node := s.name | uint64(idx)
	if slot := s.pin(node); slot != nil {
		return slot
	}
	return s.pinAny(node)
}

// pin announces that the caller is about to look at a node, and returns the
// slot of the readers that says so. The node is safe to look at only if it is
// in the index after pin has returned: a slab that took the node out earlier
// may not have seen the pin. The caller clears the slot when it is done with
// the node, by storing zero in its pin.
//
// pin returns nil if the slot that the caller prefers is taken, which is
// rare. The caller should then try pinAny.
func (s *Slab[K, V]) pin(node uint64) *Slot {
	slot := &s.readers.slots[s.preferred()]
	if atomic.CompareAndSwapUint64(&slot.pin, 0, node) {
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
func (s *Slab[K, V]) preferred() uint64 {
	var onStack byte
	//nolint:gosec // The address is only hashed, never turned back into a pointer.
	stack := uint64(uintptr(unsafe.Pointer(&onStack))) >> 11
	return (stack * 0x9e3779b97f4a7c15) >> s.readers.shift
}

// pinAny is pin for a caller whose preferred slot is taken: it tries the
// slots after that one. It returns nil if those are taken as well, and the
// caller then has to do its reading under the lock of the slab instead.
//
// It is a function of its own so that pin is small enough to be inlined.
func (s *Slab[K, V]) pinAny(node uint64) *Slot {
	slots := s.readers.slots
	i, mask := s.preferred(), uint64(len(slots)-1)
	for range 3 {
		i = (i + 1) & mask
		if slot := &slots[i]; atomic.CompareAndSwapUint64(&slot.pin, 0, node) {
			return slot
		}
	}
	return nil
}

// Find returns the index of the node that holds key, or 0 if there is none.
// It is for writers.
func (s *Slab[K, V]) Find(hash uint32, key K) uint32 {
	slots, size := s.slots, s.size
	for i := uint32(uint64(hash) * uint64(size) >> 32); ; {
		slot := atomic.LoadUint64(&slots[i])
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
// the index of the node. The node is not part of any list yet, and its Meta
// is zero. Insert is for writers.
//
// The caller must have established that key is absent. Insert panics if the
// slab is full.
func (s *Slab[K, V]) Insert(hash uint32, key K, value V) uint32 {
	if s.count.Load() >= int32(s.capacity) {
		panic("cslab: insert into a full slab")
	}

	idx := s.alloc()
	s.Links[idx].hash, s.Links[idx].Tag = hash, 0
	n := &s.Nodes[idx]
	n.SetMeta(0)
	n.Key = key
	n.Value = value

	slots, size := s.slots, s.size
	i := uint32(uint64(hash) * uint64(size) >> 32)
	for atomic.LoadUint64(&slots[i]) != 0 {
		i++
		if i == size {
			i = 0
		}
	}
	// From here on readers can find the node.
	atomic.StoreUint64(&slots[i], uint64(hash)<<32|uint64(idx))
	s.count.Add(1)

	return idx
}

// Replace gives the entry in node idx a new value, along with a key that is
// equal to the one it has. Readers may be looking at the node, so the entry
// moves to another one, which takes over the place in the index and in the
// list as well as the Meta. Replace returns the index of that node; idx no
// longer refers to the entry. It is for writers.
func (s *Slab[K, V]) Replace(idx uint32, key K, value V) uint32 {
	to := s.alloc()
	old, n := &s.Nodes[idx], &s.Nodes[to]
	n.SetMeta(old.Meta())
	n.Key = key
	n.Value = value

	link := s.Links[idx]
	s.Links[to] = link
	s.Links[link.Prev].Next = to
	s.Links[link.Next].Prev = to

	// From here on readers find the new node. The ones that found the old
	// one keep reading the old value.
	atomic.StoreUint64(&s.slots[s.slotOf(link.hash, idx)], uint64(link.hash)<<32|uint64(to))

	s.retire(idx)
	return to
}

// Remove drops the entry in node idx from the index. The caller must have
// unlinked the node from its list. Remove is for writers.
//
// The node keeps its key and its value, and with them whatever they refer to,
// until the slab comes for the nodes that have left: when more of them are
// waiting than it has patience for, or when it is asked to; see
// [Slab.Reclaim].
func (s *Slab[K, V]) Remove(idx uint32) {
	atomic.AddUint32(&s.seq, 1)
	s.unindex(s.Links[idx].hash, idx)
	atomic.AddUint32(&s.seq, 1)

	s.count.Add(-1)
	s.retire(idx)
}

// slotOf returns the slot that refers to node idx. It panics if the node holds
// no entry.
func (s *Slab[K, V]) slotOf(hash, idx uint32) uint32 {
	slots, size := s.slots, s.size
	for i := uint32(uint64(hash) * uint64(size) >> 32); ; {
		slot := atomic.LoadUint64(&slots[i])
		if slot == 0 {
			panic("cslab: node is not indexed")
		}
		if slot&(1<<32-1) == uint64(idx) {
			return i
		}
		i++
		if i == size {
			i = 0
		}
	}
}

// unindex empties the slot that refers to node idx, then closes the gap so
// that probe sequences running through it stay unbroken.
func (s *Slab[K, V]) unindex(hash, idx uint32) {
	slots, size := s.slots, s.size
	hole := s.slotOf(hash, idx)

	// Every entry between the hole and the next empty slot moves back into
	// the hole if that keeps it at or after its home slot. Distances are
	// taken around the end of the table.
	for i := hole; ; {
		i++
		if i == size {
			i = 0
		}
		slot := atomic.LoadUint64(&slots[i])
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
			atomic.StoreUint64(&slots[hole], slot)
			hole = i
		}
	}
	atomic.StoreUint64(&slots[hole], 0)
}

// retire queues up a node that has just left the index, and lets go of the
// waiting nodes if there are too many of them.
func (s *Slab[K, V]) retire(idx uint32) {
	s.enqueue(idx)
	// Looking for readers is not free. If it left the slab with more nodes
	// than it has patience for, because readers had them pinned, the slab
	// waits for some more to come in before it looks again.
	if s.waiting > s.patience && s.waiting >= s.retryAt {
		s.reclaim(forRoom)
	}
}

// enqueue adds a node that has just left the index to the nodes that wait to
// be reused.
func (s *Slab[K, V]) enqueue(idx uint32) {
	l := &s.Links[idx]
	l.Prev, l.Next = isWaiting, 0
	// The time is read after the node has left: a look at the slots that
	// begins later cannot miss a reader of this node.
	l.setLeft(s.readers.time.Load())

	if s.limbo == 0 {
		s.limbo = idx
	} else {
		s.Links[s.limboTail].Next = idx
	}
	s.limboTail = idx
	s.waiting++
}

// Overdue reports whether the slab is holding on to more entries that are
// gone than it should, because readers were looking at them when it last
// tried to let go. Anyone may call it. Whoever finds it true can help by
// calling [Slab.Reclaim] as a writer: the slab itself tries again only when
// it is written to, which may not be any time soon.
func (s *Slab[K, V]) Overdue() bool { return atomic.LoadUint32(&s.overdue) != 0 }

// Reclaim releases the keys and the values of the entries that have left and
// that no reader is looking at. It is for writers.
func (s *Slab[K, V]) Reclaim() { s.reclaim(forSure) }

// Released does what Reclaim does and reports whether the slab has let go of
// every entry that had left it by the time of mark, which is what
// [Slab.Reset] returns. It is for writers, and for the moments when holding
// on to what was removed is not acceptable: calling it until it says so waits
// for the readers that are in the way.
func (s *Slab[K, V]) Released(mark uint64) bool {
	s.reclaim(forSure)
	return s.limbo == 0 || s.Links[s.limbo].left() > mark
}

// Waits returns how often a writer to the slab has had to wait for a reader
// because every spare node was pinned. It is for writers, and for tests: in
// a program it stays at zero.
func (s *Slab[K, V]) Waits() uint64 { return s.waits }

// alloc returns a node that nobody is using.
func (s *Slab[K, V]) alloc() uint32 {
	for {
		if idx := s.free; idx != 0 {
			s.free = s.Links[idx].Next
			return idx
		}
		if int(s.fresh) < len(s.Nodes) {
			idx := s.fresh
			s.fresh++
			return idx
		}
		// Every node is either in use or waiting. There are more nodes than
		// entries, so some are waiting. If readers have every one of those
		// pinned, enough of them are held up to use up the spare nodes,
		// and the slab has to let them get on.
		if !s.reclaim(forOne) {
			s.waits++
			runtime.Gosched()
		}
	}
}

// What a slab reclaims its waiting nodes for.
const (
	// forRoom: more nodes are waiting than the slab has patience for.
	forRoom = iota
	// forOne: a node is needed and none is free.
	forOne
	// forSure: whatever no reader is looking at right now has to go.
	forSure
)

// reclaim releases the key and the value of the waiting nodes that no reader
// is looking at and makes those nodes available again. It reports whether
// there were any.
//
// Finding out what the readers are looking at means going through all of
// their slots, which takes the longer the more CPUs there are. But what one
// slab has found out holds for all the slabs that share the slots, for the
// nodes that had left by then. So unless it is to be sure, a slab first makes
// do with the latest look that another one has taken, and looks for itself
// only if that did not get it what it came for: a node, if it needs one, and
// in any case no more nodes waiting than it has patience for.
func (s *Slab[K, V]) reclaim(what int) bool {
	if s.limbo == 0 {
		return false
	}

	released := what != forSure && s.borrow()
	if what == forSure || what == forOne && !released || s.waiting > s.patience {
		released = s.look() || released
	}

	late := s.waiting > s.patience
	s.retryAt = 0
	if late {
		s.retryAt = s.waiting + max(s.patience/2, 1)
	}
	// The flag is on a cache line that readers keep coming back to, so it
	// is written only when it changes.
	if late != s.late {
		s.late = late
		var overdue uint32
		if late {
			overdue = 1
		}
		atomic.StoreUint32(&s.overdue, overdue)
	}
	return released
}

// look goes through the slots of the readers, releases the waiting nodes
// that none of them names, and leaves what it has seen for the other slabs.
func (s *Slab[K, V]) look() bool {
	r := s.readers

	// A reader looks at a node only if the node was in the index after the
	// reader had pinned it. A node that had left the index before this
	// moment is therefore pinned by now, or it is not being looked at.
	now := r.time.Add(1)

	var (
		pins  [sharedPins]uint64
		found int
	)
	for i := range r.slots {
		pin := atomic.LoadUint64(&r.slots[i].pin)
		if pin == 0 {
			continue
		}
		if found < len(pins) {
			pins[found] = pin
		}
		found++
		if pin&^math.MaxUint32 == s.name {
			s.mark(uint32(pin), now)
		}
	}
	r.share(now, pins[:min(found, len(pins))], found > len(pins))

	return s.release(now)
}

// borrow releases the waiting nodes that the latest look of another slab
// shows to be free, if it shows any.
func (s *Slab[K, V]) borrow() bool {
	r := s.readers

	// The look is being written down if the version is odd, and it has
	// been replaced by another if the version has changed by the end.
	version := r.version.Load()
	if version&1 != 0 {
		return false
	}
	at := r.sharedAt.Load()
	if s.Links[s.limbo].left() >= at {
		return false // Nothing here had left by then.
	}
	found := r.sharedFound.Load()
	if found < 0 {
		return false // There were more pins than a look writes down.
	}
	var (
		mine [sharedPins]uint32
		n    int
	)
	for i := range found {
		if pin := r.shared[i].Load(); pin&^math.MaxUint32 == s.name {
			mine[n] = uint32(pin)
			n++
		}
	}
	if r.version.Load() != version {
		return false
	}

	for _, idx := range mine[:n] {
		s.mark(idx, at)
	}
	return s.release(at)
}

// mark notes that a reader was found looking at node idx, if the node is
// among those that are waiting and had left before the look was taken.
func (s *Slab[K, V]) mark(idx uint32, looked uint64) {
	if l := &s.Links[idx]; l.Prev == isWaiting && l.left() < looked {
		l.Prev = isPinned
	}
}

// release frees the waiting nodes that had left before a look was taken and
// that have not been marked as pinned. The nodes wait in the order in which
// they left, so those are the first ones. The marked ones stay, in their
// places.
func (s *Slab[K, V]) release(looked uint64) bool {
	links := s.Links
	var (
		zeroKey   K
		zeroValue V
	)

	released := false
	var kept, keptTail uint32
	idx := s.limbo
	for idx != 0 && links[idx].left() < looked {
		l := &links[idx]
		next := l.Next

		if l.Prev == isPinned {
			l.Prev, l.Next = isWaiting, 0
			if kept == 0 {
				kept = idx
			} else {
				links[keptTail].Next = idx
			}
			keptTail = idx
		} else {
			n := &s.Nodes[idx]
			n.Key, n.Value = zeroKey, zeroValue
			l.Prev, l.Next = 0, s.free
			s.free = idx
			s.waiting--
			released = true
		}
		idx = next
	}

	// What was kept goes in front of what was not looked at.
	if kept != 0 {
		links[keptTail].Next = idx
		if idx == 0 {
			s.limboTail = keptTail
		}
		idx = kept
	} else if idx == 0 {
		s.limboTail = 0
	}
	s.limbo = idx
	return released
}

// MaxDisplacement returns the largest distance between the slot an entry is
// indexed in and the slot its hash asks for. It walks the whole index and is
// meant for tests, run while nobody is writing: a large value means that
// hashes collide more than they should, and that lookups pay for it.
func (s *Slab[K, V]) MaxDisplacement() int {
	longest := uint32(0)
	for i := range s.slots {
		slot := atomic.LoadUint64(&s.slots[i])
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

// Reset removes every entry and empties every list. Every entry must be in a
// list. Reset is for writers. The keys and values of the entries stay around
// for now, as with Remove: whoever wants them gone must go on to call
// [Slab.Released] with the mark that Reset returns, before any other writer
// gets a turn.
func (s *Slab[K, V]) Reset() (mark uint64) {
	atomic.AddUint32(&s.seq, 1)
	for i := range s.slots {
		atomic.StoreUint64(&s.slots[i], 0)
	}
	atomic.AddUint32(&s.seq, 1)
	s.count.Store(0)

	for head := range s.heads {
		for idx := s.Links[head].Next; idx != head; {
			next := s.Links[idx].Next
			s.enqueue(idx)
			idx = next
		}
		s.Links[head].Prev, s.Links[head].Next = head, head
	}
	// Nothing has left later than now. The look that Released takes moves
	// the time on, so whatever leaves after that is told apart.
	return s.readers.time.Load()
}

// PushFront links node idx in as the first node of the list at head. It is
// for writers.
func (s *Slab[K, V]) PushFront(head, idx uint32) {
	links := s.Links
	first := links[head].Next
	links[idx].Prev, links[idx].Next = head, first
	links[first].Prev = idx
	links[head].Next = idx
}

// Unlink takes node idx out of the list it is in. It is for writers.
func (s *Slab[K, V]) Unlink(idx uint32) {
	links := s.Links
	prev, next := links[idx].Prev, links[idx].Next
	links[prev].Next = next
	links[next].Prev = prev
}

// The methods below are compiled in the packages that use the caches. So that
// the compiler can inline what they call there, such as the operations on
// atomic values, it has to have inlined those calls in this package first;
// see the note in package lru. Instantiating the methods here sees to that.
var (
	_ = (*Slab[int, int]).Lookup
	_ = (*Slab[int, int]).Find
	_ = (*Slab[int, int]).Insert
	_ = (*Slab[int, int]).Replace
	_ = (*Slab[int, int]).Remove
	_ = (*Slab[int, int]).Released
	_ = (*Slab[int, int]).Reclaim
	_ = (*Slab[int, int]).Overdue
	_ = (*Slab[int, int]).Hash
	_ = (*Slab[int, int]).Reset
	_ = (*Slab[int, int]).Len
)
