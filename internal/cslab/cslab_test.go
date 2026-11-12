package cslab

import (
	"math/bits"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// check verifies that the index, the nodes and the bookkeeping of a slab that
// nobody else is using agree with each other and with want.
func check[K, V comparable](t *testing.T, s *Slab[K, V], want map[K]V) {
	t.Helper()

	if s.Len() != len(want) {
		t.Fatalf("Len() = %d, want %d", s.Len(), len(want))
	}
	if atomic.LoadUint32(&s.seq)&1 != 0 {
		t.Fatalf("seq = %d is odd while nothing is being removed", atomic.LoadUint32(&s.seq))
	}

	indexed := make(map[uint32]bool, len(want))
	for i := range s.slots {
		slot := atomic.LoadUint64(&s.slots[i])
		if slot == 0 {
			continue
		}
		hash, idx := uint32(slot>>32), uint32(slot)
		if idx < s.heads || idx >= s.fresh {
			t.Fatalf("slot %d refers to node %d outside [%d, %d)", i, idx, s.heads, s.fresh)
		}
		if indexed[idx] {
			t.Fatalf("node %d is indexed twice", idx)
		}
		indexed[idx] = true

		n := &s.Nodes[idx]
		if got := s.Links[idx].hash; got != hash {
			t.Fatalf("slot %d has hash %#x, node %d has %#x", i, hash, idx, got)
		}
		if value, ok := want[n.Key]; !ok || n.Value != value {
			t.Fatalf("node %d holds %v=%v, want %v, %t", idx, n.Key, n.Value, value, ok)
		}
		if got := s.Find(hash, n.Key); got != idx {
			t.Fatalf("Find(%v) = %d, want %d", n.Key, got, idx)
		}
		if got, found, certain := s.Lookup(hash, n.Key, 0, false); got != n.Value || !found || !certain {
			t.Fatalf("Lookup(%v) = %v, %t, %t, want %v", n.Key, got, found, certain, n.Value)
		}
	}
	if len(indexed) != len(want) {
		t.Fatalf("%d nodes are indexed, want %d", len(indexed), len(want))
	}

	// Every node that was handed out is in use, waiting, or free.
	var (
		zeroKey   K
		zeroValue V
	)
	free, waiting := 0, 0
	for idx := s.free; idx != 0; idx = s.Links[idx].Next {
		if n := &s.Nodes[idx]; indexed[idx] || n.Key != zeroKey || n.Value != zeroValue {
			t.Fatalf("free node %d is indexed (%t) or holds %v=%v", idx, indexed[idx], n.Key, n.Value)
		}
		if s.Links[idx].Prev >= isPinned {
			t.Fatalf("free node %d is marked as waiting", idx)
		}
		free++
		if free > len(s.Nodes) {
			t.Fatal("the free list has a cycle")
		}
	}
	last, before := uint32(0), uint64(0)
	for idx := s.limbo; idx != 0; idx = s.Links[idx].Next {
		l := &s.Links[idx]
		if indexed[idx] || l.Prev != isWaiting {
			t.Fatalf("waiting node %d is indexed (%t) or not marked as waiting (%#x)", idx, indexed[idx], l.Prev)
		}
		if left := l.left(); left < before || left > s.readers.time.Load() {
			t.Fatalf("waiting node %d left at %d, after one that left at %d, and it is %d now", idx, left, before, s.readers.time.Load())
		}
		last, before = idx, l.left()
		waiting++
		if waiting > len(s.Nodes) {
			t.Fatal("the queue of waiting nodes has a cycle")
		}
	}
	if s.limbo != 0 && s.limboTail != last {
		t.Fatalf("the queue of waiting nodes ends at %d, limboTail is %d", last, s.limboTail)
	}
	if int(s.waiting) != waiting {
		t.Fatalf("%d nodes are waiting, the slab counts %d", waiting, s.waiting)
	}
	if handedOut := int(s.fresh - s.heads); handedOut != len(want)+free+waiting {
		t.Fatalf("%d nodes handed out, but %d in use + %d free + %d waiting", handedOut, len(want), free, waiting)
	}
}

// list returns the keys of the list at head, front to back, after checking
// that the back links mirror the forward links.
func list[K comparable, V any](t *testing.T, s *Slab[K, V], head uint32) []K {
	t.Helper()

	var keys []K
	prev := head
	for idx := s.Links[head].Next; idx != head; idx = s.Links[idx].Next {
		if s.Links[idx].Prev != prev {
			t.Fatalf("node %d has Prev %d, want %d", idx, s.Links[idx].Prev, prev)
		}
		keys = append(keys, s.Nodes[idx].Key)
		prev = idx
		if len(keys) > len(s.Nodes) {
			t.Fatal("list has a cycle")
		}
	}
	if s.Links[head].Prev != prev {
		t.Fatalf("head %d has Prev %d, want %d", head, s.Links[head].Prev, prev)
	}
	return keys
}

func mustPanic(t *testing.T, what string, f func()) {
	t.Helper()

	defer func() {
		if recover() == nil {
			t.Errorf("%s did not panic", what)
		}
	}()
	f()
}

func TestInit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		capacity, heads      int
		wantNodes, wantSlots int
	}{
		{0, 1, 1, 1},          // No entries, so no spare nodes either.
		{1, 1, 1 + 1 + 16, 2}, // At least sixteen spare nodes.
		{100, 2, 2 + 100 + 16, 256},
		{400, 1, 1 + 400 + 50, 1024},
		{10_000, 1, 1 + 10_000 + 1024, 16384}, // At most 1024.
	}
	for _, tt := range tests {
		var s Slab[int, int]
		s.Init(tt.capacity, tt.heads, NewReaders())

		if s.Cap() != tt.capacity || s.Len() != 0 {
			t.Errorf("Init(%d, %d): Cap() = %d, Len() = %d", tt.capacity, tt.heads, s.Cap(), s.Len())
		}
		if len(s.Nodes) != tt.wantNodes || len(s.slots) != tt.wantSlots {
			t.Errorf("Init(%d, %d): %d nodes and %d slots, want %d and %d",
				tt.capacity, tt.heads, len(s.Nodes), len(s.slots), tt.wantNodes, tt.wantSlots)
		}
		for head := range uint32(tt.heads) {
			if got := list(t, &s, head); len(got) != 0 {
				t.Errorf("Init(%d, %d): list %d = %v, want empty", tt.capacity, tt.heads, head, got)
			}
		}
		if v, found, certain := s.Lookup(42, 42, 1, false); v != 0 || found || !certain {
			t.Errorf("Init(%d, %d): Lookup on empty slab = %d, %t, %t", tt.capacity, tt.heads, v, found, certain)
		}
		check(t, &s, map[int]int{})
	}

	for _, tt := range []struct{ capacity, heads int }{{8, 0}, {-1, 1}, {MaxCapacity + 1, 1}} {
		mustPanic(t, "Init", func() {
			var s Slab[int, int]
			s.Init(tt.capacity, tt.heads, NewReaders())
		})
	}
}

func TestInsertReplaceRemove(t *testing.T) {
	t.Parallel()

	var s Slab[string, int]
	s.Init(4, 1, NewReaders())
	want := map[string]int{}

	a := s.Insert(1, "a", 10)
	s.PushFront(0, a)
	b := s.Insert(1, "b", 20) // The same hash as "a".
	s.PushFront(0, b)
	c := s.Insert(2, "c", 30)
	s.PushFront(0, c)
	want["a"], want["b"], want["c"] = 10, 20, 30
	check(t, &s, want)

	if got := s.Find(1, "x"); got != 0 {
		t.Errorf(`Find("x") = %d, want 0`, got)
	}
	if got, found, certain := s.Lookup(1, "x", 1, false); got != 0 || found || !certain {
		t.Errorf(`Lookup("x") = %d, %t, %t, want a certain miss`, got, found, certain)
	}
	now := func() uint64 { return s.readers.time.Load() }

	// Replacing a value moves the entry to another node, which takes over
	// the place in the list and the Meta. The old node keeps what it had
	// for as long as a reader is looking at it. Here one reader looks at
	// "a" and another at "b".
	pinA, pinB := s.Pin(a), s.Pin(b)
	if pinA == nil || pinB == nil || pinA == pinB {
		t.Fatalf("Pin took the slots %p and %p", pinA, pinB)
	}
	s.Nodes[b].SetMeta(7)
	s.Links[b].Tag = 9
	b2 := s.Replace(b, "b", 21)
	want["b"] = 21
	check(t, &s, want)
	if b2 == b {
		t.Fatal("Replace kept the entry in its node")
	}
	if got := s.Find(1, "b"); got != b2 {
		t.Errorf(`Find("b") = %d after Replace, want %d`, got, b2)
	}
	if n := &s.Nodes[b2]; n.Value != 21 || n.Meta() != 7 || s.Links[b2].Tag != 9 {
		t.Errorf("new node holds %d with Meta %d and Tag %d, want 21 with 7 and 9", n.Value, n.Meta(), s.Links[b2].Tag)
	}
	if got := list(t, &s, 0); !slices.Equal(got, []string{"c", "b", "a"}) {
		t.Errorf("list = %v after Replace, want [c b a]", got)
	}

	// The same goes for an entry that leaves.
	s.Unlink(a)
	s.Remove(a)
	delete(want, "a")
	check(t, &s, want)
	if got := s.Find(1, "a"); got != 0 {
		t.Errorf(`Find("a") = %d after Remove, want 0`, got)
	}

	// Two entries have left by now. The slab cannot let go of either, try
	// as it may, and says so.
	for range 3 {
		if s.Released(now()) {
			t.Fatal("Released reports that nothing is held on to while readers look at both nodes")
		}
	}
	if !s.Released(0) {
		t.Error("Released(0) = false, although nothing had left at the beginning of time")
	}
	if n := &s.Nodes[b]; n.Key != "b" || n.Value != 20 {
		t.Errorf("replaced node holds %q=%d, want it untouched", n.Key, n.Value)
	}
	if n := &s.Nodes[a]; n.Key != "a" || n.Value != 10 {
		t.Errorf("removed node holds %q=%d, want it untouched", n.Key, n.Value)
	}
	check(t, &s, want)

	// The reader of "b" leaves, and the slab lets go of its node. The
	// other one is still held on to.
	pinB.Unpin()
	if s.Released(now()) {
		t.Error("Released reports that nothing is held on to while a reader looks at a node")
	}
	if n := &s.Nodes[b]; n.Key != "" || n.Value != 0 {
		t.Errorf("node %d holds %q=%d after its reader has left", b, n.Key, n.Value)
	}
	if n := &s.Nodes[a]; n.Key != "a" || n.Value != 10 {
		t.Errorf("removed node holds %q=%d, want it untouched", n.Key, n.Value)
	}
	check(t, &s, want)

	// Once nobody is reading, the slab lets go of everything.
	pinA.Unpin()
	if !s.Released(now()) {
		t.Error("Released reports that something is held on to although nobody is reading")
	}
	check(t, &s, want)
	for _, idx := range []uint32{a, b} {
		if n := &s.Nodes[idx]; n.Key != "" || n.Value != 0 {
			t.Errorf("node %d holds %q=%d after the readers have left", idx, n.Key, n.Value)
		}
	}

	// A node that is used again starts with a clean slate, whatever Meta
	// and Tag it had when it left. Both released nodes are used here, the
	// one that was tagged above among them.
	reused := []uint32{s.Insert(4, "d", 40), s.Insert(5, "e", 50)}
	if !slices.Contains(reused, b) {
		t.Fatalf("Insert used the nodes %v, not the node %d that was released", reused, b)
	}
	for _, idx := range reused {
		if s.Nodes[idx].Meta() != 0 || s.Links[idx].Tag != 0 {
			t.Errorf("reused node %d has Meta %d and Tag %d, want 0 and 0", idx, s.Nodes[idx].Meta(), s.Links[idx].Tag)
		}
		s.Remove(idx)
	}
	s.Reclaim()

	// A list of one entry survives a Replace too.
	s.Unlink(c)
	s.Remove(c)
	delete(want, "c")
	b3 := s.Replace(b2, "b", 22)
	want["b"] = 22
	check(t, &s, want)
	if got := list(t, &s, 0); !slices.Equal(got, []string{"b"}) || s.Nodes[b3].Value != 22 {
		t.Errorf("list = %v with value %d, want [b] with 22", got, s.Nodes[b3].Value)
	}
}

func TestPanics(t *testing.T) {
	t.Parallel()

	var s Slab[int, int]
	s.Init(2, 1, NewReaders())
	a := s.Insert(1, 1, 1)
	s.Insert(2, 2, 2)
	want := map[int]int{1: 1, 2: 2}

	mustPanic(t, "Insert into a full slab", func() { s.Insert(3, 3, 3) })
	check(t, &s, want)

	// Node 0 is what Find returns for a key that is not there.
	mustPanic(t, "Remove of node 0", func() { s.Remove(s.Find(9, 9)) })

	s.Remove(a)
	mustPanic(t, "Remove of a node that was removed already", func() { s.Remove(a) })

	var empty Slab[int, int]
	empty.Init(0, 1, NewReaders())
	mustPanic(t, "Insert into a slab without capacity", func() { empty.Insert(1, 1, 1) })
}

// TestChurn replaces and removes far more entries than there are nodes, with
// hashes that collide, so that nodes are reused over and over.
func TestChurn(t *testing.T) {
	t.Parallel()

	for _, capacity := range []int{1, 2, 7, 40, 300} {
		rng := rand.New(rand.NewPCG(uint64(capacity), 3))
		hashOf := func(key int) uint32 { return uint32(key%7) * 0x2000_0001 }

		var s Slab[int, int]
		s.Init(capacity, 1, NewReaders())
		want := map[int]int{}

		for step := range 30_000 {
			key := rng.IntN(2*capacity + 1)
			hash := hashOf(key)
			idx := s.Find(hash, key)

			if _, present := want[key]; (idx != 0) != present {
				t.Fatalf("capacity %d, step %d: Find(%d) = %d, but present = %t", capacity, step, key, idx, present)
			}

			switch op := rng.IntN(10); {
			case idx != 0 && op < 5:
				s.Replace(idx, key, step)
				want[key] = step
			case idx != 0:
				s.Unlink(idx)
				s.Remove(idx)
				delete(want, key)
			case len(want) < capacity:
				s.PushFront(0, s.Insert(hash, key, step))
				want[key] = step
			case op == 0:
				s.Reset()
				clear(want)
			}

			if step%97 == 0 {
				check(t, &s, want)
				if got := len(list(t, &s, 0)); got != len(want) {
					t.Fatalf("capacity %d, step %d: %d entries in the list, want %d", capacity, step, got, len(want))
				}
			}
		}
		check(t, &s, want)
	}
}

// pin makes it look as if a reader were looking at node idx of s, and returns
// the slot that says so. Unlike a reader it does not care whether the node is
// in the index, and it takes any slot that is free.
func pin[K comparable, V any](t *testing.T, s *Slab[K, V], idx uint32) *Slot {
	t.Helper()

	for i := range s.readers.slots {
		if slot := &s.readers.slots[i]; atomic.CompareAndSwapUint64(&slot.pin, 0, s.name|uint64(idx)) {
			return slot
		}
	}
	t.Fatal("every slot is taken")
	return nil
}

// TestPinnedNodeIsLeftAlone checks the promise that reading without a lock
// rests on: a node that a reader is looking at is not touched. And the one
// that makes it bearable: the reader is in nobody's way meanwhile.
func TestPinnedNodeIsLeftAlone(t *testing.T) {
	t.Parallel()

	const capacity = 8

	readers := NewReaders()
	var s, other Slab[int, string]
	s.Init(capacity, 1, readers)
	other.Init(capacity, 1, readers)

	for key := range capacity {
		s.PushFront(0, s.Insert(uint32(key), key, "old"))
		other.PushFront(0, other.Insert(uint32(key), key, "other"))
	}

	// A reader finds an entry and takes its time.
	idx := s.Find(0, 0)
	slot := s.Pin(idx)
	if idx == 0 || slot == nil {
		t.Fatalf("key 0 is in node %d, pinned in slot %p", idx, slot)
	}
	n := &s.Nodes[idx]

	// Meanwhile the entry is replaced, along with the others, far more often
	// than the slab has nodes.
	want := map[int]string{}
	for round := range 100 {
		for key := range capacity {
			s.Replace(s.Find(uint32(key), key), key, "new")
			want[key] = "new"
		}
		if n.Key != 0 || n.Value != "old" {
			t.Fatalf("round %d: the reader's node now holds %d=%q", round, n.Key, n.Value)
		}
		if int(s.waiting) > 1+int(s.patience) {
			t.Fatalf("round %d: %d nodes are waiting; the reader keeps more than its own from being reused", round, s.waiting)
		}
	}
	check(t, &s, want)

	// The node of another slab with the same index is none of its business.
	otherIdx := other.Find(0, 0)
	if otherIdx != idx {
		t.Fatalf("key 0 is in node %d of one slab and in node %d of the other; the test needs them to agree", idx, otherIdx)
	}
	other.Unlink(otherIdx)
	other.Remove(otherIdx)
	if !other.Released(readers.time.Load()) || other.Nodes[otherIdx].Value != "" {
		t.Errorf("a slab holds on to its node %d because a reader looks at node %d of another slab", otherIdx, idx)
	}

	// Once the reader is done, its node is fair game.
	slot.Unpin()
	if !s.Released(readers.time.Load()) || n.Value != "" {
		t.Errorf("the node holds %d=%q after its reader has left", n.Key, n.Value)
	}
	if s.Waits() != 0 {
		t.Errorf("the writer had to wait %d times", s.Waits())
	}
	check(t, &s, want)
}

// TestWriterWaitsForSpareNode checks what happens when readers that are held
// up have pinned every node that the slab has to spare: it waits for one of
// them rather than use a node that is being read.
func TestWriterWaitsForSpareNode(t *testing.T) {
	t.Parallel()

	const capacity = 8

	// There are slots for more readers than the slab has nodes to spare,
	// however few CPUs the test runs on.
	readers := newReaders(4 * minSpare)
	var s Slab[int, string]
	s.Init(capacity, 1, readers)
	spare := len(s.Nodes) - capacity - 1

	for key := range capacity {
		s.PushFront(0, s.Insert(uint32(key), key, "old"))
	}

	// Every replacement leaves a node behind that a reader is looking at.
	pins, nodes := make([]*Slot, 0, spare), make([]uint32, 0, spare)
	for i := range spare {
		idx := s.Find(uint32(i%capacity), i%capacity)
		pins, nodes = append(pins, pin(t, &s, idx)), append(nodes, idx)
		s.Replace(idx, i%capacity, "pinned")
	}
	if int(s.waiting) != spare {
		t.Fatalf("%d nodes are waiting, want all %d spare ones", s.waiting, spare)
	}

	looks := readers.time.Load()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Replace(s.Find(1, 1), 1, "new") // Needs a node, and has to wait for one.
	}()
	// The writer looks for a node again and again. By its second look it
	// has come away with nothing at least once; until then it may not even
	// have begun, on a machine that is busy.
	for deadline := time.Now().Add(10 * time.Second); readers.time.Load() < looks+2; runtime.Gosched() {
		select {
		case <-done:
			t.Fatal("Replace found a node to use while readers were looking at all the spare ones")
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the writer is not looking for a node")
		}
	}
	select {
	case <-done:
		t.Fatal("Replace found a node to use while readers were looking at all the spare ones")
	case <-time.After(20 * time.Millisecond):
	}
	for i, idx := range nodes {
		if n := &s.Nodes[idx]; n.Key != i%capacity || n.Value == "new" || n.Value == "" {
			t.Fatalf("the node %d that a reader is looking at now holds %d=%q", idx, n.Key, n.Value)
		}
	}

	// One reader leaving is enough.
	pins[spare/2].Unpin()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Replace is still waiting although a reader has left")
	}
	if got := s.Nodes[s.Find(1, 1)].Value; got != "new" {
		t.Errorf("key 1 has the value %q after Replace", got)
	}

	if s.Waits() == 0 {
		t.Error("the slab does not say that a writer had to wait")
	}

	for _, slot := range pins {
		slot.Unpin()
	}
	if !s.Released(readers.time.Load()) {
		t.Error("the slab holds on to nodes although no reader is left")
	}
	want := map[int]string{}
	for key := range capacity {
		want[key] = "pinned"
	}
	want[1] = "new"
	check(t, &s, want)
}

// TestOverdue checks what a slab does when readers keep it from letting go of
// more entries than it has patience for: it says so, does not look for the
// readers again at every turn, and lets go as soon as it is asked to after
// the readers have left.
func TestOverdue(t *testing.T) {
	t.Parallel()

	const (
		capacity = 64 // The slab has patience for eight nodes.
		pinned   = 12
	)

	var s Slab[int, *int]
	s.Init(capacity, 1, NewReaders())
	for key := range capacity {
		s.PushFront(0, s.Insert(uint32(key), key, new(key)))
	}
	if s.Overdue() {
		t.Fatal("a slab that nothing has left is overdue")
	}

	// Readers look at the first twelve entries, which then leave along
	// with the others.
	pins := make([]*Slot, 0, pinned)
	for key := range pinned {
		pins = append(pins, pin(t, &s, s.Find(uint32(key), key)))
	}
	most, idle := 0, 0
	for key := range capacity {
		idx := s.Find(uint32(key), key)
		s.Unlink(idx)
		before := int(s.waiting)
		s.Remove(idx)

		if got, want := s.Overdue(), key >= 8; got != want {
			t.Fatalf("after %d removals with readers around: Overdue() = %t, want %t", key+1, got, want)
		}
		most = max(most, int(s.waiting))
		if int(s.waiting) == before+1 {
			idle++
		}
	}
	if int(s.waiting) < pinned || most > pinned+4 {
		t.Errorf("%d nodes are waiting, and up to %d were; want the %d that are pinned and never over %d",
			s.waiting, most, pinned, pinned+4)
	}
	// 52 nodes were there for the taking, yet the slab did not come for
	// every one of them on its own.
	if idle < capacity/2 {
		t.Errorf("the slab went looking for readers after all but %d of %d removals", idle, capacity)
	}

	s.Reclaim() // The pinned nodes stay.
	if !s.Overdue() || int(s.waiting) != pinned {
		t.Fatalf("Reclaim with readers around: Overdue() = %t with %d nodes waiting", s.Overdue(), s.waiting)
	}
	check(t, &s, map[int]*int{})

	for _, slot := range pins {
		slot.Unpin()
	}
	s.Reclaim()
	if s.Overdue() || s.waiting != 0 {
		t.Fatalf("Reclaim without readers: Overdue() = %t with %d nodes waiting", s.Overdue(), s.waiting)
	}
	check(t, &s, map[int]*int{})
	for i := range s.Nodes {
		if n := &s.Nodes[i]; n.Key != 0 || n.Value != nil {
			t.Fatalf("node %d still holds %d=%v", i, n.Key, n.Value)
		}
	}

	// It is back to normal: a few nodes may wait, more may not.
	for key := range capacity {
		s.PushFront(0, s.Insert(uint32(key), key, new(key)))
	}
	for key := range capacity {
		idx := s.Find(uint32(key), key)
		s.Unlink(idx)
		s.Remove(idx)
		if s.Overdue() || s.waiting > 8 {
			t.Fatalf("after %d removals without readers: Overdue() = %t with %d nodes waiting", key+1, s.Overdue(), s.waiting)
		}
	}
}

// TestLookupWithoutSlot checks what a reader is told when it cannot pin a
// node: to come back as a writer if there is a node to look at, and the plain
// truth if there is none.
func TestLookupWithoutSlot(t *testing.T) {
	t.Parallel()

	readers := NewReaders()
	var s Slab[int, int]
	s.Init(8, 1, readers)
	s.PushFront(0, s.Insert(1, 10, 100))

	release := readers.Occupy(0)
	if v, found, certain := s.Lookup(1, 10, 1, false); v != 0 || found || certain {
		t.Errorf("Lookup of a key that is there = %d, %t, %t without a slot to be had, want an uncertain miss", v, found, certain)
	}
	// The same hash, another key: only a look at the node could tell.
	if v, found, certain := s.Lookup(1, 11, 1, false); v != 0 || found || certain {
		t.Errorf("Lookup of a key with the hash of another = %d, %t, %t without a slot to be had, want an uncertain miss", v, found, certain)
	}
	if v, found, certain := s.Lookup(2, 20, 1, false); v != 0 || found || !certain {
		t.Errorf("Lookup of a key that is not there = %d, %t, %t, want a certain miss", v, found, certain)
	}

	// Nor do the slots that are taken keep the slab from anything: they
	// name no node of its own.
	for i := range 100 {
		s.Replace(s.Find(1, 10), 10, i)
	}
	if !s.Released(readers.time.Load()) {
		t.Error("the slab holds on to nodes that nobody has pinned")
	}

	release()
	if v, found, certain := s.Lookup(1, 10, 1, false); v != 99 || !found || !certain {
		t.Fatalf("Lookup = %d, %t, %t after the slots were given back", v, found, certain)
	}
	// A key with the same hash is told apart, and the value of the other
	// one does not leak out.
	if v, found, certain := s.Lookup(1, 11, 1, false); v != 0 || found || !certain {
		t.Errorf("Lookup of a key with the hash of another = %d, %t, %t, want a certain miss", v, found, certain)
	}
	// No lookup leaves a node pinned.
	for i := range readers.slots {
		if pin := atomic.LoadUint64(&readers.slots[i].pin); pin != 0 {
			t.Errorf("slot %d is left with the pin %#x", i, pin)
		}
	}
}

// TestLookupWhileReplaced has readers look for the only entry of a slab while
// a writer replaces its value as fast as it can. The index of such a slab is
// as small as an index gets, so a reader that keeps finding it changed is
// soon through with it, and must then say that it does not know rather than
// that the key is not there: it is, all the time.
func TestLookupWhileReplaced(t *testing.T) {
	t.Parallel()

	replacements := 400_000
	if testing.Short() {
		replacements = 40_000
	}

	var s Slab[int, int]
	s.Init(1, 1, NewReaders())
	idx := s.Insert(7, 7, 0)
	s.PushFront(0, idx)

	var (
		wg      sync.WaitGroup
		stop    atomic.Bool
		unsure  atomic.Int64
		lookups atomic.Int64
	)
	for range 4 {
		wg.Go(func() {
			n, u := 0, 0
			for !stop.Load() {
				switch _, found, certain := s.Lookup(7, 7, 1, false); {
				case !found && certain:
					t.Error("Lookup is certain that the key is missing, and it was there all along")
					return
				case !found:
					u++
				}
				n++
			}
			lookups.Add(int64(n))
			unsure.Add(int64(u))
		})
	}
	for i := range replacements {
		idx = s.Replace(idx, 7, i)
	}
	stop.Store(true)
	wg.Wait()

	// How often a reader ran out of index depends on the machine; on one
	// with a single CPU it never does.
	t.Logf("%d of %d lookups could not tell", unsure.Load(), lookups.Load())
}

// TestLookupCounts checks what Lookup does to the Meta of the node it finds:
// it counts the read, up to the limit it is given.
func TestLookupCounts(t *testing.T) {
	t.Parallel()

	var s Slab[int, int]
	s.Init(8, 1, NewReaders())
	idx := s.Insert(1, 10, 100)
	s.PushFront(0, idx)
	node := &s.Nodes[idx]

	for range 3 {
		s.Lookup(1, 10, 0, false)
	}
	if got := node.Meta(); got != 0 {
		t.Errorf("Meta = %d after lookups with a limit of 0, want it untouched", got)
	}
	for i := range 5 {
		s.Lookup(1, 10, 3, false)
		if got, want := node.Meta(), uint32(min(i+1, 3)); got != want {
			t.Errorf("Meta = %d after %d lookups with a limit of 3, want %d", got, i+1, want)
		}
	}
	// A limit below what has been counted leaves it alone as well.
	s.Lookup(1, 10, 1, false)
	if got := node.Meta(); got != 3 {
		t.Errorf("Meta = %d after a lookup with a limit of 1, want 3", got)
	}
	// A miss counts nothing.
	s.Lookup(1, 11, 3, false)
	s.Lookup(2, 10, 3, false)
	if got := node.Meta(); got != 3 {
		t.Errorf("Meta = %d after misses, want 3", got)
	}

	// A read counts for its own key only. Lookup has to look at every node
	// whose key has the same hash to find the right one, and the ones that
	// it passes on the way are none the wiser, whether it finds its key in
	// the end or not.
	var same Slab[int, int]
	same.Init(8, 1, NewReaders())
	first, second := same.Insert(7, 1, 10), same.Insert(7, 2, 20)
	same.PushFront(0, first)
	same.PushFront(0, second)
	counts := func() [2]uint32 {
		return [2]uint32{same.Nodes[first].Meta(), same.Nodes[second].Meta()}
	}

	if v, ok, _ := same.Lookup(7, 2, 3, false); !ok || v != 20 {
		t.Fatalf("Lookup of the second of two keys with one hash = %d, %v", v, ok)
	}
	if got, want := counts(), [2]uint32{0, 1}; got != want {
		t.Errorf("counts = %v after a read of the second key, want %v", got, want)
	}
	if _, ok, certain := same.Lookup(7, 3, 3, false); ok || !certain {
		t.Fatalf("Lookup of a key that is absent = %v, certain %v", ok, certain)
	}
	if got, want := counts(), [2]uint32{0, 1}; got != want {
		t.Errorf("counts = %v after a read of a key that is absent, want %v", got, want)
	}
	// Nor does a count that is there already go missing.
	same.Lookup(7, 1, 3, false)
	same.Lookup(7, 1, 3, false)
	same.Lookup(7, 2, 3, false)
	same.Lookup(7, 3, 3, false)
	if got, want := counts(), [2]uint32{2, 2}; got != want {
		t.Errorf("counts = %v after two reads of the first key and one more of the second, want %v", got, want)
	}
	// The same with a count that is at the limit of the read.
	same.Lookup(7, 2, 2, false)
	same.Lookup(7, 3, 2, false)
	if got, want := counts(), [2]uint32{2, 2}; got != want {
		t.Errorf("counts = %v after reads with a limit that has been reached, want %v", got, want)
	}
}

// TestLookupTallies checks that the readers' tally of hits counts the lookups
// that found their key and were asked to count, and only those.
func TestLookupTallies(t *testing.T) {
	t.Parallel()

	r := NewReaders()
	var s, other Slab[int, int]
	s.Init(8, 1, r)
	other.Init(8, 1, r)
	for _, sl := range []*Slab[int, int]{&s, &other} {
		for _, key := range []int{1, 2} {
			// Two keys with one hash: a lookup of one passes the other.
			sl.PushFront(0, sl.Insert(7, key, 10*key))
		}
	}

	s.Lookup(7, 1, 3, true)
	s.Lookup(7, 2, 0, true) // Whatever the limit of the count on the node.
	s.Lookup(7, 1, 3, false)
	s.Lookup(7, 3, 3, true) // Absent, past two nodes with the same hash.
	s.Lookup(8, 1, 3, true) // Absent, and no node with its hash.
	other.Lookup(7, 2, 3, true)
	if got := r.Hits(); got != 3 {
		t.Errorf("Hits() = %d after three lookups that found their key and were to be tallied, want 3", got)
	}

	// A reader that finds no slot to pin with reads as a writer instead,
	// and the tally is the caller's business then.
	release := r.Occupy(0)
	if _, found, certain := s.Lookup(7, 1, 3, true); found || certain {
		t.Fatalf("Lookup without a slot = %t, certain %t", found, certain)
	}
	release()
	if got := r.Hits(); got != 3 {
		t.Errorf("Hits() = %d after a lookup that found no slot, want 3", got)
	}

	// Many readers at once lose no hit, whichever slots they share.
	var wg sync.WaitGroup
	for g := range 16 {
		wg.Go(func() {
			for i := range 1000 {
				if _, found, _ := s.Lookup(7, 1+(g+i)%2, 3, true); !found {
					t.Error("Lookup did not find a key with no writer about")
					return
				}
			}
		})
	}
	wg.Wait()
	if got := r.Hits(); got != 3+16*1000 {
		t.Errorf("Hits() = %d after 16,000 more hits, want %d", got, 3+16*1000)
	}
}

// TestReleasedMark checks that Released waits for what had left by the time
// of its mark, and for nothing that left later.
func TestReleasedMark(t *testing.T) {
	t.Parallel()

	readers := NewReaders()
	var s Slab[int, int]
	s.Init(64, 1, readers)
	for key := range 4 {
		s.PushFront(0, s.Insert(uint32(key), key, key))
	}
	leave := func(key int) (idx uint32, slot *Slot) {
		idx = s.Find(uint32(key), key)
		slot = pin(t, &s, idx)
		s.Unlink(idx)
		s.Remove(idx)
		return idx, slot
	}

	// An entry leaves while a reader looks at it. Then time moves on, and
	// another one does.
	early, earlyPin := leave(0)
	mark := readers.time.Load()
	if s.Released(mark) {
		t.Fatal("Released reports that nothing is held on to while a reader looks at the entry")
	}
	late, latePin := leave(1)

	earlyPin.Unpin()
	if !s.Released(mark) {
		t.Error("Released still waits although the reader of what left before the mark is gone")
	}
	if n := &s.Nodes[early]; n.Value != 0 {
		t.Errorf("the node that left before the mark holds %d", n.Value)
	}
	if n := &s.Nodes[late]; n.Value != 1 || s.Released(readers.time.Load()) {
		t.Errorf("the node that left after the mark holds %d, and Released says it is gone", n.Value)
	}

	latePin.Unpin()
	if !s.Released(readers.time.Load()) || s.Nodes[late].Value != 0 {
		t.Error("the slab holds on to a node although no reader is left")
	}

	// Reset returns the mark that covers everything it has removed.
	rest := pin(t, &s, s.Find(2, 2))
	mark = s.Reset()
	if s.Released(mark) {
		t.Error("Released reports that nothing is held on to after Reset, while a reader looks at an entry")
	}
	s.PushFront(0, s.Insert(9, 9, 9))
	later, laterPin := leave(9)
	rest.Unpin()
	if !s.Released(mark) || s.Nodes[later].Value != 9 {
		t.Errorf("after the reader of what Reset removed has left: Released = false, or the node that left later holds %d", s.Nodes[later].Value)
	}
	laterPin.Unpin()
	check(t, &s, map[int]int{})
}

// fullWithOnePinned returns two slabs that share readers. Both are full; in b
// a reader looks at the node that key 0 was in, and b has replaced as many
// values as it has spare nodes, so its next write finds no node free. Nobody
// has looked at the slots yet.
func fullWithOnePinned(t *testing.T) (readers *Readers, a, b *Slab[int, int], pinned uint32, held *Slot) {
	t.Helper()

	const capacity = 256 // Patience for 32 nodes, and as many to spare.

	readers = newReaders(64)
	a, b = new(Slab[int, int]), new(Slab[int, int])
	a.Init(capacity, 1, readers)
	b.Init(capacity, 1, readers)
	for key := range capacity {
		a.PushFront(0, a.Insert(uint32(key), key, 1000+key))
		b.PushFront(0, b.Insert(uint32(key), key, 1000+key))
	}

	pinned = b.Find(0, 0)
	held = pin(t, b, pinned)
	for key := range 32 {
		b.Replace(b.Find(uint32(key), key), key, 2000+key)
	}
	if b.waiting != 32 || b.free != 0 || int(b.fresh) != len(b.Nodes) {
		t.Fatalf("b has %d nodes waiting, free list at %d, fresh at %d of %d", b.waiting, b.free, b.fresh, len(b.Nodes))
	}
	if got := readers.time.Load(); got != 1 {
		t.Fatalf("time is %d before anyone had a reason to look", got)
	}
	return readers, a, b, pinned, held
}

// TestAllocBorrowsAroundPin checks that a slab which needs a node makes do with
// the look of another slab, that it leaves alone the node which that look
// found a reader at, and that it looks for itself once the borrowed look is
// of no more use.
func TestAllocBorrowsAroundPin(t *testing.T) {
	t.Parallel()

	readers, a, b, pinned, held := fullWithOnePinned(t)
	intact := func(when string) {
		t.Helper()
		if n := &b.Nodes[pinned]; n.Key != 0 || n.Value != 1000 {
			t.Fatalf("%s: the node that a reader looks at holds %d=%d", when, n.Key, n.Value)
		}
	}

	// a looks, and sees the reader of b's node.
	idx := a.Find(7, 7)
	a.Unlink(idx)
	a.Remove(idx)
	a.Reclaim()
	looked := readers.time.Load()
	if looked != 2 {
		t.Fatalf("time is %d after one look", looked)
	}

	// b needs a node: it goes by the look of a, and leaves the pinned one.
	b.Replace(b.Find(40, 40), 40, 2040)
	intact("after a borrowed look")
	if got := readers.time.Load(); got != looked {
		t.Errorf("b looked for itself (time %d -> %d)", looked, got)
	}
	if b.waiting != 2 || b.Overdue() || b.Waits() != 0 {
		t.Errorf("b: %d waiting, overdue %t, waits %d; want 2, false, 0", b.waiting, b.Overdue(), b.Waits())
	}

	// The same look is of no further use: b looks, and still leaves the node.
	for key := 41; key < 41+31; key++ {
		b.Replace(b.Find(uint32(key), key), key, 2000+key)
		intact("in the second round")
	}
	if got := readers.time.Load(); got != looked+1 {
		t.Errorf("time is %d, want one look by b on top of %d", got, looked)
	}
	if b.waiting != 2 {
		t.Errorf("b: %d waiting after its own look, want 2", b.waiting)
	}

	// The reader leaves. The look that b goes by next still names the node,
	// which gets b nothing, so it looks again and the node is released.
	held.Unpin()
	for key := 80; key < 80+31; key++ {
		b.Replace(b.Find(uint32(key), key), key, 2000+key)
	}
	if got := readers.time.Load(); got != looked+2 {
		t.Errorf("time is %d, want two looks by b on top of %d", got, looked)
	}
	if n := &b.Nodes[pinned]; n.Value == 1000 {
		t.Errorf("the node is still held on to after the reader left: %d=%d, %d waiting", n.Key, n.Value, b.waiting)
	}
	if b.Waits() != 0 {
		t.Errorf("b waited %d times", b.Waits())
	}
}

// TestAllocAndTooManyPins checks that a slab which needs a node does not go by
// a look that found more readers than it could write down.
func TestAllocAndTooManyPins(t *testing.T) {
	t.Parallel()

	readers, a, b, pinned, held := fullWithOnePinned(t)
	defer held.Unpin()

	// a looks while more slots are taken than a look writes down.
	release := readers.Occupy(0)
	defer release()
	idx := a.Find(7, 7)
	a.Unlink(idx)
	a.Remove(idx)
	a.Reclaim()
	looked := readers.time.Load()
	if got := readers.sharedFound.Load(); got != -1 {
		t.Fatalf("the look wrote down %d pins, want -1 for too many", got)
	}

	b.Replace(b.Find(40, 40), 40, 2040)
	if n := &b.Nodes[pinned]; n.Key != 0 || n.Value != 1000 {
		t.Fatalf("the node that a reader looks at holds %d=%d", n.Key, n.Value)
	}
	if got := readers.time.Load(); got != looked+1 {
		t.Errorf("time went from %d to %d, want b to look for itself", looked, got)
	}
	if b.waiting != 2 || b.Waits() != 0 {
		t.Errorf("b: %d waiting, %d waits; want 2, 0", b.waiting, b.Waits())
	}
}

// TestReviewStress has several full slabs that share readers do nothing but
// replace values, which is the path that now borrows looks in alloc, while
// readers look entries up and other goroutines hold nodes pinned for a while
// and watch them.
// TestSharedLooks checks that the slabs of a cache make do with each other's
// looks at the readers' slots, whichever kind of write it is that uses up
// their spare nodes. A look goes through every slot there is, and a slab that
// takes one whenever it runs out of nodes makes every write pay for the
// number of CPUs.
func TestSharedLooks(t *testing.T) {
	t.Parallel()

	const (
		slabs    = 16
		capacity = 256 // Patience for 32 nodes, and as many to spare.
		writes   = 200_000
	)
	type contents = map[int]int
	kinds := map[string]func(s *Slab[int, int], has contents, rng *rand.Rand, fresh int){
		// The value of an entry is replaced, which moves it to another node.
		"replacing": func(s *Slab[int, int], _ contents, rng *rand.Rand, _ int) {
			idx := s.Links[0].Prev
			for range rng.IntN(8) {
				idx = s.Links[idx].Prev
				if idx == 0 {
					idx = s.Links[0].Prev
				}
			}
			s.Replace(idx, s.Nodes[idx].Key, 0)
		},
		// The oldest entry goes and a new one comes.
		"evicting": func(s *Slab[int, int], has contents, _ *rand.Rand, fresh int) {
			idx := s.Links[0].Prev
			delete(has, s.Nodes[idx].Key)
			s.Unlink(idx)
			s.Remove(idx)
			s.PushFront(0, s.Insert(uint32(fresh), fresh, 0))
			has[fresh] = 0
		},
	}
	for name, write := range kinds {
		readers := newReaders(64)
		all := make([]Slab[int, int], slabs)
		has := make([]contents, slabs)
		for i := range all {
			all[i].Init(capacity, 1, readers)
			has[i] = contents{}
			for key := range capacity {
				all[i].PushFront(0, all[i].Insert(uint32(key), key, 0))
				has[i][key] = 0
			}
		}

		rng := rand.New(rand.NewPCG(1, 2))
		before := readers.time.Load()
		for i := range writes {
			at := rng.IntN(slabs)
			write(&all[at], has[at], rng, capacity+i)
		}
		looks := int(readers.time.Load() - before)

		// A slab that looks for itself every time does so once for as many
		// writes as it has spare nodes. With sixteen slabs to share with,
		// a quarter of that is nothing to be proud of.
		if alone := writes / int(all[0].patience); looks > alone/4 {
			t.Errorf("%d %s writes to %d slabs took %d looks at the readers; each slab on its own would take %d",
				writes, name, slabs, looks, alone)
		} else {
			t.Logf("%d %s writes to %d slabs took %d looks at the readers; each slab on its own would take %d",
				writes, name, slabs, looks, alone)
		}
		for i := range all {
			check(t, &all[i], has[i])
		}
	}
}

// TestBorrowedLook checks that a slab makes do with what another slab of the
// same cache has found out about the readers, where that is enough.
func TestBorrowedLook(t *testing.T) {
	t.Parallel()

	const capacity = 64 // Patience for eight nodes.

	readers := NewReaders()
	var a, b Slab[int, int]
	a.Init(capacity, 1, readers)
	b.Init(capacity, 1, readers)
	for key := range capacity {
		a.PushFront(0, a.Insert(uint32(key), key, key))
		b.PushFront(0, b.Insert(uint32(key), key, key))
	}
	remove := func(s *Slab[int, int], key int) {
		idx := s.Find(uint32(key), key)
		s.Unlink(idx)
		s.Remove(idx)
	}

	// Eight entries leave b, one of which a reader is looking at. That is
	// not yet more than b has patience for. Another reader is looking at
	// an entry of a, in the node of the same number as one of those that
	// leave b; that is no concern of b's.
	held := pin(t, &b, b.Find(3, 3))
	if a.Find(5, 5) != b.Find(5, 5) {
		t.Fatalf("key 5 is in node %d of a and in node %d of b; the test needs them to agree", a.Find(5, 5), b.Find(5, 5))
	}
	elsewhere := pin(t, &a, a.Find(5, 5))
	defer elsewhere.Unpin()
	for key := range 8 {
		remove(&b, key)
	}
	if b.waiting != 8 {
		t.Fatalf("%d nodes are waiting in b, want 8", b.waiting)
	}

	// Then a looks at the slots, for reasons of its own.
	remove(&a, 0)
	a.Reclaim()
	looked := readers.time.Load()

	// The next entry that leaves b is one too many. But b need not look:
	// a has seen that seven of the eight are free.
	remove(&b, 8)
	if got := readers.time.Load(); got != looked {
		t.Errorf("b looked at the slots itself (time went from %d to %d)", looked, got)
	}
	if b.waiting != 2 || b.Overdue() {
		t.Errorf("%d nodes are waiting in b, want the one that is pinned and the one that left after the look", b.waiting)
	}
	check(t, &b, func() map[int]int {
		want := map[int]int{}
		for key := 9; key < capacity; key++ {
			want[key] = key
		}
		return want
	}())

	// What a did not see is not taken on trust: once b has more waiting
	// than it has patience for, all of which left after the look, it looks.
	for key := 9; key < 9+7; key++ {
		remove(&b, key)
	}
	if got := readers.time.Load(); got == looked {
		t.Error("b did not look at the slots although nothing it has waiting was covered by the look of a")
	}
	if b.waiting != 1 {
		t.Errorf("%d nodes are waiting in b after its own look, want the one that is pinned", b.waiting)
	}

	// A look that is just being written down is not gone by either.
	held.Unpin()
	b.Reclaim()
	for key := 30; key < 38; key++ {
		remove(&b, key)
	}
	remove(&a, 2)
	a.Reclaim()
	looked = readers.time.Load()
	readers.version.Add(1)
	remove(&b, 38)
	if got := readers.time.Load(); got == looked {
		t.Error("b went by a look that was being written down")
	}
	readers.version.Add(1)

	// A look that found more pins than it could write down is of no use
	// to others.
	release := readers.Occupy(0)
	for key := 20; key < 28; key++ {
		remove(&b, key)
	}
	remove(&a, 1)
	a.Reclaim()
	looked = readers.time.Load()
	remove(&b, 28)
	if got := readers.time.Load(); got == looked {
		t.Error("b went by a look that could not tell it what was pinned")
	}
	release()
	if !a.Released(readers.time.Load()) || !b.Released(readers.time.Load()) {
		t.Error("the slabs hold on to nodes although no reader is left")
	}
}

// TestEverySlotCounts checks that a slab does not overlook a reader, whichever
// slot the reader has found.
func TestEverySlotCounts(t *testing.T) {
	t.Parallel()

	readers := newReaders(64)
	var s Slab[int, int]
	s.Init(8, 1, readers)
	idx := s.Insert(1, 1, 100)
	s.PushFront(0, idx)

	for i := range readers.slots {
		atomic.StoreUint64(&readers.slots[i].pin, s.name|uint64(idx))
		idx = s.Replace(idx, 1, i)
		old := s.limboTail
		for range 3 {
			s.Reclaim()
		}
		if s.waiting != 1 || s.Nodes[old].Key != 1 {
			t.Fatalf("a node that is pinned in slot %d was released", i)
		}
		atomic.StoreUint64(&readers.slots[i].pin, 0)
		s.Reclaim()
		if s.waiting != 0 {
			t.Fatalf("a node is held on to after slot %d was cleared", i)
		}
	}
}

// TestPatience checks that a slab does not hold on to more than an eighth of
// its capacity in entries that are gone, as long as no reader makes it, and
// that it does not go to the trouble of looking for readers before it holds
// on to that many.
func TestPatience(t *testing.T) {
	t.Parallel()

	for _, capacity := range []int{1, 8, 64, 800, 100_000} {
		var s Slab[int, int]
		s.Init(capacity, 1, NewReaders())
		limit := min(max(capacity/8, 1), 1024)

		for key := range capacity {
			s.PushFront(0, s.Insert(uint32(key), key, key))
		}
		most := 0
		for round := range 4 {
			for key := range capacity {
				s.Replace(s.Find(uint32(key), key), key, round)
				if int(s.waiting) > limit {
					t.Fatalf("capacity %d: %d nodes are waiting, want at most %d", capacity, s.waiting, limit)
				}
				most = max(most, int(s.waiting))
			}
		}
		if most != limit {
			t.Errorf("capacity %d: at most %d nodes were waiting at a time, want the slab to let %d wait", capacity, most, limit)
		}
		if spare := len(s.Nodes) - capacity - 1; spare < limit || spare < minSpare {
			t.Errorf("capacity %d: %d spare nodes, want at least %d and at least %d", capacity, spare, limit, minSpare)
		}
	}
}

// TestSlabLayout checks that what a reader needs of a slab fits on one cache
// line, even when the slab starts a word into one, behind the header that the
// allocator puts before most arrays of shards; and that the fields of the
// writers keep a line away from it.
func TestSlabLayout(t *testing.T) {
	t.Parallel()

	var s Slab[string, string]
	end := unsafe.Offsetof(s.overdue) + unsafe.Sizeof(s.overdue)
	if end > cacheLine-8 {
		t.Errorf("what readers need ends %d bytes into the slab, want %d at most", end, cacheLine-8)
	}
	if gap := unsafe.Offsetof(s.Links) - end; gap < cacheLine {
		t.Errorf("the fields of the writers start %d bytes after what readers need, want %d or more", gap, cacheLine)
	}
}

func TestReaders(t *testing.T) {
	t.Parallel()

	r := NewReaders()
	if n := len(r.slots); n&(n-1) != 0 || n < 16 {
		t.Fatalf("%d slots, want a power of two that leaves room", n)
	}
	for n, want := range map[int]int{1: 1, 2: 2, 3: 4, 16: 16, 17: 32, 100: 128} {
		small := newReaders(n)
		if len(small.slots) != want || int(small.shift) != 64-bits.TrailingZeros(uint(want)) {
			t.Errorf("newReaders(%d) has %d slots and shifts by %d, want %d slots", n, len(small.slots), small.shift, want)
		}
		// With the shift that goes with them, no slot is out of reach and
		// none is out of range.
		if got := (&Slab[int, int]{readers: small}).preferred(); got >= uint64(want) {
			t.Errorf("newReaders(%d): the preferred slot is %d of %d", n, got, want)
		}
	}

	// Time starts at one, so that a mark of zero is before everything.
	if got := r.time.Load(); got != 1 {
		t.Errorf("time starts at %d, want 1", got)
	}
	// A slot has a cache line to itself, and so does what the readers of
	// a Readers need.
	if size := unsafe.Sizeof(Slot{}); size != cacheLine {
		t.Errorf("a slot takes %d bytes, want %d", size, cacheLine)
	}
	if before, after := unsafe.Offsetof(r.slots), unsafe.Offsetof(r.time)-unsafe.Offsetof(r.slabs); before < cacheLine || after < cacheLine {
		t.Errorf("what readers need is %d bytes from the start and %d bytes from what writers change, want %d or more", before, after, cacheLine)
	}

	// Every slab gets a name of its own, and none gets the one that stands
	// for a free slot.
	first, second := r.name(), r.name()
	if first == 0 || second == 0 || first == second || uint32(first) != 0 || uint32(second) != 0 {
		t.Errorf("the first two slabs are called %#x and %#x", first, second)
	}

	// A slab pins nodes in the slots of its readers, and needs nothing else
	// of its own for that.
	s := &Slab[int, int]{readers: r}

	// A goroutine that pins a node gets a slot, and the same one every time.
	slot := s.pin(first | 1)
	if slot == nil {
		t.Fatal("pin found no slot in an idle Readers")
	}
	if got := atomic.LoadUint64(&slot.pin); got != first|1 {
		t.Errorf("the slot says %#x, want %#x", got, first|1)
	}
	slot.Unpin()
	if again := s.pin(first | 2); again != slot {
		t.Errorf("pin took the slot %p, then %p", slot, again)
	}

	// A goroutine that does not let go uses up the slots it may try, and is
	// then told to read under a lock instead. The other slots stay free.
	held := []*Slot{slot}
	if s.pin(first|3) != nil {
		t.Fatal("pin handed out a slot that is taken")
	}
	for {
		slot := s.pinAny(first | 3)
		if slot == nil {
			break
		}
		if slices.Contains(held, slot) {
			t.Fatal("pinAny handed out a slot that is taken")
		}
		held = append(held, slot)
		if len(held) > len(r.slots) {
			t.Fatal("pinAny keeps handing out slots")
		}
	}
	if len(held) != 4 {
		t.Errorf("one goroutine got %d slots, want its preferred one and the three after it", len(held))
	}

	// Occupy takes the rest, or the share of them it is asked for, and
	// gives back no more than it took.
	count := func() (taken int) {
		for i := range r.slots {
			if atomic.LoadUint64(&r.slots[i].pin) != 0 {
				taken++
			}
		}
		return taken
	}
	release := r.Occupy(4)
	if taken, most := count(), len(r.slots)*3/4+len(held); taken < len(r.slots)*3/4 || taken > most {
		t.Errorf("%d of %d slots are taken after Occupy(4), want three in four and up to %d more", taken, len(r.slots), len(held))
	}
	release()
	release = r.Occupy(0)
	if taken := count(); taken != len(r.slots) {
		t.Errorf("%d of %d slots are taken after Occupy(0)", taken, len(r.slots))
	}
	release()
	for i := range r.slots {
		if got := atomic.LoadUint64(&r.slots[i].pin) != 0; got != slices.Contains(held, &r.slots[i]) {
			t.Errorf("slot %d is taken: %t, after the occupied ones were given back", i, got)
		}
	}

	for _, slot := range held {
		slot.Unpin()
	}
	if slot := s.pin(first | 4); slot == nil {
		t.Error("pin found no slot after all of them were released")
	} else {
		slot.Unpin()
	}
}

// TestConcurrentReaders has goroutines read without a lock while another one
// churns the slab. The slab is tiny, so its nodes are reused all the time, and
// every key hashes to one of two values, so entries keep moving around in the
// index. A reader must never see an entry that is half written, and never
// miss one that is there throughout.
func TestConcurrentReaders(t *testing.T) {
	t.Parallel()

	const (
		capacity = 6
		keys     = 12
	)
	steps := 300_000
	if testing.Short() {
		steps = 30_000
	}

	hashOf := func(key int) uint32 { return uint32(key & 1) }
	// A value vouches for itself, and one that is all zeros does not: that
	// is what a node holds that the slab has let go of.
	const sum = 0x5ca1ab1e
	type value struct{ a, b, c int }
	valueOf := func(key, version int) value { return value{key, version, key ^ version ^ sum} }

	readers := NewReaders()
	var s Slab[int, value]
	s.Init(capacity, 1, readers)

	// present[key] is even while key is in the slab. The writer makes it odd
	// before it removes the key and even again after it has put it back, so
	// a reader that sees the same even number before and after a lookup
	// knows that the key was there all along.
	var present [keys]atomic.Uint32
	for key := range present {
		present[key].Store(1)
	}

	var (
		wg   sync.WaitGroup
		stop atomic.Bool
	)
	for range 4 {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(rand.Uint64(), 5))
			for !stop.Load() {
				key := rng.IntN(keys)
				before := present[key].Load()

				v, found, certain := s.Lookup(hashOf(key), key, 1, false)

				if found && (v.a != key || v.c != v.a^v.b^sum) {
					t.Errorf("Lookup(%d) found the value %v", key, v)
					return
				}
				if !found && certain && before&1 == 0 && present[key].Load() == before {
					t.Errorf("Lookup(%d) is certain that a key is missing that was there all along", key)
					return
				}
			}
		})
	}

	// The writer. A cache would hold the lock of the shard here.
	rng := rand.New(rand.NewPCG(7, 7))
	for step := range steps {
		key := rng.IntN(keys)
		idx := s.Find(hashOf(key), key)
		switch {
		case idx == 0 && s.Len() < capacity:
			s.PushFront(0, s.Insert(hashOf(key), key, valueOf(key, step)))
			present[key].Add(1)
		case idx != 0 && rng.IntN(3) == 0:
			s.Replace(idx, key, valueOf(key, step))
		case idx != 0:
			present[key].Add(1)
			s.Unlink(idx)
			s.Remove(idx)
		}
	}

	stop.Store(true)
	wg.Wait()

	if got := len(list(t, &s, 0)); got != s.Len() {
		t.Errorf("%d entries in the list, Len() = %d", got, s.Len())
	}
}

// TestNoAllocs is not parallel: counting allocations needs the process to
// itself.
func TestNoAllocs(t *testing.T) {
	readers := NewReaders()
	var s Slab[string, []byte]
	s.Init(64, 1, readers)
	value := []byte("value")

	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	allocs := testing.AllocsPerRun(1000, func() {
		for i, key := range keys {
			s.PushFront(0, s.Insert(uint32(i%3), key, value))
		}
		for i, key := range keys {
			s.Lookup(uint32(i%3), key, 1, false)

			idx := s.Replace(s.Find(uint32(i%3), key), key, value)
			s.Unlink(idx)
			s.Remove(idx)
		}
	})
	if allocs != 0 {
		t.Errorf("%v allocations per run, want 0", allocs)
	}
}

// TestPreferredSlots checks that goroutines do not all go for the same slot,
// which would make every one of them wait for the others.
func TestPreferredSlots(t *testing.T) {
	t.Parallel()

	const goroutines = 64

	r := newReaders(1024)
	s := &Slab[int, int]{readers: r}
	var (
		wg, started sync.WaitGroup
		release     = make(chan struct{})
		mu          sync.Mutex
		preferred   = map[uint64]int{}
	)
	// The goroutines are all there at the same time, as readers are that
	// get in each other's way: a goroutine that has ended leaves its stack
	// to the next one, and with it its slot.
	for range goroutines {
		started.Add(1)
		wg.Go(func() {
			slot := s.preferred()
			if again := s.preferred(); again != slot {
				t.Errorf("a goroutine prefers slot %d, and then slot %d", slot, again)
			}
			mu.Lock()
			preferred[slot]++
			mu.Unlock()
			started.Done()
			<-release
		})
	}
	started.Wait()
	close(release)
	wg.Wait()

	if len(preferred) < goroutines/2 {
		t.Errorf("%d goroutines prefer %d slots of %d between them, want most of them to have one to themselves",
			goroutines, len(preferred), len(r.slots))
	}
}
