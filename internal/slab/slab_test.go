package slab

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// checkIndex verifies that the index and the nodes agree: every slot refers
// to a distinct entry under that entry's hash, every entry can be reached from
// its home slot without crossing an empty one, and the free list accounts for
// every node that was handed out and is no longer in use.
func checkIndex[K comparable, V any](t *testing.T, s *Slab[K, V], want map[K]V) {
	t.Helper()

	if s.Len() != len(want) {
		t.Fatalf("Len() = %d, want %d", s.Len(), len(want))
	}

	indexed := make(map[uint32]bool, len(want))
	for i, slot := range s.slots {
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
		if n.hash != hash {
			t.Fatalf("slot %d has hash %#x, node %d has %#x", i, hash, idx, n.hash)
		}
		value, ok := want[n.Key]
		if !ok {
			t.Fatalf("node %d holds unexpected key %v", idx, n.Key)
		}
		if any(n.Value) != any(value) {
			t.Fatalf("key %v has value %v, want %v", n.Key, n.Value, value)
		}
		if got := s.Find(n.hash, n.Key); got != idx {
			t.Fatalf("Find(%v) = %d, want %d", n.Key, got, idx)
		}
	}
	if len(indexed) != len(want) {
		t.Fatalf("%d nodes are indexed, want %d", len(indexed), len(want))
	}

	free := 0
	for idx := s.free; idx != 0; idx = s.Nodes[idx].Next {
		if indexed[idx] {
			t.Fatalf("node %d is both indexed and free", idx)
		}
		var (
			zeroKey   K
			zeroValue V
		)
		if n := &s.Nodes[idx]; n.Key != zeroKey || any(n.Value) != any(zeroValue) {
			t.Fatalf("free node %d still holds %v=%v", idx, n.Key, n.Value)
		}
		free++
		if free > len(s.Nodes) {
			t.Fatal("free list has a cycle")
		}
	}
	if handedOut := int(s.fresh - s.heads); handedOut != free+len(want) {
		t.Fatalf("%d nodes handed out, but %d free + %d in use", handedOut, free, len(want))
	}
}

// list returns the keys of the list at head, front to back, after checking
// that the back links mirror the forward links.
func list[K comparable, V any](t *testing.T, s *Slab[K, V], head uint32) []K {
	t.Helper()

	var keys []K
	prev := head
	for idx := s.Nodes[head].Next; idx != head; idx = s.Nodes[idx].Next {
		if s.Nodes[idx].Prev != prev {
			t.Fatalf("node %d has Prev %d, want %d", idx, s.Nodes[idx].Prev, prev)
		}
		keys = append(keys, s.Nodes[idx].Key)
		prev = idx
		if len(keys) > len(s.Nodes) {
			t.Fatal("list has a cycle")
		}
	}
	if s.Nodes[head].Prev != prev {
		t.Fatalf("head %d has Prev %d, want %d", head, s.Nodes[head].Prev, prev)
	}
	return keys
}

// mustPanic runs f and fails the test unless f panics.
func mustPanic(t *testing.T, what string, f func()) {
	t.Helper()

	defer func() {
		if recover() == nil {
			t.Errorf("%s did not panic", what)
		}
	}()
	f()
}

func TestIndexSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		capacity int
		want     uint
	}{
		{0, 1},
		{1, 2},
		{2, 4},
		{3, 8},
		{5, 8},
		{6, 16},
		{1000, 2048},
		{1365, 2048},
		{1366, 4096},
		// The largest index still has slots that 32 bits can address.
		{MaxCapacity, 1 << 31},
	}
	for _, tt := range tests {
		got := indexSize(tt.capacity)
		if got != tt.want {
			t.Errorf("indexSize(%d) = %d, want %d", tt.capacity, got, tt.want)
		}
		// An empty slot is what ends a probe for a missing key, and the
		// load factor stays at or below 2/3. Twice the largest index is
		// more than 32 bits hold.
		if 2*uint64(got) < 3*uint64(tt.capacity) || got <= uint(tt.capacity) {
			t.Errorf("indexSize(%d) = %d is too small", tt.capacity, got)
		}
	}
}

func TestInit(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct{ capacity, heads int }{{0, 1}, {1, 1}, {3, 3}, {1000, 2}, {5, maxHeads}} {
		var s Slab[int, int]
		s.Init(tt.capacity, tt.heads)

		if s.Cap() != tt.capacity {
			t.Errorf("Init(%d, %d): Cap() = %d", tt.capacity, tt.heads, s.Cap())
		}
		if s.Len() != 0 {
			t.Errorf("Init(%d, %d): Len() = %d", tt.capacity, tt.heads, s.Len())
		}
		if got, want := uint(len(s.slots)), indexSize(tt.capacity); got != want {
			t.Errorf("Init(%d, %d): %d slots, want %d", tt.capacity, tt.heads, got, want)
		}
		for head := range uint32(tt.heads) {
			if got := list(t, &s, head); len(got) != 0 {
				t.Errorf("Init(%d, %d): list %d = %v, want empty", tt.capacity, tt.heads, head, got)
			}
		}
		if got := s.Find(42, 42); got != 0 {
			t.Errorf("Init(%d, %d): Find on empty slab = %d", tt.capacity, tt.heads, got)
		}
	}
}

func TestInitPanics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		capacity, heads int
	}{
		{"no heads", 8, 0},
		{"too many heads", 8, maxHeads + 1},
		{"negative capacity", -1, 1},
		{"capacity too large", MaxCapacity + 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mustPanic(t, "Init", func() {
				var s Slab[int, int]
				s.Init(tt.capacity, tt.heads)
			})
		})
	}
}

func TestInsertFindRemove(t *testing.T) {
	t.Parallel()

	var s Slab[string, int]
	s.Init(4, 1)
	want := map[string]int{}

	a := s.Insert(1, "a", 10)
	b := s.Insert(2, "b", 20)
	want["a"], want["b"] = 10, 20
	checkIndex(t, &s, want)

	if a == 0 || b == 0 || a == b {
		t.Fatalf("Insert returned indices %d and %d", a, b)
	}
	if got := s.Find(1, "a"); got != a {
		t.Errorf(`Find("a") = %d, want %d`, got, a)
	}
	if got := s.Find(1, "b"); got != 0 {
		t.Errorf(`Find("b") under the hash of "a" = %d, want 0`, got)
	}
	if got := s.Find(3, "c"); got != 0 {
		t.Errorf(`Find("c") = %d, want 0`, got)
	}
	if n := &s.Nodes[b]; n.Key != "b" || n.Value != 20 || n.Meta != 0 {
		t.Errorf("node b = {%q, %d, meta %d}", n.Key, n.Value, n.Meta)
	}

	s.Nodes[a].Meta = 7
	s.Remove(a)
	delete(want, "a")
	checkIndex(t, &s, want)
	if got := s.Find(1, "a"); got != 0 {
		t.Errorf(`Find("a") after Remove = %d, want 0`, got)
	}

	// The node that was just released is the next one to be used, and it
	// comes back with a clean Meta.
	c := s.Insert(3, "c", 30)
	want["c"] = 30
	checkIndex(t, &s, want)
	if c != a {
		t.Errorf("Insert reused node %d, want %d", c, a)
	}
	if got := s.Nodes[c].Meta; got != 0 {
		t.Errorf("Meta of reused node = %d, want 0", got)
	}
}

// TestFill fills the index up to its maximum load with hashes that collide,
// and empties it again.
func TestFill(t *testing.T) {
	t.Parallel()

	const capacity = 1365 // 2048 slots, of which two thirds get used.

	// Many keys share a hash, and the hashes crowd into one end of the index.
	hashOf := func(key int) uint32 { return uint32(key % 300) }

	var s Slab[int, int]
	s.Init(capacity, 2)
	want := map[int]int{}

	for round := range 3 {
		for k := range capacity {
			s.Insert(hashOf(k), k, k+round)
			want[k] = k + round
		}
		checkIndex(t, &s, want)
		if s.Len() != s.Cap() {
			t.Fatalf("Len() = %d, Cap() = %d", s.Len(), s.Cap())
		}
		if s.MaxDisplacement() < 100 {
			t.Fatalf("MaxDisplacement() = %d: the hashes were meant to collide", s.MaxDisplacement())
		}

		// Take the entries out in an order that has nothing to do with
		// the order in which they went in.
		for i := range capacity {
			k := i * 1009 % capacity
			s.Remove(s.Find(hashOf(k), k))
			delete(want, k)
			if i%100 == 0 {
				checkIndex(t, &s, want)
			}
		}
		checkIndex(t, &s, want)
	}
}

func TestMaxDisplacement(t *testing.T) {
	t.Parallel()

	var s Slab[int, int]
	s.Init(40, 1) // 64 slots.

	if got := s.MaxDisplacement(); got != 0 {
		t.Errorf("MaxDisplacement() = %d on an empty slab", got)
	}

	// Entries that sit in their home slots are not displaced.
	for k := range 10 {
		s.Insert(uint32(k), k, k)
	}
	if got := s.MaxDisplacement(); got != 0 {
		t.Errorf("MaxDisplacement() = %d without collisions", got)
	}

	// Five entries that want the last slot. One gets it; the others wrap
	// around and land behind the ten entries above, in slots 10 to 13.
	for k := 100; k < 105; k++ {
		s.Insert(63, k, k)
	}
	if got := s.MaxDisplacement(); got != 14 {
		t.Errorf("MaxDisplacement() = %d, want 14", got)
	}
}

func TestInsertIntoFullSlabPanics(t *testing.T) {
	t.Parallel()

	var s Slab[int, int]
	s.Init(2, 1)
	s.Insert(1, 1, 1)
	s.Insert(2, 2, 2)
	want := map[int]int{1: 1, 2: 2}

	mustPanic(t, "Insert into a full slab", func() { s.Insert(3, 3, 3) })

	// The slab is none the worse for it.
	checkIndex(t, &s, want)
	s.Remove(s.Find(1, 1))
	s.Insert(3, 3, 3)
	checkIndex(t, &s, map[int]int{2: 2, 3: 3})
	s.Reset()
	checkIndex(t, &s, map[int]int{})

	var empty Slab[int, int]
	empty.Init(0, 1)
	mustPanic(t, "Insert into a slab without capacity", func() { empty.Insert(1, 1, 1) })
}

func TestRemovePanics(t *testing.T) {
	t.Parallel()

	var s Slab[string, int]
	s.Init(4, 1)
	a := s.Insert(1, "a", 1)
	b := s.Insert(1, "b", 2)
	s.PushFront(0, a)
	s.PushFront(0, b)
	want := map[string]int{"a": 1, "b": 2}

	// Node 0 is what Find returns for a key that is not there.
	mustPanic(t, "Remove of node 0", func() { s.Remove(s.Find(9, "missing")) })
	checkIndex(t, &s, want)
	if got := list(t, &s, 0); !slices.Equal(got, []string{"b", "a"}) {
		t.Errorf("list = %v after the failed Remove, want [b a]", got)
	}

	s.Unlink(a)
	s.Remove(a)
	delete(want, "a")
	mustPanic(t, "Remove of a node that was removed already", func() { s.Remove(a) })
	checkIndex(t, &s, want)
}

// TestCollisions drives the index with hashes picked to collide in every way
// that matters: equal hashes, equal home slots under different hashes, and
// runs that wrap around the end of the table.
func TestCollisions(t *testing.T) {
	t.Parallel()

	const capacity = 40 // 64 slots.

	hashes := []uint32{
		0, 0, 0, // Equal hashes: only the keys tell the entries apart.
		63, 63, 63, 63, // Runs that start in the last slot and wrap.
		62, 61,
		63 + 64, 63 + 128, // The same home slot under different hashes.
		1 << 31, 1<<31 + 63, 1<<32 - 1,
		5, 5 + 64, 6, 7, 7,
	}
	hashOf := func(key int) uint32 { return hashes[key%len(hashes)] }

	rng := rand.New(rand.NewPCG(1, 2))
	var s Slab[int, int]
	s.Init(capacity, 1)
	want := map[int]int{}

	for step := range 20_000 {
		key := rng.IntN(3 * capacity)
		hash := hashOf(key)
		idx := s.Find(hash, key)

		_, present := want[key]
		if (idx != 0) != present {
			t.Fatalf("step %d: Find(%d) = %d, but present = %t", step, key, idx, present)
		}

		switch {
		case present && rng.IntN(2) == 0:
			s.Remove(idx)
			delete(want, key)
		case !present && len(want) < capacity:
			s.Insert(hash, key, step)
			want[key] = step
		}

		if step%64 == 0 {
			checkIndex(t, &s, want)
		}
	}
	checkIndex(t, &s, want)
}

func TestLists(t *testing.T) {
	t.Parallel()

	const (
		first  = 0
		second = 1
	)

	var s Slab[string, int]
	s.Init(8, 2)

	idx := map[string]uint32{}
	for i, key := range []string{"a", "b", "c", "d"} {
		idx[key] = s.Insert(uint32(i), key, i)
		s.PushFront(first, idx[key])
	}
	expect := func(head uint32, want ...string) {
		t.Helper()
		if got := list(t, &s, head); !slices.Equal(got, want) {
			t.Fatalf("list %d = %v, want %v", head, got, want)
		}
	}
	expect(first, "d", "c", "b", "a")
	expect(second)

	s.MoveToFront(first, idx["d"]) // Already first.
	expect(first, "d", "c", "b", "a")

	s.MoveToFront(first, idx["a"]) // From the back.
	expect(first, "a", "d", "c", "b")

	s.MoveToFront(first, idx["c"]) // From the middle.
	expect(first, "c", "a", "d", "b")

	s.MoveToFront(first, idx["a"]) // From the second place.
	expect(first, "a", "c", "d", "b")

	if back := s.Nodes[first].Prev; back != idx["b"] {
		t.Fatalf("back of the list is node %d, want %d", back, idx["b"])
	}

	// Move nodes over to the second list.
	s.Unlink(idx["a"])
	s.PushFront(second, idx["a"])
	s.Unlink(idx["b"])
	s.PushFront(second, idx["b"])
	expect(first, "c", "d")
	expect(second, "b", "a")

	s.MoveToFront(second, idx["a"])
	expect(first, "c", "d")
	expect(second, "a", "b")

	s.Unlink(idx["c"])
	s.Unlink(idx["d"])
	expect(first)
	expect(second, "a", "b")

	// A list of one.
	s.PushFront(first, idx["c"])
	s.MoveToFront(first, idx["c"])
	expect(first, "c")
}

func TestReset(t *testing.T) {
	t.Parallel()

	var s Slab[string, *int]
	s.Init(8, 2)

	for i, key := range []string{"a", "b", "c", "d", "e"} {
		idx := s.Insert(uint32(i), key, new(i))
		s.Nodes[idx].Meta = 1
		s.PushFront(uint32(i%2), idx)
	}
	b := s.Find(1, "b")
	s.Unlink(b)
	s.Remove(b)

	s.Reset()

	checkIndex(t, &s, map[string]*int{})
	for head := range uint32(2) {
		if got := list(t, &s, head); len(got) != 0 {
			t.Errorf("list %d = %v after Reset, want empty", head, got)
		}
	}
	for i := range s.Nodes[2:] {
		if n := &s.Nodes[2+i]; n.Key != "" || n.Value != nil || n.Meta != 0 {
			t.Errorf("node %d is not cleared after Reset", 2+i)
		}
	}

	// The slab is as good as new.
	want := map[string]*int{}
	for i, key := range []string{"v", "w", "x", "y", "z", "a", "b", "c"} {
		want[key] = new(i)
		s.Insert(uint32(i), key, want[key])
	}
	checkIndex(t, &s, want)
}

// TestNoAllocs is not parallel: counting allocations needs the process to
// itself.
func TestNoAllocs(t *testing.T) {
	var s Slab[string, []byte]
	s.Init(64, 1)
	value := []byte("value")

	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	allocs := testing.AllocsPerRun(1000, func() {
		for i, key := range keys {
			s.PushFront(0, s.Insert(uint32(i%3), key, value))
		}
		for i, key := range keys {
			idx := s.Find(uint32(i%3), key)
			s.MoveToFront(0, idx)
			s.Unlink(idx)
			s.Remove(idx)
		}
		s.Reset()
	})
	if allocs != 0 {
		t.Errorf("%v allocations per run, want 0", allocs)
	}
}

// FuzzSlab interprets its input as a sequence of operations on a slab with
// two lists and checks the slab against a map and two slices after every
// step. Keys share few hashes, so collisions are the norm.
func FuzzSlab(f *testing.F) {
	f.Add(uint8(5), []byte{0, 1, 2, 3, 0, 1, 2, 3})
	f.Add(uint8(1), []byte{255, 254, 253, 255, 254, 253, 7, 7, 7})
	f.Add(uint8(40), []byte("the quick brown fox jumps over the lazy dog"))
	f.Add(uint8(0), []byte{1, 2, 3})

	f.Fuzz(func(t *testing.T, size uint8, ops []byte) {
		capacity := int(size % 48)

		var s Slab[byte, int]
		s.Init(capacity, 2)
		want := map[byte]int{}
		lists := [2][]byte{}

		for step, op := range ops {
			key := op & 31
			hash := uint32(key%5) * 0x4000_0003
			idx := s.Find(hash, key)

			switch action := op >> 5; {
			case action == 7 && key == 31:
				s.Reset()
				clear(want)
				lists = [2][]byte{}

			case idx == 0:
				if len(want) == capacity {
					continue
				}
				head := uint32(action & 1)
				s.PushFront(head, s.Insert(hash, key, step))
				want[key] = step
				lists[head] = slices.Insert(lists[head], 0, key)

			case action < 4:
				// Move the entry to the front of the list it is in.
				head := uint32(0)
				if slices.Contains(lists[1], key) {
					head = 1
				}
				s.MoveToFront(head, idx)
				l := lists[head]
				lists[head] = slices.Insert(slices.DeleteFunc(l, func(k byte) bool { return k == key }), 0, key)

			default:
				s.Unlink(idx)
				s.Remove(idx)
				delete(want, key)
				for head := range lists {
					lists[head] = slices.DeleteFunc(lists[head], func(k byte) bool { return k == key })
				}
			}

			checkIndex(t, &s, want)
			for head := range lists {
				if got := list(t, &s, uint32(head)); !slices.Equal(got, lists[head]) {
					t.Fatalf("step %d: list %d = %v, want %v", step, head, got, lists[head])
				}
			}
		}
	})
}
