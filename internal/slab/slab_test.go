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
		if idx <= Head || idx >= s.fresh {
			t.Fatalf("slot %d refers to node %d outside [%d, %d)", i, idx, Head+1, s.fresh)
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
	if handedOut := int(s.fresh - (Head + 1)); handedOut != free+len(want) {
		t.Fatalf("%d nodes handed out, but %d free + %d in use", handedOut, free, len(want))
	}
}

// list returns the keys of the list, front to back, after checking that the
// back links mirror the forward links.
func list[K comparable, V any](t *testing.T, s *Slab[K, V]) []K {
	t.Helper()

	var keys []K
	prev := uint32(Head)
	for idx := s.Nodes[Head].Next; idx != Head; idx = s.Nodes[idx].Next {
		if s.Nodes[idx].Prev != prev {
			t.Fatalf("node %d has Prev %d, want %d", idx, s.Nodes[idx].Prev, prev)
		}
		keys = append(keys, s.Nodes[idx].Key)
		prev = idx
		if len(keys) > len(s.Nodes) {
			t.Fatal("list has a cycle")
		}
	}
	if s.Nodes[Head].Prev != prev {
		t.Fatalf("Head has Prev %d, want %d", s.Nodes[Head].Prev, prev)
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

// homeAt returns the lowest hash whose home is the given slot of an index
// with size slots, for tests that want entries in particular places.
func homeAt(size, slot uint32) uint32 {
	return uint32((uint64(slot)<<32 + uint64(size) - 1) / uint64(size))
}

func TestIndexSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		capacity int
		want     uint
	}{
		{0, 1},
		{1, 3},
		{2, 6},
		{1000, 3000},
		// The largest index still has slots that 32 bits can address.
		{MaxCapacity, 3 << 30},
	}
	for _, tt := range tests {
		got := indexSize(tt.capacity)
		if got != tt.want {
			t.Errorf("indexSize(%d) = %d, want %d", tt.capacity, got, tt.want)
		}
		// An empty slot is what ends a probe for a missing key, and the
		// load factor stays at or below a third.
		if got < 3*uint(tt.capacity) || got <= uint(tt.capacity) {
			t.Errorf("indexSize(%d) = %d is too small", tt.capacity, got)
		}
	}
}

func TestInit(t *testing.T) {
	t.Parallel()

	for _, capacity := range []int{0, 1, 3, 1000} {
		var s Slab[int, int]
		s.Init(capacity)

		if s.Cap() != capacity {
			t.Errorf("Init(%d): Cap() = %d", capacity, s.Cap())
		}
		if s.Len() != 0 {
			t.Errorf("Init(%d): Len() = %d", capacity, s.Len())
		}
		if got, want := uint(len(s.slots)), indexSize(capacity); got != want {
			t.Errorf("Init(%d): %d slots, want %d", capacity, got, want)
		}
		if got := list(t, &s); len(got) != 0 {
			t.Errorf("Init(%d): list = %v, want empty", capacity, got)
		}
		if got := s.Find(42, 42); got != 0 {
			t.Errorf("Init(%d): Find on empty slab = %d", capacity, got)
		}
	}
}

func TestInitPanics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		capacity int
	}{
		{"negative capacity", -1},
		{"capacity too large", MaxCapacity + 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mustPanic(t, "Init", func() {
				var s Slab[int, int]
				s.Init(tt.capacity)
			})
		})
	}
}

func TestInsertFindRemove(t *testing.T) {
	t.Parallel()

	var s Slab[string, int]
	s.Init(4)
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
	if n := &s.Nodes[b]; n.Key != "b" || n.Value != 20 {
		t.Errorf("node b = {%q, %d}", n.Key, n.Value)
	}

	s.Remove(a)
	delete(want, "a")
	checkIndex(t, &s, want)
	if got := s.Find(1, "a"); got != 0 {
		t.Errorf(`Find("a") after Remove = %d, want 0`, got)
	}

	// The node that was just released is the next one to be used.
	c := s.Insert(3, "c", 30)
	want["c"] = 30
	checkIndex(t, &s, want)
	if c != a {
		t.Errorf("Insert reused node %d, want %d", c, a)
	}
}

// TestFill fills the index up to its maximum load with hashes that collide,
// and empties it again.
func TestFill(t *testing.T) {
	t.Parallel()

	const capacity = 1365 // 4095 slots, of which a third get used.

	var s Slab[int, int]
	s.Init(capacity)
	want := map[int]int{}

	// Many keys share a hash, and the hashes crowd into one end of the index.
	size := uint32(len(s.slots))
	hashOf := func(key int) uint32 { return homeAt(size, uint32(key%300)) }

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
	s.Init(40) // 120 slots.
	size := uint32(len(s.slots))

	if got := s.MaxDisplacement(); got != 0 {
		t.Errorf("MaxDisplacement() = %d on an empty slab", got)
	}

	// Entries that sit in their home slots are not displaced.
	for k := range 10 {
		s.Insert(homeAt(size, uint32(k)), k, k)
	}
	if got := s.MaxDisplacement(); got != 0 {
		t.Errorf("MaxDisplacement() = %d without collisions", got)
	}

	// Five entries that want the last slot. One gets it; the others wrap
	// around and land behind the ten entries above, in slots 10 to 13.
	for k := 100; k < 105; k++ {
		s.Insert(homeAt(size, size-1), k, k)
	}
	if got := s.MaxDisplacement(); got != 14 {
		t.Errorf("MaxDisplacement() = %d, want 14", got)
	}
}

func TestInsertIntoFullSlabPanics(t *testing.T) {
	t.Parallel()

	var s Slab[int, int]
	s.Init(2)
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
	empty.Init(0)
	mustPanic(t, "Insert into a slab without capacity", func() { empty.Insert(1, 1, 1) })
}

func TestRemovePanics(t *testing.T) {
	t.Parallel()

	var s Slab[string, int]
	s.Init(4)
	a := s.Insert(1, "a", 1)
	b := s.Insert(1, "b", 2)
	s.PushFront(a)
	s.PushFront(b)
	want := map[string]int{"a": 1, "b": 2}

	// Node 0 is what Find returns for a key that is not there.
	mustPanic(t, "Remove of node 0", func() { s.Remove(s.Find(9, "missing")) })
	checkIndex(t, &s, want)
	if got := list(t, &s); !slices.Equal(got, []string{"b", "a"}) {
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

	const (
		capacity = 40
		size     = 3 * capacity // slots
	)

	last := homeAt(size, size-1)
	hashes := []uint32{
		0, 0, 0, // Equal hashes: only the keys tell the entries apart.
		last, last, last, last, // Runs that start in the last slot and wrap.
		homeAt(size, size-2), homeAt(size, size-3),
		last + 1, 1<<32 - 1, // The same home slot under different hashes.
		homeAt(size, size/2), homeAt(size, size/2) + 1,
		homeAt(size, 5), homeAt(size, 5) + 1, homeAt(size, 6), homeAt(size, 7), homeAt(size, 7),
	}
	hashOf := func(key int) uint32 { return hashes[key%len(hashes)] }

	rng := rand.New(rand.NewPCG(1, 2))
	var s Slab[int, int]
	s.Init(capacity)
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

func TestList(t *testing.T) {
	t.Parallel()

	var s Slab[string, int]
	s.Init(8)

	idx := map[string]uint32{}
	for i, key := range []string{"a", "b", "c", "d"} {
		idx[key] = s.Insert(uint32(i), key, i)
		s.PushFront(idx[key])
	}
	expect := func(want ...string) {
		t.Helper()
		if got := list(t, &s); !slices.Equal(got, want) {
			t.Fatalf("list = %v, want %v", got, want)
		}
	}
	expect("d", "c", "b", "a")

	s.MoveToFront(idx["d"]) // Already first.
	expect("d", "c", "b", "a")

	s.MoveToFront(idx["a"]) // From the back.
	expect("a", "d", "c", "b")

	s.MoveToFront(idx["c"]) // From the middle.
	expect("c", "a", "d", "b")

	s.MoveToFront(idx["a"]) // From the second place.
	expect("a", "c", "d", "b")

	if back := s.Nodes[Head].Prev; back != idx["b"] {
		t.Fatalf("back of the list is node %d, want %d", back, idx["b"])
	}

	// Unlink from the back, the middle and the front, and link back in.
	s.Unlink(idx["b"])
	expect("a", "c", "d")
	s.Unlink(idx["c"])
	expect("a", "d")
	s.Unlink(idx["a"])
	expect("d")
	s.PushFront(idx["b"])
	expect("b", "d")

	s.Unlink(idx["b"])
	s.Unlink(idx["d"])
	expect()

	// A list of one.
	s.PushFront(idx["c"])
	s.MoveToFront(idx["c"])
	expect("c")
}

func TestDeleteFunc(t *testing.T) {
	t.Parallel()

	keys := []string{"a", "b", "c", "d", "e"} // The list runs e, d, c, b, a.
	tests := []struct {
		name string
		del  []string
		want []string
	}{
		{"none", nil, []string{"e", "d", "c", "b", "a"}},
		{"front", []string{"e"}, []string{"d", "c", "b", "a"}},
		{"back", []string{"a"}, []string{"e", "d", "c", "b"}},
		{"middle", []string{"c"}, []string{"e", "d", "b", "a"}},
		{"front and back", []string{"e", "a"}, []string{"d", "c", "b"}},
		{"two in front", []string{"e", "d"}, []string{"c", "b", "a"}},
		{"all", keys, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var s Slab[string, int]
			s.Init(len(keys))
			want := map[string]int{}
			for i, key := range keys {
				s.PushFront(s.Insert(uint32(i%2), key, i)) // Pairs of keys share a hash.
				want[key] = i
			}

			var visited []string
			n := s.DeleteFunc(func(key string, value int) bool {
				if value != want[key] {
					t.Errorf("DeleteFunc passed %q=%d, want %d", key, value, want[key])
				}
				visited = append(visited, key)
				return slices.Contains(tt.del, key)
			}, func(idx uint32) {
				s.Unlink(idx)
				s.Remove(idx)
			})
			for _, key := range tt.del {
				delete(want, key)
			}

			if n != len(tt.del) {
				t.Errorf("DeleteFunc returned %d, want %d", n, len(tt.del))
			}
			if !slices.Equal(visited, []string{"e", "d", "c", "b", "a"}) {
				t.Errorf("DeleteFunc visited %v, want every entry front to back", visited)
			}
			checkIndex(t, &s, want)
			if got := list(t, &s); !slices.Equal(got, tt.want) {
				t.Fatalf("list = %v, want %v", got, tt.want)
			}

			// The nodes it released take new entries.
			for i := range tt.del {
				key := string(rune('v' + i))
				s.PushFront(s.Insert(9, key, 10+i))
				want[key] = 10 + i
			}
			checkIndex(t, &s, want)
			if got := list(t, &s); len(got) != len(keys) {
				t.Errorf("list = %v after refilling, want %d entries", got, len(keys))
			}
		})
	}
}

func TestReset(t *testing.T) {
	t.Parallel()

	var s Slab[string, *int]
	s.Init(8)

	for i, key := range []string{"a", "b", "c", "d", "e"} {
		s.PushFront(s.Insert(uint32(i), key, new(i)))
	}
	b := s.Find(1, "b")
	s.Unlink(b)
	s.Remove(b)

	s.Reset()

	checkIndex(t, &s, map[string]*int{})
	if got := list(t, &s); len(got) != 0 {
		t.Errorf("list = %v after Reset, want empty", got)
	}
	for i := Head + 1; i < len(s.Nodes); i++ {
		if n := &s.Nodes[i]; n.Key != "" || n.Value != nil || n.Prev != 0 || n.Next != 0 {
			t.Errorf("node %d is not cleared after Reset", i)
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
	s.Init(64)
	value := []byte("value")

	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	allocs := testing.AllocsPerRun(1000, func() {
		for i, key := range keys {
			s.PushFront(s.Insert(uint32(i%3), key, value))
		}
		for i, key := range keys {
			idx := s.Find(uint32(i%3), key)
			s.MoveToFront(idx)
			s.Unlink(idx)
			s.Remove(idx)
		}
		s.Reset()
	})
	if allocs != 0 {
		t.Errorf("%v allocations per run, want 0", allocs)
	}
}

// FuzzSlab interprets its input as a sequence of operations on a slab and
// checks the slab against a map and a slice after every step. Keys share few
// hashes, so collisions are the norm.
func FuzzSlab(f *testing.F) {
	f.Add(uint8(5), []byte{0, 1, 2, 3, 0, 1, 2, 3})
	f.Add(uint8(1), []byte{255, 254, 253, 255, 254, 253, 7, 7, 7})
	f.Add(uint8(40), []byte("the quick brown fox jumps over the lazy dog"))
	f.Add(uint8(0), []byte{1, 2, 3})

	f.Fuzz(func(t *testing.T, size uint8, ops []byte) {
		capacity := int(size % 48)

		var s Slab[byte, int]
		s.Init(capacity)
		want := map[byte]int{}
		var order []byte

		for step, op := range ops {
			key := op & 31
			hash := uint32(key%5) * 0x4000_0003
			idx := s.Find(hash, key)

			switch action := op >> 5; {
			case action == 7 && key == 31:
				s.Reset()
				clear(want)
				order = nil

			case idx == 0:
				if len(want) == capacity {
					continue
				}
				s.PushFront(s.Insert(hash, key, step))
				want[key] = step
				order = slices.Insert(order, 0, key)

			case action < 4:
				s.MoveToFront(idx)
				order = slices.Insert(slices.DeleteFunc(order, func(k byte) bool { return k == key }), 0, key)

			default:
				s.Unlink(idx)
				s.Remove(idx)
				delete(want, key)
				order = slices.DeleteFunc(order, func(k byte) bool { return k == key })
			}

			checkIndex(t, &s, want)
			if got := list(t, &s); !slices.Equal(got, order) {
				t.Fatalf("step %d: list = %v, want %v", step, got, order)
			}
		}
	})
}
