package ghost

import (
	"math/rand/v2"
	"slices"
	"testing"
)

// model is a Set at its simplest: the hashes in the order in which they were
// added, with a zero where one has been removed.
type model struct {
	capacity int
	counting bool
	ring     []uint64
	asked    []int // for every place in ring: how often its hash was asked for
}

func (m *model) add(hash uint64) {
	if m.capacity == 0 || slices.Contains(m.ring, hash) {
		return
	}
	m.ring, m.asked = append(m.ring, hash), append(m.asked, 0)
	if len(m.ring) > m.capacity {
		m.ring, m.asked = m.ring[1:], m.asked[1:]
	}
}

func (m *model) remove(hash uint64) bool {
	i := slices.Index(m.ring, hash)
	if i < 0 {
		return false
	}
	m.ring[i] = 0
	return true
}

// recall removes the hash if this is at least the given number of times that
// it has been asked for, or if the model does not count.
func (m *model) recall(hash uint64, times int) bool {
	i := slices.Index(m.ring, hash)
	if i < 0 {
		return false
	}
	m.asked[i]++
	if m.counting && m.asked[i] < times {
		return false
	}
	m.ring[i] = 0
	return true
}

func (m *model) len() int {
	n := 0
	for _, hash := range m.ring {
		if hash != 0 {
			n++
		}
	}
	return n
}

// check verifies that the index refers to every hash in the ring exactly
// once, and to nothing else.
func check(t *testing.T, s *Set) {
	t.Helper()

	places := map[uint32]bool{}
	for _, place := range s.index {
		if place == 0 {
			continue
		}
		if places[place] || s.ring[place-1] == 0 {
			t.Fatalf("the index refers to place %d twice, or it is free", place-1)
		}
		places[place] = true
	}
	inRing := 0
	for i, hash := range s.ring {
		if hash == 0 {
			continue
		}
		inRing++
		if slot, ok := s.find(hash); !ok || s.index[slot] != uint32(i)+1 {
			t.Fatalf("the hash in place %d cannot be found", i)
		}
	}
	if len(places) != inRing || s.Len() != inRing {
		t.Fatalf("%d hashes in the ring, %d in the index, Len() = %d", inRing, len(places), s.Len())
	}
}

func TestSet(t *testing.T) {
	t.Parallel()

	var s Set
	s.Init(3)
	if s.Len() != 0 || s.Cap() != 3 || s.Remove(1) {
		t.Fatalf("a new set has Len() = %d and Cap() = %d", s.Len(), s.Cap())
	}

	s.Add(1)
	s.Add(2)
	s.Add(3)
	s.Add(2) // It is there already, and does not become any younger.
	if s.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", s.Len())
	}

	s.Add(4) // The oldest one, 1, makes way.
	if s.Remove(1) {
		t.Error("the set remembers 1 after three more hashes were added")
	}
	s.Add(5) // Now 2, although it was added again in between.
	if s.Remove(2) {
		t.Error("the set remembers 2 after three more hashes were added")
	}

	if !s.Remove(4) || s.Remove(4) || s.Len() != 2 {
		t.Errorf("removing 4 twice left Len() = %d", s.Len())
	}
	// The place of 4 stays free until its turn comes, so 3 and 5 are
	// remembered for as long as they would have been.
	s.Add(6)
	if !s.Remove(5) || s.Remove(3) {
		t.Error("after 6 was added, the set should remember 5 and not 3")
	}
	check(t, &s)

	s.Reset()
	if s.Len() != 0 || s.Remove(6) {
		t.Error("the set remembers something after Reset")
	}
	check(t, &s)
}

func TestNoCapacity(t *testing.T) {
	t.Parallel()

	var s Set
	s.Init(0)
	s.Add(1)
	if s.Len() != 0 || s.Remove(1) {
		t.Error("a set without room remembers a hash")
	}
	s.Reset()

	for _, capacity := range []int{-1, MaxCapacity + 1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Init(%d) did not panic", capacity)
				}
			}()
			s.Init(capacity)
		}()
	}
}

// TestAgainstModel adds and removes hashes that collide in every way: equal
// in the bits that the index goes by, equal but for the highest bit, zero.
func TestAgainstModel(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		capacity int
		counting bool
	}{{1, false}, {2, false}, {3, false}, {10, false}, {40, false}, {300, false}, {1, true}, {2, true}, {10, true}, {40, true}, {300, true}} {
		capacity := tt.capacity
		rng := rand.New(rand.NewPCG(uint64(capacity), 9))
		hashOf := func() uint64 {
			h := uint64(rng.IntN(3 * capacity))
			switch rng.IntN(3) {
			case 0:
				return h<<32 | 7 // The same home slot for all of these.
			case 1:
				return h * 0x9e3779b97f4a7c15
			default:
				return h
			}
		}

		var s Set
		if tt.counting {
			s.InitCounting(capacity)
		} else {
			s.Init(capacity)
		}
		m := model{capacity: capacity, counting: tt.counting}

		for step := range 20_000 {
			hash := hashOf()
			switch rng.IntN(4) {
			case 0:
				// The set tells hashes apart by all bits but the highest.
				got, want := s.Remove(hash), m.remove(hash|present)
				if got != want {
					t.Fatalf("capacity %d, step %d: Remove(%#x) = %t, want %t", capacity, step, hash, got, want)
				}
			case 1:
				times := rng.IntN(4)
				got, want := s.Recall(hash, times), m.recall(hash|present, times)
				if got != want {
					t.Fatalf("capacity %d, counting %t, step %d: Recall(%#x, %d) = %t, want %t",
						capacity, tt.counting, step, hash, times, got, want)
				}
			default:
				s.Add(hash)
				m.add(hash | present)
			}
			if s.Len() != m.len() {
				t.Fatalf("capacity %d, step %d: Len() = %d, want %d", capacity, step, s.Len(), m.len())
			}
			if step%256 == 0 {
				check(t, &s)
			}
		}
		check(t, &s)

		// What the set remembers in the end is what the model does.
		for _, hash := range m.ring {
			if hash != 0 && !s.Remove(hash) {
				t.Fatalf("capacity %d: the set has forgotten %#x", capacity, hash)
			}
		}
		if s.Len() != 0 {
			t.Fatalf("capacity %d: the set remembers %d hashes that the model does not", capacity, s.Len())
		}
	}
}

// TestRecall checks the count that a set keeps of how often a hash has been
// asked for: it is the hash's own, starts over when the hash is added anew,
// and is not there at all in a set that was not made to count.
func TestRecall(t *testing.T) {
	t.Parallel()

	var s Set
	s.InitCounting(2)

	s.Add(1)
	if s.Recall(1, 2) || s.Len() != 1 {
		t.Fatal("Recall took a hash out the first time it was asked for, where it takes two")
	}
	s.Add(1) // It is there already, and stays as it is.
	if !s.Recall(1, 2) || s.Len() != 0 {
		t.Fatal("Recall did not take a hash out the second time it was asked for")
	}
	if s.Recall(1, 2) || s.Recall(1, 1) {
		t.Fatal("Recall found a hash that it had taken out")
	}

	// A hash that is added again has not been asked for.
	s.Add(1)
	if s.Recall(1, 2) {
		t.Fatal("a hash that was added again counts from where it had been before")
	}
	// Nor has the hash that takes the place of another.
	s.Add(2)
	s.Add(3) // The set has room for two: this one takes the place of 1.
	if s.Recall(1, 1) {
		t.Fatal("the set has room for two hashes and remembers a third")
	}
	if s.Recall(3, 2) {
		t.Fatal("a hash has inherited the count of the one whose place it took")
	}
	if !s.Recall(3, 2) {
		t.Fatal("Recall did not take a hash out the second time it was asked for")
	}
	// A count of one, or of none, is reached the first time.
	if !s.Recall(2, 1) {
		t.Fatal("Recall for the first time did not take the hash out")
	}
	s.Add(7)
	if !s.Recall(7, 0) || s.Len() != 0 {
		t.Fatal("Recall with no count to reach did not take the hash out")
	}

	// The count is kept in a byte. Whoever asks for more than that holds
	// gets the hash at the 256th time, whatever the number.
	for _, times := range []int{256, 257, 1 << 20} {
		s.Add(8)
		for asked := 1; asked < 256; asked++ {
			if s.Recall(8, times) {
				t.Fatalf("Recall took a hash out when it was asked for the %d. time of %d", asked, times)
			}
		}
		if !s.Recall(8, times) || s.Len() != 0 {
			t.Fatalf("Recall did not take a hash out when it was asked for the 256th time of %d", times)
		}
	}

	// Reset forgets the counts with the hashes.
	s.Add(4)
	s.Recall(4, 3)
	s.Recall(4, 3)
	s.Reset()
	s.Add(4)
	if s.Recall(4, 2) {
		t.Fatal("a count has outlived Reset")
	}

	// A set that does not count takes a hash out whatever it is told.
	s.Init(2)
	s.Add(5)
	if !s.Recall(5, 100) {
		t.Fatal("a set that does not count has kept a hash that was asked for")
	}
	// And one that is set up again without counting has stopped.
	s.InitCounting(2)
	s.Init(2)
	s.Add(6)
	if !s.Recall(6, 2) {
		t.Fatal("Init has left the set counting")
	}
}

// TestNoAllocs is not parallel: counting allocations needs the process to
// itself.
func TestNoAllocs(t *testing.T) {
	var s Set
	s.InitCounting(100)

	hash, step := uint64(0), uint64(0x9e3779b97f4a7c15)
	allocs := testing.AllocsPerRun(1, func() {
		for range 10_000 {
			hash += step
			s.Add(hash)
			s.Remove(hash - 50*step)
			s.Recall(hash-20*step, 2)
		}
		s.Reset()
	})
	if allocs != 0 {
		t.Errorf("%v allocations, want 0", allocs)
	}
}
