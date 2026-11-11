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
	ring     []uint64
}

func (m *model) add(hash uint64) {
	if m.capacity == 0 || slices.Contains(m.ring, hash) {
		return
	}
	m.ring = append(m.ring, hash)
	if len(m.ring) > m.capacity {
		m.ring = m.ring[1:]
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

	for _, capacity := range []int{1, 2, 3, 10, 40, 300} {
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
		s.Init(capacity)
		m := model{capacity: capacity}

		for step := range 20_000 {
			hash := hashOf()
			if rng.IntN(3) == 0 {
				// The set tells hashes apart by all bits but the highest.
				got, want := s.Remove(hash), m.remove(hash|present)
				if got != want {
					t.Fatalf("capacity %d, step %d: Remove(%#x) = %t, want %t", capacity, step, hash, got, want)
				}
			} else {
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

// TestNoAllocs is not parallel: counting allocations needs the process to
// itself.
func TestNoAllocs(t *testing.T) {
	var s Set
	s.Init(100)

	hash, step := uint64(0), uint64(0x9e3779b97f4a7c15)
	allocs := testing.AllocsPerRun(1, func() {
		for range 10_000 {
			hash += step
			s.Add(hash)
			s.Remove(hash - 50*step)
		}
		s.Reset()
	})
	if allocs != 0 {
		t.Errorf("%v allocations, want 0", allocs)
	}
}
