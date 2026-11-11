package sketch

import (
	"math/rand/v2"
	"testing"
)

// model is a Sketch at its simplest: a counter in a byte for every place
// that a key can have one, and the arithmetic spelled out.
type model struct {
	s        *Sketch // for where the counters of a key are, which is not the model's business
	counters map[[2]uint64]int
	size     int
}

func (m *model) estimate(hash uint64) int {
	least := Most
	for _, p := range m.s.places(hash) {
		least = min(least, m.counters[[2]uint64{p.word, uint64(p.shift)}])
	}
	return least
}

func (m *model) add(hash uint64, n int) {
	if n <= 0 {
		return
	}
	added := false
	for _, p := range m.s.places(hash) {
		at := [2]uint64{p.word, uint64(p.shift)}
		if m.counters[at] < Most {
			m.counters[at] = min(m.counters[at]+n, Most)
			added = true
		}
	}
	if !added {
		return
	}
	m.size += n
	if m.size >= m.s.sample {
		odds := 0
		for at, count := range m.counters {
			odds += count % 2
			m.counters[at] = count / 2
		}
		m.size = (m.size - odds/4) / 2
	}
}

func TestInit(t *testing.T) {
	t.Parallel()

	// A word for every entry, in whole blocks, of which there is a power of
	// two.
	for capacity, words := range map[int]int{
		0: 8, 1: 8, 8: 8, 9: 16, 16: 16, 17: 32, 64: 64, 65: 128, 1000: 1024, 1024: 1024, 1025: 2048, 100_000: 131_072,
	} {
		var s Sketch
		s.Init(capacity, 10)
		if len(s.table) != words || int(s.mask) != words/wordsPerBlock-1 {
			t.Errorf("capacity %d: %d words in %d blocks, want %d words", capacity, len(s.table), s.mask+1, words)
		}
	}

	// Init empties a sketch that has been in use, the tally of what was
	// added included.
	var s Sketch
	s.Init(64, 1000)
	s.Add(1, 5)
	s.Add(2, 7)
	s.Init(64, 1000)
	if s.size != 0 || s.Estimate(1) != 0 || s.Estimate(2) != 0 {
		t.Errorf("after Init again: %d additions on record and estimates of %d and %d", s.size, s.Estimate(1), s.Estimate(2))
	}

	for name, init := range map[string]func(){
		"a negative capacity": func() { new(Sketch).Init(-1, 10) },
		"no sample":           func() { new(Sketch).Init(10, 0) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Init with %s did not panic", name)
				}
			}()
			init()
		}()
	}
}

// TestPlaces checks that the four counters of a key are in four words of one
// block, each with a pair of words to itself, and that keys spread over all
// there is, also when their hashes differ in a few bits only.
func TestPlaces(t *testing.T) {
	t.Parallel()

	var s Sketch
	s.Init(1024, 10)

	blocks, words, shifts := map[uint64]bool{}, map[uint64]bool{}, map[uint]bool{}
	// first has the places that the keys of a block have their first
	// counter in.
	first := map[uint64]map[place]bool{}
	for key := range uint64(20_000) {
		// The hashes of the keys of a shard: the upper bits are those that
		// picked the shard, and the same for all.
		p := s.places(0xabcd<<48 | key)
		block := p[0].word / wordsPerBlock
		for i, at := range p {
			if at.word/wordsPerBlock != block || at.word%wordsPerBlock/2 != uint64(i) {
				t.Fatalf("key %d: counter %d is in word %d, want one of the pair %d of block %d", key, i, at.word, i, block)
			}
			if at.shift%4 != 0 || at.shift > 60 {
				t.Fatalf("key %d: counter %d begins at bit %d", key, i, at.shift)
			}
			words[at.word], shifts[at.shift] = true, true
		}
		blocks[block] = true
		if first[block] == nil {
			first[block] = map[place]bool{}
		}
		first[block][p[0]] = true
	}
	// What picks the block of a key must not pick its counters as well, or
	// the keys of a block would all have the same ones. Some 150 keys to a
	// block leave few of the 32 places for a counter unused.
	for block, places := range first {
		if len(places) < 24 {
			t.Fatalf("the keys of block %d have their first counter in %d places, want most of 32", block, len(places))
		}
	}
	if len(blocks) != 128 || len(words) != 1024 || len(shifts) != 16 {
		t.Errorf("20,000 keys reach %d blocks, %d words and %d places in a word; want 128, 1024 and 16",
			len(blocks), len(words), len(shifts))
	}
}

// TestCounting follows single keys: what is added is what is estimated, up to
// the most that a counter holds, and a key that was never added is not there.
func TestCounting(t *testing.T) {
	t.Parallel()

	var s Sketch
	s.Init(1024, 1<<30)

	if got := s.Estimate(1); got != 0 {
		t.Fatalf("Estimate of a key in an empty sketch = %d", got)
	}
	s.Add(1, 1)
	s.Add(1, 2)
	s.Add(1, 0)
	s.Add(1, -5)
	if got := s.Estimate(1); got != 3 {
		t.Errorf("Estimate = %d after 1 and 2 were added, want 3", got)
	}
	if got := s.Estimate(2); got != 0 {
		t.Errorf("Estimate of another key = %d, want 0", got)
	}
	s.Add(1, 100)
	if got := s.Estimate(1); got != Most {
		t.Errorf("Estimate = %d after far more was added than a counter holds, want %d", got, Most)
	}
	// Nothing spills over into the counters next to them.
	for i, word := range s.table {
		for shift := 0; shift < 64; shift += 4 {
			if count := word >> shift & Most; count != 0 && count != Most {
				t.Fatalf("the counter at bit %d of word %d is %d", shift, i, count)
			}
		}
	}
	size := s.size
	s.Add(1, 1)
	if s.size != size {
		t.Errorf("a request that no counter had room for was counted: %d additions, then %d", size, s.size)
	}

	s.Reset()
	if got := s.Estimate(1); got != 0 || s.size != 0 {
		t.Errorf("after Reset the estimate is %d and %d additions are on record", got, s.size)
	}
}

// TestHalving checks that the counters are halved when the sample is full,
// and the tally of additions with them.
func TestHalving(t *testing.T) {
	t.Parallel()

	var s Sketch
	s.Init(1024, 20)
	s.Add(1, 7)
	s.Add(2, 6)
	s.Add(3, 6) // 19 so far.
	if s.Estimate(1) != 7 || s.Estimate(2) != 6 || s.size != 19 {
		t.Fatalf("before the sample is full: estimates %d and %d, %d additions", s.Estimate(1), s.Estimate(2), s.size)
	}
	s.Add(4, 3) // 22, which is over the sample.
	// Seven becomes three, six three, and three one. The eight counters of
	// the first and the last key were odd: rounding took two requests, a
	// quarter of one from each of them. That leaves 20 to be halved.
	if got := [4]int{s.Estimate(1), s.Estimate(2), s.Estimate(3), s.Estimate(4)}; got != [4]int{3, 3, 3, 1} {
		t.Errorf("after halving the estimates are %v, want [3 3 3 1]", got)
	}
	if s.size != 10 {
		t.Errorf("after halving %d additions are on record, want 10", s.size)
	}
}

// TestAgainstModel adds and estimates at random, with few enough places for
// keys to share counters and a sample that fills up many times.
func TestAgainstModel(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct{ capacity, sample, keys int }{
		{1, 50, 40}, {8, 100, 200}, {64, 640, 300}, {64, 100, 3000}, {1000, 10_000, 5000},
	} {
		var s Sketch
		s.Init(tt.capacity, tt.sample)
		m := model{s: &s, counters: map[[2]uint64]int{}}
		rng := rand.New(rand.NewPCG(uint64(tt.capacity), uint64(tt.sample)))

		for step := range 100_000 {
			// Some keys are far more popular than others.
			hash := uint64(rng.IntN(tt.keys)) * 0x9e3779b97f4a7c15
			if rng.IntN(4) == 0 {
				hash = uint64(rng.IntN(1 + tt.keys/50))
			}
			if rng.IntN(3) == 0 {
				if got, want := s.Estimate(hash), m.estimate(hash); got != want {
					t.Fatalf("capacity %d, step %d: Estimate(%#x) = %d, want %d", tt.capacity, step, hash, got, want)
				}
				continue
			}
			n := 1 + rng.IntN(3)
			s.Add(hash, n)
			m.add(hash, n)
			if s.size != m.size {
				t.Fatalf("capacity %d, step %d: %d additions on record, want %d", tt.capacity, step, s.size, m.size)
			}
		}
	}
}

// TestNeverTooLow checks the promise of a count-min sketch: until the
// counters are halved for the first time, no key is estimated at less than
// it was counted, as far as a counter goes. And with as many keys as it was
// made for, it is seldom too high.
func TestNeverTooLow(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		capacity, keys int
		exact          float64 // the share of estimates that are to be right
	}{
		{256, 2000, 0}, // Eight times the keys that it was made for.
		{2048, 2000, 0.95},
	} {
		var s Sketch
		s.Init(tt.capacity, 1<<30)
		rng := rand.New(rand.NewPCG(3, 4))
		counted := map[uint64]int{}
		for range 5000 {
			hash := uint64(rng.IntN(tt.keys)) * 0x9e3779b97f4a7c15
			s.Add(hash, 1)
			counted[hash]++
		}
		exact := 0
		for hash, n := range counted {
			got := s.Estimate(hash)
			if got < min(n, Most) {
				t.Fatalf("capacity %d: a key that was counted %d times is estimated at %d", tt.capacity, n, got)
			}
			if got == min(n, Most) {
				exact++
			}
		}
		if float64(exact) < tt.exact*float64(len(counted)) {
			t.Errorf("capacity %d: %d of %d estimates are exact, want %.0f%% at least", tt.capacity, exact, len(counted), 100*tt.exact)
		}
	}
}

// TestShared checks that two keys are said to share a counter exactly when
// counting one of them shows in a counter of the other.
func TestShared(t *testing.T) {
	t.Parallel()

	var s Sketch
	s.Init(16, 1<<30) // Two blocks: keys meet often.
	rng := rand.New(rand.NewPCG(7, 8))
	shared := 0
	for range 20_000 {
		a, b := rng.Uint64(), rng.Uint64()
		s.Reset()
		s.Add(a, 1)
		touched := false
		for _, p := range s.places(b) {
			touched = touched || s.table[p.word]>>p.shift&Most != 0
		}
		if got := s.Shared(a, b); got != touched || got != s.Shared(b, a) {
			t.Fatalf("Shared(%#x, %#x) = %t and %t the other way round, want %t", a, b, got, s.Shared(b, a), touched)
		}
		if touched {
			shared++
		}
	}
	if !s.Shared(1, 1) {
		t.Error("a key does not share its counters with itself")
	}
	// Half the pairs are in the same block, and of those about one in
	// eight has a counter in common.
	if shared < 800 || shared > 1700 {
		t.Errorf("%d of 20,000 pairs of keys share a counter, want about 1,200", shared)
	}
}

// TestNoAllocs is not parallel: counting allocations needs the process to
// itself.
func TestNoAllocs(t *testing.T) {
	var s Sketch
	s.Init(1000, 500)
	hash, step, sum := uint64(0), uint64(0x9e3779b97f4a7c15), 0
	allocs := testing.AllocsPerRun(1, func() {
		for range 10_000 {
			hash += step
			s.Add(hash, 1)
			sum += s.Estimate(hash - 10*step)
		}
		s.Reset()
	})
	if allocs != 0 {
		t.Errorf("%v allocations, want 0", allocs)
	}
	_ = sum
}

func BenchmarkAddAndEstimate(b *testing.B) {
	var s Sketch
	s.Init(1<<20, 10<<20)
	hash, step, sum := uint64(0), uint64(0x9e3779b97f4a7c15), 0
	for b.Loop() {
		hash += step
		s.Add(hash, 1)
		sum += s.Estimate(hash - 1000*step)
	}
	_ = sum
}
