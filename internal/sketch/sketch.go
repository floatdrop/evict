// Package sketch provides the memory that a cache has of how often keys have
// been asked for of late, whether it holds them or not: the frequency sketch
// of TinyLFU.
//
// It is a count-min sketch. A key has four counters among many, picked by its
// hash, and shares each of them with other keys. Counting a request adds to
// all four, and the lowest of the four is the estimate: it is never too low,
// and too high only if every one of the four has been pushed up by other
// keys. A counter has four bits and stops at fifteen, and all counters are
// halved every so often, so that what was popular once does not stay so for
// good.
//
// The layout is the one that Caffeine uses: sixteen counters to a word, eight
// words to a block, and the four counters of a key in four different words of
// one block, which is one cache line.
//
// TinyLFU is described in "TinyLFU: A Highly Efficient Cache Admission Policy"
// by Einziger, Friedman and Manes (ACM Transactions on Storage, 2017).
package sketch

import "math/bits"

// Most is as far as a counter goes.
const Most = 15

// A word holds sixteen counters and a block eight words.
const (
	wordsPerBlock = 8

	// odd has the lowest bit of every counter in a word, and halved all the
	// bits that a counter may have set once it has been halved.
	odd    = 0x1111111111111111
	halved = 0x7777777777777777
)

// Sketch counts requests for keys, by their hashes. It does not allocate once
// it has been initialized. A Sketch is not safe for concurrent use.
type Sketch struct {
	table []uint64
	mask  uint64 // number of blocks minus one

	// size is what has been added since the counters were last halved,
	// and sample how much may be added before they are halved again.
	size, sample int
}

// Init readies the sketch for a cache of the given capacity and empties it.
// It gets a word for every entry of the cache, which is sixteen counters,
// rounded up to a whole number of blocks that is a power of two. It halves its
// counters, and with them its tally of the requests counted, whenever that
// tally reaches sample: the first time after sample requests, and after about
// half as many from then on. It panics if capacity is negative or sample is
// less than one.
func (s *Sketch) Init(capacity, sample int) {
	if capacity < 0 || sample < 1 {
		panic("sketch: invalid capacity or sample")
	}

	blocks := uint64(1)
	if words := uint64(capacity); words > wordsPerBlock {
		blocks = 1 << bits.Len64((words-1)/wordsPerBlock)
	}
	s.table = make([]uint64, blocks*wordsPerBlock)
	s.mask = blocks - 1
	s.size, s.sample = 0, sample
}

// spread scatters the bits of a hash, so that keys whose hashes have bits in
// common, as those of one shard of a cache have, do not share their counters
// for it.
func spread(x uint64) uint64 {
	x = (x ^ x>>30) * 0xbf58476d1ce4e5b9
	x = (x ^ x>>27) * 0x94d049bb133111eb
	return x ^ x>>31
}

// A place is where one of the four counters of a key is: the word, and how
// far into it the counter begins.
type place struct {
	word  uint64
	shift uint
}

// places returns where the counters of the key with the given hash are. The
// upper half of the scattered hash picks the block. Of the lower half, each
// byte picks a counter: its lowest bit one of two words that are that
// counter's alone, and the next four bits the counter in the word.
func (s *Sketch) places(hash uint64) [4]place {
	h := spread(hash)
	block := (h >> 32 & s.mask) * wordsPerBlock
	var p [4]place
	for i := range uint64(4) {
		b := h >> (8 * i)
		p[i] = place{word: block + 2*i + b&1, shift: uint(b >> 1 & 15 * 4)}
	}
	return p
}

// Estimate returns how often the key with the given hash has been counted of
// late, or more: the lowest of its four counters.
func (s *Sketch) Estimate(hash uint64) int {
	least := uint64(Most)
	for _, p := range s.places(hash) {
		least = min(least, s.table[p.word]>>p.shift&Most)
	}
	return int(least)
}

// Add counts n more requests for the key with the given hash. A counter that
// has reached [Most] stays there, and if all four have, nothing is counted.
func (s *Sketch) Add(hash uint64, n int) {
	if n <= 0 {
		return
	}
	added := false
	for _, p := range s.places(hash) {
		if count := s.table[p.word] >> p.shift & Most; count < Most {
			s.table[p.word] += min(uint64(n), Most-count) << p.shift
			added = true
		}
	}
	if !added {
		return
	}

	s.size += n
	if s.size >= s.sample {
		s.halve()
	}
}

// halve halves every counter, rounding down, and with them the tally of what
// has been added, less what the rounding took: a quarter of a request for
// every counter that was odd, since a request is in four counters.
func (s *Sketch) halve() {
	odds := 0
	for i, word := range s.table {
		odds += bits.OnesCount64(word & odd)
		s.table[i] = word >> 1 & halved
	}
	s.size = (s.size - odds/4) / 2
}

// Shared reports whether the keys with the two hashes share any of their
// counters. It is for the tests of a cache, which need keys that do not: of
// those the sketch knows exactly how often each one was counted.
func (s *Sketch) Shared(a, b uint64) bool {
	pa, pb := s.places(a), s.places(b)
	for i := range pa {
		// The counters of a key are in four pairs of words, one in each,
		// so only those with the same number can be the same.
		if pa[i] == pb[i] {
			return true
		}
	}
	return false
}

// Reset empties the sketch.
func (s *Sketch) Reset() {
	clear(s.table)
	s.size = 0
}
