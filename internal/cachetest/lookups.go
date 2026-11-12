package cachetest

import (
	"math/rand/v2"
	"strconv"
	"strings"
)

// The functions below make the keys for benchmarks of lookups in caches that
// are too large for the CPU's caches. A sequence of keys to look up lies in
// memory one after the other, so that reading the next key costs next to
// nothing and what is measured is the cache. A string key in a sequence is a
// copy of the key in the cache, as a key that has just come in would be: its
// bytes are at hand, and comparing it with the key in the cache loads those of
// the other one.

// SeqLen is the length of a sequence of lookups: long enough that a benchmark
// does not come back to the same keys while they are still in the CPU's
// caches. It is a power of two.
const SeqLen = 1 << 22

// width is the length of a key: that of "user:profile:1000000000", as in the
// bench module.
const width = 23

// names returns n keys of width bytes, cut out of one string.
func names(n int) []string {
	var b strings.Builder
	b.Grow(width * n)
	for i := range n {
		b.WriteString("user:profile:")
		b.WriteString(strconv.Itoa(1_000_000_000 + i))
	}
	all := b.String()
	keys := make([]string, n)
	for i := range keys {
		keys[i] = all[width*i : width*(i+1)]
	}
	return keys
}

// copies returns a copy of keys[k] for every k in picks, one after the other.
func copies(keys []string, picks []uint32) []string {
	var b strings.Builder
	b.Grow(width * len(picks))
	for _, k := range picks {
		b.WriteString(keys[k])
	}
	all := b.String()
	out := make([]string, len(picks))
	for i := range out {
		out[i] = all[width*i : width*(i+1)]
	}
	return out
}

// uniform returns SeqLen numbers picked at random from [0, n).
func uniform(n int) []uint32 {
	rng := rand.New(rand.NewPCG(3, 4))
	picks := make([]uint32, SeqLen)
	for i := range picks {
		picks[i] = uint32(rng.IntN(n))
	}
	return picks
}

// UniformInts returns the keys 0 to n-1, to fill a cache with, and a sequence
// of lookups of them picked at random.
func UniformInts(n int) (keys, seq []int) {
	keys = make([]int, n)
	for i := range keys {
		keys[i] = i
	}
	seq = make([]int, SeqLen)
	for i, k := range uniform(n) {
		seq[i] = int(k)
	}
	return keys, seq
}

// UniformStrings is UniformInts with keys of 23 bytes.
func UniformStrings(n int) (keys, seq []string) {
	keys = names(n)
	return keys, copies(keys, uniform(n))
}

// ZipfStrings returns requests as the bench module's ReadThrough makes them
// for a cache of the given capacity: for four times as many keys as it has
// room for, with a popularity that follows a Zipf distribution of skew 1.01.
// The cache is to be read through with the first sixteen requests per entry
// that it has room for, a Get and, if that misses, a Set, so that it holds
// what its policy keeps; seq is the sequence of lookups that follows.
func ZipfStrings(capacity int) (warm, seq []string) {
	n := 4 * capacity
	all := names(n)
	keys := make([]string, n)
	// The popular keys are spread over the key space, as they would be.
	for i, p := range rand.New(rand.NewPCG(5, 6)).Perm(n) {
		keys[i] = all[p]
	}
	z := rand.NewZipf(rand.New(rand.NewPCG(7, 8)), 1.01, 1, uint64(n-1))
	draw := func(m int) []uint32 {
		picks := make([]uint32, m)
		for i := range picks {
			picks[i] = uint32(z.Uint64())
		}
		return picks
	}
	warm = make([]string, 16*capacity)
	for i, k := range draw(len(warm)) {
		warm[i] = keys[k]
	}
	return warm, copies(keys, draw(SeqLen))
}
