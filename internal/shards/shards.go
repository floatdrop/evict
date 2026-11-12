// Package shards holds the arithmetic for splitting a cache into shards, each
// with its own lock and its own share of the capacity.
package shards

import (
	"math"
	"math/bits"
	"runtime"
)

const (
	// perCPU is how many shards a cache gets by default for every CPU that
	// can run Go code at once. Having several keeps two goroutines from
	// meeting in the same shard often.
	perCPU = 8

	// minCapacity is the capacity below which a shard is not created by
	// default. A policy that decides over few entries decides poorly, so a
	// small cache stays in one piece. Replaying traces against caches in
	// one piece and in shards of this size shows differences of a few
	// hundredths of a percentage point for most policies on most traces,
	// and of several points for SIEVE on some; see
	// bench/results/sharding.md.
	minCapacity = 256
)

// Line is the size of a cache line on Apple's chips, and of the pair of lines
// that Intel's chips fetch together.
const Line = 128

// Pad keeps fields that are written independently off each other's lines.
type Pad [Line]byte

// Count returns the number of shards for a cache of the given capacity: a
// power of two, so that a shard can be picked with a mask.
//
// If requested is positive it is rounded up to a power of two; otherwise the
// count follows from the number of CPUs and the capacity. Either way there
// are no more shards than entries, unless it takes more to keep every shard
// at or below maxPerShard entries, which must be at least one.
func Count(capacity, requested, maxPerShard int) int {
	if maxPerShard < 1 {
		panic("shards: a shard must be allowed to hold an entry")
	}
	if capacity <= 1 {
		return 1
	}

	// With more shards than entries some shards would stay empty.
	n := floorPow2(capacity)
	switch {
	case requested <= 0:
		n = floorPow2(max(1, min(perCPU*runtime.GOMAXPROCS(0), capacity/minCapacity)))
	case requested < n:
		n = ceilPow2(requested)
	}

	for Capacity(capacity, n, 0) > maxPerShard {
		if n > math.MaxInt/2 {
			panic("shards: the capacity cannot be split into shards that are small enough")
		}
		n *= 2
	}
	return n
}

// Capacity returns the capacity of shard i when a total is spread over n
// shards as evenly as it goes. The first shard gets the largest share.
func Capacity(total, n, i int) int {
	if total <= 0 {
		return 0
	}
	share := total / n
	if i < total%n {
		share++
	}
	return share
}

// floorPow2 returns the largest power of two that is at most n, for n >= 1.
func floorPow2(n int) int {
	return 1 << (bits.Len(uint(n)) - 1)
}

// ceilPow2 returns the smallest power of two that is at least n, for n >= 1.
func ceilPow2(n int) int {
	return 1 << bits.Len(uint(n-1))
}
