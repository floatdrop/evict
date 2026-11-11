package shards

import (
	"math"
	"math/bits"
	"runtime"
	"testing"
)

func TestCountRequested(t *testing.T) {
	t.Parallel()

	const unlimited = 1 << 30

	tests := []struct {
		name                             string
		capacity, requested, maxPerShard int
		want                             int
	}{
		{"no capacity", 0, 16, unlimited, 1},
		{"negative capacity", -5, 16, unlimited, 1},
		{"one entry", 1, 16, unlimited, 1},
		{"one shard", 1000, 1, unlimited, 1},
		{"power of two", 1000, 16, unlimited, 16},
		{"rounded up", 1000, 5, unlimited, 8},
		{"rounded up to capacity", 8, 7, unlimited, 8},
		{"limited by capacity", 5, 16, unlimited, 4},
		{"limited by capacity after rounding", 5, 5, unlimited, 4},
		{"raised to respect the shard limit", 1000, 1, 100, 16},
		{"shard limit met exactly", 800, 1, 100, 8},
		{"shard limit just missed", 801, 1, 100, 16},
		{"the shard limit wins over the capacity", 3, 1, 1, 4},
		{"huge request", 1000, math.MaxInt, unlimited, 512},
		{"huge request and capacity", math.MaxInt, math.MaxInt, math.MaxInt, 1 << (bits.UintSize - 2)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := Count(tt.capacity, tt.requested, tt.maxPerShard); got != tt.want {
				t.Errorf("Count(%d, %d, %d) = %d, want %d", tt.capacity, tt.requested, tt.maxPerShard, got, tt.want)
			}
		})
	}
}

func TestCountPanics(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name                  string
		capacity, maxPerShard int
	}{
		{"shards that may not hold an entry", 8, 0},
		{"more shards than there are numbers", math.MaxInt, 1},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Count did not panic for %s", tt.name)
				}
			}()
			Count(tt.capacity, 1, tt.maxPerShard)
		}()
	}
}

// TestDefaults pins the numbers that the documentation of the WithShards
// options of the caches quotes. Change them there as well.
func TestDefaults(t *testing.T) {
	t.Parallel()

	if perCPU != 8 || minCapacity != 256 {
		t.Errorf("%d shards per CPU of at least %d entries; the documentation says 8 and 256", perCPU, minCapacity)
	}
}

func TestCountDefault(t *testing.T) {
	t.Parallel()

	const unlimited = 1 << 30

	limit := floorPow2(perCPU * runtime.GOMAXPROCS(0))

	for _, capacity := range []int{0, 1, 2, 100, minCapacity - 1, 2*minCapacity - 1} {
		if got := Count(capacity, 0, unlimited); got != 1 {
			t.Errorf("Count(%d, 0) = %d, want 1: a small cache stays in one shard", capacity, got)
		}
	}

	for _, capacity := range []int{2 * minCapacity, 10_000, 100_000, 1 << 20, 1 << 28} {
		got := Count(capacity, 0, unlimited)

		if bits.OnesCount(uint(got)) != 1 {
			t.Errorf("Count(%d, 0) = %d, want a power of two", capacity, got)
		}
		if got > limit {
			t.Errorf("Count(%d, 0) = %d, want at most %d", capacity, got, limit)
		}
		if smallest := Capacity(capacity, got, got-1); smallest < minCapacity {
			t.Errorf("Count(%d, 0) = %d leaves a shard with %d entries, want at least %d",
				capacity, got, smallest, minCapacity)
		}
		// Twice as many shards would be too many for the CPUs or too
		// small for the policy.
		if 2*got <= limit && capacity/(2*got) >= minCapacity {
			t.Errorf("Count(%d, 0) = %d, but %d shards would do", capacity, got, 2*got)
		}
	}

	if got, want := Count(1<<20, -1, unlimited), Count(1<<20, 0, unlimited); got != want {
		t.Errorf("Count with a negative request = %d, want the default %d", got, want)
	}
}

func TestCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		total, n int
	}{
		{0, 1}, {-3, 4}, {1, 1}, {7, 1}, {8, 8}, {9, 8}, {15, 8}, {1000, 16}, {1_000_003, 64},
	}
	for _, tt := range tests {
		sum := 0
		for i := range tt.n {
			share := Capacity(tt.total, tt.n, i)
			if i > 0 {
				if prev := Capacity(tt.total, tt.n, i-1); share > prev || prev-share > 1 {
					t.Errorf("Capacity(%d, %d, %d) = %d follows %d: shares are uneven", tt.total, tt.n, i, share, prev)
				}
			}
			sum += share
		}
		if want := max(tt.total, 0); sum != want {
			t.Errorf("shares of Capacity(%d, %d, i) add up to %d, want %d", tt.total, tt.n, sum, want)
		}
	}
}

func TestPow2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		n, floor, ceil int
	}{
		{1, 1, 1}, {2, 2, 2}, {3, 2, 4}, {4, 4, 4}, {5, 4, 8}, {1023, 512, 1024}, {1024, 1024, 1024}, {1025, 1024, 2048},
	}
	for _, tt := range tests {
		if got := floorPow2(tt.n); got != tt.floor {
			t.Errorf("floorPow2(%d) = %d, want %d", tt.n, got, tt.floor)
		}
		if got := ceilPow2(tt.n); got != tt.ceil {
			t.Errorf("ceilPow2(%d) = %d, want %d", tt.n, got, tt.ceil)
		}
	}
}
