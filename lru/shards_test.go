package lru

import (
	"math/rand/v2"
	"runtime"
	"testing"

	"github.com/floatdrop/evict/internal/cachetest"
)

func TestShards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		capacity int
		opts     []Option
		want     int
	}{
		{"small caches stay in one piece", 511, nil, 1},
		{"no capacity", 0, []Option{WithShards(8)}, 1},
		{"as requested", 1000, []Option{WithShards(4)}, 4},
		{"rounded up to a power of two", 1000, []Option{WithShards(5)}, 8},
		{"limited to the capacity", 6, []Option{WithShards(64)}, 4},
		{"zero means the default", 100, []Option{WithShards(0)}, 1},
		{"the last option wins", 1000, []Option{WithShards(2), WithShards(16)}, 16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := New[int, int](tt.capacity, tt.opts...)

			if got := len(c.shards); got != tt.want {
				t.Errorf("%d shards, want %d", got, tt.want)
			}
			if got := int(c.mask) + 1; got != len(c.shards) {
				t.Errorf("mask selects among %d shards, there are %d", got, len(c.shards))
			}

			total := 0
			for i := range c.shards {
				total += c.shards[i].slab.Cap()
			}
			if want := max(tt.capacity, 0); total != want || c.Cap() != want {
				t.Errorf("shards hold %d entries, Cap() = %d, want %d", total, c.Cap(), want)
			}
		})
	}
}

func TestDefaultShards(t *testing.T) {
	t.Parallel()

	c := New[int, int](1 << 20)

	if runtime.GOMAXPROCS(0) > 1 && len(c.shards) == 1 {
		t.Error("a large cache has a single shard by default")
	}

	// Every shard gets its share of the keys. With twice as many keys as
	// there is room for, each of them is as good as certain to fill up.
	for key := range 2 * c.Cap() {
		c.Set(key, key)
	}
	for i := range c.shards {
		s := &c.shards[i]
		if got, want := s.slab.Len(), s.slab.Cap(); got < want*9/10 {
			t.Errorf("shard %d holds %d entries of %d after %d insertions", i, got, want, c.Cap())
		}
	}
}

// TestHashSplit checks that a shard does not index its keys by the same bits
// of the hash that put them into the shard. If it did, the keys of a shard
// would all ask for the same few slots of its index, and every lookup would
// have to wade through the others.
func TestHashSplit(t *testing.T) {
	t.Parallel()

	// Many small shards: the index of a shard has fewer slots than there
	// are shards, so bits that were shared would leave no choice at all.
	const shardCount, perShard = 1024, 256

	c := New[uint64, struct{}](shardCount*perShard, WithShards(shardCount))
	rng := rand.New(rand.NewPCG(1, 2))
	for range 2 * shardCount * perShard {
		c.Set(rng.Uint64(), struct{}{})
	}

	worst := 0
	for i := range c.shards {
		worst = max(worst, c.shards[i].slab.MaxDisplacement())
	}
	// With hashes that are independent, an entry is rarely more than a
	// dozen slots from where it belongs. With shared bits it is hundreds.
	if worst > 64 {
		t.Errorf("an entry is %d slots away from the slot its hash asks for", worst)
	}
}

// TestShardPadding checks that the shards of a cache keep out of each other's
// cache lines and take up whole lines, whatever the keys and values.
func TestShardPadding(t *testing.T) {
	t.Parallel()

	cachetest.Padded(t, Cache[int, int]{}.shards)
	cachetest.Padded(t, Cache[string, []byte]{}.shards)
}
