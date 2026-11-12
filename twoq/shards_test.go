package twoq

import (
	"hash/maphash"
	"math"
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
				total += c.shards[i].capacity
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
		if got, want := s.recents+s.frequents, s.capacity; got < want*9/10 {
			t.Errorf("shard %d holds %d entries of %d after %d insertions", i, got, want, c.Cap())
		}
	}
}

func TestQueueSizes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		capacity      int
		opts          []Option
		recent, ghost int
	}{
		{1000, nil, 250, 500}, // The defaults of the paper.
		{1000, []Option{WithRecentRatio(0.1), WithGhostRatio(1)}, 100, 1000},
		{1000, []Option{WithRecentRatio(0), WithGhostRatio(0)}, 0, 0},
		{1000, []Option{WithRecentRatio(1)}, 1000, 500},
		{1000, []Option{WithRecentRatio(-3), WithGhostRatio(7)}, 0, 1000},
		{1000, []Option{WithRecentRatio(math.NaN()), WithGhostRatio(math.NaN())}, 0, 0},
		{1000, []Option{WithRecentRatio(math.Inf(1)), WithGhostRatio(math.Inf(-1))}, 1000, 0},
		{10, []Option{WithRecentRatio(0.29)}, 3, 5}, // Rounded to the nearest entry.
		{2, nil, 1, 1},
		{1, nil, 0, 1},
		{0, nil, 0, 0},
	}
	for _, tt := range tests {
		c := New[int, int](tt.capacity, append([]Option{WithShards(1)}, tt.opts...)...)
		s := &c.shards[0]

		if s.capacity != tt.capacity || s.recentCap != tt.recent || s.ghostCap != tt.ghost {
			t.Errorf("capacity %d: the shard holds %d entries, %d of them recent, and remembers %d keys; want %d, %d and %d",
				tt.capacity, s.capacity, s.recentCap, s.ghostCap, tt.capacity, tt.recent, tt.ghost)
		}
		if got, want := s.slab.Cap(), tt.capacity+tt.ghost; got != want {
			t.Errorf("capacity %d: the slab has %d nodes, want %d", tt.capacity, got, want)
		}
	}

	// The queue sizes of a shard follow from its own share of the capacity.
	c := New[int, int](1000, WithShards(8))
	for i := range c.shards {
		s := &c.shards[i]
		if s.capacity != 125 || s.recentCap != 31 || s.ghostCap != 63 {
			t.Errorf("shard %d holds %d entries, %d of them recent, and remembers %d keys; want 125, 31 and 63",
				i, s.capacity, s.recentCap, s.ghostCap)
		}
	}
}

// TestBookkeeping checks through a long run of operations that the counters
// of a shard agree with its lists, that the lists never outgrow the shard,
// and that a ghost holds a key and nothing else.
func TestBookkeeping(t *testing.T) {
	t.Parallel()

	c := New[int, *int](20, WithShards(1))
	s := &c.shards[0]

	count := func(head uint32) int {
		n := 0
		for idx := s.slab.Nodes[head].Next; idx != head; idx = s.slab.Nodes[idx].Next {
			node := &s.slab.Nodes[idx]
			if node.Meta != head {
				t.Fatalf("node %d is in list %d but marked for list %d", idx, head, node.Meta)
			}
			if (node.Value == nil) != (head == ghost) {
				t.Fatalf("node %d in list %d has the value %v", idx, head, node.Value)
			}
			n++
		}
		return n
	}

	rng := rand.New(rand.NewPCG(5, 6))
	deleted := [lists]int{}
	for step := range 50_000 {
		key := rng.IntN(61)
		switch op := rng.IntN(100); {
		case op < 45:
			c.Set(key, &step)
		case op < 85:
			c.Get(key)
		case op < 99:
			// Entries are deleted from both queues; a ghost stays.
			if idx := s.slab.Find(uint32(maphash.Comparable(c.seed, key)), key); idx != 0 {
				deleted[s.slab.Nodes[idx].Meta]++
			}
			c.Delete(key)
		default:
			c.Clear()
		}

		recents, frequents, ghosts := count(recent), count(frequent), count(ghost)
		if recents != s.recents || frequents != s.frequents || ghosts != s.ghosts {
			t.Fatalf("step %d: the lists hold %d, %d and %d nodes, the shard counts %d, %d and %d",
				step, recents, frequents, ghosts, s.recents, s.frequents, s.ghosts)
		}
		if recents+frequents > s.capacity || ghosts > s.ghostCap {
			t.Fatalf("step %d: %d entries and %d ghosts, want at most %d and %d",
				step, recents+frequents, ghosts, s.capacity, s.ghostCap)
		}
		if got := s.slab.Len(); got != recents+frequents+ghosts {
			t.Fatalf("step %d: the slab holds %d nodes, the lists %d", step, got, recents+frequents+ghosts)
		}
		if got := c.Len(); got != recents+frequents {
			t.Fatalf("step %d: Len() = %d, want %d", step, got, recents+frequents)
		}
	}

	// The run is only worth something if it went down every road.
	for list, n := range deleted {
		if n == 0 {
			t.Errorf("no key in list %d was ever deleted", list)
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
