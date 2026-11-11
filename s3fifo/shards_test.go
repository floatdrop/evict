package s3fifo

import (
	"hash/maphash"
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
				if got, want := c.shards[i].slab.Cap(), c.shards[i].capacity; got != want {
					t.Errorf("shard %d has a slab for %d entries, want %d", i, got, want)
				}
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
		if got, want := s.slab.Len(), s.capacity; got < want*9/10 {
			t.Errorf("shard %d holds %d entries of %d after %d insertions", i, got, want, 2*c.Cap())
		}
	}
}

func TestQueueSizes(t *testing.T) {
	t.Parallel()

	// A tenth of a shard is for the small queue, rounded down, and there
	// are nine tenths as many ghosts as entries, rounded down as well.
	for capacity, want := range map[int]struct{ mainCap, ghosts int }{
		0: {0, 0}, 1: {1, 0}, 9: {9, 8}, 10: {9, 9}, 19: {18, 17}, 100: {90, 90}, 1005: {905, 904},
	} {
		c := New[int, int](capacity, WithShards(1))
		s := &c.shards[0]
		if s.capacity != capacity || s.mainCap != want.mainCap || s.ghosts.Cap() != want.ghosts {
			t.Errorf("capacity %d: the shard holds %d entries, %d of them in the main queue, and %d ghosts; want %d, %d and %d",
				capacity, s.capacity, s.mainCap, s.ghosts.Cap(), capacity, want.mainCap, want.ghosts)
		}
	}

	c := New[int, int](1000, WithShards(8))
	for i := range c.shards {
		if s := &c.shards[i]; s.capacity != 125 || s.mainCap != 113 {
			t.Errorf("shard %d holds %d entries, %d of them in the main queue; want 125 and 113", i, s.capacity, s.mainCap)
		}
	}
}

// TestBookkeeping checks through a long run of operations that the counters
// of a shard agree with its lists, that every entry is tagged with the queue
// it is in, and that the queues stay within their bounds.
func TestBookkeeping(t *testing.T) {
	t.Parallel()

	c := New[int, int](40, WithShards(1))
	s := &c.shards[0]

	count := func(head uint32) int {
		n := 0
		for idx := s.slab.Links[head].Next; idx != head; idx = s.slab.Links[idx].Next {
			if got := s.slab.Links[idx].Tag & 1; got != head {
				t.Fatalf("node %d is in queue %d but tagged for queue %d", idx, head, got)
			}
			if f := s.slab.Nodes[idx].Meta.Load(); f > maxFrequency {
				t.Fatalf("node %d has counted %d reads, want at most %d", idx, f, maxFrequency)
			}
			n++
		}
		return n
	}

	rng := rand.New(rand.NewPCG(5, 6))
	deleted, promoted, over := [queues]int{}, 0, 0
	for step := range 100_000 {
		key := rng.IntN(120)
		mains := s.mains
		switch op := rng.IntN(100); {
		case op < 30:
			c.Set(key, step)
		case op < 90:
			c.Get(rng.IntN(60)) // Some keys are read much more than others.
		case op < 99:
			if idx := s.slab.Find(uint32(maphash.Comparable(c.seed, key)), key); idx != 0 {
				deleted[s.slab.Links[idx].Tag&1]++
			}
			c.Delete(key)
		default:
			c.Clear()
		}
		if s.mains > mains {
			promoted++
		}

		smalls, mains := count(small), count(main)
		if smalls != s.smalls || mains != s.mains {
			t.Fatalf("step %d: the queues hold %d and %d entries, the shard counts %d and %d", step, smalls, mains, s.smalls, s.mains)
		}
		if smalls+mains > s.capacity || s.ghosts.Len() > s.ghosts.Cap() {
			t.Fatalf("step %d: %d entries and %d ghosts, want at most %d and %d", step, smalls+mains, s.ghosts.Len(), s.capacity, s.ghosts.Cap())
		}
		// The main queue may be over its share: an entry that moves up or
		// comes back as a ghost joins it whether it has room or not. It is
		// then the one to evict next.
		if mains > s.mainCap {
			over++
		}
		if got := c.Len(); got != smalls+mains {
			t.Fatalf("step %d: Len() = %d, want %d", step, got, smalls+mains)
		}
	}

	// The run is only worth something if it went down every road.
	if deleted[small] == 0 || deleted[main] == 0 || promoted == 0 || over == 0 {
		t.Errorf("%d and %d entries were deleted from the queues, %d moved to the main queue, and it was over its share %d times; want some of each",
			deleted[small], deleted[main], promoted, over)
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

// TestGhostOf checks that an entry leaves the ghost that its key is asked
// for by when it comes back.
func TestGhostOf(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(3, 4))
	for range 10_000 {
		h := rng.Uint64()
		want := ghostOf(uint32(h), tagOf(h, small))
		if got := ghostOf(uint32(h), tagOf(h, main)); got != want {
			t.Fatalf("the ghost of %#x is %#x in the small queue and %#x in the main queue", h, want, got)
		}
		// It is the hash, less the one bit that the ghosts keep to
		// themselves.
		if want != (h>>33)<<32|h&(1<<32-1) || want>>63 != 0 {
			t.Fatalf("the ghost of %#x is %#x", h, want)
		}
		if tagOf(h, main)&1 != main || tagOf(h, small)&1 != small {
			t.Fatalf("the tags of %#x do not say which queue they are for", h)
		}
	}
}

// TestShardPadding checks that the shards of a cache keep out of each other's
// cache lines.
func TestShardPadding(t *testing.T) {
	t.Parallel()

	cachetest.Padded(t, shard[int, int]{})
	cachetest.Padded(t, shard[string, []byte]{})
}
