package s3fifo_test

import (
	"fmt"
	"iter"
	"math/rand/v2"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	"github.com/floatdrop/evict"
	"github.com/floatdrop/evict/internal/cachetest"
	"github.com/floatdrop/evict/s3fifo"
)

var factories = cachetest.Factories{
	Int: func(capacity, shards int) evict.Cache[int, int] {
		return s3fifo.New[int, int](capacity, s3fifo.WithShards(shards))
	},
	String: func(capacity, shards int) evict.Cache[string, string] {
		return s3fifo.New[string, string](capacity, s3fifo.WithShards(shards))
	},
	Float: func(capacity, shards int) evict.Cache[float64, int] {
		return s3fifo.New[float64, int](capacity, s3fifo.WithShards(shards))
	},
	Blob: func(capacity, shards int) evict.Cache[int, *cachetest.Blob] {
		return s3fifo.New[int, *cachetest.Blob](capacity, s3fifo.WithShards(shards))
	},
	BlobKey: func(capacity, shards int) evict.Cache[*cachetest.Blob, int] {
		return s3fifo.New[*cachetest.Blob, int](capacity, s3fifo.WithShards(shards))
	},
	Wide: func(capacity, shards int) evict.Cache[string, cachetest.Wide] {
		return s3fifo.New[string, cachetest.Wide](capacity, s3fifo.WithShards(shards))
	},
	// The ghosts are hashes: they keep no key alive.
}

func TestCache(t *testing.T) {
	t.Parallel()

	cachetest.Run(t, factories)
}

// TestNoAllocs is not parallel: counting allocations needs the process to
// itself.
func TestNoAllocs(t *testing.T) {
	cachetest.NoAllocs(t, factories)
}

// keys returns the keys of the cache in the order of All.
func keys[K comparable, V any](c *s3fifo.Cache[K, V]) []K {
	var all []K
	for k := range c.All() {
		all = append(all, k)
	}
	return all
}

// TestSmallQueue follows entries through the small queue: out of the cache,
// or on into the main queue.
func TestSmallQueue(t *testing.T) {
	t.Parallel()

	// Of ten entries, one is the share of the small queue.
	c := s3fifo.New[int, string](10, s3fifo.WithShards(1))

	// expect checks the main queue and the small queue, each from the
	// newest to the oldest entry.
	type in = []int
	expect := func(inMain, inSmall []int) {
		t.Helper()
		gotMain, gotSmall := c.Queues()
		if !slices.Equal(gotMain, inMain) || !slices.Equal(gotSmall, inSmall) {
			t.Fatalf("the main queue holds %v and the small queue %v, want %v and %v", gotMain, gotSmall, inMain, inSmall)
		}
		// All goes through them in the same order.
		if got := keys(c); !slices.Equal(got, slices.Concat(inMain, inSmall)) {
			t.Fatalf("All yields %v, want %v followed by %v", got, inMain, inSmall)
		}
	}
	evicts := func(key, want int) {
		t.Helper()
		if k, _, displaced := c.Set(key, ""); !displaced || k != want {
			t.Fatalf("Set(%d) evicted %d, %t, want %d", key, k, displaced, want)
		}
	}

	// While the cache fills up for the first time, the small queue takes
	// its share, which is the first entry, and the rest goes straight to
	// the main queue.
	for key := 1; key <= 10; key++ {
		c.Set(key, "")
	}
	expect(in{10, 9, 8, 7, 6, 5, 4, 3, 2}, in{1})

	// From here on, what is new goes through the small queue, and what has
	// not been read there leaves from it.
	evicts(11, 1)
	expect(in{10, 9, 8, 7, 6, 5, 4, 3, 2}, in{11})

	// Two reads take 11 to the main queue when its turn comes. That puts
	// the main queue over its share, so its oldest entry is the one to go.
	c.Get(11)
	c.Get(11)
	evicts(12, 2)
	expect(in{11, 10, 9, 8, 7, 6, 5, 4, 3}, in{12})

	// One read is not enough.
	c.Get(12)
	evicts(13, 12)
	expect(in{11, 10, 9, 8, 7, 6, 5, 4, 3}, in{13})

	// 12 is a ghost now: it misses, but when it is set again it goes
	// straight to the main queue. 13 makes room for it.
	if _, ok := c.Get(12); ok {
		t.Fatal("Get(12) found a ghost")
	}
	evicts(12, 13)
	expect(in{12, 11, 10, 9, 8, 7, 6, 5, 4, 3}, nil)

	// With the main queue over its share, the next entry to go is its
	// oldest.
	evicts(14, 3)
	expect(in{12, 11, 10, 9, 8, 7, 6, 5, 4}, in{14})

	// Writing to an entry counts like reading it: 14 has two now.
	c.Set(14, "again")
	c.Get(14)
	evicts(15, 4)
	expect(in{14, 12, 11, 10, 9, 8, 7, 6, 5}, in{15})

	// Peeking at one does not count.
	c.Peek(15)
	c.Peek(15)
	evicts(16, 15)
	expect(in{14, 12, 11, 10, 9, 8, 7, 6, 5}, in{16})

	// A ghost that came back is no ghost any more: 12 starts over in the
	// small queue when it is deleted and set again. 13 is still one.
	c.Delete(12)
	c.Set(12, "")
	expect(in{14, 11, 10, 9, 8, 7, 6, 5}, in{12, 16})
	evicts(13, 16)
	expect(in{13, 14, 11, 10, 9, 8, 7, 6, 5}, in{12})

	// A cache that was cleared fills up like a new one. Had all ten entries
	// gone into the small queue, the second one to be evicted would be 22.
	c.Clear()
	for key := 21; key <= 30; key++ {
		c.Set(key, "")
	}
	evicts(31, 21)
	evicts(32, 31)
	expect(in{30, 29, 28, 27, 26, 25, 24, 23, 22}, in{32})
}

// TestMovingUp checks which queue gives when entries move from the small
// queue to the main queue, and what Delete does to a ghost.
func TestMovingUp(t *testing.T) {
	t.Parallel()

	// Of twenty entries, two are the share of the small queue.
	c := s3fifo.New[int, string](20, s3fifo.WithShards(1))
	type in = []int
	expect := func(inMain, inSmall []int) {
		t.Helper()
		gotMain, gotSmall := c.Queues()
		if !slices.Equal(gotMain, inMain) || !slices.Equal(gotSmall, inSmall) {
			t.Fatalf("the main queue holds %v and the small queue %v, want %v and %v", gotMain, gotSmall, inMain, inSmall)
		}
	}
	evicts := func(key, want int) {
		t.Helper()
		if k, _, displaced := c.Set(key, ""); !displaced || k != want {
			t.Fatalf("Set(%d) evicted %d, %t, want %d", key, k, displaced, want)
		}
	}

	for key := 1; key <= 20; key++ {
		c.Set(key, "")
	}
	expect(in{20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3}, in{2, 1})

	// 1 has been read twice and moves up. That puts the main queue over
	// its share, but it is still the small queue's turn: 2 goes.
	c.Get(1)
	c.Get(1)
	evicts(21, 2)
	expect(in{1, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3}, in{21})
	// The next time it is the main queue's.
	evicts(22, 3)
	expect(in{1, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4}, in{22, 21})

	// If everything in the small queue moves up, there is nothing left in
	// it to evict, and the main queue has to give after all.
	c.Get(21)
	c.Get(21)
	c.Get(22)
	c.Get(22)
	evicts(23, 4)
	expect(in{22, 21, 1, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5}, in{23})
	evicts(24, 5)
	expect(in{22, 21, 1, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6}, in{24, 23})

	// 2 is a ghost. Deleting it makes the cache forget it: when it is set
	// again, it starts in the small queue like any key that is new. 23,
	// which goes for it, is a ghost that nobody has deleted.
	if _, ok := c.Delete(2); ok {
		t.Fatal("Delete(2) found a ghost")
	}
	evicts(2, 23)
	expect(in{22, 21, 1, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6}, in{2, 24})
	evicts(23, 24)
	expect(in{23, 22, 21, 1, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6}, in{2})
}

// TestMainQueue follows entries around the main queue.
func TestMainQueue(t *testing.T) {
	t.Parallel()

	// With room for three entries the small queue has no share of its own:
	// the cache fills its main queue first, and whatever is in the small
	// queue goes before anything else does.
	c := s3fifo.New[string, int](3, s3fifo.WithShards(1))
	type in = []string
	expect := func(inMain, inSmall []string) {
		t.Helper()
		gotMain, gotSmall := c.Queues()
		if !slices.Equal(gotMain, inMain) || !slices.Equal(gotSmall, inSmall) {
			t.Fatalf("the main queue holds %v and the small queue %v, want %v and %v", gotMain, gotSmall, inMain, inSmall)
		}
	}
	evicts := func(key, want string) {
		t.Helper()
		if k, _, displaced := c.Set(key, 0); !displaced || k != want {
			t.Fatalf("Set(%q) evicted %q, %t, want %q", key, k, displaced, want)
		}
	}

	c.Set("a", 0)
	c.Set("b", 0)
	c.Set("c", 0)
	expect(in{"c", "b", "a"}, nil)

	// The oldest entry, a, has been read, so it gets another round, and b
	// goes. The newcomer is in the small queue.
	c.Get("a")
	evicts("d", "b")
	expect(in{"a", "c"}, in{"d"})

	// Being evicted from the main queue leaves no ghost: b starts over in
	// the small queue, from which d has to go for it. And d, which does
	// have a ghost for that, comes back into the main queue.
	evicts("b", "d")
	expect(in{"a", "c"}, in{"b"})
	evicts("d", "b")
	expect(in{"d", "a", "c"}, nil)

	// An entry gets a round for every read, up to three, however often it
	// was read. Here a is read four times and d once.
	for range 4 {
		c.Get("a")
	}
	c.Get("d")
	evicts("x", "c")
	expect(in{"d", "a"}, in{"x"})
	// The main queue is looked at only when it is over its share or when
	// there is nothing else. The ghost of b fills it up again.
	evicts("b", "x")
	expect(in{"b", "d", "a"}, nil)

	// In their first round, a and d are spared, and b has no read to show.
	evicts("z", "b")
	expect(in{"d", "a"}, in{"z"})
	evicts("x", "z") // The ghost of x.
	expect(in{"x", "d", "a"}, nil)
	// The second round of a. For d there is none.
	evicts("w", "d")
	expect(in{"a", "x"}, in{"w"})
	evicts("z", "w") // The ghost of z.
	expect(in{"z", "a", "x"}, nil)
	evicts("v", "x")
	expect(in{"z", "a"}, in{"v"})
	evicts("w", "v") // The ghost of w.
	expect(in{"w", "z", "a"}, nil)
	// The third round of a, and its last.
	evicts("u", "z")
	expect(in{"a", "w"}, in{"u"})
	evicts("v", "u") // The ghost of v.
	expect(in{"v", "a", "w"}, nil)
	evicts("t", "w")
	expect(in{"v", "a"}, in{"t"})
	evicts("u", "t") // The ghost of u.
	expect(in{"u", "v", "a"}, nil)
	// After that it is an entry like any other.
	evicts("s", "a")
	expect(in{"u", "v"}, in{"s"})
}

// TestTuning follows single entries through caches that are not as the paper
// has them, one option at a time.
func TestTuning(t *testing.T) {
	t.Parallel()

	type in = []int
	// full returns a cache of ten entries with the given options that holds
	// the keys from 1 to 10, and a function that checks its queues.
	full := func(t *testing.T, options ...s3fifo.Option) (c *s3fifo.Cache[int, string], expect func(inMain, inSmall []int)) {
		t.Helper()
		c = s3fifo.New[int, string](10, append(options, s3fifo.WithShards(1))...)
		for key := 1; key <= 10; key++ {
			c.Set(key, "")
		}
		return c, func(inMain, inSmall []int) {
			t.Helper()
			if gotMain, gotSmall := c.Queues(); !slices.Equal(gotMain, inMain) || !slices.Equal(gotSmall, inSmall) {
				t.Fatalf("the main queue holds %v and the small queue %v, want %v and %v", gotMain, gotSmall, inMain, inSmall)
			}
		}
	}
	evicts := func(t *testing.T, c *s3fifo.Cache[int, string], key, want int) {
		t.Helper()
		if k, _, displaced := c.Set(key, ""); !displaced || k != want {
			t.Fatalf("Set(%d) evicted %d, %t, want %d", key, k, displaced, want)
		}
	}

	t.Run("small queue", func(t *testing.T) {
		t.Parallel()

		// Half of the room: the small queue takes five entries while the
		// cache fills up, and a new entry outlasts four more arrivals.
		c, expect := full(t, s3fifo.WithSmallRatio(0.5))
		expect(in{10, 9, 8, 7, 6}, in{5, 4, 3, 2, 1})
		for key := 11; key <= 14; key++ {
			evicts(t, c, key, key-10)
		}
		expect(in{10, 9, 8, 7, 6}, in{14, 13, 12, 11, 5})

		// No room at all: once the cache is full, a new entry is the next
		// one to go, unless it is read twice before that.
		c, expect = full(t, s3fifo.WithSmallRatio(0))
		expect(in{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, nil)
		evicts(t, c, 11, 1)
		expect(in{10, 9, 8, 7, 6, 5, 4, 3, 2}, in{11})
		evicts(t, c, 12, 11)
		c.Get(12)
		c.Get(12)
		evicts(t, c, 13, 2)
		expect(in{12, 10, 9, 8, 7, 6, 5, 4, 3}, in{13})
	})

	t.Run("promotion", func(t *testing.T) {
		t.Parallel()

		// One read is enough to move up.
		c, expect := full(t, s3fifo.WithPromotionReads(1))
		c.Get(1)
		evicts(t, c, 11, 2)
		expect(in{1, 10, 9, 8, 7, 6, 5, 4, 3}, in{11})

		// Three reads it takes, and two are not enough.
		c, expect = full(t, s3fifo.WithPromotionReads(3))
		c.Get(1)
		c.Get(1)
		evicts(t, c, 11, 1)
		c.Get(11)
		c.Get(11)
		c.Get(11)
		evicts(t, c, 12, 2)
		expect(in{11, 10, 9, 8, 7, 6, 5, 4, 3}, in{12})
	})

	t.Run("ghosts", func(t *testing.T) {
		t.Parallel()

		// With none, a key that comes back is as new as any other.
		c, expect := full(t, s3fifo.WithGhostRatio(0))
		evicts(t, c, 11, 1)
		evicts(t, c, 1, 11)
		expect(in{10, 9, 8, 7, 6, 5, 4, 3, 2}, in{1})

		// With three times as many as entries, a key is remembered through
		// twenty-nine others that were evicted after it, and not through
		// thirty.
		for _, others := range []int{29, 30} {
			c, expect = full(t, s3fifo.WithGhostRatio(3))
			evicts(t, c, 11, 1)
			for key := 12; key < 12+others; key++ {
				evicts(t, c, key, key-1)
			}
			last := 11 + others
			evicts(t, c, 1, last)
			if others == 29 {
				expect(in{1, 10, 9, 8, 7, 6, 5, 4, 3, 2}, nil)
			} else {
				expect(in{10, 9, 8, 7, 6, 5, 4, 3, 2}, in{1})
			}
		}
	})

	t.Run("returns", func(t *testing.T) {
		t.Parallel()

		// The first time a key comes back it goes through the small queue
		// again, and the second time it goes to the main queue.
		c, expect := full(t, s3fifo.WithGhostReturns(2))
		evicts(t, c, 11, 1)
		evicts(t, c, 1, 11)
		expect(in{10, 9, 8, 7, 6, 5, 4, 3, 2}, in{1})
		evicts(t, c, 12, 1)
		evicts(t, c, 1, 12)
		expect(in{1, 10, 9, 8, 7, 6, 5, 4, 3, 2}, nil)

		// The ghost keeps its place in line the while. There is room for
		// nine ghosts, so it is forgotten when nine more keys have become
		// one since it first did, not since it last came back.
		for _, last := range []int{19, 20} {
			c, expect = full(t, s3fifo.WithGhostReturns(2))
			evicts(t, c, 11, 1) // 1 is a ghost.
			evicts(t, c, 12, 11)
			evicts(t, c, 13, 12)
			evicts(t, c, 1, 13) // It comes back for the first time,
			evicts(t, c, 14, 1) // and leaves again.
			for key := 15; key <= last; key++ {
				evicts(t, c, key, key-1)
			}
			// The ghosts that have come after it are 11, 12, 13 and those
			// from 14 on: eight of them if 19 was the last key to be set,
			// and nine if 20 was.
			evicts(t, c, 1, last)
			if last == 19 {
				expect(in{1, 10, 9, 8, 7, 6, 5, 4, 3, 2}, nil)
			} else {
				expect(in{10, 9, 8, 7, 6, 5, 4, 3, 2}, in{1})
			}
		}
	})
}

// TestTunedNoAllocs is not parallel: counting allocations needs the process
// to itself.
func TestTunedNoAllocs(t *testing.T) {
	c := s3fifo.New[int, int](256,
		s3fifo.WithSmallRatio(0.3), s3fifo.WithGhostRatio(3), s3fifo.WithPromotionReads(1), s3fifo.WithGhostReturns(2))
	allocs := testing.AllocsPerRun(1, func() {
		for i := range 20_000 {
			key := i * 7 % 1000
			if _, ok := c.Get(key); !ok {
				c.Set(key, i)
			}
			if i%50 == 0 {
				c.Delete(key)
			}
		}
		c.Clear()
	})
	if allocs != 0 {
		t.Errorf("%v allocations, want 0", allocs)
	}
}

// model is S3-FIFO at its simplest: a slice for each queue, with the newest
// entry first, and the keys of the ghosts in the order they became one.
type model struct {
	capacity, mainCap, ghostCap int
	// promote is the number of reads that take an entry from the small
	// queue to the main one, and returns the number of times that a ghost
	// has to come back to go there at once.
	promote, returns int
	// evicted says that the cache has been full since it was made or
	// cleared.
	evicted bool

	small, main []*entry
	// ghosts holds a place for each of the last ghostCap ghosts. The place
	// of one that has come back for good, or that was deleted, is empty.
	ghosts []ghost
}

// ghost is a key that the model remembers, and how often it has come back
// since. A place that is empty has the key -1.
type ghost struct {
	key, returned int
}

func (m *model) ghostOf(key int) int {
	return slices.IndexFunc(m.ghosts, func(g ghost) bool { return g.key == key })
}

type entry struct {
	key, value int
	reads      int
}

func (m *model) find(key int) *entry {
	for _, queue := range [][]*entry{m.main, m.small} {
		if i := slices.IndexFunc(queue, func(e *entry) bool { return e.key == key }); i >= 0 {
			return queue[i]
		}
	}
	return nil
}

func (m *model) Get(key int) (int, bool) {
	e := m.find(key)
	if e == nil {
		return 0, false
	}
	e.reads = min(e.reads+1, 3)
	return e.value, true
}

func (m *model) Peek(key int) (int, bool) {
	e := m.find(key)
	if e == nil {
		return 0, false
	}
	return e.value, true
}

func (m *model) Set(key, value int) (oldKey, oldValue int, displaced bool) {
	if e := m.find(key); e != nil {
		oldValue, e.value = e.value, value
		e.reads = min(e.reads+1, 3)
		return key, oldValue, true
	}
	if m.capacity == 0 {
		return key, value, true
	}

	// Whether the key has a ghost is settled before room is made for it,
	// and so is whether the ghost has come back often enough to count.
	remembered := false
	if at := m.ghostOf(key); at >= 0 {
		m.ghosts[at].returned++
		if m.ghosts[at].returned >= m.returns {
			m.ghosts[at].key = -1
			remembered = true
		}
	}
	if len(m.small)+len(m.main) == m.capacity {
		oldKey, oldValue = m.evict()
		displaced = true
	}

	// A cache that has never been full does not put more into the small
	// queue than its share.
	e := &entry{key: key, value: value}
	if remembered || !m.evicted && len(m.small) >= m.capacity-m.mainCap {
		m.main = slices.Insert(m.main, 0, e)
	} else {
		m.small = slices.Insert(m.small, 0, e)
	}
	return oldKey, oldValue, displaced
}

// evict is the eviction of the paper: from the small queue while that has its
// share or more, which is to say while the main queue is not over its own,
// and from the main queue otherwise.
func (m *model) evict() (key, value int) {
	m.evicted = true
	// Which queue it is to be is decided once. Entries that move up from
	// the small queue do not change it, however many they are.
	for fromSmall := len(m.main) <= m.mainCap; fromSmall && len(m.small) > 0; {
		last := len(m.small) - 1
		e := m.small[last]
		m.small = m.small[:last]

		if e.reads >= m.promote {
			e.reads = 0
			m.main = slices.Insert(m.main, 0, e)
			continue
		}

		// A key whose ghost is still there, because it has not come back
		// often enough, keeps that one.
		if m.ghostCap > 0 && m.ghostOf(e.key) < 0 {
			m.ghosts = append(m.ghosts, ghost{key: e.key})
			if len(m.ghosts) > m.ghostCap {
				m.ghosts = m.ghosts[1:]
			}
		}
		return e.key, e.value
	}

	for {
		last := len(m.main) - 1
		e := m.main[last]
		m.main = m.main[:last]
		if e.reads == 0 {
			return e.key, e.value
		}
		e.reads--
		m.main = slices.Insert(m.main, 0, e)
	}
}

func (m *model) Delete(key int) (int, bool) {
	e := m.find(key)
	if e == nil {
		// A key that is only remembered is forgotten.
		if at := m.ghostOf(key); at >= 0 {
			m.ghosts[at].key = -1
		}
		return 0, false
	}
	same := func(other *entry) bool { return other == e }
	m.small = slices.DeleteFunc(m.small, same)
	m.main = slices.DeleteFunc(m.main, same)
	return e.value, true
}

func (m *model) Len() int { return len(m.small) + len(m.main) }
func (m *model) Cap() int { return m.capacity }

func (m *model) Clear() {
	m.small, m.main, m.ghosts, m.evicted = nil, nil, nil, false
}

func (m *model) All() iter.Seq2[int, int] {
	return func(yield func(int, int) bool) {
		for _, e := range slices.Concat(m.main, m.small) {
			if !yield(e.key, e.value) {
				return
			}
		}
	}
}

func newModel(capacity int) *model {
	return &model{capacity: capacity, mainCap: capacity - capacity/10, ghostCap: capacity * 9 / 10, promote: 2, returns: 1}
}

// A setting is a cache that is not as the paper has it, and the model of it.
type setting struct {
	name    string
	options []s3fifo.Option
	model   func(capacity int) *model
}

// settings are the ways of tuning the cache that the model is compared with,
// the plain one first.
var settings = []setting{
	{"default", nil, newModel},
	{"small=0.3", []s3fifo.Option{s3fifo.WithSmallRatio(0.3)}, func(c int) *model {
		m := newModel(c)
		m.mainCap = c - c*3/10
		return m
	}},
	{"small=0", []s3fifo.Option{s3fifo.WithSmallRatio(0)}, func(c int) *model {
		m := newModel(c)
		m.mainCap = c
		return m
	}},
	{"small=1", []s3fifo.Option{s3fifo.WithSmallRatio(1)}, func(c int) *model {
		m := newModel(c)
		m.mainCap = 0
		return m
	}},
	{"ghosts=0", []s3fifo.Option{s3fifo.WithGhostRatio(0)}, func(c int) *model {
		m := newModel(c)
		m.ghostCap = 0
		return m
	}},
	{"ghosts=3", []s3fifo.Option{s3fifo.WithGhostRatio(3)}, func(c int) *model {
		m := newModel(c)
		m.ghostCap = 3 * c
		return m
	}},
	{"promotion=1", []s3fifo.Option{s3fifo.WithPromotionReads(1)}, func(c int) *model {
		m := newModel(c)
		m.promote = 1
		return m
	}},
	{"promotion=3", []s3fifo.Option{s3fifo.WithPromotionReads(3)}, func(c int) *model {
		m := newModel(c)
		m.promote = 3
		return m
	}},
	{"returns=2", []s3fifo.Option{s3fifo.WithGhostReturns(2)}, func(c int) *model {
		m := newModel(c)
		m.returns = 2
		return m
	}},
	{"returns=3", []s3fifo.Option{s3fifo.WithGhostReturns(3), s3fifo.WithGhostRatio(2)}, func(c int) *model {
		m := newModel(c)
		m.returns, m.ghostCap = 3, 2*c
		return m
	}},
	{"all at once", []s3fifo.Option{
		s3fifo.WithSmallRatio(0.5), s3fifo.WithGhostRatio(6), s3fifo.WithPromotionReads(1), s3fifo.WithGhostReturns(2),
	}, func(c int) *model {
		m := newModel(c)
		m.mainCap, m.ghostCap, m.promote, m.returns = c-c/2, 6*c, 1, 2
		return m
	}},
}

func TestAgainstModel(t *testing.T) {
	t.Parallel()

	for i, tt := range []struct{ capacity, keys, steps int }{
		{0, 3, 100},
		{1, 3, 2_000},
		{2, 4, 5_000},
		{3, 6, 5_000},
		{8, 12, 20_000},
		{8, 100, 20_000},
		{10, 15, 20_000},
		{20, 30, 20_000},
		{50, 40, 20_000}, // Never full.
		{50, 80, 50_000},
		{50, 500, 50_000},
		{300, 1000, 50_000},
	} {
		// Once the way it is meant to be, and once with readers that have
		// to take the lock; and each with every setting.
		for _, crowded := range []bool{false, true} {
			for _, set := range settings {
				t.Run(fmt.Sprintf("%s/capacity=%d/crowded=%t", set.name, tt.capacity, crowded), func(t *testing.T) {
					t.Parallel()

					c := s3fifo.New[int, int](tt.capacity, append(set.options, s3fifo.WithShards(1))...)
					if crowded {
						c.OccupyReaders(0)
					}
					rng := rand.New(rand.NewPCG(uint64(i), 1))
					cachetest.Compare(t, c, set.model(tt.capacity), cachetest.RandomOps(rng, tt.keys, tt.steps))
				})
			}
		}
	}
}

// TestKeysWithOneHash checks that a read of one key does not count for another
// key that happens to have the same hash. A reader finds its entry by the
// hash and has to look at the other one to see that it is not the one it
// wants; what the cache keeps must not depend on that, or it would depend on
// the seed of the hash.
func TestKeysWithOneHash(t *testing.T) {
	t.Parallel()

	// Of twenty entries, two are the share of the small queue.
	c := s3fifo.New[int, int](20, s3fifo.WithShards(1))
	stored, other := s3fifo.Twins(c)

	// A cache that has been full sends its new entries through the small
	// queue.
	for key := -1; key >= -40; key-- {
		c.Set(key, 0)
	}
	c.Set(stored, 0)
	for range 3 {
		if _, ok := c.Get(other); ok {
			t.Fatalf("Get found key %d, which was never set", other)
		}
		if _, ok := c.Peek(other); ok {
			t.Fatalf("Peek found key %d, which was never set", other)
		}
	}
	// Nobody has read the entry, so it leaves from the small queue when
	// its turn comes rather than moving up.
	for key := -41; key >= -60; key-- {
		c.Set(key, 0)
	}
	if inMain, inSmall := c.Queues(); slices.Contains(inMain, stored) || slices.Contains(inSmall, stored) {
		t.Errorf("key %d is still cached, with %v in the main queue and %v in the small one: the reads of %d, which has the same hash, were counted for it",
			stored, inMain, inSmall, other)
	}
}

// TestHeldUpReader checks what a reader costs the others that stops in the
// middle of reading an entry, as one does whose goroutine has to make way: the
// value it is looking at stays alive, and that is all. Nobody waits for the
// reader, except for Clear, which promises to leave nothing behind.
func TestHeldUpReader(t *testing.T) {
	t.Parallel()

	for _, shards := range []int{1, 4} {
		c := s3fifo.New[int, *cachetest.Blob](64, s3fifo.WithShards(shards))

		ref := func() weak.Pointer[cachetest.Blob] {
			blob := new(cachetest.Blob)
			c.Set(0, blob)
			return weak.Make(blob)
		}()

		release, ok := c.Hold(0)
		if !ok {
			t.Fatal("Hold found no entry for a key that was just set")
		}

		// The entry is replaced and deleted, and so is everything else, many
		// times over. None of it waits.
		for i := range 10_000 {
			c.Set(i%200, new(cachetest.Blob))
			if i%3 == 0 {
				c.Delete((i + 1) % 200)
			}
		}
		runtime.GC()
		runtime.GC()
		if ref.Value() == nil {
			t.Fatalf("%d shards: the value that a reader is looking at was let go of", shards)
		}

		// Clear does wait, and keeps the cache usable while it does.
		done := make(chan struct{})
		go func() {
			defer close(done)
			c.Clear()
		}()
		select {
		case <-done:
			t.Fatalf("%d shards: Clear returned while a reader was looking at an entry", shards)
		case <-time.After(20 * time.Millisecond):
		}
		for i := range 1_000 {
			c.Set(i%200, nil)
			c.Get(i % 200)
			c.Delete(i % 200)
		}

		release()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("%d shards: Clear is still waiting although the reader has left", shards)
		}
		if cachetest.Alive([]weak.Pointer[cachetest.Blob]{ref}, 0) != 0 {
			t.Errorf("%d shards: the value is alive after Clear", shards)
		}

		// The same for an entry that is in the cache when it is cleared,
		// rather than one that has left it before. And Clear waits for
		// that reader only: one that looks at an entry which came and
		// went after Clear had begun is none of its business.
		c.Set(1, new(cachetest.Blob))
		release, ok = c.Hold(1)
		if !ok {
			t.Fatal("Hold found no entry for a key that was just set")
		}
		done = make(chan struct{})
		go func() {
			defer close(done)
			c.Clear()
		}()
		select {
		case <-done:
			t.Fatalf("%d shards: Clear returned while a reader was looking at an entry it removed", shards)
		case <-time.After(20 * time.Millisecond):
		}
		// Clear has begun once the cache is empty. An entry that comes and
		// goes before that is one that Clear is right to wait for, and a
		// goroutine may be that late to start when the machine is busy.
		for deadline := time.Now().Add(10 * time.Second); c.Len() != 0; runtime.Gosched() {
			if time.Now().After(deadline) {
				t.Fatalf("%d shards: Clear has not removed the entry yet", shards)
			}
		}
		c.Set(1, new(cachetest.Blob))
		later, ok := c.Hold(1)
		if !ok {
			t.Fatal("Hold found no entry for a key that was set while Clear was waiting")
		}
		c.Delete(1)

		release()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("%d shards: Clear is waiting for a reader of an entry that came after it", shards)
		}
		later()
		if waits := c.Waits(); waits != 0 {
			t.Errorf("%d shards: writers had to wait for a reader %d times", shards, waits)
		}
		runtime.KeepAlive(c)
	}
}

// TestKeysThatTakeACallToCompare checks that readers do not hold up writers
// when comparing two keys is not a matter of a few instructions.
//
// Go takes the CPU away from a goroutine that has had it for a while at the
// next function it calls. Were a reader to call one while it has an entry
// pinned, and comparing keys of some types is a call, readers would keep
// getting held up at just that point whenever there are more of them than
// CPUs. Enough of them, and a writer finds every spare node of its shard
// pinned and has to wait.
func TestKeysThatTakeACallToCompare(t *testing.T) {
	t.Parallel()

	type wide struct {
		a, b, c, d, e string
		n             int
	}
	keys := make([]any, 64)
	for i := range keys {
		if i%2 == 0 {
			keys[i] = i
		} else {
			keys[i] = wide{a: "a", b: "b", c: "c", d: "d", e: strconv.Itoa(i), n: i}
		}
	}

	c := s3fifo.New[any, int](len(keys), s3fifo.WithShards(1))
	for i, key := range keys {
		c.Set(key, i)
	}

	var (
		wg   sync.WaitGroup
		stop atomic.Bool
	)
	for r := range 16 * runtime.GOMAXPROCS(0) {
		wg.Go(func() {
			for i := r; !stop.Load(); i++ {
				if v, ok := c.Get(keys[i%len(keys)]); ok && v < 0 {
					t.Errorf("Get(%v) = %d", keys[i%len(keys)], v)
					return
				}
			}
		})
	}
	for start, i := time.Now(), 0; time.Since(start) < 300*time.Millisecond; i++ {
		c.Set(keys[i%len(keys)], i)
	}
	stop.Store(true)
	wg.Wait()

	// The race detector turns what a reader does while it has an entry
	// pinned into a series of calls, so with it readers are held up there
	// after all. That says nothing about a program.
	if waits := c.Waits(); waits != 0 && !cachetest.Race {
		t.Errorf("the writer had to wait for readers %d times", waits)
	}
}

// TestReadersHelp checks that what a shard could not let go of, because
// readers were looking at it, does not stay around for want of a write: once
// those readers are gone, the ones that come after them see to it.
func TestReadersHelp(t *testing.T) {
	t.Parallel()

	// Whichever way the readers read, they help.
	t.Run("Get", func(t *testing.T) {
		t.Parallel()
		testReadersHelp(t, func(c *s3fifo.Cache[int, *cachetest.Blob], key int) { c.Get(key) })
	})
	t.Run("Peek", func(t *testing.T) {
		t.Parallel()
		testReadersHelp(t, func(c *s3fifo.Cache[int, *cachetest.Blob], key int) { c.Peek(key) })
	})
}

func testReadersHelp(t *testing.T, read func(c *s3fifo.Cache[int, *cachetest.Blob], key int)) {
	t.Helper()

	const (
		capacity = 16 // The shard has patience for two entries that are gone.
		held     = 4
	)
	c := s3fifo.New[int, *cachetest.Blob](capacity, s3fifo.WithShards(1))

	refs := make([]weak.Pointer[cachetest.Blob], capacity)
	for key := range capacity {
		blob := new(cachetest.Blob)
		refs[key] = weak.Make(blob)
		c.Set(key, blob)
	}

	// Four readers stop in the middle of reading an entry each. Every one
	// of them is a goroutine of its own, as readers are, and stays one for
	// as long as it looks: a goroutine can only look at so many entries at
	// once, and four of them find a place to say so however few there are.
	var (
		looking, readers sync.WaitGroup
		moveOn           = make(chan struct{})
	)
	for key := range held {
		looking.Add(1)
		readers.Go(func() {
			release, ok := c.Hold(key)
			if !ok {
				t.Errorf("Hold(%d) found no entry", key)
			}
			looking.Done()
			<-moveOn
			release()
		})
	}
	looking.Wait()

	alive := func(limit int) int { return cachetest.Alive(refs, limit) }

	// Everything is deleted. The shard lets go of what it can.
	for key := range capacity {
		c.Delete(key)
	}
	if got := alive(held + 2); got < held || got > held+2 {
		t.Fatalf("%d values are alive after all were deleted, want the %d that readers look at and up to 2 more", got, held)
	}

	// The readers that are not held up try to help every so often, to no
	// avail for now. They must not get in the way of a writer when they do.
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 20_000 {
			c.Set(capacity+i%capacity, nil)
			c.Delete(capacity + i%capacity)
		}
	})
	for range 4 {
		wg.Go(func() {
			for i := range 100_000 {
				read(c, i%(2*capacity))
			}
		})
	}
	wg.Wait()
	if got := alive(len(refs)); got < held {
		t.Fatalf("%d values are alive, fewer than the %d that readers are looking at", got, held)
	}

	// The readers move on, and nobody writes to the cache again.
	close(moveOn)
	readers.Wait()
	for i := range 400_000 {
		read(c, i%capacity)
	}
	if got := alive(0); got != 0 {
		t.Errorf("%d values are alive after plenty of reads, want none: the readers did not help", got)
	}
	runtime.KeepAlive(c)
}

// TestCrowdedReaders runs the tests of reading and writing at the same time
// once more, with caches whose readers find few of the slots free in which
// they say what they are looking at, or none. They then read under the lock
// of the shard, as they do when the index changes under them, and that must
// not make a difference to anyone.
func TestCrowdedReaders(t *testing.T) {
	t.Parallel()

	for _, leave := range []int{0, 3} {
		t.Run(fmt.Sprintf("leave=%d", leave), func(t *testing.T) {
			t.Parallel()

			cachetest.Versions(t, func(capacity, shards int) evict.Cache[string, cachetest.Wide] {
				c := s3fifo.New[string, cachetest.Wide](capacity, s3fifo.WithShards(shards))
				c.OccupyReaders(leave)
				return c
			})
			cachetest.Present(t, func(capacity, shards int) evict.Cache[int, int] {
				c := s3fifo.New[int, int](capacity, s3fifo.WithShards(shards))
				c.OccupyReaders(leave)
				return c
			})
		})
	}
}

func FuzzAgainstModel(f *testing.F) {
	f.Add(3, uint8(0), []byte{})
	f.Add(1, uint8(0), []byte{128, 129, 0, 130, 1})
	f.Add(3, uint8(8), []byte{128, 129, 130, 131, 128, 129, 130, 0, 131})
	f.Add(10, uint8(0), []byte("an arbitrary sequence of operations \xff on a small cache"))
	f.Add(20, uint8(10), []byte("the same \x80\x81\x82\x83 on a cache \x80\x81 that is tuned"))

	f.Fuzz(func(t *testing.T, capacity int, tuned uint8, ops []byte) {
		capacity %= 40
		set := settings[int(tuned)%len(settings)]
		cachetest.Compare(t,
			s3fifo.New[int, int](capacity, append(set.options, s3fifo.WithShards(1))...),
			set.model(max(capacity, 0)),
			cachetest.BytesOps(ops),
		)
	})
}
