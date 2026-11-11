package tinylfu_test

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
	"github.com/floatdrop/evict/internal/sketch"
	"github.com/floatdrop/evict/lru"
	"github.com/floatdrop/evict/tinylfu"
)

var factories = cachetest.Factories{
	Int: func(capacity, shards int) evict.Cache[int, int] {
		return tinylfu.New[int, int](capacity, tinylfu.WithShards(shards))
	},
	String: func(capacity, shards int) evict.Cache[string, string] {
		return tinylfu.New[string, string](capacity, tinylfu.WithShards(shards))
	},
	Float: func(capacity, shards int) evict.Cache[float64, int] {
		return tinylfu.New[float64, int](capacity, tinylfu.WithShards(shards))
	},
	Blob: func(capacity, shards int) evict.Cache[int, *cachetest.Blob] {
		return tinylfu.New[int, *cachetest.Blob](capacity, tinylfu.WithShards(shards))
	},
	BlobKey: func(capacity, shards int) evict.Cache[*cachetest.Blob, int] {
		return tinylfu.New[*cachetest.Blob, int](capacity, tinylfu.WithShards(shards))
	},
	Wide: func(capacity, shards int) evict.Cache[string, cachetest.Wide] {
		return tinylfu.New[string, cachetest.Wide](capacity, tinylfu.WithShards(shards))
	},
	// The sketch holds counters, not keys: it keeps no key alive.
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

// A scene is a cache in one piece in which single entries can be followed
// around. Its keys go by the names 1, 2, 3 and so on, and no two of them
// share a counter of the sketch: what the sketch says of a key is how often
// it was asked for, to the letter, as long as the counters have not been
// halved. With ten additions for every entry until then, the scenes are over
// before that.
type scene struct {
	t     *testing.T
	c     *tinylfu.Cache[int, string]
	keys  []int       // by name
	names map[int]int // by key
}

func newScene(t *testing.T, capacity, names int) *scene {
	t.Helper()

	c := tinylfu.New[int, string](capacity, tinylfu.WithShards(1))
	s := &scene{t: t, c: c, names: map[int]int{}}
	s.keys = append([]int{-1}, tinylfu.Apart(c, names)...) // Nothing goes by the name of 0.
	for name, key := range s.keys {
		s.names[key] = name
	}
	return s
}

// set stores entries that need no room made for them.
func (s *scene) set(names ...int) {
	s.t.Helper()
	for _, name := range names {
		if k, _, displaced := s.c.Set(s.keys[name], ""); displaced {
			s.t.Fatalf("Set(%d) displaced %d", name, s.names[k])
		}
	}
}

// evicts stores an entry and checks which one made room for it.
func (s *scene) evicts(name, want int) {
	s.t.Helper()
	if k, _, displaced := s.c.Set(s.keys[name], ""); !displaced || s.names[k] != want {
		s.t.Fatalf("Set(%d) evicted %d, %t, want %d", name, s.names[k], displaced, want)
	}
}

func (s *scene) get(name, times int) {
	s.t.Helper()
	for range times {
		if _, ok := s.c.Get(s.keys[name]); !ok {
			s.t.Fatalf("Get(%d) found nothing", name)
		}
	}
}

// expect checks the main queue and the small queue, each from the newest to
// the oldest entry.
func (s *scene) expect(inMain, inSmall []int) {
	s.t.Helper()

	named := func(keys []int) []int {
		names := make([]int, 0, len(keys))
		for _, key := range keys {
			names = append(names, s.names[key])
		}
		return names
	}
	gotMain, gotSmall := s.c.Queues()
	if !slices.Equal(named(gotMain), inMain) || !slices.Equal(named(gotSmall), inSmall) {
		s.t.Fatalf("the main queue holds %v and the small queue %v, want %v and %v", named(gotMain), named(gotSmall), inMain, inSmall)
	}
	// All goes through them in the same order.
	var all []int
	for key := range s.c.All() {
		all = append(all, key)
	}
	if !slices.Equal(named(all), slices.Concat(inMain, inSmall)) {
		s.t.Fatalf("All yields %v, want %v followed by %v", named(all), inMain, inSmall)
	}
}

// asked checks how often the sketch has a key down as asked for.
func (s *scene) asked(name, want int) {
	s.t.Helper()
	if got := s.c.Asked(s.keys[name]); got != want {
		s.t.Fatalf("the sketch has %d down as asked for %d times, want %d", name, got, want)
	}
}

type in = []int

// TestAdmission follows entries through the small queue: out of the cache, or
// on into the main queue in place of the entry that it would evict next.
func TestAdmission(t *testing.T) {
	t.Parallel()

	// Of ten entries, one is the share of the small queue to begin with.
	s := newScene(t, 10, 30)
	if got := s.c.Window(); got != 1 {
		t.Fatalf("the small queue is to have %d entries, want 1", got)
	}

	// While the cache fills up for the first time, the small queue takes
	// its share, which is the first entry, and the rest goes straight to
	// the main queue.
	s.set(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	s.expect(in{10, 9, 8, 7, 6, 5, 4, 3, 2}, in{1})

	// From here on, what is new goes through the small queue. When its
	// turn to leave comes, it is up against the oldest entry of the main
	// queue, which is 2. Both have been asked for once, and a tie goes to
	// the one that was there first.
	s.evicts(11, 1)
	s.expect(in{10, 9, 8, 7, 6, 5, 4, 3, 2}, in{11})
	s.evicts(12, 11)
	s.expect(in{10, 9, 8, 7, 6, 5, 4, 3, 2}, in{12})

	// 11 is asked for again. That makes twice, which the sketch knows
	// although the cache had let go of it: this time it gets in, and 2 goes.
	s.evicts(11, 12)
	s.asked(11, 2)
	s.evicts(13, 2)
	s.expect(in{11, 10, 9, 8, 7, 6, 5, 4, 3}, in{13})

	// A read is as good as that. The sketch learns of it when a write next
	// looks at the entry.
	s.get(13, 1)
	s.asked(13, 1)
	s.evicts(14, 3)
	s.asked(13, 2)
	s.expect(in{13, 11, 10, 9, 8, 7, 6, 5, 4}, in{14})

	// Peeking at an entry does not count.
	s.c.Peek(s.keys[14])
	s.c.Peek(s.keys[14])
	s.evicts(15, 14)
	s.asked(14, 1)
	s.expect(in{13, 11, 10, 9, 8, 7, 6, 5, 4}, in{15})

	// Writing to one counts like reading it.
	if k, _, displaced := s.c.Set(s.keys[15], "again"); !displaced || k != s.keys[15] {
		t.Fatalf("Set of 15, which is cached, displaced %d, %t", s.names[k], displaced)
	}
	s.evicts(16, 4)
	s.asked(15, 2)
	s.expect(in{15, 13, 11, 10, 9, 8, 7, 6, 5}, in{16})

	// Reads count up to three with the sketch between one look and the
	// next, however many there were.
	s.get(16, 9)
	s.evicts(17, 5)
	s.asked(16, 4)
	s.expect(in{16, 15, 13, 11, 10, 9, 8, 7, 6}, in{17})

	// Five more entries that were read once take the places of 6 to 10.
	for name := 17; name <= 21; name++ {
		s.get(name, 1)
		s.evicts(name+1, name-11)
	}
	s.expect(in{21, 20, 19, 18, 17, 16, 15, 13, 11}, in{22})

	// Now the oldest entry of the main queue is 11, which was asked for
	// twice. One that was asked for as often does not get in,
	s.get(22, 1)
	s.evicts(23, 22)
	s.expect(in{21, 20, 19, 18, 17, 16, 15, 13, 11}, in{23})
	// and one that was asked for more often does.
	s.get(23, 2)
	s.evicts(24, 11)
	s.expect(in{23, 21, 20, 19, 18, 17, 16, 15, 13}, in{24})

	// Deleting an entry does not make the sketch forget its key. It learns
	// of the reads that the entry had counted, even.
	s.get(24, 2)
	if _, ok := s.c.Delete(s.keys[24]); !ok {
		t.Fatal("Delete(24) found nothing")
	}
	s.asked(24, 3)
	s.expect(in{23, 21, 20, 19, 18, 17, 16, 15, 13}, nil)
	// So when 24 is back, it is one that has been asked for four times,
	// and gets in ahead of 13 with its two.
	s.set(24)
	s.evicts(25, 13)
	s.expect(in{24, 23, 21, 20, 19, 18, 17, 16, 15}, in{25})

	// A cache that was cleared has forgotten all of it, and fills up like
	// a new one.
	s.c.Clear()
	for name := 1; name <= 25; name++ {
		s.asked(name, 0)
	}
	s.set(16, 24, 3, 4, 5, 6, 7, 8, 9, 10)
	s.expect(in{10, 9, 8, 7, 6, 5, 4, 3, 24}, in{16})
	s.evicts(11, 16)
	s.evicts(12, 11)
	s.expect(in{10, 9, 8, 7, 6, 5, 4, 3, 24}, in{12})
}

// TestMainQueue follows entries around the main queue.
func TestMainQueue(t *testing.T) {
	t.Parallel()

	// A main queue of three entries is quick to go round: the small queue
	// has seven of the ten.
	s := newScene(t, 10, 30)
	s.c.Resize(7)
	s.set(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	s.expect(in{10, 9, 8}, in{7, 6, 5, 4, 3, 2, 1})

	// 8 and 9 have been read, so they get another round, and the one that
	// 1 is up against is 10. An entry gets a round for every read, up to
	// three, however often it was read: 8 has three coming and 9 this one.
	s.get(8, 5)
	s.get(9, 1)
	s.evicts(11, 1)
	s.expect(in{9, 8, 10}, in{11, 7, 6, 5, 4, 3, 2})
	s.asked(8, 4)
	s.asked(9, 2)

	// 10 stays where it is for as long as it holds its own.
	s.evicts(12, 2)
	s.expect(in{9, 8, 10}, in{12, 11, 7, 6, 5, 4, 3})
	s.get(3, 1)
	s.evicts(13, 10)
	s.expect(in{3, 9, 8}, in{13, 12, 11, 7, 6, 5, 4})

	// The second round of 8. For 9 there is none: it is the one that 4 is
	// up against, and holds its own with its two requests.
	s.evicts(14, 4)
	s.expect(in{8, 3, 9}, in{14, 13, 12, 11, 7, 6, 5})
	// The sketch was told of the reads of 8 once and is not told again.
	s.asked(8, 4)

	s.get(5, 2)
	s.evicts(15, 9)
	s.expect(in{5, 8, 3}, in{15, 14, 13, 12, 11, 7, 6})
	s.evicts(16, 6)
	s.expect(in{5, 8, 3}, in{16, 15, 14, 13, 12, 11, 7})
	s.get(7, 2)
	s.evicts(17, 3)
	s.expect(in{7, 5, 8}, in{17, 16, 15, 14, 13, 12, 11})

	// The third round of 8, and its last.
	s.evicts(18, 11)
	s.expect(in{8, 7, 5}, in{18, 17, 16, 15, 14, 13, 12})
	s.asked(8, 4)

	s.get(12, 3)
	s.evicts(19, 5)
	s.expect(in{12, 8, 7}, in{19, 18, 17, 16, 15, 14, 13})
	s.get(13, 3)
	s.evicts(20, 7)
	s.expect(in{13, 12, 8}, in{20, 19, 18, 17, 16, 15, 14})

	// After that it is an entry like any other. It is the one that 14 is
	// up against, and stays because it was asked for four times.
	s.evicts(21, 14)
	s.expect(in{13, 12, 8}, in{21, 20, 19, 18, 17, 16, 15})
	// Until it is read again.
	s.get(8, 1)
	s.evicts(22, 15)
	s.expect(in{8, 13, 12}, in{22, 21, 20, 19, 18, 17, 16})
	s.asked(8, 5)
}

// TestWindow checks what the queues do when the share of the small one
// changes.
func TestWindow(t *testing.T) {
	t.Parallel()

	s := newScene(t, 10, 30)
	s.c.Resize(5)
	s.set(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	s.expect(in{10, 9, 8, 7, 6}, in{5, 4, 3, 2, 1})

	// The share of the small queue shrinks to two entries. When room is
	// next needed, it hands its oldest entries on to the main queue until
	// it is within its share, read or not; what they were read goes on
	// record. After that it is 4 that is up against 6.
	s.get(1, 2)
	s.c.Resize(2)
	s.evicts(11, 4)
	s.expect(in{3, 2, 1, 10, 9, 8, 7, 6}, in{11, 5})
	s.asked(1, 3)

	// The share grows to four. Until the small queue has that many, the
	// main queue is the one to make room: its oldest entry that has not
	// been read goes, and has nothing to be up against.
	s.c.Resize(4)
	s.evicts(12, 6)
	s.expect(in{3, 2, 1, 10, 9, 8, 7}, in{12, 11, 5})
	s.get(7, 1)
	s.evicts(13, 8)
	s.expect(in{7, 3, 2, 1, 10, 9}, in{13, 12, 11, 5})
	// Now it has.
	s.evicts(14, 5)
	s.expect(in{7, 3, 2, 1, 10, 9}, in{14, 13, 12, 11})

	// After deletions there is room, and what is new joins the small
	// queue all the same once the cache has been full: it may be over its
	// share for a while.
	for _, name := range []int{10, 9} {
		if _, ok := s.c.Delete(s.keys[name]); !ok {
			t.Fatalf("Delete(%d) found nothing", name)
		}
	}
	s.set(15, 16)
	s.expect(in{7, 3, 2, 1}, in{16, 15, 14, 13, 12, 11})
	// 11 and 12 are handed on, and 13 is up against 1, which was asked
	// for three times.
	s.evicts(17, 13)
	s.expect(in{12, 11, 7, 3, 2, 1}, in{17, 16, 15, 14})

	// A cache that is cleared keeps the share that it has found: of the
	// next ten entries, four fill the small queue.
	s.c.Clear()
	if got := s.c.Window(); got != 4 {
		t.Fatalf("after Clear the small queue is to have %d entries, want 4", got)
	}
	s.set(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	s.expect(in{10, 9, 8, 7, 6, 5}, in{4, 3, 2, 1})
}

// TestSmallCaches checks the caches that have no room for two queues to speak
// of.
func TestSmallCaches(t *testing.T) {
	t.Parallel()

	// With room for one entry there is the small queue only, and what is
	// new takes the place of what is there.
	s := newScene(t, 1, 5)
	s.set(1)
	s.expect(nil, in{1})
	s.get(1, 3)
	s.evicts(2, 1)
	s.evicts(1, 2)
	s.expect(nil, in{1})
	s.asked(1, 5)

	// With room for two there is one of each.
	s = newScene(t, 2, 6)
	s.set(1, 2)
	s.expect(in{2}, in{1})
	s.evicts(3, 1)
	s.get(3, 1)
	s.evicts(4, 2)
	s.expect(in{3}, in{4})
}

// A workload is the keys that a program asks its cache for, one after the
// other.
type workload = iter.Seq[int]

// replay runs the workload against a cache the way a program uses one, and
// returns the share of the requests that the cache answered.
func replay(c evict.Cache[int, int], w workload) float64 {
	hits, requests := 0, 0
	for key := range w {
		requests++
		if _, ok := c.Get(key); ok {
			hits++
		} else {
			c.Set(key, key)
		}
	}
	return float64(hits) / float64(requests)
}

// TestFindsItsWindow checks that the cache finds a size for its small queue
// that suits the workload, by what comes of it: on a workload that an LRU
// cache is bad at, it does much better than one, and on a workload that an
// LRU cache is as good at as a cache can be, it does not do much worse.
func TestFindsItsWindow(t *testing.T) {
	t.Parallel()

	const capacity, requests = 1000, 1_500_000

	// Popular keys among a flood of keys that are asked for once: the
	// newest entries are the ones least worth keeping.
	t.Run("frequency", func(t *testing.T) {
		t.Parallel()

		w := func(yield func(int) bool) {
			rng := rand.New(rand.NewPCG(1, 2))
			zipf := rand.NewZipf(rng, 1.01, 1, 20_000)
			for i := range requests {
				key := int(zipf.Uint64())
				if i%2 == 0 {
					key = -i - 1
				}
				if !yield(key) {
					return
				}
			}
		}
		c := tinylfu.New[int, int](capacity, tinylfu.WithShards(1))
		got, plain := replay(c, w), replay(lru.New[int, int](capacity), w)
		t.Logf("answered %.1f%%, an LRU cache %.1f%%; the small queue has ended up with %d entries", 100*got, 100*plain, c.Window())
		if got < plain+0.03 {
			t.Errorf("answered %.1f%% of the requests, want at least three points more than the %.1f%% of an LRU cache", 100*got, 100*plain)
		}
	})

	// The keys that are asked for are the newest six hundred, whichever
	// they are at the time, and every third request is for one that is
	// new: how long ago a key was asked for says everything, and how often
	// says nothing. An LRU cache answers all there is to answer. The small
	// queue has to be large for that; with the hundredth of the room that
	// it starts with, the cache answers 15% of the requests.
	t.Run("recency", func(t *testing.T) {
		t.Parallel()

		w := func(yield func(int) bool) {
			rng := rand.New(rand.NewPCG(1, 2))
			newest := 0
			for range requests {
				key := newest - rng.IntN(600)
				if rng.IntN(10) < 3 {
					newest++
					key = newest
				}
				if !yield(key) {
					return
				}
			}
		}
		c := tinylfu.New[int, int](capacity, tinylfu.WithShards(1))
		got, plain := replay(c, w), replay(lru.New[int, int](capacity), w)
		t.Logf("answered %.1f%%, an LRU cache %.1f%%; the small queue has ended up with %d entries", 100*got, 100*plain, c.Window())
		if got < plain-0.15 {
			t.Errorf("answered %.1f%% of the requests, want no more than fifteen points fewer than the %.1f%% of an LRU cache", 100*got, 100*plain)
		}
		if c.Window() < capacity/4 {
			t.Errorf("the small queue has ended up with %d entries of %d, want a quarter at least", c.Window(), capacity)
		}
	})
}

// model is the policy at its simplest: a slice for each queue, with the
// newest entry first, and the search for the size of the small queue as
// Caffeine has it, every so many misses.
type model struct {
	capacity int
	// known is what the sketch of the cache knows a key by. Keys that
	// share counters there share them here.
	known func(key int) uint64
	asked sketch.Sketch

	small, main []*entry
	// full says that the cache has been full since it was made or cleared.
	full bool

	window                 int
	target, step, previous float64
	// measured says that previous has been measured since the model was
	// made or cleared.
	measured bool
	// The requests answered and not since the size last moved, and the
	// misses after which it moves next.
	hits, misses, due int
	moves             int
}

type entry struct {
	key, value int
	reads      int // since a write last looked, up to three
	count      int // what the main queue left it with
}

func newModel(capacity int, known func(key int) uint64) *model {
	m := &model{capacity: capacity, known: known}
	m.asked.Init(capacity, max(10*capacity, 1))
	m.target = max(0.01*float64(capacity), 1)
	m.window = int(m.target)
	m.step = 0.0625 * float64(capacity)
	m.due = 40 * capacity
	return m
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
	m.hits++
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
		m.hits++
		return key, oldValue, true
	}
	if m.capacity == 0 {
		return key, value, true
	}

	m.asked.Add(m.known(key), 1)
	m.misses++
	if m.misses >= m.due {
		m.climb()
	}
	if len(m.small)+len(m.main) == m.capacity {
		oldKey, oldValue = m.evict()
		displaced = true
	}

	// A cache that has never been full does not put more into the small
	// queue than its share.
	e := &entry{key: key, value: value}
	if !m.full && len(m.small) >= m.window {
		m.main = slices.Insert(m.main, 0, e)
	} else {
		m.small = slices.Insert(m.small, 0, e)
	}
	return oldKey, oldValue, displaced
}

// climb is the hill climber of Caffeine. The first time, there is nothing to
// compare with but the share of requests answered that it has just measured.
// It moves again after the misses that forty requests per entry would bring
// at that share, or after one at least.
func (m *model) climb() {
	rate := float64(m.hits) / float64(m.hits+m.misses)
	if !m.measured {
		m.previous, m.measured = rate, true
	}
	change := rate - m.previous
	amount := m.step
	if change < 0 {
		amount = -m.step
	}
	if change >= 0.05 || change <= -0.05 {
		m.step = 0.0625 * float64(m.capacity)
		if amount < 0 {
			m.step = -m.step
		}
	} else {
		m.step = 0.98 * amount
	}
	m.previous, m.hits, m.misses = rate, 0, 0
	m.due = max(int(40*float64(m.capacity)*(1-rate)), 1)
	m.moves++

	least, most := max(0.01*float64(m.capacity), 1), max(0.8*float64(m.capacity), 1)
	m.target = min(max(m.target+amount, least), most)
	m.window = int(m.target)
}

// look is what a write does when it comes by an entry: the sketch learns of
// the reads that count and that it has not been told of. It returns the
// count of the entry.
func (m *model) look(e *entry) int {
	count := min(e.count+e.reads, 3)
	m.asked.Add(m.known(e.key), count-e.count)
	e.reads = 0
	return count
}

// victim turns the main queue until its oldest entry is one that has no
// count left, and returns that.
func (m *model) victim() *entry {
	for len(m.main) > 0 {
		last := len(m.main) - 1
		e := m.main[last]
		count := m.look(e)
		if count == 0 {
			return e
		}
		e.count = count - 1
		m.main = slices.Insert(m.main[:last], 0, e)
	}
	return nil
}

func (m *model) evict() (key, value int) {
	m.full = true

	moveUp := func() {
		last := len(m.small) - 1
		e := m.small[last]
		m.small = m.small[:last]
		m.main = slices.Insert(m.main, 0, e)
	}
	for len(m.small) > m.window {
		m.look(m.small[len(m.small)-1])
		moveUp()
	}

	if len(m.small) < m.window {
		gone := m.victim()
		m.main = m.main[:len(m.main)-1]
		return gone.key, gone.value
	}

	candidate := m.small[len(m.small)-1]
	m.look(candidate)
	against := m.victim()
	if against == nil || m.asked.Estimate(m.known(candidate.key)) <= m.asked.Estimate(m.known(against.key)) {
		m.small = m.small[:len(m.small)-1]
		return candidate.key, candidate.value
	}
	m.main = m.main[:len(m.main)-1]
	moveUp()
	return against.key, against.value
}

func (m *model) Delete(key int) (int, bool) {
	e := m.find(key)
	if e == nil {
		return 0, false
	}
	m.look(e)
	same := func(other *entry) bool { return other == e }
	m.small = slices.DeleteFunc(m.small, same)
	m.main = slices.DeleteFunc(m.main, same)
	return e.value, true
}

func (m *model) Len() int { return len(m.small) + len(m.main) }
func (m *model) Cap() int { return m.capacity }

// Clear keeps the size of the small queue, and what the last measurement says
// of how many misses to wait for, but measures afresh.
func (m *model) Clear() {
	m.small, m.main, m.full = nil, nil, false
	m.asked.Reset()
	m.measured, m.hits, m.misses = false, 0, 0
	m.due = max(int(40*float64(m.capacity)*(1-m.previous)), 1)
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
		{300, 1000, 100_000},
	} {
		// Once the way it is meant to be, and once with readers that have
		// to take the lock. And once with the odd Clear among the
		// operations, and once without: Clear starts the measurements for
		// the size of the small queue over, and the cache is cleared too
		// often here for one of them ever to be complete.
		for _, crowded := range []bool{false, true} {
			for _, clears := range []bool{true, false} {
				t.Run(fmt.Sprintf("capacity=%d/keys=%d/crowded=%t/clears=%t", tt.capacity, tt.keys, crowded, clears), func(t *testing.T) {
					t.Parallel()

					c := tinylfu.New[int, int](tt.capacity, tinylfu.WithShards(1))
					if crowded {
						c.OccupyReaders(0)
					}
					m := newModel(tt.capacity, c.Print)
					rng := rand.New(rand.NewPCG(uint64(i), 1))
					ops := cachetest.RandomOps(rng, tt.keys, tt.steps)
					if !clears {
						ops = withoutClear(ops)
					}
					cachetest.Compare(t, c, m, ops)

					if got := c.Window(); got != m.window {
						t.Errorf("the small queue is to have %d entries, want %d", got, m.window)
					}
					// The comparison is only worth something if the size of
					// the small queue has moved, where there is room for it
					// to.
					if !clears && tt.capacity >= 8 && tt.keys > tt.capacity && m.moves < 2 {
						t.Errorf("the size of the small queue has moved %d times in %d steps, want 2 at least", m.moves, tt.steps)
					}
				})
			}
		}
	}
}

// withoutClear returns the operations with a Peek of no consequence in place of
// every Clear.
func withoutClear(ops iter.Seq[cachetest.Op]) iter.Seq[cachetest.Op] {
	return func(yield func(cachetest.Op) bool) {
		for op := range ops {
			if op.Kind == cachetest.Clear {
				op.Kind = cachetest.Peek
			}
			if !yield(op) {
				return
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

	c := tinylfu.New[int, int](20, tinylfu.WithShards(1))
	stored, other := tinylfu.Twins(c)

	c.Set(stored, 0)
	for range 3 {
		if _, ok := c.Get(other); ok {
			t.Fatalf("Get found key %d, which was never set", other)
		}
		if _, ok := c.Peek(other); ok {
			t.Fatalf("Peek found key %d, which was never set", other)
		}
	}
	if reads, ok := c.Reads(stored); !ok || reads != 0 {
		t.Errorf("the entry for key %d has counted %d reads, %t; want none: those of %d, which has the same hash, were counted for it",
			stored, reads, ok, other)
	}

	// Its own are counted, up to three.
	for want := uint32(1); want <= 5; want++ {
		c.Get(stored)
		c.Peek(stored)
		if reads, _ := c.Reads(stored); reads != min(want, 3) {
			t.Fatalf("the entry has counted %d reads after %d, want %d", reads, want, min(want, 3))
		}
	}
}

// TestHeldUpReader checks what a reader costs the others that stops in the
// middle of reading an entry, as one does whose goroutine has to make way: the
// value it is looking at stays alive, and that is all. Nobody waits for the
// reader, except for Clear, which promises to leave nothing behind.
func TestHeldUpReader(t *testing.T) {
	t.Parallel()

	for _, shards := range []int{1, 4} {
		c := tinylfu.New[int, *cachetest.Blob](64, tinylfu.WithShards(shards))

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

	c := tinylfu.New[any, int](len(keys), tinylfu.WithShards(1))
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
		testReadersHelp(t, func(c *tinylfu.Cache[int, *cachetest.Blob], key int) { c.Get(key) })
	})
	t.Run("Peek", func(t *testing.T) {
		t.Parallel()
		testReadersHelp(t, func(c *tinylfu.Cache[int, *cachetest.Blob], key int) { c.Peek(key) })
	})
}

func testReadersHelp(t *testing.T, read func(c *tinylfu.Cache[int, *cachetest.Blob], key int)) {
	t.Helper()

	const (
		capacity = 16 // The shard has patience for two entries that are gone.
		held     = 4
	)
	c := tinylfu.New[int, *cachetest.Blob](capacity, tinylfu.WithShards(1))

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
				c := tinylfu.New[string, cachetest.Wide](capacity, tinylfu.WithShards(shards))
				c.OccupyReaders(leave)
				return c
			})
			cachetest.Present(t, func(capacity, shards int) evict.Cache[int, int] {
				c := tinylfu.New[int, int](capacity, tinylfu.WithShards(shards))
				c.OccupyReaders(leave)
				return c
			})
		})
	}
}

func FuzzAgainstModel(f *testing.F) {
	f.Add(3, []byte{})
	f.Add(1, []byte{128, 129, 0, 130, 1})
	f.Add(3, []byte{128, 129, 130, 131, 128, 129, 130, 0, 131})
	f.Add(10, []byte("an arbitrary sequence of operations \xff on a small cache"))

	f.Fuzz(func(t *testing.T, capacity int, ops []byte) {
		capacity = max(capacity%40, 0)
		c := tinylfu.New[int, int](capacity, tinylfu.WithShards(1))
		cachetest.Compare(t, c, newModel(capacity, c.Print), cachetest.BytesOps(ops))
	})
}
