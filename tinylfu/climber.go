package tinylfu

import (
	"math"
	"sync"
	"sync/atomic"

	"github.com/floatdrop/evict/internal/cslab"
	"github.com/floatdrop/evict/internal/shards"
)

// A climber finds the size that the small queue should have, by trying: it
// moves the size a step, sees whether the cache answers a larger share of its
// requests than before, and goes on in the same direction if so and back if
// not. The steps get shorter as it goes, so that it slows down near the top,
// and long again when the share of requests answered changes by much: the
// workload is another one then, and the search starts over.
//
// This is how Caffeine finds the size of its window, and the numbers are
// Caffeine's but for how often the size moves. It cannot do better than the
// share of requests answered lets it: where a larger small queue makes little
// difference, the noise in that share does, and the size wanders.
type climber struct {
	// target is the size of the small queue, as a number of entries of a
	// shard of the capacity that the climber was made for, before it is
	// rounded down. step is how far it moves next, and by its sign which
	// way, if things have got better by then; rate is what they have to be
	// better than.
	target, step, rate float64

	// measured says that rate has been measured. The first measurement
	// after the climber was made or told to start over sets it, and the
	// size moves as if nothing had changed.
	measured bool

	// The bounds of the target, and the step that the search starts with.
	least, most, first float64

	// period is the number of requests that the size is to stay put for.
	// What the climber waits for is misses, though; see due.
	period float64
}

const (
	// The small queue has this share of a shard to begin with, and never
	// less, and never more than maxShare of it.
	minShare = 0.01
	maxShare = 0.8

	// The first step is this share of the capacity. Every step after it is
	// shorter by the factor decay, until the share of requests answered
	// changes by restart or more from one step to the next.
	firstStep = 0.0625
	decay     = 0.98
	restart   = 0.05

	// The size moves after about this many requests for every entry that
	// the cache has room for.
	periodPerEntry = 40
)

// init readies the climber for shards of the given capacity, of which there
// are as many as make up total.
func (c *climber) init(capacity, total int) {
	n := float64(capacity)
	*c = climber{
		least:  max(minShare*n, 1),
		most:   max(maxShare*n, 1),
		first:  firstStep * n,
		period: periodPerEntry * float64(total),
	}
	c.target, c.step = c.least, c.first
}

// size returns the number of entries that the small queue of a shard of the
// capacity that the climber was made for is to have.
func (c *climber) size() int { return int(c.target) }

// move takes a step, given how many of the requests since the last one the
// cache answered and how many it did not: on in the direction of the last
// step if the share of requests answered has grown since, or stayed the
// same, and back if it has shrunk. It returns the number of misses after
// which the next step is due.
func (c *climber) move(hits, misses uint64) (due uint64) {
	rate := float64(hits) / float64(max(hits+misses, 1))
	if !c.measured {
		c.rate, c.measured = rate, true
	}
	gain := rate - c.rate
	c.rate = rate

	by := c.step
	if gain < 0 {
		by = -by
	}
	if math.Abs(gain) >= restart {
		c.step = math.Copysign(c.first, by)
	} else {
		c.step = decay * by
	}
	c.target = min(max(c.target+by, c.least), c.most)
	return c.due()
}

// due returns the number of misses that come with a period's worth of
// requests if the cache answers the share of them that was measured last, or
// as many as there are requests if nothing has been measured yet. It is one at
// least.
func (c *climber) due() uint64 {
	return uint64(max(c.period*(1-c.rate), 1))
}

// forget has the next measurement taken for what the ones after it are
// compared with, rather than compared with the last one. The size, the step
// and what due goes by stay as they are.
func (c *climber) forget() { c.measured = false }

// A window is what the shards of a cache share of their small queues: the
// size that they are to have, and the climber that looks for it.
//
// The climber goes by the share of requests that the cache answers, of all
// shards together. A miss is a key that the cache is asked to put in and does
// not hold, as Caffeine has it: for a cache that is read through, that is
// the Set after a Get that found nothing, and the Get itself does not count.
// A shard counts its misses and the hits that it sees under its lock, and
// hands its counts on to the window every so often. The other hits are those
// of readers that take no lock and count in the slots of the cache's
// [cslab.Readers], which the window adds up when it is time to move. That is
// not often: adding up the slots takes a while.
type window struct {
	// target holds the bits of climber.target, for the shards to read
	// without taking mu.
	target atomic.Uint64

	_ shards.Pad

	// What the shards have handed on since the cache was made: the hits that
	// they counted themselves and their misses. due is the number of misses
	// at which the size moves next.
	hits, misses, due atomic.Uint64

	_ shards.Pad

	// mu is taken by a writer that moves the size, while it holds the lock
	// of its own shard, and by Clear, which takes the locks of the shards
	// while it holds mu. Writers only try for it, and give up if it is
	// taken, so nobody waits for anybody in a circle.
	mu      sync.Mutex
	climber climber
	readers *cslab.Readers

	// The hits and misses that had been counted when the size last moved.
	hitsThen, missesThen uint64

	// base is the capacity of the largest shard, the one that the climber
	// was made for.
	base int
}

// init readies the window for a cache whose shards have the given capacities
// at most, and which reads with the given readers.
func (w *window) init(base, total int, readers *cslab.Readers) {
	w.base, w.readers = base, readers
	w.climber.init(base, total)
	w.target.Store(math.Float64bits(w.climber.target))
	w.due.Store(w.climber.due())
}

// sizeFor returns the number of entries that the small queue of a shard of
// the given capacity is to have: the climber's size, shrunk in proportion
// for a shard that is smaller than the largest one. It is one at least.
func (w *window) sizeFor(capacity int) int {
	target := math.Float64frombits(w.target.Load())
	if capacity == w.base {
		return int(target)
	}
	return max(int(target*float64(capacity)/float64(w.base)), 1)
}

// note hands on what a shard has counted, and moves the size if that is due.
// The caller holds the lock of its shard.
func (w *window) note(hits, misses uint64) {
	if hits != 0 {
		w.hits.Add(hits)
	}
	// If somebody else has the window, another writer that is moving the
	// size or Clear, the next batch tries again.
	if w.misses.Add(misses) >= w.due.Load() && w.mu.TryLock() {
		w.move()
		w.mu.Unlock()
	}
}

// move moves the size if that is due. The caller holds mu, and has seen that
// a move is due; but another writer may have seen that too, and moved it in
// between.
func (w *window) move() {
	misses := w.misses.Load()
	if misses < w.due.Load() {
		return
	}
	hits := w.readers.Hits() + w.hits.Load()
	due := w.climber.move(hits-w.hitsThen, misses-w.missesThen)
	w.hitsThen, w.missesThen = hits, misses
	w.target.Store(math.Float64bits(w.climber.target))
	w.due.Store(misses + due)
}

// forget makes the window measure afresh from here, and take the first
// measurement for what the ones after it are compared with. The size stays.
// The caller holds mu.
func (w *window) forget() {
	w.hitsThen, w.missesThen = w.readers.Hits()+w.hits.Load(), w.misses.Load()
	w.climber.forget()
	w.due.Store(w.missesThen + w.climber.due())
}

// resize sets the size, as the climber might have; the search goes on from
// there. It is for tests.
func (w *window) resize(size int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.climber.target = float64(size)
	w.target.Store(math.Float64bits(w.climber.target))
}
