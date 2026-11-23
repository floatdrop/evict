// Package expire gives the entries of a cache a time to live.
//
// [Wrap] puts a clock in front of any [evict.Cache] that holds [Entry] values:
//
//	cache := expire.Wrap(tinylfu.New[string, expire.Entry[int]](10_000), time.Minute)
//	cache.Set("a", 1)                        // Expires in a minute,
//	cache.SetWithTTL("b", 2, 10*time.Second) // and this one in ten seconds.
//	value, ok := cache.Get("a")
//
// # Expired entries
//
// An entry that has expired is not found: Get, Peek, GetMany and Delete report
// it missing, and All passes it over. It stays in the cache all the same, until
// it is evicted, replaced or deleted, or the cache is cleared. Until then it
// takes up room, Len counts it, and what it holds on to is not garbage.
//
// Set hands an expired entry back when it pushes it out or replaces it, as it
// does any entry, so that what it holds on to can be released. Delete and
// Clear do not. Where values have to be released, use the wrapped cache for
// those: its Delete and All return entries whether they have expired or not,
// and [Entry.Value] gives their values.
//
// A read that finds an expired entry leaves it there, for the Set that usually
// follows: that replaces the entry in place, and the policy goes on treating
// it as the entry it was, which it would not after a Delete. Reads count with
// the policy, expired or not, so an entry that is still asked for but never set
// again stays; delete its key when there is no new value for it. Expired or
// not, there are never more entries than the capacity of the cache.
//
// # Time
//
// An entry expires when its time to live is up, by a monotonic clock: it is
// found until then and not after. Reading the time takes about as long as a
// Get, so readers look at a copy of it instead, which a timer updates every
// 10 ms. Only an entry that is due to expire within 50 ms of that copy is
// checked against the time itself. If the program is so busy that the timer
// runs late by more than 40 ms, an entry may be found after it has expired,
// for as long as the delay exceeds those 40 ms.
//
// The timer starts with the first call to Wrap and keeps running after that,
// one for all caches. Each tick runs a goroutine, for which the runtime may
// start a thread, until it has enough of them. A test that counts allocations
// soon after the first Wrap may count those.
//
// # Statistics
//
// Wrapped in stats, as stats.Wrap(expire.Wrap(...)), a cache counts a read of
// an expired entry as a miss. The stats.Cache has no SetWithTTL: call that of
// the expire.Cache, and the entries it evicts go uncounted. Wrapped the other
// way around, a read of an expired entry counts as a hit.
//
// # Cost
//
// On an Apple M3 Max, a Get of tinylfu that found its key took 15% longer
// through the wrapper on one CPU, and 11 to 13% on eight. One of an entry that
// was due to expire within 50 ms took 15 ns longer again, for reading the
// time. Set reads it every time, and took 17 ns longer. GetMany of a million
// entries of s3fifo took 3 to 9% longer per key in the median, in batches of 8
// to 128 keys, though single runs varied by 20% either way. An entry takes 8
// bytes more for the time at which it expires, and padding for a value whose
// size is not a multiple of 8.
//
// The wrapper reaches the cache through an interface, which the compiler
// cannot see through: a key that would otherwise stay where it is may have to
// move to the heap, as one converted from a []byte on the spot does.
package expire

import (
	"iter"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/floatdrop/evict"
	"github.com/floatdrop/evict/internal/cslab"
)

const (
	// tick is how often the copy of the time that readers look at is
	// updated.
	tick = 10 * time.Millisecond

	// near is how close to that copy an entry has to be due to expire for a
	// reader to check it against the time itself. It leaves the timer 40 ms
	// to be late by before an entry is found for longer than it should be.
	near = 50 * time.Millisecond

	// cacheLine is a size that no two CPUs' cache lines straddle on the
	// platforms Go runs on.
	cacheLine = 128
)

// Entry is a value as the wrapped cache holds it, together with the time at
// which it expires. Create the cache with Entry[V] for its values and hand it
// to [Wrap]. An Entry that is made otherwise has expired.
type Entry[V any] struct {
	value    V
	deadline int64 // in nanoseconds of the clock of the cache
}

// Value returns the value of the entry, whether it has expired or not.
func (e Entry[V]) Value() V { return e.value }

// Cache is an [evict.Cache] whose entries expire; see the package
// documentation. Its methods are safe for concurrent use as long as those of
// the cache it wraps are, as they are for every cache in this module.
type Cache[K comparable, V any] struct {
	cache evict.Cache[K, Entry[V]]
	many  manyGetter[K, Entry[V]] // cache, if it has a GetMany
	clock *clock
	ttl   time.Duration

	// batches holds arrays that GetMany looks up entries in. Its own
	// would move to the heap, since the cache is behind an interface.
	batches sync.Pool
}

// manyGetter is a cache that looks up several keys at once, as sieve, s3fifo
// and tinylfu do.
type manyGetter[K comparable, V any] interface {
	GetMany(keys []K, values []V, found []bool)
}

// batch is as many entries as GetMany looks up at once: as many as the caches
// of this module load from memory at a time.
type batch[V any] = [cslab.Batch]Entry[V]

// A clock tells the time in nanoseconds since it was made.
//
// The words that readers look at are plain integers that are only ever
// accessed with the functions of sync/atomic, which the compiler builds in
// wherever it compiles the methods of a Cache. The methods of the atomic
// types it would inline there only if a package that is imported there had
// inlined them itself; see the package internal/inlinecheck.
type clock struct {
	_ [cacheLine]byte

	// now is the time of the last tick: what readers look at.
	now int64

	_ [cacheLine - 8]byte

	// The rest is read by writers, and by readers of entries that are due
	// to expire, but changes only in tests. fake comes first, where a
	// 32-bit platform has it aligned for atomic access.
	fake  int64 // the time, if it is fake; accessed like now
	fixed bool  // the time is fake, and moves only when a test says so
	epoch time.Time
	begin func() // starts the timer, the first time it is called
	timer *time.Timer
}

// system is the clock that every Cache made by Wrap goes by.
var system = newClock()

func newClock() *clock {
	c := &clock{epoch: time.Now()}
	c.begin = sync.OnceFunc(func() {
		atomic.StoreInt64(&c.now, c.read())
		// The timer is made first and set going after, so that the
		// function finds it when it runs.
		c.timer = time.AfterFunc(math.MaxInt64, c.advance)
		c.timer.Reset(tick)
	})
	return c
}

// advance updates the time that readers look at, and is called by the timer.
func (c *clock) advance() {
	atomic.StoreInt64(&c.now, c.read())
	c.timer.Reset(tick)
}

// due reports whether an entry that expires at deadline has, given that it was
// left ahead of the copy of the time that readers look at, which is near. It
// is not inlined, so that Cache.expired stays small enough to be.
//
//go:noinline
func (c *clock) due(left, deadline int64) bool { return left <= 0 || c.read() >= deadline }

// read returns the time.
func (c *clock) read() int64 {
	if c.fixed {
		return atomic.LoadInt64(&c.fake)
	}
	return int64(time.Since(c.epoch))
}

// deadline returns the time at which an entry expires that is set now with the
// given time to live. One whose time to live is not positive has expired at
// the start of the clock, so that no deadline is below zero, and one that
// would not expire before the clock runs out never does.
func (c *clock) deadline(ttl time.Duration) int64 {
	if ttl <= 0 {
		return 0
	}
	now := c.read()
	return now + min(int64(ttl), math.MaxInt64-now)
}

// Wrap returns a Cache that keeps its entries in cache, and has them expire ttl
// after they are set, unless they are set with [Cache.SetWithTTL]. It panics if
// ttl is not positive.
//
// Using cache directly as well is fine: its entries are the ones the Cache
// holds, and its methods do not look at the time. They return entries that
// have expired, which those of the Cache do not.
func Wrap[K comparable, V any](cache evict.Cache[K, Entry[V]], ttl time.Duration) *Cache[K, V] {
	if ttl <= 0 {
		panic("expire: Wrap: ttl must be positive")
	}
	return wrap(cache, ttl, system)
}

func wrap[K comparable, V any](cache evict.Cache[K, Entry[V]], ttl time.Duration, clock *clock) *Cache[K, V] {
	clock.begin()
	many, _ := cache.(manyGetter[K, Entry[V]])
	return &Cache[K, V]{cache: cache, many: many, clock: clock, ttl: ttl}
}

// expired reports whether an entry that expires at deadline has. Neither the
// deadline nor the copy of the time is ever below zero, so the difference of
// the two cannot overflow.
func (c *Cache[K, V]) expired(deadline int64) bool {
	left := deadline - atomic.LoadInt64(&c.clock.now)
	return left <= int64(near) && c.clock.due(left, deadline)
}

// Get returns the value stored under key, unless it has expired. Like the Get
// of the cache, it records the access with the eviction policy, also if the
// entry has expired.
func (c *Cache[K, V]) Get(key K) (value V, ok bool) {
	e, ok := c.cache.Get(key)
	if !ok || c.expired(e.deadline) {
		return value, false
	}
	return e.value, true
}

// GetMany does what Get does for each of keys, and stores the value it finds
// for keys[i] in values[i] and whether it found one in found[i]. It panics if
// values or found is shorter than keys. values and found may each be keys
// itself, but must not otherwise overlap it.
//
// If the cache has a GetMany of its own, as sieve, s3fifo and tinylfu do,
// GetMany uses it: for a cache that is too large for the CPU's caches it is
// faster than Get for each key. Otherwise it calls Get for each key.
func (c *Cache[K, V]) GetMany(keys []K, values []V, found []bool) {
	if len(values) < len(keys) || len(found) < len(keys) {
		panic("expire: GetMany: values and found must be at least as long as keys")
	}
	if c.many == nil {
		for i, key := range keys {
			values[i], found[i] = c.Get(key)
		}
		return
	}

	b, _ := c.batches.Get().(*batch[V])
	if b == nil {
		b = new(batch[V])
	}
	for len(keys) > 0 {
		n := min(len(keys), len(b))
		entries := b[:n]
		c.many.GetMany(keys[:n], entries, found[:n])
		for i := range entries {
			if found[i] && !c.expired(entries[i].deadline) {
				values[i] = entries[i].value
			} else {
				var zero V
				values[i], found[i] = zero, false
			}
		}
		// What the batch holds must not be kept alive by the pool.
		clear(entries)
		keys, values, found = keys[n:], values[n:], found[n:]
	}
	c.batches.Put(b)
}

// Peek returns the value stored under key, unless it has expired, without
// telling the eviction policy about it.
func (c *Cache[K, V]) Peek(key K) (value V, ok bool) {
	e, ok := c.cache.Peek(key)
	if !ok || c.expired(e.deadline) {
		return value, false
	}
	return e.value, true
}

// Set stores value under key, to expire after the time to live that the Cache
// was made with, and reports the entry that left the cache as a result, if
// there is one: the previous entry for key, or one that was evicted to make
// room, whether it had expired or not. See [evict.Cache].
func (c *Cache[K, V]) Set(key K, value V) (oldKey K, oldValue V, displaced bool) {
	oldKey, old, displaced := c.cache.Set(key, Entry[V]{value: value, deadline: c.clock.deadline(c.ttl)})
	return oldKey, old.value, displaced
}

// SetWithTTL is Set with a time to live of its own for the entry.
//
// A time to live of zero or less has run out before the entry is stored. If key
// has an entry when SetWithTTL looks, the new one takes its place all the same,
// and is never found. If not, SetWithTTL stores nothing and hands the new entry
// straight back, as a cache without capacity does, rather than evict another
// entry for one that would never be found. An entry of key that leaves in
// between is stored as Set would store it.
func (c *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) (oldKey K, oldValue V, displaced bool) {
	if ttl <= 0 {
		if _, ok := c.cache.Peek(key); !ok {
			return key, value, true
		}
	}
	oldKey, old, displaced := c.cache.Set(key, Entry[V]{value: value, deadline: c.clock.deadline(ttl)})
	return oldKey, old.value, displaced
}

// Delete removes key and returns the value it had, unless that had expired.
// An expired entry is removed all the same; delete it from the wrapped cache
// to have its value back.
func (c *Cache[K, V]) Delete(key K) (value V, ok bool) {
	e, ok := c.cache.Delete(key)
	if !ok || c.expired(e.deadline) {
		return value, false
	}
	return e.value, true
}

// Len returns the number of entries in the cache, those that have expired
// included.
func (c *Cache[K, V]) Len() int { return c.cache.Len() }

// Cap returns the number of entries the cache can hold.
func (c *Cache[K, V]) Cap() int { return c.cache.Cap() }

// Clear removes every entry, without reporting any of them. To release what
// they hold, go over the All of the wrapped cache first, which yields the
// entries that have expired as well. See [evict.Cache].
func (c *Cache[K, V]) Clear() { c.cache.Clear() }

// All iterates over the entries that have not expired, as the All of the
// cache does over all of them.
func (c *Cache[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		for key, e := range c.cache.All() {
			if !c.expired(e.deadline) && !yield(key, e.value) {
				return
			}
		}
	}
}

var _ evict.Cache[int, int] = (*Cache[int, int])(nil)
