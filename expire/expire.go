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
// it missing, All passes it over, and SetIfAbsent stores a new entry in its
// place. It stays in the cache all the same, until it is evicted, replaced,
// deleted or purged, or the cache is cleared. Until then it takes up room and
// Len counts it. Call [Cache.PurgeExpired] to remove expired entries; the
// cache does not run a background sweep.
//
// Set and SetIfAbsent hand an expired entry back when they push it out or
// replace it, as they do any entry, and PurgeExpired hands those it removes to
// a function of yours, so that what they hold on to can be released. Delete
// and Clear do not. Where values have to be released, use the wrapped cache
// for those: its Delete and All return entries whether they have expired or
// not, and [Entry.Value] gives their values.
//
// A read that finds an expired entry leaves it there, for the Set that usually
// follows: that replaces the entry in place, and the policy goes on treating
// it as the entry it was, which it would not after a Delete or a purge. Reads
// count with the policy, expired or not, so an entry that is still asked for
// but never set again stays until it is deleted or purged. Expired or not,
// there are never more entries than the capacity of the cache.
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
// an expired entry as a miss. The stats.Cache has no SetWithTTL or
// PurgeExpired: call those of the expire.Cache, and the entries that
// SetWithTTL evicts go uncounted. Wrapped the other way around, a read of an
// expired entry counts as a hit.
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
	"hash/maphash"
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

	// stripes is how many locks the keys share for replacing expired
	// entries in SetIfAbsent.
	stripes = 64
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

	// locks have the SetIfAbsent calls that find an expired entry of the
	// same key take turns, and seed picks one for a key. They are apart
	// from the fields that readers look at, since they are written to.
	locks *[stripes]sync.Mutex
	seed  maphash.Seed
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
// have expired, which those of the Cache do not. The methods of a cache of your
// own must not call those of the Cache, though: SetIfAbsent holds a lock while
// it calls them.
func Wrap[K comparable, V any](cache evict.Cache[K, Entry[V]], ttl time.Duration) *Cache[K, V] {
	if ttl <= 0 {
		panic("expire: Wrap: ttl must be positive")
	}
	return wrap(cache, ttl, system)
}

func wrap[K comparable, V any](cache evict.Cache[K, Entry[V]], ttl time.Duration, clock *clock) *Cache[K, V] {
	clock.begin()
	many, _ := cache.(manyGetter[K, Entry[V]])
	return &Cache[K, V]{
		cache: cache, many: many, clock: clock, ttl: ttl,
		locks: new([stripes]sync.Mutex), seed: maphash.MakeSeed(),
	}
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

// SetIfAbsent stores value under key, to expire after the time to live that
// the Cache was made with, unless key has an entry that has not expired. If it
// has, SetIfAbsent changes nothing but records the access with the eviction
// policy, and returns the value of the entry with present true. Otherwise it
// returns value with present false, and reports the entry that left the cache
// for it, if there is one: one of another key, evicted to make room, or the
// expired entry of key, which the new one replaces as Set would. See
// [evict.Cache].
//
// Of the goroutines that call SetIfAbsent for one key at the same time, one
// stores its value and the others get it back, unless it leaves the cache or
// expires in between. Those that find an expired entry take turns replacing
// it, and look again first. The exception is a value stored just after one of
// them has looked: by a Set of key, or by a SetIfAbsent that found no entry
// because the expired one had just left the cache, which PurgeExpired makes
// more likely. That value is replaced and handed back, with oldKey == key, so
// two goroutines may each be told that they stored theirs.
func (c *Cache[K, V]) SetIfAbsent(key K, value V) (actual V, present bool, oldKey K, oldValue V, evicted bool) {
	e := Entry[V]{value: value, deadline: c.clock.deadline(c.ttl)}
	a, present, oldKey, old, evicted := c.cache.SetIfAbsent(key, e)
	if present && c.expired(a.deadline) {
		return c.replace(key, e)
	}
	return a.value, present, oldKey, old.value, evicted
}

// replace stores e under key for SetIfAbsent, which found an expired entry
// there, unless another goroutine has stored one that has not expired since.
// It looks again under a lock that goroutines doing the same for key share,
// with Peek, since SetIfAbsent has told the policy about the access already.
func (c *Cache[K, V]) replace(key K, e Entry[V]) (actual V, present bool, oldKey K, oldValue V, evicted bool) {
	mu := &c.locks[maphash.Comparable(c.seed, key)%stripes]
	mu.Lock()
	defer mu.Unlock()

	a, ok := c.cache.Peek(key)
	if !ok {
		// The expired entry has left, so e goes in as if there had been
		// none, unless an entry has come in its place.
		b, found, k, old, out := c.cache.SetIfAbsent(key, e)
		if !found {
			return e.value, false, k, old.value, out
		}
		a = b
	}
	if !c.expired(a.deadline) {
		var none K
		var zero V
		return a.value, true, none, zero, false
	}
	// It has expired, as one that came in may have too: SetWithTTL stores
	// such entries.
	k, old, out := c.cache.Set(key, e)
	return e.value, false, k, old.value, out
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

// DeleteFunc calls del only for entries that have not expired, removing those
// for which it returns true, and reports how many it removed. Expired entries
// are left for [Cache.PurgeExpired]. del runs with a shard of the wrapped
// cache locked; see [evict.Cache].
func (c *Cache[K, V]) DeleteFunc(del func(K, V) bool) int {
	return c.cache.DeleteFunc(func(k K, e Entry[V]) bool {
		return !c.expired(e.deadline) && del(k, e.value)
	})
}

// PurgeExpired removes entries whose deadlines are at or before the time the
// call began and returns the number removed. It checks the current entry as
// it visits each shard: a value refreshed past the cutoff is not removed.
// Entries that expire during the scan are left for the next call. A key that
// is set again after its entry was purged comes in as a new entry, as after
// Delete.
//
// Unless removed is nil, PurgeExpired calls it with the key and the value of
// each entry it removes, so that what they hold on to can be released. It
// runs with a shard of the wrapped cache locked, as the del of DeleteFunc
// does: it must not call back into the cache, and should return promptly;
// collect what takes longer and deal with it after PurgeExpired returns. If
// it panics, the entry it was called with stays.
func (c *Cache[K, V]) PurgeExpired(removed func(K, V)) int {
	cutoff := c.clock.read()
	return c.cache.DeleteFunc(func(k K, e Entry[V]) bool {
		if e.deadline > cutoff {
			return false
		}
		if removed != nil {
			removed(k, e.value)
		}
		return true
	})
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
