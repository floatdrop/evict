package expire_test

import (
	"maps"
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/evict"
	"github.com/floatdrop/evict/expire"
	"github.com/floatdrop/evict/internal/cachetest"
	"github.com/floatdrop/evict/lru"
	"github.com/floatdrop/evict/s3fifo"
	"github.com/floatdrop/evict/sieve"
	"github.com/floatdrop/evict/stats"
	"github.com/floatdrop/evict/tinylfu"
)

// A wrapped cache must still be a cache, as long as nothing expires. One with
// a lock and one without get the suite.
var (
	lruFactories = cachetest.Factories{
		Int: func(capacity, shards int) evict.Cache[int, int] {
			return expire.Wrap(lru.New[int, expire.Entry[int]](capacity, lru.WithShards(shards)), time.Hour)
		},
		String: func(capacity, shards int) evict.Cache[string, string] {
			return expire.Wrap(lru.New[string, expire.Entry[string]](capacity, lru.WithShards(shards)), time.Hour)
		},
		Float: func(capacity, shards int) evict.Cache[float64, int] {
			return expire.Wrap(lru.New[float64, expire.Entry[int]](capacity, lru.WithShards(shards)), time.Hour)
		},
		Blob: func(capacity, shards int) evict.Cache[int, *cachetest.Blob] {
			return expire.Wrap(lru.New[int, expire.Entry[*cachetest.Blob]](capacity, lru.WithShards(shards)), time.Hour)
		},
		BlobKey: func(capacity, shards int) evict.Cache[*cachetest.Blob, int] {
			return expire.Wrap(lru.New[*cachetest.Blob, expire.Entry[int]](capacity, lru.WithShards(shards)), time.Hour)
		},
		Wide: func(capacity, shards int) evict.Cache[string, cachetest.Wide] {
			return expire.Wrap(lru.New[string, expire.Entry[cachetest.Wide]](capacity, lru.WithShards(shards)), time.Hour)
		},
	}

	s3fifoFactories = cachetest.Factories{
		Int: func(capacity, shards int) evict.Cache[int, int] {
			return expire.Wrap(s3fifo.New[int, expire.Entry[int]](capacity, s3fifo.WithShards(shards)), time.Hour)
		},
		String: func(capacity, shards int) evict.Cache[string, string] {
			return expire.Wrap(s3fifo.New[string, expire.Entry[string]](capacity, s3fifo.WithShards(shards)), time.Hour)
		},
		Float: func(capacity, shards int) evict.Cache[float64, int] {
			return expire.Wrap(s3fifo.New[float64, expire.Entry[int]](capacity, s3fifo.WithShards(shards)), time.Hour)
		},
		Blob: func(capacity, shards int) evict.Cache[int, *cachetest.Blob] {
			return expire.Wrap(s3fifo.New[int, expire.Entry[*cachetest.Blob]](capacity, s3fifo.WithShards(shards)), time.Hour)
		},
		BlobKey: func(capacity, shards int) evict.Cache[*cachetest.Blob, int] {
			return expire.Wrap(s3fifo.New[*cachetest.Blob, expire.Entry[int]](capacity, s3fifo.WithShards(shards)), time.Hour)
		},
		Wide: func(capacity, shards int) evict.Cache[string, cachetest.Wide] {
			return expire.Wrap(s3fifo.New[string, expire.Entry[cachetest.Wide]](capacity, s3fifo.WithShards(shards)), time.Hour)
		},
		Ghosts: 0.9,
	}
)

func TestCache(t *testing.T) {
	t.Parallel()

	t.Run("lru", func(t *testing.T) { t.Parallel(); cachetest.Run(t, lruFactories) })
	t.Run("s3fifo", func(t *testing.T) { t.Parallel(); cachetest.Run(t, s3fifoFactories) })
}

// TestNoAllocs is not parallel: counting allocations needs the process to
// itself.
func TestNoAllocs(t *testing.T) {
	warmClock(t)
	t.Run("lru", func(t *testing.T) { cachetest.NoAllocs(t, lruFactories) })
	t.Run("s3fifo", func(t *testing.T) { cachetest.NoAllocs(t, s3fifoFactories) })

	// GetMany takes its batch from a pool, which the collector empties now
	// and then, and which a goroutine that has moved to another CPU may
	// find empty. Once in a while is fine; once a call is not.
	t.Run("GetMany", func(t *testing.T) {
		c := expire.Wrap(s3fifo.New[int, expire.Entry[int]](1000), time.Hour)
		keys := make([]int, 100)
		for i := range keys {
			keys[i] = i
			c.Set(i, i)
		}
		values, found := make([]int, len(keys)), make([]bool, len(keys))
		if allocs := testing.AllocsPerRun(100, func() { c.GetMany(keys, values, found) }); allocs != 0 {
			t.Errorf("%v allocations per GetMany, want 0", allocs)
		}
	})
}

// warmClock starts the clock that Wrap uses, if no test has yet, and waits for
// it to tick twice. Then it keeps every CPU busy for a moment.
//
// Each tick runs a goroutine, and the runtime may start a thread for it, which
// allocates. Threads are never stopped, so once there are enough of them to
// keep every CPU busy, the ticks need no new ones, and a test of the caches
// counts no allocations of theirs.
func warmClock(t *testing.T) {
	t.Helper()

	expire.Wrap(lru.New[int, expire.Entry[int]](1), time.Hour)
	start := time.Now()
	for range 2 {
		before := expire.SystemNow()
		for expire.SystemNow() == before {
			if time.Since(start) > 5*time.Second {
				t.Fatal("the clock that Wrap uses has not ticked for seconds")
			}
			time.Sleep(time.Millisecond)
		}
	}

	var wg sync.WaitGroup
	for range runtime.GOMAXPROCS(0) {
		wg.Go(func() {
			spin := time.Now()
			for time.Since(spin) < 20*time.Millisecond {
				runtime.Gosched()
			}
		})
	}
	wg.Wait()
}

// policies make a cache of each policy, of 100 entries in 4 shards.
var policies = map[string]func() evict.Cache[string, expire.Entry[int]]{
	"lru": func() evict.Cache[string, expire.Entry[int]] {
		return lru.New[string, expire.Entry[int]](100, lru.WithShards(4))
	},
	"sieve": func() evict.Cache[string, expire.Entry[int]] {
		return sieve.New[string, expire.Entry[int]](100, sieve.WithShards(4))
	},
	"s3fifo": func() evict.Cache[string, expire.Entry[int]] {
		return s3fifo.New[string, expire.Entry[int]](100, s3fifo.WithShards(4))
	},
	"tinylfu": func() evict.Cache[string, expire.Entry[int]] {
		return tinylfu.New[string, expire.Entry[int]](100, tinylfu.WithShards(4))
	},
}

// found reports whether key is found, and checks that every way of reading
// agrees: Get, Peek, GetMany with a batch large enough to be looked up as one,
// and All. A key that is found must have the value want.
func found(t *testing.T, c *expire.Cache[string, int], key string, want int) bool {
	t.Helper()

	v, ok := c.Get(key)
	if pv, pok := c.Peek(key); pv != v || pok != ok {
		t.Fatalf("Get(%q) = %d, %t but Peek gives %d, %t", key, v, ok, pv, pok)
	}
	keys := slices.Repeat([]string{key}, 8)
	values, founds := make([]int, len(keys)), make([]bool, len(keys))
	c.GetMany(keys, values, founds)
	for i := range keys {
		if values[i] != v || founds[i] != ok {
			t.Fatalf("Get(%q) = %d, %t but GetMany gives %d, %t", key, v, ok, values[i], founds[i])
		}
	}
	all := false
	for k, av := range c.All() {
		if k == key {
			all = true
			if av != v {
				t.Fatalf("Get(%q) = %d but All yields %d", key, v, av)
			}
		}
	}
	if all != ok {
		t.Fatalf("Get(%q) finds it: %t, but All yields it: %t", key, ok, all)
	}
	if ok && v != want {
		t.Fatalf("Get(%q) = %d, want %d", key, v, want)
	}
	return ok
}

// An entry is found until its time is up, and from then on not at all, by any
// policy, whatever way it is looked for.
func TestExpiry(t *testing.T) {
	t.Parallel()

	for name, newCache := range policies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			clock := expire.NewClock()
			c := expire.WrapWith(newCache(), time.Minute, clock)
			c.Set("a", 1)
			c.SetWithTTL("b", 2, time.Second)
			c.SetWithTTL("c", 3, 2*time.Second)

			clock.Advance(time.Second - 1)
			if !found(t, c, "a", 1) || !found(t, c, "b", 2) || !found(t, c, "c", 3) {
				t.Fatal("an entry is gone before its time")
			}

			clock.Advance(1)
			if found(t, c, "b", 2) {
				t.Error("b is found when its second is up")
			}
			if !found(t, c, "a", 1) || !found(t, c, "c", 3) {
				t.Fatal("an entry is gone before its time")
			}
			// It is still there, and takes up room.
			if n := c.Len(); n != 3 {
				t.Errorf("Len() = %d, want 3 with b expired", n)
			}

			// Deleting it says it was not there, and removes it.
			if v, ok := c.Delete("b"); ok || v != 0 {
				t.Errorf("Delete(b) = %d, %t after it expired, want 0, false", v, ok)
			}
			if n := c.Len(); n != 2 {
				t.Errorf("Len() = %d after b was deleted, want 2", n)
			}

			// Setting it again starts it over.
			c.SetWithTTL("b", 20, time.Second)
			clock.Advance(time.Second / 2)
			if !found(t, c, "b", 20) {
				t.Error("b is not found after it was set again")
			}
			if v, ok := c.Delete("b"); !ok || v != 20 {
				t.Errorf("Delete(b) = %d, %t, want 20, true", v, ok)
			}

			clock.Advance(time.Minute)
			if found(t, c, "a", 1) || found(t, c, "c", 3) {
				t.Error("an entry is found when its time is up")
			}
		})
	}
}

func TestPurgeExpired(t *testing.T) {
	t.Parallel()

	for name, newCache := range policies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			clock := expire.NewClock()
			inner := stats.Wrap(newCache())
			c := expire.WrapWith(inner, time.Second, clock)
			c.Set("due", 1)
			c.SetWithTTL("later", 2, time.Second+1)
			inner.Set("zero", expire.Entry[int]{}) // A zero Entry is already expired.
			clock.Advance(time.Second)
			clock.Lag(time.Second) // Purging must use the clock, not the readers' copy.

			removed := map[string]int{}
			n := c.PurgeExpired(func(k string, v int) {
				if _, ok := removed[k]; ok {
					t.Errorf("PurgeExpired handed over %q twice", k)
				}
				removed[k] = v
			})
			if n != 2 {
				t.Fatalf("PurgeExpired = %d, want 2", n)
			}
			if want := map[string]int{"due": 1, "zero": 0}; !maps.Equal(removed, want) {
				t.Errorf("PurgeExpired handed over %v, want %v", removed, want)
			}
			if s := inner.Stats(); s.Len != 1 || s.Evictions != 0 || s.Hits != 0 || s.Misses != 0 {
				t.Errorf("stats after purge = %+v, want one entry and no hits, misses or evictions", s)
			}
			if v, ok := c.Peek("later"); !ok || v != 2 {
				t.Errorf("Peek(later) = %d, %t, want 2, true", v, ok)
			}
			if n := c.PurgeExpired(nil); n != 0 {
				t.Errorf("repeat PurgeExpired = %d, want 0", n)
			}
		})
	}
}

func TestDeleteFuncSkipsExpired(t *testing.T) {
	t.Parallel()

	clock := expire.NewClock()
	inner := lru.New[string, expire.Entry[int]](2)
	c := stats.Wrap(expire.WrapWith(inner, time.Second, clock))
	c.Set("expired", 1)
	clock.Advance(time.Second)
	c.Set("live", 2)
	n := c.DeleteFunc(func(k string, v int) bool {
		if k != "live" || v != 2 {
			t.Errorf("DeleteFunc visited %s=%d, want live=2", k, v)
		}
		return true
	})
	if n != 1 {
		t.Errorf("DeleteFunc = %d, want 1", n)
	}
	if e, ok := inner.Peek("expired"); !ok || e.Value() != 1 || inner.Len() != 1 {
		t.Error("DeleteFunc did not leave just the expired entry")
	}
}

// An expired entry that leaves the cache is handed back by Set, as any entry
// is, whether it is replaced or evicted.
func TestExpiredLeave(t *testing.T) {
	t.Parallel()

	clock := expire.NewClock()
	c := expire.WrapWith(lru.New[string, expire.Entry[int]](1), time.Second, clock)

	c.Set("a", 1)
	clock.Advance(time.Second)
	if k, v, displaced := c.Set("a", 2); !displaced || k != "a" || v != 1 {
		t.Errorf("Set(a) over an expired a = %q, %d, %t; want a, 1, true", k, v, displaced)
	}
	if !found(t, c, "a", 2) {
		t.Error("a is not found after it was replaced")
	}

	clock.Advance(time.Second)
	if k, v, displaced := c.Set("b", 3); !displaced || k != "a" || v != 2 {
		t.Errorf("Set(b) in place of an expired a = %q, %d, %t; want a, 2, true", k, v, displaced)
	}
}

// A time to live that is zero or less has run out before the entry is stored,
// and one that is too long for the clock never does.
func TestTTLBounds(t *testing.T) {
	t.Parallel()

	clock := expire.NewClock()
	c := expire.WrapWith(lru.New[string, expire.Entry[int]](10), time.Second, clock)
	clock.Advance(time.Hour) // So that a negative time to live has room to go below zero.

	// For a key with an entry, expired or not, the new entry takes its
	// place. Each round replaces the one that the round before stored, which
	// had expired before it was stored.
	c.Set("a", 1)
	prev := 1
	for i, ttl := range []time.Duration{0, -1, -time.Hour, math.MinInt64} {
		if k, v, displaced := c.SetWithTTL("a", 10+i, ttl); !displaced || k != "a" || v != prev {
			t.Errorf("SetWithTTL(a, %d, %v) = %q, %d, %t; want a, %d, true", 10+i, ttl, k, v, displaced, prev)
		}
		if found(t, c, "a", 10+i) {
			t.Errorf("a is found after it was set with a time to live of %v", ttl)
		}
		prev = 10 + i
	}

	// For a key without an entry, nothing is stored, and the new entry comes
	// straight back. Nothing is evicted for it either.
	one := expire.WrapWith(lru.New[string, expire.Entry[int]](1), time.Second, clock)
	one.Set("live", 1)
	if k, v, displaced := one.SetWithTTL("dead", 2, 0); !displaced || k != "dead" || v != 2 {
		t.Errorf("SetWithTTL(dead, 2, 0) = %q, %d, %t; want dead, 2, true", k, v, displaced)
	}
	if !found(t, one, "live", 1) || one.Len() != 1 {
		t.Error("SetWithTTL with a time to live of 0 evicted an entry of another key")
	}

	c.SetWithTTL("b", 2, math.MaxInt64)
	c.SetWithTTL("c", 3, math.MaxInt64-2*time.Hour)
	if d := expire.DeadlineOf(c, "b"); d != math.MaxInt64 {
		t.Errorf("a time to live of MaxInt64 gives a deadline of %d, want MaxInt64", d)
	}
	clock.Advance(100 * 365 * 24 * time.Hour)
	if !found(t, c, "b", 2) || !found(t, c, "c", 3) {
		t.Error("an entry with a time to live too long for the clock expired")
	}

	for _, ttl := range []time.Duration{0, -1} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Wrap with a time to live of %v did not panic", ttl)
				}
			}()
			expire.Wrap(lru.New[string, expire.Entry[int]](10), ttl)
		}()
	}
}

// hooked is a cache that runs a function after each Peek, to let an entry come
// or go between SetWithTTL's look and its Set. It can also interpose after a
// SetIfAbsent or before a DeleteFunc scan.
type hooked struct {
	evict.Cache[string, expire.Entry[int]]
	after, afterAbsent, beforeScan func()
}

func (h *hooked) Peek(key string) (expire.Entry[int], bool) {
	e, ok := h.Cache.Peek(key)
	if h.after != nil {
		h.after()
	}
	return e, ok
}

func (h *hooked) SetIfAbsent(key string, e expire.Entry[int]) (actual expire.Entry[int], present bool, oldKey string, oldValue expire.Entry[int], evicted bool) {
	actual, present, oldKey, oldValue, evicted = h.Cache.SetIfAbsent(key, e)
	if h.afterAbsent != nil {
		h.afterAbsent()
	}
	return actual, present, oldKey, oldValue, evicted
}

func (h *hooked) DeleteFunc(del func(string, expire.Entry[int]) bool) int {
	if h.beforeScan != nil {
		h.beforeScan()
	}
	return h.Cache.DeleteFunc(del)
}

// A refresh between taking the cutoff and scanning a shard must survive.
// A deadline that passes in that interval belongs to the next purge.
func TestPurgeExpiredCutoff(t *testing.T) {
	t.Parallel()

	clock := expire.NewClock()
	h := &hooked{Cache: lru.New[string, expire.Entry[int]](3)}
	c := expire.WrapWith(h, time.Second, clock)
	c.Set("expired", 1)
	c.Set("refresh", 2)
	c.SetWithTTL("later", 3, 2*time.Second)
	clock.Advance(time.Second)
	h.beforeScan = func() {
		clock.Advance(time.Second)
		c.Set("refresh", 4)
	}
	if n := c.PurgeExpired(nil); n != 1 {
		t.Fatalf("PurgeExpired = %d, want 1", n)
	}
	if v, ok := c.Peek("refresh"); !ok || v != 4 {
		t.Error("purge removed the refreshed value")
	}
	if _, ok := h.Peek("later"); !ok {
		t.Error("purge removed an entry that expired during the scan")
	}
	h.beforeScan = nil
	if n := c.PurgeExpired(nil); n != 1 {
		t.Errorf("next PurgeExpired = %d, want 1", n)
	}
}

// A panic in the function that PurgeExpired hands entries to stops the purge,
// and the entry it was handed stays.
func TestPurgeExpiredPanics(t *testing.T) {
	t.Parallel()

	clock := expire.NewClock()
	inner := lru.New[string, expire.Entry[int]](2, lru.WithShards(1))
	c := expire.WrapWith(inner, time.Second, clock)
	c.Set("a", 1)
	c.Set("b", 2)
	clock.Advance(time.Second)
	const stop = "stop the purge"
	func() {
		defer func() {
			if p := recover(); p != stop {
				t.Errorf("PurgeExpired panic = %v, want %q", p, stop)
			}
		}()
		c.PurgeExpired(func(string, int) { panic(stop) })
	}()
	if inner.Len() != 2 {
		t.Errorf("Len() = %d after a panic in the first call, want 2", inner.Len())
	}
	if n := c.PurgeExpired(nil); n != 2 {
		t.Errorf("PurgeExpired after a panic = %d, want 2", n)
	}
}

// SetWithTTL with a time to live of zero or less decides by what it sees when
// it looks. An entry of key that leaves in between is stored as Set would
// store it: with eviction, if that is what it takes. One that comes in between
// is handed back, as if it had come after.
func TestSetWithTTLRace(t *testing.T) {
	t.Parallel()

	clock := expire.NewClock()
	h := &hooked{Cache: lru.New[string, expire.Entry[int]](2)}
	c := expire.WrapWith(h, time.Hour, clock)

	// x leaves and another key takes its place, so the cache is full.
	c.Set("x", 1)
	c.Set("live", 2)
	h.after = func() {
		h.after = nil
		c.Delete("x")
		c.Set("other", 3)
	}
	if k, v, displaced := c.SetWithTTL("x", 9, 0); !displaced || k != "live" || v != 2 {
		t.Errorf("SetWithTTL(x, 9, 0) after x left = %q, %d, %t; want live to be evicted", k, v, displaced)
	}
	if found(t, c, "x", 9) {
		t.Error("x is found after it was set with a time to live of 0")
	}

	// y comes in: the new entry is handed back, and y's stays.
	h.after = func() {
		h.after = nil
		c.Set("y", 4)
	}
	if k, v, displaced := c.SetWithTTL("y", 8, 0); !displaced || k != "y" || v != 8 {
		t.Errorf("SetWithTTL(y, 8, 0) as y came in = %q, %d, %t; want y, 8, true", k, v, displaced)
	}
	if !found(t, c, "y", 4) {
		t.Error("y is gone after SetWithTTL found no entry of it")
	}
}

// SetIfAbsent takes an expired entry for no entry, by any policy: it stores the
// new one in its place, with a time to live of its own, and hands the expired
// one back.
func TestSetIfAbsentExpired(t *testing.T) {
	t.Parallel()

	type result struct {
		actual    int
		present   bool
		oldKey    string
		oldValue  int
		displaced bool
	}
	setIfAbsent := func(c *expire.Cache[string, int], key string, value int) result {
		var r result
		r.actual, r.present, r.oldKey, r.oldValue, r.displaced = c.SetIfAbsent(key, value)
		return r
	}

	for name, newCache := range policies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			clock := expire.NewClock()
			c := expire.WrapWith(newCache(), time.Second, clock)

			c.Set("a", 1)
			clock.Advance(time.Second - 1)
			if r, want := setIfAbsent(c, "a", 2), (result{1, true, "", 0, false}); r != want {
				t.Errorf("SetIfAbsent(a, 2) before a expired = %+v, want %+v", r, want)
			}
			clock.Advance(1)
			if r, want := setIfAbsent(c, "a", 3), (result{3, false, "a", 1, true}); r != want {
				t.Errorf("SetIfAbsent(a, 3) after a expired = %+v, want %+v", r, want)
			}
			if !found(t, c, "a", 3) {
				t.Fatal("a is not found after SetIfAbsent replaced it")
			}

			clock.Advance(time.Second - 1)
			if r, want := setIfAbsent(c, "a", 4), (result{3, true, "", 0, false}); r != want {
				t.Errorf("SetIfAbsent(a, 4) before the new a expired = %+v, want %+v", r, want)
			}
			clock.Advance(1)
			if found(t, c, "a", 3) {
				t.Error("a is found when the second of the new entry is up")
			}

			// One that SetWithTTL stored expired is no entry either.
			c.SetWithTTL("a", 5, 0)
			if r, want := setIfAbsent(c, "a", 6), (result{6, false, "a", 5, true}); r != want {
				t.Errorf("SetIfAbsent(a, 6) over an entry set to expire at once = %+v, want %+v", r, want)
			}
			if n := c.Len(); n != 1 {
				t.Errorf("Len() = %d, want 1", n)
			}
		})
	}
}

// SetIfAbsent that finds an expired entry looks again before it replaces it.
// An entry that has not expired, stored in between, is found. If the expired
// one has left, before the second look or after, the new one is stored as if
// there had been none, with eviction if that is what it takes, and an entry
// that comes instead is found, or replaced if it has expired too. A Set just
// after the second look is replaced and handed back, as the documentation
// says.
func TestSetIfAbsentRace(t *testing.T) {
	t.Parallel()

	clock := expire.NewClock()
	h := &hooked{Cache: lru.New[string, expire.Entry[int]](2)}
	c := expire.WrapWith(h, time.Second, clock)

	// Another SetIfAbsent replaces it first.
	c.Set("a", 1)
	clock.Advance(time.Second)
	h.afterAbsent = func() {
		h.afterAbsent = nil
		if v, present, _, _, _ := c.SetIfAbsent("a", 2); v != 2 || present {
			t.Errorf("the other SetIfAbsent(a, 2) = %d, %t; want 2, false", v, present)
		}
	}
	if v, present, k, old, evicted := c.SetIfAbsent("a", 3); v != 2 || !present || k != "" || old != 0 || evicted {
		t.Errorf("SetIfAbsent(a, 3) = %d, %t, %q, %d, %t; want 2, true, \"\", 0, false", v, present, k, old, evicted)
	}

	// The expired entry leaves, and another key takes its place, so the
	// cache is full.
	c.Set("live", 4)
	clock.Advance(time.Second)
	c.Set("live", 4)
	h.afterAbsent = func() {
		h.afterAbsent = nil
		c.Delete("a")
		c.Set("other", 5)
	}
	if v, present, k, old, evicted := c.SetIfAbsent("a", 6); v != 6 || present || k != "live" || old != 4 || !evicted {
		t.Errorf("SetIfAbsent(a, 6) as a left = %d, %t, %q, %d, %t; want 6, false, live, 4, true", v, present, k, old, evicted)
	}
	if !found(t, c, "a", 6) || !found(t, c, "other", 5) {
		t.Fatal("a or other is gone")
	}

	// The expired entry leaves, and one comes instead that expires too.
	clock.Advance(time.Second)
	h.afterAbsent = func() {
		h.afterAbsent = nil
		c.Delete("a")
		h.after = func() {
			h.after = nil
			c.Set("a", 7)
			clock.Advance(time.Second)
		}
	}
	if v, present, k, old, evicted := c.SetIfAbsent("a", 8); v != 8 || present || k != "a" || old != 7 || !evicted {
		t.Errorf("SetIfAbsent(a, 8) as an entry came and expired = %d, %t, %q, %d, %t; want 8, false, a, 7, true", v, present, k, old, evicted)
	}

	// A Set just after SetIfAbsent has looked again.
	clock.Advance(time.Second)
	h.after = func() {
		h.after = nil
		c.Set("a", 9)
	}
	if v, present, k, old, evicted := c.SetIfAbsent("a", 10); v != 10 || present || k != "a" || old != 9 || !evicted {
		t.Errorf("SetIfAbsent(a, 10) as a was set = %d, %t, %q, %d, %t; want 10, false, a, 9, true", v, present, k, old, evicted)
	}
	if !found(t, c, "a", 10) {
		t.Error("a does not have the value of SetIfAbsent")
	}

	// The expired entry leaves just after SetIfAbsent has looked again:
	// nothing leaves for the new one while there is room, and the least
	// recently used entry when there is not.
	clock.Advance(time.Second)
	h.after = func() {
		h.after = nil
		c.Delete("a")
	}
	if v, present, k, old, evicted := c.SetIfAbsent("a", 11); v != 11 || present || k != "" || old != 0 || evicted {
		t.Errorf("SetIfAbsent(a, 11) as a left = %d, %t, %q, %d, %t; want 11, false, \"\", 0, false", v, present, k, old, evicted)
	}
	clock.Advance(time.Second)
	h.after = func() {
		h.after = nil
		c.Delete("a")
		c.Set("live", 12)
	}
	if v, present, k, old, evicted := c.SetIfAbsent("a", 13); v != 13 || present || k != "other" || old != 5 || !evicted {
		t.Errorf("SetIfAbsent(a, 13) as a left a full cache = %d, %t, %q, %d, %t; want 13, false, other, 5, true", v, present, k, old, evicted)
	}

	// The expired entry leaves, and one that has not expired comes before
	// SetIfAbsent stores its own: that is found.
	clock.Advance(time.Second)
	h.afterAbsent = func() {
		h.afterAbsent = nil
		c.Delete("a")
		h.after = func() {
			h.after = nil
			c.Set("a", 14)
		}
	}
	if v, present, k, old, evicted := c.SetIfAbsent("a", 15); v != 14 || !present || k != "" || old != 0 || evicted {
		t.Errorf("SetIfAbsent(a, 15) as a came back = %d, %t, %q, %d, %t; want 14, true, \"\", 0, false", v, present, k, old, evicted)
	}
	if !found(t, c, "a", 14) {
		t.Error("a does not have the value that came in")
	}
}

// Of the goroutines that call SetIfAbsent for a key whose entry has expired,
// one replaces it and the others find what it stored. In each round the
// entries of several keys expire, and the goroutines go over them in the same
// order, so that they race for each. The keys are few enough for no shard to
// be full.
func TestSetIfAbsentExpiredOnce(t *testing.T) {
	t.Parallel()

	rounds := 200
	if testing.Short() {
		rounds = 20
	}
	const goroutines, keys = 8, 16
	names := make([]string, keys)
	for k := range names {
		names[k] = "k" + strconv.Itoa(k)
	}
	for name, newCache := range policies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			clock := expire.NewClock()
			c := expire.WrapWith(newCache(), time.Second, clock)
			for round := range rounds {
				for _, key := range names {
					c.Set(key, -1)
				}
				clock.Advance(time.Second)

				start := make(chan struct{})
				var stored [keys]atomic.Int32
				var actual [goroutines][keys]int
				var wg sync.WaitGroup
				for g := range goroutines {
					wg.Go(func() {
						<-start
						for k, key := range names {
							v, present, oldKey, old, evicted := c.SetIfAbsent(key, g)
							if !present {
								stored[k].Add(1)
								if v != g || oldKey != key || old != -1 || !evicted {
									t.Errorf("round %d: SetIfAbsent(%s, %d) stored it but returned %d, %q, %d, %t; want %d, %s, -1, true", round, key, g, v, oldKey, old, evicted, g, key)
								}
							}
							actual[g][k] = v
						}
					})
				}
				close(start)
				wg.Wait()

				for k, key := range names {
					if n := stored[k].Load(); n != 1 {
						t.Fatalf("round %d: %d goroutines stored their value under %s, want 1", round, n, key)
					}
					want, _ := c.Get(key)
					for g := range actual {
						if actual[g][k] != want {
							t.Fatalf("round %d: goroutine %d got %d for %s, but it has %d", round, g, actual[g][k], key, want)
						}
					}
				}
			}
		})
	}
}

// Readers look at a copy of the time that may lag behind. An entry that is due
// to expire within 50 ms of it is checked against the time itself, so a lag of
// up to 50 ms changes nothing. With more than that, an entry is found for up to
// as much longer.
func TestLag(t *testing.T) {
	t.Parallel()

	clock := expire.NewClock()
	c := expire.WrapWith(lru.New[string, expire.Entry[int]](10), time.Second, clock)
	c.SetWithTTL("a", 1, 100*time.Millisecond)
	c.SetWithTTL("b", 2, 150*time.Millisecond)

	// At 99 ms, nothing has expired, whatever the lag: the copy of the
	// time is never ahead of it.
	clock.Advance(99 * time.Millisecond)
	for _, lag := range []time.Duration{0, 10 * time.Millisecond, 49 * time.Millisecond, 99 * time.Millisecond} {
		clock.Lag(lag)
		if !found(t, c, "a", 1) {
			t.Errorf("a is gone 1 ms before its time, with the copy of the time %v behind", lag)
		}
	}

	// At 100 ms exactly, a has expired, also where the time itself has to
	// tell.
	clock.Advance(time.Millisecond)
	clock.Lag(10 * time.Millisecond)
	if found(t, c, "a", 1) {
		t.Error("a is found at its deadline, with the copy of the time 10 ms behind")
	}
	clock.Lag(0)
	if found(t, c, "a", 1) {
		t.Error("a is found at its deadline")
	}

	// At 120 ms, a has expired 20 ms ago. A copy of the time that lags by up
	// to 50 ms shows it, and one that lags by 70 ms still does: a is due 50
	// ms after that copy, which is near enough to be checked.
	clock.Advance(20 * time.Millisecond)
	for _, lag := range []time.Duration{0, 10 * time.Millisecond, 50 * time.Millisecond, 70 * time.Millisecond} {
		clock.Lag(lag)
		if found(t, c, "a", 1) {
			t.Errorf("a is found 20 ms after it expired, with the copy of the time %v behind", lag)
		}
		if !found(t, c, "b", 2) {
			t.Errorf("b is gone 30 ms before its time, with the copy of the time %v behind", lag)
		}
	}

	// One that lags by 80 ms does not: a is due 60 ms after it, and taken
	// to be there.
	clock.Lag(80 * time.Millisecond)
	if !found(t, c, "a", 1) {
		t.Error("a was checked against the time when it was due 60 ms after the copy of it; the test expects it was not")
	}
}

// GetMany has to find what Peek finds, among entries of which some expired,
// with batches of every size, whether the cache has a GetMany of its own or
// not.
func TestGetMany(t *testing.T) {
	t.Parallel()

	const capacity = 500
	for name, cache := range map[string]evict.Cache[int, expire.Entry[int]]{
		"lru":     lru.New[int, expire.Entry[int]](capacity),
		"s3fifo":  s3fifo.New[int, expire.Entry[int]](capacity),
		"tinylfu": tinylfu.New[int, expire.Entry[int]](capacity),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			clock := expire.NewClock()
			c := expire.WrapWith(cache, time.Hour, clock)
			for k := range capacity / 2 {
				// Every third entry expires in a second.
				ttl := time.Hour
				if k%3 == 0 {
					ttl = time.Second
				}
				c.SetWithTTL(k, -k, ttl)
			}
			clock.Advance(time.Second)

			rng := rand.New(rand.NewPCG(1, 2))
			for _, n := range []int{0, 1, 3, 4, 31, 32, 33, 64, 100} {
				keys := make([]int, n)
				for i := range keys {
					keys[i] = rng.IntN(capacity)
				}
				// The values and the found are filled with garbage, which
				// GetMany must overwrite.
				values, founds := make([]int, n), make([]bool, n)
				for i := range values {
					values[i], founds[i] = 1, true
				}
				c.GetMany(keys, values, founds)
				for i, k := range keys {
					v, ok := c.Peek(k)
					if values[i] != v || founds[i] != ok {
						t.Fatalf("GetMany of %d keys: key %d gave %d, %t; Peek gives %d, %t", n, k, values[i], founds[i], v, ok)
					}
					if want := k < capacity/2 && k%3 != 0; ok != want {
						t.Fatalf("key %d found: %t, want %t", k, ok, want)
					}
				}
			}

			// The values may be the keys themselves.
			keys := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
			founds := make([]bool, len(keys))
			c.GetMany(keys, keys, founds)
			want := []int{0, -1, -2, 0, -4, -5, 0, -7, -8, 0}
			if !slices.Equal(keys, want) {
				t.Errorf("GetMany into its own keys gives %v, want %v", keys, want)
			}

			for _, short := range [][2]int{{9, 10}, {10, 9}} {
				func() {
					defer func() {
						if recover() == nil {
							t.Errorf("GetMany with %d values and %d found for 10 keys did not panic", short[0], short[1])
						}
					}()
					c.GetMany(make([]int, 10), make([]int, short[0]), make([]bool, short[1]))
				}()
			}
		})
	}
}

// The clock that Wrap uses keeps time: an entry is found until its time to
// live is up, and stops being found soon after. How soon depends on how busy
// the machine is, so the test gives it a few seconds.
func TestSystemClock(t *testing.T) {
	t.Parallel()

	const ttl = 30 * time.Millisecond
	c := expire.Wrap(lru.New[string, expire.Entry[int]](10), time.Hour)

	start := time.Now()
	c.SetWithTTL("a", 1, ttl)
	for {
		// The copy of the time that readers look at is never ahead.
		if now, read := expire.SystemNow(), expire.SystemRead(); now > read {
			t.Fatalf("the copy of the time is %v, ahead of the time, %v", now, read)
		}
		if _, ok := c.Get("a"); !ok {
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatalf("a is still found after %v, with a time to live of %v", time.Since(start), ttl)
		}
		time.Sleep(time.Millisecond)
	}
	if elapsed := time.Since(start); elapsed < ttl {
		t.Errorf("a expired after %v, with a time to live of %v", elapsed, ttl)
	}

	// The timer keeps the copy up to date: it lags by half a tick of 10 ms
	// on average. A busy machine may hold the timer up now and then, so the
	// test looks at the median of many samples, and allows for four ticks.
	lags := make([]time.Duration, 200)
	for i := range lags {
		now := expire.SystemNow()
		lags[i] = expire.SystemRead() - now
		time.Sleep(time.Millisecond + time.Duration(i%7)*100*time.Microsecond)
	}
	slices.Sort(lags)
	if median := lags[len(lags)/2]; median > 40*time.Millisecond {
		t.Errorf("the copy of the time lags by %v in the median, want a few ms", median)
	}
}

// Goroutines that read and write while time passes must only ever find what
// was stored under a key, and nothing once the time to live of every entry is
// up.
func TestConcurrent(t *testing.T) {
	t.Parallel()

	for _, name := range []string{"lru", "s3fifo", "tinylfu"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			const (
				goroutines = 8
				requests   = 3000
				keys       = 200
			)
			clock := expire.NewClock()
			c := expire.WrapWith(policies[name](), time.Second, clock)

			var done atomic.Bool
			var wg sync.WaitGroup
			wg.Go(func() {
				for !done.Load() {
					clock.Advance(time.Millisecond)
					calls := 0
					n := c.PurgeExpired(func(k string, v int) {
						calls++
						if strconv.Itoa(v) != k {
							t.Errorf("PurgeExpired handed over %q=%d", k, v)
						}
					})
					if calls != n {
						t.Errorf("PurgeExpired removed %d entries and handed over %d", n, calls)
					}
					time.Sleep(time.Microsecond)
				}
			})
			var workers sync.WaitGroup
			for g := range goroutines {
				workers.Go(func() {
					rng := rand.New(rand.NewPCG(uint64(g), 3))
					batch, values, founds := make([]string, 8), make([]int, 8), make([]bool, 8)
					for range requests {
						k := rng.IntN(keys)
						key := strconv.Itoa(k)
						switch rng.IntN(4) {
						case 0:
							if rng.IntN(2) == 0 {
								c.SetWithTTL(key, k, time.Duration(rng.IntN(20))*time.Millisecond)
							} else if v, _, _, _, _ := c.SetIfAbsent(key, k); v != k {
								t.Errorf("SetIfAbsent(%q) = %d", key, v)
							}
						case 1:
							if v, ok := c.Delete(key); ok && v != k {
								t.Errorf("Delete(%q) = %d", key, v)
							}
						case 2:
							for i := range batch {
								batch[i] = strconv.Itoa(rng.IntN(keys))
							}
							c.GetMany(batch, values, founds)
							for i, key := range batch {
								if founds[i] && strconv.Itoa(values[i]) != key {
									t.Errorf("GetMany found %d for %q", values[i], key)
								}
							}
						default:
							if v, ok := c.Get(key); ok && v != k {
								t.Errorf("Get(%q) = %d", key, v)
							}
						}
					}
				})
			}
			workers.Wait()
			done.Store(true)
			wg.Wait()

			clock.Advance(time.Second)
			for k, v := range c.All() {
				t.Errorf("All yields %q=%d after every entry expired", k, v)
			}
			c.PurgeExpired(nil)
			if c.Len() != 0 {
				t.Errorf("Len() = %d after purging every entry", c.Len())
			}
		})
	}
}

// Wrapped in stats, an expired entry counts as a miss, through Get and
// GetMany alike.
func TestStats(t *testing.T) {
	t.Parallel()

	clock := expire.NewClock()
	c := stats.Wrap(expire.WrapWith(s3fifo.New[int, expire.Entry[int]](100), time.Second, clock))
	for k := range 10 {
		c.Set(k, k)
	}
	clock.Advance(time.Second)
	for k := 10; k < 20; k++ {
		c.Set(k, k)
	}

	c.Get(0)
	c.Get(10)
	keys := []int{1, 2, 3, 11, 12, 13, 14, 15}
	c.GetMany(keys, make([]int, len(keys)), make([]bool, len(keys)))

	if s := c.Stats(); s.Hits != 6 || s.Misses != 4 {
		t.Errorf("%d hits and %d misses, want 6 and 4", s.Hits, s.Misses)
	}

	// An expired entry that SetIfAbsent replaces is handed back, but was
	// not evicted, as one that Set replaces was not.
	if _, present, k, old, displaced := c.SetIfAbsent(5, 100); present || k != 5 || old != 5 || !displaced {
		t.Errorf("SetIfAbsent(5, 100) over an expired 5 = %t, %d, %d, %t; want false, 5, 5, true", present, k, old, displaced)
	}
	c.Set(6, 101)
	if s := c.Stats(); s.Evictions != 0 || s.Hits != 6 || s.Misses != 4 {
		t.Errorf("%d evictions, %d hits and %d misses, want 0, 6 and 4", s.Evictions, s.Hits, s.Misses)
	}
}

// Readers look at the copy of the time on every read. It has to have a cache
// line to itself, or whatever shares the line would take it away from the
// readers' CPUs when it is written to. The clock is not aligned to a line, so
// that takes 128 bytes on either side.
func TestClockLayout(t *testing.T) {
	t.Parallel()

	if expire.NowOffset < 128 || expire.FakeOffset-expire.NowOffset < 128 {
		t.Errorf("the copy of the time is at %d, and the next field at %d; want a line of 128 bytes to itself",
			expire.NowOffset, expire.FakeOffset)
	}
}

// batcher is a cache with a GetMany of its own, which says how often it was
// called.
type batcher struct {
	evict.Cache[int, expire.Entry[int]]
	calls int
}

func (b *batcher) GetMany(keys []int, values []expire.Entry[int], found []bool) {
	b.calls++
	for i, k := range keys {
		values[i], found[i] = b.Get(k)
	}
}

// GetMany has to go to the GetMany of the cache, in batches as large as those
// of the caches of this module.
func TestGetManyForwards(t *testing.T) {
	t.Parallel()

	b := &batcher{Cache: lru.New[int, expire.Entry[int]](200)}
	c := expire.WrapWith(b, time.Hour, expire.NewClock())
	keys := make([]int, 100)
	for i := range keys {
		keys[i] = i
		c.Set(i, -i)
	}
	values, founds := make([]int, len(keys)), make([]bool, len(keys))
	c.GetMany(keys, values, founds)
	if b.calls != 4 {
		t.Errorf("GetMany of 100 keys called that of the cache %d times, want 4", b.calls)
	}
	for i, k := range keys {
		if !founds[i] || values[i] != -k {
			t.Fatalf("GetMany found %d, %t for %d", values[i], founds[i], k)
		}
	}
}

// What GetMany looks up goes through a batch that it keeps for the next call.
// The batch must not keep the values alive until then.
func TestGetManyLetsGo(t *testing.T) {
	t.Parallel()

	c := expire.WrapWith(s3fifo.New[int, expire.Entry[*cachetest.Blob]](100), time.Hour, expire.NewClock())
	keys := make([]int, 40)
	for i := range keys {
		keys[i] = i
		c.Set(i, new(cachetest.Blob))
	}
	values, founds := make([]*cachetest.Blob, len(keys)), make([]bool, len(keys))
	for range 10 {
		c.GetMany(keys, values, founds)
		clean, got := expire.PooledBatch(c)
		if !got {
			continue // Taken from another CPU's share of the pool; try again.
		}
		if !clean {
			t.Error("the batch that GetMany keeps holds entries")
		}
		return
	}
	t.Skip("the pool never had a batch to hand")
}

// The values of expired entries can be had from the wrapped cache, for those
// that need releasing.
func TestRelease(t *testing.T) {
	t.Parallel()

	clock := expire.NewClock()
	inner := lru.New[string, expire.Entry[int]](10)
	c := expire.WrapWith(inner, time.Second, clock)
	c.Set("a", 1)
	c.Set("b", 2)
	clock.Advance(time.Second)

	values := map[string]int{}
	for k, e := range inner.All() {
		values[k] = e.Value()
	}
	if len(values) != 2 || values["a"] != 1 || values["b"] != 2 {
		t.Errorf("the wrapped cache yields %v, want a=1 and b=2", values)
	}
	if e, ok := inner.Delete("a"); !ok || e.Value() != 1 {
		t.Errorf("the wrapped cache's Delete(a) = %d, %t; want 1, true", e.Value(), ok)
	}
	if v := (expire.Entry[int]{}).Value(); v != 0 {
		t.Errorf("Entry{}.Value() = %d", v)
	}
}
