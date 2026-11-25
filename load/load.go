// Package load fills a cache from a source, and has the goroutines that ask
// for a key at the same time share one load of it.
//
// [Wrap] puts a loader in front of any [evict.Cache]:
//
//	users := load.Wrap(tinylfu.New[string, User](10_000), func(ctx context.Context, id string) (User, error) {
//		return db.User(ctx, id)
//	})
//	u, err := users.Fetch(ctx, "42")
//
// [Cache.Fetch] returns the value stored under a key. If there is none, it
// calls the loader, stores what that returns, and returns it. The goroutines
// that fetch a key while it is loading wait for that load, and get its value
// or its error. An error is not stored: the next Fetch loads again. The other
// methods are those of [evict.Cache], and none of them loads.
//
// # Waiting
//
// A load runs in a goroutine of its own. Its context has the values of the
// context of the Fetch that started it, but not its deadline or its
// cancellation. A Fetch whose context ends stops waiting and returns the
// context's error. The load goes on for the others. When none is left, its
// context is canceled, and a value that it gets all the same is not stored but
// passed to the function of [WithDropped], if there is one. The next Fetch
// starts a load of its own, which may run alongside it. A loader must not
// fetch the key it loads: it would wait for itself.
//
// # Writes during a load
//
// A Set, SetIfAbsent or Delete of a key that is loading, and any DeleteFunc or
// Clear, keep the load from storing its value: it may have been read from the
// source before the change that the write is for. The Fetch calls that wait
// for the load still get the value. Those that come after a Delete, DeleteFunc
// or Clear start a load of their own. Writes to the wrapped cache itself do
// not count.
//
// Storing a loaded value may push an entry out of the wrapped cache, which no
// Set hands back. [WithDropped] passes such entries to a function of yours, so
// that what they hold on to can be released.
//
// # Panics
//
// A panic in the loader is raised again in every Fetch that waits for the
// load, as a [*PanicError] that holds the value and the stack of the panic.
// If none is waiting, the panic ends the program, as a panic in any goroutine
// does. A loader that calls runtime.Goexit leaves the waiting Fetch calls
// with an error.
//
// # Other wrappers
//
// Wrapped around stats, as load.Wrap(stats.Wrap(...)), a cache counts a Fetch
// as a hit if it finds the key and as a miss if it does not, and storing a
// loaded value as a Set. Wrapped around expire, it loads expired keys again,
// and the loaded values get the time to live of the expire.Cache.
//
// The wrapped cache must not call back into the Cache: the Cache holds a lock
// of its own around its calls of Set, SetIfAbsent and Delete of the wrapped
// cache, and of the Peek with which it looks once more before a load.
//
// # Cost
//
// On an Apple M3 Max, a Fetch of tinylfu that found its key took 20% longer
// than a Get on one CPU, and 23% on eight. A Set took 10 ns longer on one CPU,
// and 60% longer on eight. A load takes a goroutine and six allocations, 352
// bytes in all with int keys and values, on top of what the loader takes: 0.56
// µs with one that returns at once.
package load

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"iter"
	"math/bits"
	"runtime"
	"runtime/debug"
	"sync"
	"unsafe"

	"github.com/floatdrop/evict"
)

// cacheLine is a size that no two CPUs' cache lines straddle on the
// platforms Go runs on.
const cacheLine = 128

// minStripes is the fewest stripes a Cache has, however few CPUs there are.
const minStripes = 64

// Cache is an [evict.Cache] that loads what it does not hold when asked with
// [Cache.Fetch]; see the package documentation. Its methods are safe for
// concurrent use as long as those of the cache it wraps are, as they are for
// every cache in this module.
type Cache[K comparable, V any] struct {
	cache evict.Cache[K, V]
	load  func(ctx context.Context, key K) (V, error)

	// A key belongs to the stripe that its hash picks. The stripe's lock
	// orders the writes of the key with the loads of it.
	stripes []stripe[K, V]
	shift   uint8 // 64 minus log2 of the number of stripes
	seed    maphash.Seed

	dropped func(key K, value V) // see WithDropped; may be nil

	// For tests: called, if not nil, when a load starts and when it is over.
	started, finished func()
}

// A stripe holds the loads in progress of its keys, in a cache line of its
// own.
type stripe[K comparable, V any] struct {
	mu sync.Mutex

	// loads has the loads whose values are still to be stored. A write of
	// the key takes its load out.
	loads map[K]*call[V]

	_ [cacheLine - unsafe.Sizeof(sync.Mutex{}) - unsafe.Sizeof(uintptr(0))]byte
}

// A call is a load in progress, and what it got once it is done.
type call[V any] struct {
	done   chan struct{} // closed when the load is over
	cancel context.CancelFunc

	// waiters is the number of Fetch calls that wait for the load. It is
	// read and written under the lock of the stripe.
	waiters int

	// The result is written before done is closed, and read after.
	value    V
	err      error
	panicked *PanicError
}

// PanicError is what Fetch panics with when the loader has panicked.
type PanicError struct {
	// Value is what the loader panicked with.
	Value any

	// Stack is the stack of the goroutine that ran the loader, as it was
	// when it panicked.
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("load: the loader panicked: %v\n\n%s", e.Value, e.Stack)
}

// Unwrap returns the value that the loader panicked with if it is an error.
func (e *PanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}

// errGoexit is what a load returns if its loader calls runtime.Goexit.
var errGoexit = errors.New("load: the loader called runtime.Goexit")

// An Option changes what [Wrap] makes.
type Option[K comparable, V any] func(*Cache[K, V])

// WithDropped has the Cache call dropped with the entries that would leave it
// without anyone getting them, so that what they hold on to can be released:
// the entry that storing a loaded value pushes out of the wrapped cache, which
// a Set would have handed back, and the value of a load that no Fetch waited
// for to the end, unless the load failed. It is called in the goroutine of the
// load, with no lock of the Cache held, and may call back into the Cache.
//
// What leaves through the Cache's own methods is theirs to hand back, as
// [evict.Cache] has it: Set and SetIfAbsent return what they push out, Delete
// what it deletes, and Clear nothing.
func WithDropped[K comparable, V any](dropped func(key K, value V)) Option[K, V] {
	return func(c *Cache[K, V]) { c.dropped = dropped }
}

// Wrap returns a Cache that keeps its entries in cache, and fills it by calling
// load with the key that a Fetch did not find. It panics if load is nil.
//
// Using cache directly as well is fine, but its writes do not keep a load
// from storing a value that they have made out of date; see the package
// documentation.
func Wrap[K comparable, V any](cache evict.Cache[K, V], load func(ctx context.Context, key K) (V, error), opts ...Option[K, V]) *Cache[K, V] {
	if load == nil {
		panic("load: Wrap: load must not be nil")
	}
	// Eight stripes per CPU make it unlikely that writers to different
	// keys wait for each other, as the shards of the caches do.
	n := max(minStripes, 8*runtime.GOMAXPROCS(0))
	log := bits.Len(uint(n - 1))
	c := &Cache[K, V]{
		cache:   cache,
		load:    load,
		stripes: make([]stripe[K, V], 1<<log),
		shift:   uint8(64 - log),
		seed:    maphash.MakeSeed(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// stripe returns the stripe of a key with hash h. The callers hash the key
// themselves: with the hashing in it, stripe would be too large to inline.
func (c *Cache[K, V]) stripe(h uint64) *stripe[K, V] {
	return &c.stripes[h>>c.shift]
}

// Fetch returns the value stored under key, and records the access with the
// eviction policy as Get does. If there is none, Fetch loads it, unless key is
// loading already, in which case it waits for that load. It returns the value
// that the load got, which it has stored unless a write came in between, or
// its error, which it has not. If ctx ends first, Fetch returns its error.
//
// A value that is stored is returned whatever the state of ctx. A key that is
// not equal to itself, one with a NaN in it, is never stored, since it could
// not be found: every Fetch of it calls the loader itself, with ctx, as a
// plain function call.
func (c *Cache[K, V]) Fetch(ctx context.Context, key K) (V, error) {
	if value, ok := c.cache.Get(key); ok {
		return value, nil
	}
	return c.fetch(ctx, key)
}

// fetch is Fetch for a key that the cache did not have.
func (c *Cache[K, V]) fetch(ctx context.Context, key K) (value V, err error) {
	if err := ctx.Err(); err != nil {
		return value, err
	}
	if key != key { //nolint:gocritic // Only a key with a NaN in it is not equal to itself.
		return c.load(ctx, key)
	}

	s := c.stripe(maphash.Comparable(c.seed, key))
	s.mu.Lock()
	l := s.loads[key]
	if l != nil {
		l.waiters++
	} else {
		// The load of another goroutine may have stored the key since Get
		// missed it.
		if value, ok := c.cache.Peek(key); ok {
			s.mu.Unlock()
			return value, nil
		}
		l = c.start(ctx, s, key)
	}
	s.mu.Unlock()

	return c.wait(ctx, s, key, l)
}

// start starts a load of key, which belongs to stripe s, whose lock the caller
// holds.
func (c *Cache[K, V]) start(ctx context.Context, s *stripe[K, V], key K) *call[V] {
	// finish calls cancel, or the last Fetch to give up on the load does.
	ctx, cancel := context.WithCancel(context.WithoutCancel(ctx)) //nolint:gosec // See above.
	l := &call[V]{done: make(chan struct{}), cancel: cancel, waiters: 1}
	if s.loads == nil {
		s.loads = make(map[K]*call[V])
	}
	s.loads[key] = l
	if c.started != nil {
		c.started()
	}
	go c.run(ctx, s, key, l)
	return l
}

// run loads key for l, and finishes l whichever way the loader ends.
func (c *Cache[K, V]) run(ctx context.Context, s *stripe[K, V], key K, l *call[V]) {
	returned := false
	defer func() {
		if !returned {
			// recover stops a panic, but not runtime.Goexit, which goes
			// on once this function is done.
			if p := recover(); p != nil {
				l.panicked = &PanicError{Value: p, Stack: debug.Stack()}
			} else {
				l.err = errGoexit
			}
		}
		waiters, oldKey, oldValue, dropped := c.finish(s, key, l)
		if dropped && c.dropped != nil {
			c.dropped(oldKey, oldValue)
		}
		if c.finished != nil {
			c.finished()
		}
		if waiters == 0 && l.panicked != nil {
			panic(l.panicked)
		}
	}()
	l.value, l.err = c.load(ctx, key)
	returned = true
}

// finish stores what load l of key got, unless the load was taken out, wakes
// up the Fetch calls that wait for it, and returns how many there are. It also
// returns the entry that has gone to nobody, if there is one: the one that the
// store pushed out, or the loaded one if no Fetch waited for it.
func (c *Cache[K, V]) finish(s *stripe[K, V], key K, l *call[V]) (waiters int, oldKey K, oldValue V, dropped bool) {
	defer l.cancel()
	s.mu.Lock()
	defer s.mu.Unlock()
	// Closing done with the stripe locked lets a Fetch that gives up see
	// that the load is over, and take its result, before it counts itself
	// out: else a panic could reach nobody.
	defer close(l.done)

	loaded := l.err == nil && l.panicked == nil
	switch {
	case s.loads[key] == l:
		delete(s.loads, key)
		if loaded {
			oldKey, oldValue, dropped = c.cache.Set(key, l.value)
			// A cache without capacity hands back the loaded entry,
			// which the Fetch calls that wait get. A cache of somebody
			// else's may not always have had the capacity it has.
			dropped = dropped && (oldKey != key || c.cache.Cap() != 0)
		}
	case loaded && l.waiters == 0:
		oldKey, oldValue, dropped = key, l.value, true
	}
	return l.waiters, oldKey, oldValue, dropped
}

// wait waits for load l of key, which belongs to stripe s, or for ctx to end.
func (c *Cache[K, V]) wait(ctx context.Context, s *stripe[K, V], key K, l *call[V]) (value V, err error) {
	select {
	case <-l.done:
		return l.result()
	case <-ctx.Done():
	}

	s.mu.Lock()
	select {
	case <-l.done:
		// The load is over after all.
		s.mu.Unlock()
		return l.result()
	default:
	}
	l.waiters--
	if l.waiters == 0 {
		// Nobody wants the value any more.
		if s.loads[key] == l {
			delete(s.loads, key)
		}
		l.cancel()
	}
	s.mu.Unlock()
	return value, ctx.Err()
}

// result returns what the load got, or raises the panic of its loader again.
func (l *call[V]) result() (V, error) {
	if l.panicked != nil {
		panic(l.panicked)
	}
	return l.value, l.err
}

// forget takes the load of key, if there is one, out of stripe s, whose lock
// the caller holds. The load goes on for the Fetch calls that wait for it, but
// its value is not stored.
func (s *stripe[K, V]) forget(key K) {
	if len(s.loads) != 0 {
		delete(s.loads, key)
	}
}

// forgetAll takes every load out.
func (c *Cache[K, V]) forgetAll() {
	for i := range c.stripes {
		s := &c.stripes[i]
		s.mu.Lock()
		clear(s.loads)
		s.mu.Unlock()
	}
}

// Get returns the value stored under key and records the access with the
// eviction policy. It does not load.
func (c *Cache[K, V]) Get(key K) (value V, ok bool) { return c.cache.Get(key) }

// GetMany does what Get does for each of keys: it does not load. See
// [evict.Cache].
func (c *Cache[K, V]) GetMany(keys []K, values []V, found []bool) {
	c.cache.GetMany(keys, values, found)
}

// Peek returns the value stored under key without telling the eviction policy
// about it. It does not load.
func (c *Cache[K, V]) Peek(key K) (value V, ok bool) { return c.cache.Peek(key) }

// Set stores value under key, and keeps a load of key that is in progress from
// storing its value. See [evict.Cache].
func (c *Cache[K, V]) Set(key K, value V) (oldKey K, oldValue V, displaced bool) {
	s := c.stripe(maphash.Comparable(c.seed, key))
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forget(key)
	return c.cache.Set(key, value)
}

// SetIfAbsent stores value under key unless the cache holds key, and keeps a
// load of key that is in progress from storing its value. It does not load.
// See [evict.Cache].
func (c *Cache[K, V]) SetIfAbsent(key K, value V) (actual V, present bool, oldKey K, oldValue V, evicted bool) {
	s := c.stripe(maphash.Comparable(c.seed, key))
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forget(key)
	return c.cache.SetIfAbsent(key, value)
}

// Delete removes key and returns the value it had, and keeps a load of key
// that is in progress from storing its value.
func (c *Cache[K, V]) Delete(key K) (value V, ok bool) {
	s := c.stripe(maphash.Comparable(c.seed, key))
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forget(key)
	return c.cache.Delete(key)
}

// DeleteFunc removes the entries for which del returns true and reports how
// many it removed. It keeps every load in progress from storing its value,
// since del cannot be asked about one that is not there yet. See
// [evict.Cache].
func (c *Cache[K, V]) DeleteFunc(del func(K, V) bool) int {
	c.forgetAll()
	return c.cache.DeleteFunc(del)
}

// Len returns the number of entries in the cache.
func (c *Cache[K, V]) Len() int { return c.cache.Len() }

// Cap returns the number of entries the cache can hold.
func (c *Cache[K, V]) Cap() int { return c.cache.Cap() }

// Clear removes every entry, and keeps every load in progress from storing its
// value. See [evict.Cache].
func (c *Cache[K, V]) Clear() {
	c.forgetAll()
	c.cache.Clear()
}

// All iterates over the entries of the cache. It does not load.
func (c *Cache[K, V]) All() iter.Seq2[K, V] { return c.cache.All() }

var _ evict.Cache[int, int] = (*Cache[int, int])(nil)
