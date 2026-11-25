// Package ref counts the users of a value that has to be released, such as a
// file or a buffer from a pool, so that a cache can share it out and hand it
// back without the value being released while someone still uses it.
//
// A cache hands back what leaves it, so that it can be released. But a
// goroutine that got the value from Get a moment before may still be using it,
// and the cache cannot know. Keep a [Ref] in the cache instead of the value:
//
//	cache := tinylfu.New[string, *ref.Ref[*os.File]](1000)
//
//	r := ref.New(f, func(f *os.File) { f.Close() })
//	if _, old, displaced := cache.Set(name, r); displaced {
//		old.Drop()
//	}
//
//	if r, f, ok := ref.Get(cache, name); ok {
//		defer r.Release()
//		// ... use f ...
//	}
//
// A Ref starts out with one reference, the cache's, which [Ref.Drop] gives up
// when the cache hands the Ref back: from Set, SetIfAbsent or Delete, to the
// function of expire's PurgeExpired, or to that of load.WithDropped. Each
// [Ref.Acquire] that succeeds adds a reference, and its [Ref.Release] takes it
// away. The last reference to go releases the value. From then on Acquire
// fails, and the reader treats the entry as missing: it can be on its way out
// of the cache, or not have been dropped by the time the reader got it.
//
// # Refs that are lost
//
// A Ref that leaves the cache unseen, as an entry does that Clear removes, may
// be released after the garbage collector finds it unreachable, since nobody
// can use its value then. That is up to the runtime: it runs such releases in
// goroutines of its own, possibly at the same time as one another, and does
// not promise to run them at all, not before the program exits in particular.
// It is a backstop, not something to count on for a release that has to
// happen, such as a flush: drop what the cache hands back. A value or a
// release function that refers to its Ref keeps the Ref reachable, and leaves
// it to Drop and Release alone.
//
// # Rules
//
// Store a Ref under one key once. A Ref that is stored twice is handed back
// twice, and the first Drop gives up the reference of the cache, which still
// holds the other copy; Acquire may then fail for that one, as if it were
// missing. Drop is safe to call more than once.
//
// Keep the Ref until the Release that goes with an Acquire, and use the value
// only in between. Release without Acquire panics, if it is found out.
//
// # Cost
//
// Acquire is an atomic load and a compare-and-swap, which it tries again if
// another reader changed the count in between, and Release an atomic add: all
// the readers of a value write its count. On an Apple M3 Max, a Get of tinylfu
// with them took about 30% longer than a Get on one CPU, and 42% on eight,
// with reads spread over 16,384 entries. Eight CPUs that all read one key took
// 200 to 400 ns per read, where a Get of one key takes 1.1 ns. New takes five
// allocations, 96 bytes with an int, and 0.11 µs with Drop.
package ref

import (
	"context"
	"runtime"
	"sync/atomic"

	"github.com/floatdrop/evict"
	"github.com/floatdrop/evict/load"
)

// A Ref is a value with a count of the references to it; see the package
// documentation.
type Ref[T any] struct {
	// The count lives apart from the Ref, so that the garbage collector can
	// find the Ref unreachable while the function that releases the value as
	// a backstop holds on to the count.
	s *state[T]
}

// state is the count of a Ref and what it releases.
//
// The words are plain integers that are only ever accessed with the functions
// of sync/atomic, which the compiler builds in wherever it compiles the
// methods of a Ref. The methods of the atomic types it would inline there only
// if a package that is imported there had inlined them itself; see the package
// internal/inlinecheck.
type state[T any] struct {
	// refs is the number of references: the cache's until it is dropped,
	// and one for each Acquire that is not released yet. It is zero once
	// the value is released, and never goes up again then.
	refs int64

	dropped  uint32 //nolint:modernize // One once the cache's reference is gone.
	released uint32 //nolint:modernize // One once release has been called.

	value   T
	release func(T)
	cleanup runtime.Cleanup
}

// New returns a Ref to value with one reference, which is the cache's to
// drop. The last reference to go calls release with value, and the garbage
// collector may if the Ref becomes unreachable before that; see the package
// documentation.
func New[T any](value T, release func(T)) *Ref[T] {
	s := &state[T]{refs: 1, value: value, release: release}
	r := &Ref[T]{s: s}
	s.cleanup = runtime.AddCleanup(r, (*state[T]).backstop, s)
	return r
}

// Acquire adds a reference and returns the value, unless the value has been
// released: then it returns false, and the caller should go on as if it had
// not found the Ref. A reference that Acquire adds is the caller's to Release.
func (r *Ref[T]) Acquire() (value T, ok bool) {
	s := r.s
	for {
		n := atomic.LoadInt64(&s.refs)
		if n <= 0 {
			return value, false
		}
		if atomic.CompareAndSwapInt64(&s.refs, n, n+1) {
			return s.value, true
		}
	}
}

// Release takes away a reference that Acquire added. The last one to go
// releases the value, in the goroutine that calls Release.
func (r *Ref[T]) Release() {
	r.s.put()
	// Until put is done, the garbage collector must not find r unreachable
	// and release the value on its own, in another goroutine.
	runtime.KeepAlive(r)
}

// Drop takes away the reference of the cache, the first time it is called; it
// does nothing after that. The last reference to go releases the value, in
// the goroutine that calls Drop. Call it with what the cache hands back.
func (r *Ref[T]) Drop() {
	s := r.s
	if atomic.CompareAndSwapUint32(&s.dropped, 0, 1) {
		s.put()
	}
	runtime.KeepAlive(r) // As in Release.
}

// put takes away a reference.
func (s *state[T]) put() {
	switch n := atomic.AddInt64(&s.refs, -1); {
	case n == 0:
		s.cleanup.Stop()
		s.finish()
	case n < 0:
		panic("ref: Release of a reference that was not acquired")
	}
}

// backstop releases the value of a Ref that has become unreachable.
func (s *state[T]) backstop() {
	// No reference can be added or taken away any more: Release and Drop
	// keep the Ref reachable until they are done, and a reader that follows
	// the rules keeps it from its Acquire to its Release. The count says
	// nothing that matters.
	s.finish()
}

// finish releases the value, once.
func (s *state[T]) finish() {
	if atomic.CompareAndSwapUint32(&s.released, 0, 1) && s.release != nil {
		s.release(s.value)
	}
}

// Get looks up key in cache with Get and acquires the Ref it finds. It returns
// false if there is none, or if its value has been released. A Ref that Get
// returns is the caller's to Release.
func Get[K comparable, T any](cache evict.Cache[K, *Ref[T]], key K) (r *Ref[T], value T, ok bool) {
	r, ok = cache.Get(key)
	if !ok {
		return nil, value, false
	}
	if value, ok = r.Acquire(); !ok {
		return nil, value, false
	}
	return r, value, true
}

// Fetch is Get with the Fetch of a load.Cache: it loads key if the cache does
// not hold it. If the Ref it gets has been released, it fetches again, and
// deletes the Ref first if the cache still holds it, which it does only if the
// Ref was stored twice. A Ref that Fetch returns is the caller's to Release.
//
// The loader returns a Ref with one reference, which the cache takes when it
// stores the Ref. A Ref that the cache does not store, because a write to its
// key came in during the load, is released by the garbage collector. Pass
// load.WithDropped(ref.Drop[K, T]) to Wrap to have the others released at
// once.
func Fetch[K comparable, T any](ctx context.Context, cache *load.Cache[K, *Ref[T]], key K) (r *Ref[T], value T, err error) {
	for {
		r, err = cache.Fetch(ctx, key)
		if err != nil {
			return nil, value, err
		}
		if value, ok := r.Acquire(); ok {
			return r, value, nil
		}
		if cur, ok := cache.Peek(key); ok && cur == r {
			if old, ok := cache.Delete(key); ok {
				old.Drop()
			}
		}
	}
}

// Drop drops r, and is for the functions that a cache hands back its entries
// to, such as load.WithDropped(ref.Drop[K, T]), where Go cannot infer K and T.
//
// The function of expire's PurgeExpired runs with a shard of the cache locked,
// and so would a release that Drop makes there: c.PurgeExpired(ref.Drop) is
// fine for a release that is quick and does not use the cache. Otherwise
// collect the Refs that PurgeExpired hands over, and drop them after it
// returns. The same goes for del of DeleteFunc.
func Drop[K, T any](_ K, r *Ref[T]) { r.Drop() }
