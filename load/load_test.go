package load_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/evict"
	"github.com/floatdrop/evict/expire"
	"github.com/floatdrop/evict/internal/cachetest"
	"github.com/floatdrop/evict/load"
	"github.com/floatdrop/evict/lru"
	"github.com/floatdrop/evict/s3fifo"
	"github.com/floatdrop/evict/stats"
)

// never is a loader for caches that are only read and written.
func never[K comparable, V any](context.Context, K) (V, error) {
	var zero V
	return zero, errors.New("loaded a key")
}

// A wrapped cache must still be a cache. One with a lock and one without get
// the suite.
var (
	lruFactories = cachetest.Factories{
		Int: func(capacity, shards int) evict.Cache[int, int] {
			return load.Wrap(lru.New[int, int](capacity, lru.WithShards(shards)), never[int, int])
		},
		String: func(capacity, shards int) evict.Cache[string, string] {
			return load.Wrap(lru.New[string, string](capacity, lru.WithShards(shards)), never[string, string])
		},
		Float: func(capacity, shards int) evict.Cache[float64, int] {
			return load.Wrap(lru.New[float64, int](capacity, lru.WithShards(shards)), never[float64, int])
		},
		Blob: func(capacity, shards int) evict.Cache[int, *cachetest.Blob] {
			return load.Wrap(lru.New[int, *cachetest.Blob](capacity, lru.WithShards(shards)), never[int, *cachetest.Blob])
		},
		BlobKey: func(capacity, shards int) evict.Cache[*cachetest.Blob, int] {
			return load.Wrap(lru.New[*cachetest.Blob, int](capacity, lru.WithShards(shards)), never[*cachetest.Blob, int])
		},
		Wide: func(capacity, shards int) evict.Cache[string, cachetest.Wide] {
			return load.Wrap(lru.New[string, cachetest.Wide](capacity, lru.WithShards(shards)), never[string, cachetest.Wide])
		},
	}

	s3fifoFactories = cachetest.Factories{
		Int: func(capacity, shards int) evict.Cache[int, int] {
			return load.Wrap(s3fifo.New[int, int](capacity, s3fifo.WithShards(shards)), never[int, int])
		},
		String: func(capacity, shards int) evict.Cache[string, string] {
			return load.Wrap(s3fifo.New[string, string](capacity, s3fifo.WithShards(shards)), never[string, string])
		},
		Float: func(capacity, shards int) evict.Cache[float64, int] {
			return load.Wrap(s3fifo.New[float64, int](capacity, s3fifo.WithShards(shards)), never[float64, int])
		},
		Blob: func(capacity, shards int) evict.Cache[int, *cachetest.Blob] {
			return load.Wrap(s3fifo.New[int, *cachetest.Blob](capacity, s3fifo.WithShards(shards)), never[int, *cachetest.Blob])
		},
		BlobKey: func(capacity, shards int) evict.Cache[*cachetest.Blob, int] {
			return load.Wrap(s3fifo.New[*cachetest.Blob, int](capacity, s3fifo.WithShards(shards)), never[*cachetest.Blob, int])
		},
		Wide: func(capacity, shards int) evict.Cache[string, cachetest.Wide] {
			return load.Wrap(s3fifo.New[string, cachetest.Wide](capacity, s3fifo.WithShards(shards)), never[string, cachetest.Wide])
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
	t.Run("lru", func(t *testing.T) { cachetest.NoAllocs(t, lruFactories) })
	t.Run("s3fifo", func(t *testing.T) { cachetest.NoAllocs(t, s3fifoFactories) })

	t.Run("Fetch", func(t *testing.T) {
		c := load.Wrap(s3fifo.New[int, int](1000), never[int, int])
		c.Set(1, 1)
		ctx := t.Context()
		if allocs := testing.AllocsPerRun(100, func() { _, _ = c.Fetch(ctx, 1) }); allocs != 0 {
			t.Errorf("%v allocations per Fetch that finds its key, want 0", allocs)
		}
	})
}

func TestNilLoader(t *testing.T) {
	t.Parallel()

	defer func() {
		if recover() == nil {
			t.Error("Wrap with a nil loader did not panic")
		}
	}()
	load.Wrap[int, int](lru.New[int, int](1), nil)
}

// until waits until cond holds, which some other goroutine is to bring about.
func until(t *testing.T, what string, cond func() bool) {
	t.Helper()

	for deadline := time.Now().Add(10 * time.Second); !cond(); {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(10 * time.Microsecond)
	}
}

// gated is a loader whose loads wait until gate is closed, and that counts
// them. The nth load of key returns 100 key + n.
type gated struct {
	gate  chan struct{}
	calls atomic.Int32
}

func newGated() *gated { return &gated{gate: make(chan struct{})} }

func (g *gated) load(_ context.Context, key int) (int, error) {
	n := g.calls.Add(1)
	<-g.gate
	return 100*key + int(n), nil
}

// fetch starts a Fetch of key and returns where its result goes.
func fetch(ctx context.Context, c *load.Cache[int, int], key int) <-chan result {
	out := make(chan result, 1)
	go func() {
		v, err := c.Fetch(ctx, key)
		out <- result{v, err}
	}()
	return out
}

type result struct {
	value int
	err   error
}

func TestFetch(t *testing.T) {
	t.Parallel()

	calls := 0
	c := load.Wrap(lru.New[int, int](10), func(_ context.Context, key int) (int, error) {
		calls++
		return -key, nil
	})
	c.Set(1, 10)
	if v, err := c.Fetch(t.Context(), 1); v != 10 || err != nil || calls != 0 {
		t.Errorf("Fetch of a stored key = %d, %v after %d loads, want 10, nil after none", v, err, calls)
	}
	if v, err := c.Fetch(t.Context(), 2); v != -2 || err != nil || calls != 1 {
		t.Errorf("Fetch of a missing key = %d, %v after %d loads, want -2, nil after one", v, err, calls)
	}
	if v, ok := c.Peek(2); !ok || v != -2 {
		t.Errorf("Peek after the load = %d, %t, want -2, true", v, ok)
	}
	if v, err := c.Fetch(t.Context(), 2); v != -2 || err != nil || calls != 1 {
		t.Errorf("Fetch of a loaded key = %d, %v after %d loads, want -2, nil after one", v, err, calls)
	}
	if n := load.Loading(c); n != 0 {
		t.Errorf("%d loads in progress after the last one returned", n)
	}
}

// The goroutines that fetch a key while it loads wait for that one load.
func TestFetchOnce(t *testing.T) {
	t.Parallel()

	const fetches = 16
	g := newGated()
	c := load.Wrap(lru.New[int, int](10), g.load)
	results := make([]<-chan result, fetches)
	for i := range results {
		results[i] = fetch(t.Context(), c, 1)
	}
	until(t, "every Fetch waits", func() bool { return load.Waiting(c, 1) == fetches })
	close(g.gate)
	for _, r := range results {
		if r := <-r; r.value != 101 || r.err != nil {
			t.Errorf("Fetch = %d, %v, want 101, nil", r.value, r.err)
		}
	}
	if n := g.calls.Load(); n != 1 {
		t.Errorf("%d loads, want 1", n)
	}
	if v, ok := c.Peek(1); !ok || v != 101 {
		t.Errorf("Peek after the load = %d, %t, want 101, true", v, ok)
	}
}

// An error goes to every Fetch that waits for the load, and is not stored.
func TestFetchError(t *testing.T) {
	t.Parallel()

	errDown := errors.New("the source is down")
	gate, calls := make(chan struct{}), atomic.Int32{}
	c := load.Wrap(lru.New[int, int](10), func(context.Context, int) (int, error) {
		calls.Add(1)
		<-gate
		return 0, errDown
	})
	a, b := fetch(t.Context(), c, 1), fetch(t.Context(), c, 1)
	until(t, "both Fetch calls wait", func() bool { return load.Waiting(c, 1) == 2 })
	close(gate)
	for _, r := range []result{<-a, <-b} {
		if !errors.Is(r.err, errDown) {
			t.Errorf("Fetch = %d, %v, want the loader's error", r.value, r.err)
		}
	}
	if _, ok := c.Peek(1); ok || c.Len() != 0 {
		t.Error("the error stored an entry")
	}
	if _, err := c.Fetch(t.Context(), 1); !errors.Is(err, errDown) || calls.Load() != 2 {
		t.Errorf("Fetch after the error = %v after %d loads, want the loader's error after 2", err, calls.Load())
	}
}

// A Fetch whose context ends stops waiting, and the load goes on for the
// others until none is left.
func TestFetchCanceled(t *testing.T) {
	t.Parallel()

	gate, contexts := make(chan struct{}), make(chan context.Context, 2)
	finished := make(chan struct{}, 2)
	c := load.WrapNotify(lru.New[int, int](10), func(ctx context.Context, key int) (int, error) {
		contexts <- ctx
		<-gate
		return 10 * key, nil // Stored unless the load was given up on.
	}, nil, func() { finished <- struct{}{} })

	// Of two, the one that gives up gets the error of its context, and the
	// other the value.
	ctx, cancel := context.WithCancel(t.Context())
	quitter := fetch(ctx, c, 1)
	loading := <-contexts
	stayer := fetch(t.Context(), c, 1)
	until(t, "both Fetch calls wait", func() bool { return load.Waiting(c, 1) == 2 })
	cancel()
	if r := <-quitter; !errors.Is(r.err, context.Canceled) {
		t.Errorf("Fetch with a canceled context = %d, %v, want context.Canceled", r.value, r.err)
	}
	if err := loading.Err(); err != nil {
		t.Errorf("the load's context ended (%v) while a Fetch was still waiting", err)
	}
	if n := load.Waiting(c, 1); n != 1 {
		t.Errorf("%d Fetch calls wait after one gave up, want 1", n)
	}

	// When the last one gives up, the load's context ends, and its value is
	// not stored.
	ctx, cancel = context.WithCancel(t.Context())
	quitter = fetch(ctx, c, 2)
	abandoned := <-contexts
	until(t, "the Fetch waits", func() bool { return load.Waiting(c, 2) == 1 })
	cancel()
	if r := <-quitter; !errors.Is(r.err, context.Canceled) {
		t.Errorf("Fetch with a canceled context = %d, %v, want context.Canceled", r.value, r.err)
	}
	if abandoned.Err() == nil {
		t.Error("the load's context did not end when no Fetch was left")
	}
	if n := load.Waiting(c, 2); n != -1 {
		t.Errorf("the abandoned load is still there, with %d waiting", n)
	}

	close(gate)
	if r := <-stayer; r.value != 10 || r.err != nil {
		t.Errorf("Fetch that stayed = %d, %v, want 10, nil", r.value, r.err)
	}
	<-finished
	<-finished
	if v, ok := c.Peek(1); !ok || v != 10 {
		t.Errorf("Peek(1) = %d, %t, want the value that was waited for", v, ok)
	}
	if v, ok := c.Peek(2); ok {
		t.Errorf("Peek(2) = %d, want the abandoned load not stored", v)
	}
}

// A load has the values of the context of the Fetch that started it, but not
// its deadline.
func TestFetchContext(t *testing.T) {
	t.Parallel()

	type ctxKey struct{}
	started := 0 // Counted in the goroutine of the Fetch that starts the load.
	c := load.WrapNotify(lru.New[int, int](10), func(ctx context.Context, key int) (int, error) {
		if v := ctx.Value(ctxKey{}); v != "trace" {
			t.Errorf("the load's context has %v, want the Fetch's value", v)
		}
		if d, ok := ctx.Deadline(); ok {
			t.Errorf("the load's context has the Fetch's deadline %v", d)
		}
		return key, nil
	}, func() { started++ }, nil)
	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), ctxKey{}, "trace"), time.Hour)
	defer cancel()
	if _, err := c.Fetch(ctx, 1); err != nil {
		t.Fatal(err)
	}

	// A context that has ended stops a load from starting, but not a stored
	// value from being returned.
	ctx, cancel = context.WithCancel(t.Context())
	cancel()
	if v, err := c.Fetch(ctx, 1); v != 1 || err != nil {
		t.Errorf("Fetch of a stored key with an ended context = %d, %v, want 1, nil", v, err)
	}
	if _, err := c.Fetch(ctx, 2); !errors.Is(err, context.Canceled) || started != 1 {
		t.Errorf("Fetch of a missing key with an ended context = %v after %d loads, want context.Canceled after 1", err, started)
	}
}

// A write during a load keeps it from storing its value, but the Fetch calls
// that wait for it get the value all the same.
func TestWriteDuringLoad(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		write  func(c *load.Cache[int, int])
		stored bool // the write stores 7 under key 1
	}{
		{"Set", func(c *load.Cache[int, int]) { c.Set(1, 7) }, true},
		{"SetIfAbsent", func(c *load.Cache[int, int]) { c.SetIfAbsent(1, 7) }, true},
		{"Delete", func(c *load.Cache[int, int]) { c.Delete(1) }, false},
		{"DeleteFunc", func(c *load.Cache[int, int]) { c.DeleteFunc(func(int, int) bool { return false }) }, false},
		{"Clear", func(c *load.Cache[int, int]) { c.Clear() }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := newGated()
			c := load.Wrap(lru.New[int, int](10), g.load)
			waiter := fetch(t.Context(), c, 1)
			until(t, "the Fetch waits", func() bool { return load.Waiting(c, 1) == 1 })
			tt.write(c)
			if n := load.Waiting(c, 1); n != -1 {
				t.Errorf("the load is still there after %s, with %d waiting", tt.name, n)
			}
			close(g.gate)
			if r := <-waiter; r.value != 101 || r.err != nil {
				t.Errorf("Fetch = %d, %v, want the loaded 101, nil", r.value, r.err)
			}
			v, ok := c.Peek(1)
			switch {
			case tt.stored && (!ok || v != 7):
				t.Errorf("Peek after the load = %d, %t, want what %s stored", v, ok, tt.name)
			case !tt.stored && ok:
				t.Errorf("Peek after the load = %d, want the load not stored", v)
			}
		})
	}
}

// A Fetch after a Delete does not wait for the load that the Delete made out
// of date: it loads anew, and its value is the one that is stored.
func TestFetchAfterDelete(t *testing.T) {
	t.Parallel()

	g := newGated()
	c := load.Wrap(lru.New[int, int](10), g.load)
	before := fetch(t.Context(), c, 1)
	until(t, "the first load runs", func() bool { return load.Waiting(c, 1) == 1 && g.calls.Load() == 1 })
	c.Delete(1)
	after := fetch(t.Context(), c, 1)
	until(t, "the second Fetch waits", func() bool { return load.Waiting(c, 1) == 1 && g.calls.Load() == 2 })
	close(g.gate)
	if r := <-before; r.value != 101 || r.err != nil {
		t.Errorf("Fetch before the Delete = %d, %v, want 101, nil", r.value, r.err)
	}
	if r := <-after; r.value != 102 || r.err != nil {
		t.Errorf("Fetch after the Delete = %d, %v, want 102, nil", r.value, r.err)
	}
	if v, ok := c.Peek(1); !ok || v != 102 {
		t.Errorf("Peek = %d, %t, want the value of the second load", v, ok)
	}
}

// A Fetch that gives up on a load that a Delete took out leaves the load that
// came after the Delete alone.
func TestGiveUpAfterDelete(t *testing.T) {
	t.Parallel()

	g := newGated()
	c := load.Wrap(lru.New[int, int](10), g.load)
	ctx, cancel := context.WithCancel(t.Context())
	before := fetch(ctx, c, 1)
	until(t, "the first load runs", func() bool { return load.Waiting(c, 1) == 1 && g.calls.Load() == 1 })
	c.Delete(1)
	after := fetch(t.Context(), c, 1)
	until(t, "the second load runs", func() bool { return load.Waiting(c, 1) == 1 && g.calls.Load() == 2 })
	cancel()
	if r := <-before; !errors.Is(r.err, context.Canceled) {
		t.Errorf("Fetch that gave up = %d, %v, want context.Canceled", r.value, r.err)
	}
	if n := load.Waiting(c, 1); n != 1 {
		t.Errorf("Waiting = %d after the first Fetch gave up, want the second load still there", n)
	}
	close(g.gate)
	if r := <-after; r.value != 102 || r.err != nil {
		t.Errorf("Fetch after the Delete = %d, %v, want 102, nil", r.value, r.err)
	}
	if v, ok := c.Peek(1); !ok || v != 102 {
		t.Errorf("Peek = %d, %t, want the value of the second load", v, ok)
	}
}

// A load's context ends when the load is over.
func TestLoadContextEnds(t *testing.T) {
	t.Parallel()

	contexts := make(chan context.Context, 1)
	c := load.Wrap(lru.New[int, int](10), func(ctx context.Context, key int) (int, error) {
		contexts <- ctx
		return key, nil
	})
	if _, err := c.Fetch(t.Context(), 1); err != nil {
		t.Fatal(err)
	}
	ctx := <-contexts
	until(t, "the load's context ends", func() bool { return ctx.Err() != nil })
}

// locked is a cache that checks that the Cache writes to it with the stripe of
// the key locked, so that a write through the Cache and the store of a load
// cannot interleave.
type locked struct {
	evict.Cache[int, int]
	t *testing.T
	c *load.Cache[int, int]
}

func (l *locked) check(op string, key int) {
	if !load.Held(l.c, key) {
		l.t.Errorf("%s(%d) of the wrapped cache with the stripe unlocked", op, key)
	}
}

func (l *locked) Set(key, value int) (oldKey, oldValue int, displaced bool) {
	l.check("Set", key)
	return l.Cache.Set(key, value)
}

func (l *locked) SetIfAbsent(key, value int) (actual int, present bool, oldKey, oldValue int, evicted bool) {
	l.check("SetIfAbsent", key)
	return l.Cache.SetIfAbsent(key, value)
}

func (l *locked) Delete(key int) (int, bool) {
	l.check("Delete", key)
	return l.Cache.Delete(key)
}

// Peek is called only by a Fetch that looks once more before it loads.
func (l *locked) Peek(key int) (int, bool) {
	l.check("Peek", key)
	return l.Cache.Peek(key)
}

func TestWritesLockTheStripe(t *testing.T) {
	t.Parallel()

	inner := &locked{Cache: lru.New[int, int](10), t: t}
	c := load.Wrap[int, int](inner, func(_ context.Context, key int) (int, error) { return key, nil })
	inner.c = c
	c.Set(1, 1)
	c.SetIfAbsent(2, 2)
	c.Delete(1)
	if _, err := c.Fetch(t.Context(), 3); err != nil { // Stores what it loads.
		t.Fatal(err)
	}
}

// purging is a cache that runs a function at the end of its Clear and
// DeleteFunc.
type purging struct {
	evict.Cache[int, int]
	after func()
}

func (p *purging) Clear() {
	p.Cache.Clear()
	p.after()
}

func (p *purging) DeleteFunc(del func(int, int) bool) int {
	n := p.Cache.DeleteFunc(del)
	p.after()
	return n
}

// A load that ends just after the Clear or DeleteFunc of the wrapped cache,
// before the one of the Cache returns, has been taken out already.
func TestLoadEndsDuringPurge(t *testing.T) {
	t.Parallel()

	for name, purge := range map[string]func(c *load.Cache[int, int]){
		"Clear":      func(c *load.Cache[int, int]) { c.Clear() },
		"DeleteFunc": func(c *load.Cache[int, int]) { c.DeleteFunc(func(int, int) bool { return true }) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			g, finished := newGated(), make(chan struct{}, 1)
			inner := &purging{Cache: lru.New[int, int](10)}
			c := load.WrapNotify[int, int](inner, g.load, nil, func() { finished <- struct{}{} })
			waiter := fetch(t.Context(), c, 1)
			until(t, "the load runs", func() bool { return load.Waiting(c, 1) == 1 && g.calls.Load() == 1 })
			inner.after = func() {
				close(g.gate)
				<-finished
			}
			purge(c)
			if r := <-waiter; r.value != 101 || r.err != nil {
				t.Errorf("Fetch = %d, %v, want 101, nil", r.value, r.err)
			}
			if v, ok := c.Peek(1); ok {
				t.Errorf("Peek after %s = %d, want the load not stored", name, v)
			}
		})
	}
}

// ending is a context that ends when its Done is first asked for, and lets
// the load panic before that.
type ending struct {
	context.Context
	once   sync.Once
	before func()
	done   chan struct{}
}

func (e *ending) Done() <-chan struct{} {
	e.once.Do(func() {
		e.before()
		close(e.done)
	})
	return e.done
}

func (e *ending) Err() error {
	select {
	case <-e.done:
		return context.Canceled
	default:
		return nil
	}
}

// A Fetch whose context ends just as the load panics gets the panic all the
// same. Which of the two it sees first is up to select, so this tries often.
func TestPanicAsContextEnds(t *testing.T) {
	t.Parallel()

	for range 64 {
		gate, finished := make(chan struct{}), make(chan struct{}, 1)
		c := load.WrapNotify(lru.New[int, int](10), func(context.Context, int) (int, error) {
			<-gate
			panic("late")
		}, nil, func() { finished <- struct{}{} })
		ctx := &ending{Context: t.Context(), done: make(chan struct{}), before: func() {
			close(gate)
			<-finished
		}}
		func() {
			defer func() {
				if p, _ := recover().(*load.PanicError); p == nil || p.Value != "late" {
					t.Fatalf("Fetch did not panic with the loader's panic, but with %v", p)
				}
			}()
			v, err := c.Fetch(ctx, 1)
			t.Fatalf("Fetch = %d, %v, want a panic", v, err)
		}()
	}
}

type entry struct{ key, value int }

// dropping wraps inner with WithDropped, and returns the entries that it has
// been called with so far. finished gets a value when each load is over, after
// what it dropped is in the list.
func dropping(t *testing.T, inner evict.Cache[int, int], loader func(context.Context, int) (int, error)) (c *load.Cache[int, int], dropped *[]entry, finished chan struct{}) {
	t.Helper()

	dropped, finished = new([]entry), make(chan struct{}, 1)
	c = load.WrapNotify(inner, loader, nil, func() { finished <- struct{}{} },
		load.WithDropped(func(key, value int) {
			// Another load of the key may hold its stripe for a moment,
			// but this goroutine must not: then it would never let go.
			for deadline := time.Now().Add(10 * time.Second); load.Held(c, key); time.Sleep(10 * time.Microsecond) {
				if time.Now().After(deadline) {
					t.Errorf("dropped(%d, %d) is called with the stripe of the key locked", key, value)
					break
				}
			}
			*dropped = append(*dropped, entry{key, value})
		}))
	return c, dropped, finished
}

func tenfold(_ context.Context, key int) (int, error) { return 10 * key, nil }

// resizable is a cache whose capacity can drop to nothing, after which its Set
// hands every entry straight back.
type resizable struct {
	evict.Cache[int, int]
	none atomic.Bool
}

func (r *resizable) Cap() int {
	if r.none.Load() {
		return 0
	}
	return r.Cache.Cap()
}

func (r *resizable) Set(key, value int) (oldKey, oldValue int, displaced bool) {
	if r.none.Load() {
		return key, value, true
	}
	return r.Cache.Set(key, value)
}

// The entries that a load pushes out, and the values that no Fetch waited for,
// go to WithDropped; what the Fetch calls get does not.
func TestDropped(t *testing.T) {
	t.Parallel()

	t.Run("evicted", func(t *testing.T) {
		t.Parallel()

		c, dropped, finished := dropping(t, lru.New[int, int](1), tenfold)
		c.Set(0, 0)
		if v, err := c.Fetch(t.Context(), 1); v != 10 || err != nil {
			t.Fatalf("Fetch = %d, %v, want 10, nil", v, err)
		}
		<-finished
		if want := []entry{{0, 0}}; !slices.Equal(*dropped, want) {
			t.Errorf("dropped %v, want the evicted %v", *dropped, want)
		}
	})

	t.Run("expired", func(t *testing.T) {
		t.Parallel()

		e := expire.Wrap(lru.New[int, expire.Entry[int]](10), time.Hour)
		c, dropped, finished := dropping(t, e, tenfold)
		e.Set(1, 5)
		e.SetWithTTL(1, 6, 0) // Replaces the entry with one that has expired.
		if v, err := c.Fetch(t.Context(), 1); v != 10 || err != nil {
			t.Fatalf("Fetch = %d, %v, want 10, nil", v, err)
		}
		<-finished
		if want := []entry{{1, 6}}; !slices.Equal(*dropped, want) {
			t.Errorf("dropped %v, want the expired %v that the load replaced", *dropped, want)
		}
	})

	t.Run("no capacity", func(t *testing.T) {
		t.Parallel()

		c, dropped, finished := dropping(t, lru.New[int, int](0), tenfold)
		if v, err := c.Fetch(t.Context(), 1); v != 10 || err != nil {
			t.Fatalf("Fetch = %d, %v, want 10, nil", v, err)
		}
		<-finished
		if len(*dropped) != 0 {
			t.Errorf("dropped %v, the value that Fetch got", *dropped)
		}
	})

	t.Run("given up", func(t *testing.T) {
		t.Parallel()

		g := newGated()
		c, dropped, finished := dropping(t, lru.New[int, int](10), g.load)
		ctx, cancel := context.WithCancel(t.Context())
		quitter := fetch(ctx, c, 1)
		until(t, "the load runs", func() bool { return load.Waiting(c, 1) == 1 && g.calls.Load() == 1 })
		cancel()
		<-quitter
		close(g.gate) // The load returns its value all the same.
		<-finished
		if want := []entry{{1, 101}}; !slices.Equal(*dropped, want) {
			t.Errorf("dropped %v, want the value nobody waited for, %v", *dropped, want)
		}
		if _, ok := c.Peek(1); ok {
			t.Error("the value nobody waited for was stored")
		}
	})

	t.Run("given up, then loaded again", func(t *testing.T) {
		t.Parallel()

		g := newGated()
		c, dropped, finished := dropping(t, lru.New[int, int](10), g.load)
		ctx, cancel := context.WithCancel(t.Context())
		quitter := fetch(ctx, c, 1)
		until(t, "the first load runs", func() bool { return load.Waiting(c, 1) == 1 && g.calls.Load() == 1 })
		cancel()
		<-quitter
		stayer := fetch(t.Context(), c, 1)
		until(t, "the second load runs", func() bool { return load.Waiting(c, 1) == 1 && g.calls.Load() == 2 })
		close(g.gate)
		if r := <-stayer; r.value != 102 || r.err != nil {
			t.Fatalf("Fetch = %d, %v, want 102, nil", r.value, r.err)
		}
		<-finished
		<-finished
		if want := []entry{{1, 101}}; !slices.Equal(*dropped, want) {
			t.Errorf("dropped %v, want the value of the first load, %v", *dropped, want)
		}
		if v, ok := c.Peek(1); !ok || v != 102 {
			t.Errorf("Peek = %d, %t, want the value of the second load", v, ok)
		}
	})

	t.Run("given up, failed", func(t *testing.T) {
		t.Parallel()

		gate := make(chan struct{})
		c, dropped, finished := dropping(t, lru.New[int, int](10), func(context.Context, int) (int, error) {
			<-gate
			return 7, errors.New("no")
		})
		ctx, cancel := context.WithCancel(t.Context())
		quitter := fetch(ctx, c, 1)
		until(t, "the Fetch waits", func() bool { return load.Waiting(c, 1) == 1 })
		cancel()
		<-quitter
		close(gate)
		<-finished
		if len(*dropped) != 0 {
			t.Errorf("dropped %v from a load that failed", *dropped)
		}
	})

	// A cache of somebody else's may change its capacity after Wrap.
	t.Run("capacity changes", func(t *testing.T) {
		t.Parallel()

		inner := &resizable{Cache: lru.New[int, int](1)}
		c, dropped, finished := dropping(t, inner, tenfold)
		inner.none.Store(true)
		if v, err := c.Fetch(t.Context(), 1); v != 10 || err != nil {
			t.Fatalf("Fetch = %d, %v, want 10, nil", v, err)
		}
		<-finished
		if len(*dropped) != 0 {
			t.Errorf("dropped %v, the value that Fetch got from a cache that has lost its capacity", *dropped)
		}

		inner = &resizable{Cache: lru.New[int, int](1)}
		inner.none.Store(true)
		c, dropped, finished = dropping(t, inner, tenfold)
		inner.none.Store(false)
		c.Set(0, 0)
		if _, err := c.Fetch(t.Context(), 2); err != nil {
			t.Fatal(err)
		}
		<-finished
		if want := []entry{{0, 0}}; !slices.Equal(*dropped, want) {
			t.Errorf("dropped %v, want %v, evicted by a cache that has gained capacity", *dropped, want)
		}
	})

	t.Run("taken out", func(t *testing.T) {
		t.Parallel()

		g := newGated()
		c, dropped, finished := dropping(t, lru.New[int, int](10), g.load)
		waiter := fetch(t.Context(), c, 1)
		until(t, "the load runs", func() bool { return load.Waiting(c, 1) == 1 && g.calls.Load() == 1 })
		c.Delete(1)
		close(g.gate)
		if r := <-waiter; r.value != 101 || r.err != nil {
			t.Fatalf("Fetch = %d, %v, want 101, nil", r.value, r.err)
		}
		<-finished
		if len(*dropped) != 0 {
			t.Errorf("dropped %v, the value that Fetch got", *dropped)
		}
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()

		c, dropped, finished := dropping(t, lru.New[int, int](1), func(context.Context, int) (int, error) {
			return 7, errors.New("no")
		})
		c.Set(0, 0) // Storing would evict it.
		if _, err := c.Fetch(t.Context(), 1); err == nil {
			t.Fatal("Fetch did not return the loader's error")
		}
		<-finished
		if len(*dropped) != 0 {
			t.Errorf("dropped %v from a load that failed", *dropped)
		}
	})
}

// late is a cache that stores a key just after a Get has missed it, as the
// load of another goroutine may.
type late struct {
	evict.Cache[int, int]
}

func (l late) Get(key int) (int, bool) {
	v, ok := l.Cache.Get(key)
	if !ok {
		l.Set(key, -key)
	}
	return v, ok
}

// A Fetch that misses a key just before it is stored does not load it again.
func TestFetchStoredMeanwhile(t *testing.T) {
	t.Parallel()

	calls := 0
	c := load.Wrap[int, int](late{lru.New[int, int](10)}, func(_ context.Context, key int) (int, error) {
		calls++
		return key, nil
	})
	if v, err := c.Fetch(t.Context(), 1); v != -1 || err != nil || calls != 0 {
		t.Errorf("Fetch = %d, %v after %d loads, want the stored -1, nil after none", v, err, calls)
	}
}

// A panic in the loader is raised again in every Fetch that waits for it.
func TestPanic(t *testing.T) {
	t.Parallel()

	errBoom := errors.New("boom")
	gate := make(chan struct{})
	c := load.Wrap(lru.New[int, int](10), func(context.Context, int) (int, error) {
		<-gate
		panic(errBoom)
	})
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			defer func() {
				p, _ := recover().(*load.PanicError)
				switch {
				case p == nil:
					t.Error("Fetch did not panic with a *PanicError")
				case !errors.Is(p, errBoom):
					t.Errorf("PanicError has %v, want the loader's value", p.Value)
				case !bytes.Contains(p.Stack, []byte("TestPanic")):
					t.Errorf("PanicError's stack is not the loader's:\n%s", p.Stack)
				case !strings.Contains(p.Error(), "boom"):
					t.Errorf("PanicError says %q", p.Error())
				}
			}()
			_, _ = c.Fetch(t.Context(), 1)
		})
	}
	until(t, "both Fetch calls wait", func() bool { return load.Waiting(c, 1) == 2 })
	close(gate)
	wg.Wait()
	if _, ok := c.Peek(1); ok || load.Loading(c) != 0 {
		t.Error("the panic left an entry or a load behind")
	}
}

// A panic that no Fetch waits for ends the program.
func TestPanicUnwaited(t *testing.T) {
	t.Parallel()

	if os.Getenv("LOAD_TEST_PANIC") == "1" {
		gate := make(chan struct{})
		c := load.Wrap(lru.New[int, int](10), func(context.Context, int) (int, error) {
			<-gate
			panic("nobody waits")
		})
		ctx, cancel := context.WithCancel(context.Background())
		r := fetch(ctx, c, 1)
		until(t, "the Fetch waits", func() bool { return load.Waiting(c, 1) == 1 })
		cancel()
		<-r
		close(gate)
		time.Sleep(time.Minute)
		return
	}
	if testing.Short() {
		t.Skip("runs the test binary again")
	}
	//nolint:gosec // The command is this test binary.
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestPanicUnwaited$")
	cmd.Env = append(os.Environ(), "LOAD_TEST_PANIC=1")
	out, err := cmd.CombinedOutput()
	if err == nil || !bytes.Contains(out, []byte("load: the loader panicked: nobody waits")) {
		t.Errorf("the program went on after a panic that no Fetch waited for (%v):\n%s", err, out)
	}
}

// A loader that calls runtime.Goexit does not leave the Fetch calls waiting.
func TestGoexit(t *testing.T) {
	t.Parallel()

	c := load.Wrap(lru.New[int, int](10), func(context.Context, int) (int, error) {
		runtime.Goexit()
		return 0, nil
	})
	if _, err := c.Fetch(t.Context(), 1); !errors.Is(err, load.ErrGoexit) {
		t.Errorf("Fetch = %v, want ErrGoexit", err)
	}
	if _, ok := c.Peek(1); ok || load.Loading(c) != 0 {
		t.Error("Goexit left an entry or a load behind")
	}
}

// A key that is not equal to itself is loaded by every Fetch and never stored.
func TestNaN(t *testing.T) {
	t.Parallel()

	calls := 0
	c := load.Wrap(lru.New[float64, int](10), func(ctx context.Context, _ float64) (int, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("the load of NaN does not have the context of its Fetch")
		}
		calls++
		return calls, nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	defer cancel()
	nan := math.NaN()
	for want := 1; want <= 2; want++ {
		if v, err := c.Fetch(ctx, nan); v != want || err != nil {
			t.Errorf("Fetch(NaN) = %d, %v, want %d, nil", v, err, want)
		}
	}
	if c.Len() != 0 || load.Loading(c) != 0 {
		t.Errorf("Fetch(NaN) left %d entries and %d loads", c.Len(), load.Loading(c))
	}
}

// Wrapped around stats, a Fetch counts as a hit or a miss, and storing what
// it loaded counts as a Set.
func TestStats(t *testing.T) {
	t.Parallel()

	s := stats.Wrap(lru.New[int, int](1))
	c := load.Wrap[int, int](s, func(_ context.Context, key int) (int, error) { return key, nil })
	for _, key := range []int{1, 1, 1, 2} { // The load of 2 evicts 1.
		if _, err := c.Fetch(t.Context(), key); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.Stats(); got.Hits != 2 || got.Misses != 2 || got.Evictions != 1 {
		t.Errorf("stats = %+v, want 2 hits, 2 misses and 1 eviction", got)
	}
}

// Wrapped around expire, a Fetch loads an expired key again.
func TestExpire(t *testing.T) {
	t.Parallel()

	e := expire.Wrap(lru.New[int, expire.Entry[int]](10), time.Hour)
	calls := 0
	c := load.Wrap[int, int](e, func(_ context.Context, key int) (int, error) {
		calls++
		return 10*key + calls, nil
	})
	if v, err := c.Fetch(t.Context(), 1); v != 11 || err != nil {
		t.Fatalf("Fetch = %d, %v, want 11, nil", v, err)
	}
	e.SetWithTTL(1, 5, 0) // Replaces the entry with one that has expired.
	if v, err := c.Fetch(t.Context(), 1); v != 12 || err != nil || c.Len() != 1 {
		t.Errorf("Fetch of an expired key = %d, %v, Len() = %d, want 12, nil, 1", v, err, c.Len())
	}
}

// source is what the caches of TestConcurrent load from. Writers change it and
// then write the cache, as a program that keeps a cache in front of a database
// would. The value of a key is 1000 times the key plus its version.
type source struct {
	mu      sync.Mutex
	version map[int]int
}

func (s *source) load(_ context.Context, key int) (int, error) {
	s.mu.Lock()
	v := 1000*key + s.version[key]
	s.mu.Unlock()
	runtime.Gosched() // Leaves time for a write to come in before the load is over.
	return v, nil
}

// update changes key in the source and writes the cache to match, with the
// source locked, so that the writes of a key reach the cache in order.
func (s *source) update(c *load.Cache[int, int], key int, del bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.version[key]++
	if del {
		c.Delete(key)
	} else {
		c.Set(key, 1000*key+s.version[key])
	}
}

// Under any mix of fetches, writes, purges and Fetch calls that give up, no
// load stores a value that a write has made out of date.
func TestConcurrent(t *testing.T) {
	t.Parallel()

	for name, inner := range map[string]func() evict.Cache[int, int]{
		"lru":    func() evict.Cache[int, int] { return lru.New[int, int](64) },
		"s3fifo": func() evict.Cache[int, int] { return s3fifo.New[int, int](64) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			const (
				goroutines = 8
				requests   = 2000
				keys       = 8
			)
			src := &source{version: map[int]int{}}
			c := load.Wrap(inner(), src.load)
			var wg sync.WaitGroup
			for g := range goroutines {
				wg.Go(func() {
					rng := rand.New(rand.NewPCG(uint64(g), 7))
					for range requests {
						key := rng.IntN(keys)
						switch rng.IntN(20) {
						case 0, 1:
							src.update(c, key, false)
						case 2, 3:
							src.update(c, key, true)
						case 4:
							c.Clear()
						case 5:
							c.DeleteFunc(func(k, _ int) bool { return k == key })
						case 6, 7:
							ctx, cancel := context.WithTimeout(t.Context(), time.Duration(rng.IntN(20))*time.Microsecond)
							if v, err := c.Fetch(ctx, key); err == nil && v/1000 != key {
								t.Errorf("Fetch(%d) = %d", key, v)
							}
							cancel()
						default:
							if v, err := c.Fetch(t.Context(), key); err != nil || v/1000 != key {
								t.Errorf("Fetch(%d) = %d, %v", key, v, err)
							}
						}
					}
				})
			}
			wg.Wait()

			// Every load that a Fetch still waited for is over; the ones
			// that were given up on never store.
			for key, v := range c.All() {
				if want := 1000*key + src.version[key]; v != want {
					t.Errorf("the cache holds %d for %d, the source %d", v, key, want)
				}
			}
		})
	}
}
