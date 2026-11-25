package ref_test

import (
	"context"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/evict/expire"
	"github.com/floatdrop/evict/load"
	"github.com/floatdrop/evict/lru"
	"github.com/floatdrop/evict/ref"
	"github.com/floatdrop/evict/tinylfu"
)

// resource is a value that has to be released, and knows whether it has been.
type resource struct {
	id       int
	released atomic.Int32
}

func release(r *resource) { r.released.Add(1) }

func newRef(id int) (*ref.Ref[*resource], *resource) {
	res := &resource{id: id}
	return ref.New(res, release), res
}

func TestCount(t *testing.T) {
	t.Parallel()

	r, res := newRef(1)
	v, ok := r.Acquire()
	if !ok || v != res {
		t.Fatalf("Acquire = %v, %t, want the value", v, ok)
	}
	r.Drop()
	if res.released.Load() != 0 {
		t.Fatal("Drop released the value while it was acquired")
	}
	r.Drop() // Does nothing the second time.
	if _, ok := r.Acquire(); !ok {
		t.Fatal("Acquire failed while another reference was left")
	}
	r.Release()
	if res.released.Load() != 0 {
		t.Fatal("a Release released the value while it was acquired")
	}
	r.Release()
	if n := res.released.Load(); n != 1 {
		t.Fatalf("the last Release released the value %d times, want once", n)
	}
	if _, ok := r.Acquire(); ok {
		t.Error("Acquire succeeded after the value was released")
	}
}

func TestDropFirst(t *testing.T) {
	t.Parallel()

	r, res := newRef(1)
	r.Drop()
	if n := res.released.Load(); n != 1 {
		t.Fatalf("Drop of the only reference released the value %d times, want once", n)
	}
	defer func() {
		if recover() == nil {
			t.Error("Release without Acquire did not panic")
		}
	}()
	r.Release()
}

func TestNilRelease(t *testing.T) {
	t.Parallel()

	r := ref.New(1, nil)
	r.Drop()
	if _, ok := r.Acquire(); ok {
		t.Error("Acquire succeeded after the value was released")
	}
}

// No Acquire that succeeds sees the value released before its Release, and the
// value is released once, whoever lets go last.
func TestConcurrent(t *testing.T) {
	t.Parallel()

	for range 200 {
		r, res := newRef(1)
		// The readers go on until the value is released, so that the count
		// comes down to zero while they acquire.
		var started, wg sync.WaitGroup
		started.Add(4)
		for range 4 {
			wg.Go(func() {
				first := true
				for range 10_000 {
					_, ok := r.Acquire()
					if ok && res.released.Load() != 0 {
						t.Error("a value was released while it was acquired")
					}
					if ok {
						r.Release()
					}
					if first {
						started.Done()
						first = false
					}
					if !ok {
						return
					}
				}
			})
		}
		started.Wait()
		r.Drop()
		wg.Wait()
		if n := res.released.Load(); n != 1 {
			t.Fatalf("the value was released %d times, want once", n)
		}
	}
}

// waitReleased runs the garbage collector until res is released.
func waitReleased(t *testing.T, res *resource) {
	t.Helper()

	for deadline := time.Now().Add(10 * time.Second); res.released.Load() == 0; {
		if time.Now().After(deadline) {
			t.Fatal("the garbage collector did not release a Ref that nobody could reach")
		}
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
}

// A Ref that is lost before it is dropped is released all the same.
func TestBackstop(t *testing.T) {
	t.Parallel()

	_, res := newRef(1)
	waitReleased(t, res)

	// And so is one that a reader acquired and lost.
	r, res := newRef(2)
	if _, ok := r.Acquire(); !ok {
		t.Fatal("Acquire failed")
	}
	r = nil
	runtime.KeepAlive(r)
	waitReleased(t, res)
	if n := res.released.Load(); n != 1 {
		t.Errorf("released %d times, want once", n)
	}
}

// A value that the backstop has released is not released again when the last
// reference goes after all, as when the two race.
func TestBackstopThenDrop(t *testing.T) {
	t.Parallel()

	r, res := newRef(1)
	ref.Backstop(r)
	r.Drop()
	if n := res.released.Load(); n != 1 {
		t.Errorf("released %d times, want once", n)
	}
}

// A Ref that was released at once is not released again by the collector.
func TestBackstopStopped(t *testing.T) {
	t.Parallel()

	r, res := newRef(1)
	r.Drop()
	r = nil
	runtime.KeepAlive(r)
	for range 5 {
		runtime.GC()
		time.Sleep(time.Millisecond)
	}
	if n := res.released.Load(); n != 1 {
		t.Errorf("released %d times, want once", n)
	}
}

func TestGet(t *testing.T) {
	t.Parallel()

	c := tinylfu.New[int, *ref.Ref[*resource]](10)
	if _, _, ok := ref.Get(c, 1); ok {
		t.Fatal("Get found a key that is not there")
	}
	old, oldRes := newRef(1)
	c.Set(1, old)
	r, v, ok := ref.Get(c, 1)
	if !ok || r != old || v != oldRes {
		t.Fatalf("Get = %v, %v, %t, want the stored Ref and its value", r, v, ok)
	}

	// The Ref that a Set pushes out stays acquired until it is released.
	fresh, _ := newRef(2)
	if _, out, displaced := c.Set(1, fresh); !displaced || out != old {
		t.Fatal("Set did not hand back the old Ref")
	}
	old.Drop()
	if oldRes.released.Load() != 0 {
		t.Fatal("the old value was released while a reader had it")
	}
	r.Release()
	if oldRes.released.Load() != 1 {
		t.Fatal("the old value was not released when its reader let go")
	}
	if _, v, ok := ref.Get(c, 1); !ok || v.id != 2 {
		t.Errorf("Get after the Set = %v, %t, want the new value", v, ok)
	}
}

// A Ref that is stored twice is dropped when its first copy is handed back,
// and the other copy reads as missing.
func TestGetStoredTwice(t *testing.T) {
	t.Parallel()

	c := lru.New[int, *ref.Ref[*resource]](10)
	r, res := newRef(1)
	c.Set(1, r)
	if _, out, displaced := c.Set(1, r); displaced {
		out.Drop()
	}
	if res.released.Load() != 1 {
		t.Fatal("dropping the cache's reference did not release the value")
	}
	if _, _, ok := ref.Get(c, 1); ok {
		t.Error("Get acquired a released value")
	}
}

func TestFetch(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	made := map[int][]*resource{}
	c := load.Wrap(lru.New[int, *ref.Ref[*resource]](1), func(_ context.Context, key int) (*ref.Ref[*resource], error) {
		r, res := newRef(key)
		mu.Lock()
		made[key] = append(made[key], res)
		mu.Unlock()
		return r, nil
	}, load.WithDropped(ref.Drop[int, *resource]))

	r, v, err := ref.Fetch(t.Context(), c, 1)
	if err != nil || v.id != 1 {
		t.Fatalf("Fetch = %v, %v, want the loaded value", v, err)
	}
	r.Release()
	if _, _, err := ref.Fetch(t.Context(), c, 2); err != nil { // Evicts 1.
		t.Fatal(err)
	}
	mu.Lock()
	first := made[1][0]
	mu.Unlock()
	// WithDropped runs in the load's goroutine, after the Fetch is back.
	for deadline := time.Now().Add(10 * time.Second); first.released.Load() == 0; time.Sleep(10 * time.Microsecond) {
		if time.Now().After(deadline) {
			t.Fatal("the value that the load evicted was not released")
		}
	}
}

// Fetch does not keep getting a Ref that is stored but released: it deletes
// the Ref and loads anew.
func TestFetchStoredTwice(t *testing.T) {
	t.Parallel()

	loads := 0
	c := load.Wrap(lru.New[int, *ref.Ref[*resource]](10), func(_ context.Context, key int) (*ref.Ref[*resource], error) {
		loads++
		r, _ := newRef(key)
		return r, nil
	})
	r, res := newRef(1)
	c.Set(1, r)
	if _, out, displaced := c.Set(1, r); displaced {
		out.Drop()
	}
	if res.released.Load() != 1 {
		t.Fatal("dropping the cache's reference did not release the value")
	}
	got, v, err := ref.Fetch(t.Context(), c, 1)
	if err != nil || got == r || v.released.Load() != 0 || loads != 1 {
		t.Fatalf("Fetch = %v, %v after %d loads, want a fresh value after one", v, err, loads)
	}
	got.Release()
}

// Drop fits the functions that expire hands back its entries to.
func TestPurgeExpired(t *testing.T) {
	t.Parallel()

	c := expire.Wrap(lru.New[int, expire.Entry[*ref.Ref[*resource]]](10), time.Hour)
	r, res := newRef(1)
	c.SetWithTTL(1, r, time.Nanosecond)
	for deadline := time.Now().Add(10 * time.Second); c.PurgeExpired(ref.Drop) == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the entry did not expire")
		}
	}
	if res.released.Load() != 1 {
		t.Error("PurgeExpired(ref.Drop) did not release the value")
	}
}

// Readers of a cache that reads without a lock never use a value after it is
// released, while writers replace entries and drop what is handed back; and
// every value is released once in the end.
func TestCacheConcurrent(t *testing.T) {
	t.Parallel()

	const (
		keys       = 16
		goroutines = 8
		requests   = 2000
	)
	c := tinylfu.New[int, *ref.Ref[*resource]](keys / 2)
	var (
		mu  sync.Mutex
		all []*resource
	)
	put := func(key int) {
		r, res := newRef(key)
		mu.Lock()
		all = append(all, res)
		mu.Unlock()
		if _, out, displaced := c.Set(key, r); displaced {
			out.Drop()
		}
	}
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(g), 9))
			for range requests {
				key := rng.IntN(keys)
				if rng.IntN(4) == 0 {
					put(key)
					continue
				}
				r, v, ok := ref.Get(c, key)
				if !ok {
					continue
				}
				if v.released.Load() != 0 || v.id != key {
					t.Errorf("Get(%d) acquired %d, released %d times", key, v.id, v.released.Load())
				}
				runtime.Gosched()
				if v.released.Load() != 0 {
					t.Errorf("the value of %d was released while acquired", key)
				}
				r.Release()
			}
		})
	}
	wg.Wait()
	c.DeleteFunc(func(_ int, r *ref.Ref[*resource]) bool {
		r.Drop()
		return true
	})
	for _, res := range all {
		if n := res.released.Load(); n != 1 {
			t.Errorf("value of %d released %d times, want once", res.id, n)
		}
	}
}
