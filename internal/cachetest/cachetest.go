// Package cachetest holds the tests that every cache in this module has to
// pass, whatever its eviction policy. What a policy evicts, and when, is for
// the tests of its own package.
package cachetest

import (
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
	"weak"

	"github.com/floatdrop/evict"
	"github.com/floatdrop/evict/internal/shards"
)

// Factory creates a cache with the given capacity and number of shards, where
// zero shards stands for the default of the cache.
type Factory[K comparable, V any] func(capacity, shards int) evict.Cache[K, V]

// Blob is a value that is large enough for the garbage collector to keep
// track of it on its own, which the tests for letting go of values rely on.
type Blob [32]byte

// Alive returns how many of the blobs are still alive after the garbage
// collector has had its chance, and gives the collector more chances while
// there are more than limit of them.
//
// What a cache holds on to stays alive however often the collector runs, so a
// cache that fails to let go is found out all the same. But the collector is
// not exact about stacks: of a goroutine that it stops in the middle of
// something, it searches the topmost frame for anything that looks like a
// pointer, and that may be what an earlier use of the memory has left behind.
// A blob that is alive for that reason is gone a few collections later, when
// the goroutine has moved on. The tests of this module have come across it on
// a 32-bit platform, where everything that a function is handed goes through
// the stack.
func Alive(refs []weak.Pointer[Blob], limit int) int {
	alive := 0
	for try := range 20 {
		if try > 0 {
			time.Sleep(time.Millisecond)
		}
		// The first collection finds the blobs unreachable; it takes
		// another one to be sure that the weak pointers know.
		runtime.GC()
		runtime.GC()
		alive = 0
		for _, ref := range refs {
			if ref.Value() != nil {
				alive++
			}
		}
		if alive <= limit {
			break
		}
	}
	return alive
}

// Factories make the caches that the suite needs.
type Factories struct {
	Int     Factory[int, int]
	String  Factory[string, string]
	Float   Factory[float64, int]
	Blob    Factory[int, *Blob]
	BlobKey Factory[*Blob, int]
	Wide    Factory[string, Wide]

	// Ghosts is the number of keys that a cache remembers after it has
	// evicted their entries, as a share of its capacity. Such keys stay
	// alive although the cache no longer holds an entry for them.
	Ghosts float64
}

// Run runs the suite against caches made by the factories. The tests in it
// run in parallel; see [NoAllocs] for the one that cannot.
func Run(t *testing.T, f Factories) {
	t.Helper()

	t.Run("Empty", func(t *testing.T) { t.Parallel(); testEmpty(t, f.Int) })
	t.Run("SetGet", func(t *testing.T) { t.Parallel(); testSetGet(t, f.Int) })
	t.Run("SetIfAbsent", func(t *testing.T) { t.Parallel(); testSetIfAbsent(t, f.Int) })
	t.Run("SetIfAbsentOnce", func(t *testing.T) { t.Parallel(); SetIfAbsentOnce(t, f.Int) })
	t.Run("ZeroCapacity", func(t *testing.T) { t.Parallel(); testZeroCapacity(t, f.Int) })
	t.Run("Full", func(t *testing.T) { t.Parallel(); testFull(t, f.Int) })
	t.Run("Clear", func(t *testing.T) { t.Parallel(); testClear(t, f.Int) })
	t.Run("All", func(t *testing.T) { t.Parallel(); testAll(t, f.Int) })
	t.Run("DeleteFunc", func(t *testing.T) { t.Parallel(); testDeleteFunc(t, f.Int) })
	t.Run("GetMany", func(t *testing.T) { t.Parallel(); testGetMany(t, f.Int) })
	t.Run("Strings", func(t *testing.T) { t.Parallel(); testStrings(t, f.String) })
	t.Run("Model", func(t *testing.T) { t.Parallel(); testModel(t, f.Int) })
	t.Run("Concurrent", func(t *testing.T) { t.Parallel(); testConcurrent(t, f.Int) })
	t.Run("Versions", func(t *testing.T) { t.Parallel(); Versions(t, f.Wide) })
	t.Run("Present", func(t *testing.T) { t.Parallel(); Present(t, f.Int) })
	t.Run("ClearAtOnce", func(t *testing.T) { t.Parallel(); ClearAtOnce(t, f.Int) })
	t.Run("KeyIdentity", func(t *testing.T) { t.Parallel(); testKeyIdentity(t, f.Float) })
	t.Run("LetsGo", func(t *testing.T) {
		t.Parallel()
		letsGo(t, f.Blob, 0, func(i int) (int, *Blob, *Blob) {
			blob := new(Blob)
			return i, blob, blob
		})
	})
	t.Run("LetsGoOfKeys", func(t *testing.T) {
		t.Parallel()
		letsGo(t, f.BlobKey, f.Ghosts, func(i int) (*Blob, int, *Blob) {
			blob := new(Blob)
			return blob, i, blob
		})
	})
}

func testEmpty(t *testing.T, newCache Factory[int, int]) {
	t.Helper()

	c := newCache(10, 1)

	if got := c.Cap(); got != 10 {
		t.Errorf("Cap() = %d, want 10", got)
	}
	if got := c.Len(); got != 0 {
		t.Errorf("Len() = %d, want 0", got)
	}
	if v, ok := c.Get(1); ok || v != 0 {
		t.Errorf("Get(1) = %d, %t on an empty cache", v, ok)
	}
	if v, ok := c.Peek(1); ok || v != 0 {
		t.Errorf("Peek(1) = %d, %t on an empty cache", v, ok)
	}
	if v, ok := c.Delete(1); ok || v != 0 {
		t.Errorf("Delete(1) = %d, %t on an empty cache", v, ok)
	}
	for k, v := range c.All() {
		t.Errorf("All() yields %d=%d on an empty cache", k, v)
	}
	c.Clear()
}

func testSetGet(t *testing.T, newCache Factory[int, int]) {
	t.Helper()

	c := newCache(10, 1)

	// Zero is a key like any other.
	for key := range 3 {
		if k, v, displaced := c.Set(key, key+100); displaced || k != 0 || v != 0 {
			t.Errorf("Set(%d) = %d, %d, %t, want nothing displaced", key, k, v, displaced)
		}
	}
	if got := c.Len(); got != 3 {
		t.Errorf("Len() = %d, want 3", got)
	}
	for key := range 3 {
		if v, ok := c.Get(key); !ok || v != key+100 {
			t.Errorf("Get(%d) = %d, %t, want %d", key, v, ok, key+100)
		}
		if v, ok := c.Peek(key); !ok || v != key+100 {
			t.Errorf("Peek(%d) = %d, %t, want %d", key, v, ok, key+100)
		}
	}
	if _, ok := c.Get(3); ok {
		t.Error("Get(3) found a key that was never set")
	}

	// Setting a key again hands back its previous value.
	if k, v, displaced := c.Set(1, 201); !displaced || k != 1 || v != 101 {
		t.Errorf("Set(1) = %d, %d, %t, want the previous entry 1, 101", k, v, displaced)
	}
	if v, ok := c.Get(1); !ok || v != 201 {
		t.Errorf("Get(1) = %d, %t, want 201", v, ok)
	}
	if got := c.Len(); got != 3 {
		t.Errorf("Len() = %d after an update, want 3", got)
	}

	if v, ok := c.Delete(1); !ok || v != 201 {
		t.Errorf("Delete(1) = %d, %t, want 201", v, ok)
	}
	if v, ok := c.Delete(1); ok || v != 0 {
		t.Errorf("second Delete(1) = %d, %t", v, ok)
	}
	if _, ok := c.Get(1); ok {
		t.Error("Get(1) found a deleted key")
	}
	if _, ok := c.Peek(1); ok {
		t.Error("Peek(1) found a deleted key")
	}
	if got := c.Len(); got != 2 {
		t.Errorf("Len() = %d after Delete, want 2", got)
	}

	// A deleted key can come back.
	if _, _, displaced := c.Set(1, 301); displaced {
		t.Error("Set(1) after Delete displaced an entry")
	}
	if v, ok := c.Get(1); !ok || v != 301 {
		t.Errorf("Get(1) = %d, %t, want 301", v, ok)
	}
}

func testSetIfAbsent(t *testing.T, newCache Factory[int, int]) {
	t.Helper()

	c := newCache(2, 1)

	// A key that is not there gets the value, which is a zero here: it is
	// present that tells whether the key was there, not the value.
	if a, present, k, v, evicted := c.SetIfAbsent(1, 0); present || a != 0 || evicted || k != 0 || v != 0 {
		t.Errorf("SetIfAbsent(1, 0) = %d, %t, %d, %d, %t, want 0 stored and nothing evicted", a, present, k, v, evicted)
	}
	if a, present, k, v, evicted := c.SetIfAbsent(1, 10); !present || a != 0 || evicted || k != 0 || v != 0 {
		t.Errorf("SetIfAbsent(1, 10) = %d, %t, %d, %d, %t, want the value 0 that 1 has", a, present, k, v, evicted)
	}
	if v, ok := c.Peek(1); !ok || v != 0 {
		t.Errorf("Peek(1) = %d, %t after SetIfAbsent found it, want 0", v, ok)
	}

	// What Set stores, SetIfAbsent finds.
	c.Set(1, 11)
	if a, present, _, _, _ := c.SetIfAbsent(1, 12); !present || a != 11 {
		t.Errorf("SetIfAbsent(1, 12) = %d, %t after Set(1, 11), want 11, true", a, present)
	}

	// A deleted key is absent again.
	c.Delete(1)
	if a, present, _, _, evicted := c.SetIfAbsent(1, 13); present || a != 13 || evicted {
		t.Errorf("SetIfAbsent(1, 13) = %d, %t, evicted %t after Delete, want 13 stored", a, present, evicted)
	}
	if got := c.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}

	// A full cache makes room as Set does, and says for whom.
	c.SetIfAbsent(2, 20)
	a, present, k, v, evicted := c.SetIfAbsent(3, 30)
	held := k == 1 && v == 13 || k == 2 && v == 20
	if present || a != 30 || !evicted || !held {
		t.Errorf("SetIfAbsent(3, 30) = %d, %t, %d, %d, %t, want 30 stored and 1=13 or 2=20 evicted", a, present, k, v, evicted)
	}
	if got := c.Len(); got != 2 {
		t.Errorf("Len() = %d, want 2", got)
	}
	if _, ok := c.Peek(k); ok {
		t.Errorf("Peek(%d) found the key that SetIfAbsent evicted", k)
	}
	if v, ok := c.Peek(3); !ok || v != 30 {
		t.Errorf("Peek(3) = %d, %t, want 30", v, ok)
	}
}

func testZeroCapacity(t *testing.T, newCache Factory[int, int]) {
	t.Helper()

	for _, capacity := range []int{0, -1} {
		for _, shards := range []int{0, 1, 4} {
			c := newCache(capacity, shards)

			if got := c.Cap(); got != 0 {
				t.Errorf("capacity %d: Cap() = %d, want 0", capacity, got)
			}
			if k, v, displaced := c.Set(1, 2); !displaced || k != 1 || v != 2 {
				t.Errorf("capacity %d: Set(1, 2) = %d, %d, %t, want the entry handed back", capacity, k, v, displaced)
			}
			if a, present, k, v, evicted := c.SetIfAbsent(1, 2); present || a != 2 || !evicted || k != 1 || v != 2 {
				t.Errorf("capacity %d: SetIfAbsent(1, 2) = %d, %t, %d, %d, %t, want the entry handed back", capacity, a, present, k, v, evicted)
			}
			if _, ok := c.Get(1); ok {
				t.Errorf("capacity %d: Get found a key", capacity)
			}
			if _, ok := c.Peek(1); ok {
				t.Errorf("capacity %d: Peek found a key", capacity)
			}
			if _, ok := c.Delete(1); ok {
				t.Errorf("capacity %d: Delete found a key", capacity)
			}
			if got := c.Len(); got != 0 {
				t.Errorf("capacity %d: Len() = %d, want 0", capacity, got)
			}
			for range c.All() {
				t.Errorf("capacity %d: All() yields an entry", capacity)
			}
			c.Clear()
		}
	}
}

// testFull checks that a cache holds as much as it says and no more.
func testFull(t *testing.T, newCache Factory[int, int]) {
	t.Helper()

	for _, tt := range []struct{ capacity, shards int }{
		{1, 1}, {2, 1}, {3, 1}, {100, 1}, {100, 4}, {7, 8}, {5000, 0},
	} {
		c := newCache(tt.capacity, tt.shards)
		if got := c.Cap(); got != tt.capacity {
			t.Errorf("capacity %d, %d shards: Cap() = %d", tt.capacity, tt.shards, got)
		}

		displacements := 0
		for key := range 3 * tt.capacity {
			if k, _, displaced := c.Set(key, key); displaced {
				if k == key {
					t.Fatalf("capacity %d, %d shards: Set(%d) displaced the key it was setting", tt.capacity, tt.shards, key)
				}
				displacements++
			}
			if got := c.Len(); got != key+1-displacements {
				t.Fatalf("capacity %d, %d shards: Len() = %d after %d insertions and %d evictions",
					tt.capacity, tt.shards, got, key+1, displacements)
			}
			if got := c.Len(); got > tt.capacity {
				t.Fatalf("capacity %d, %d shards: Len() = %d", tt.capacity, tt.shards, got)
			}
		}

		// A cache in one piece is full before it evicts anything.
		if tt.shards == 1 && c.Len() != tt.capacity {
			t.Errorf("capacity %d, one shard: Len() = %d after %d insertions", tt.capacity, c.Len(), 3*tt.capacity)
		}
	}
}

func testClear(t *testing.T, newCache Factory[int, int]) {
	t.Helper()

	for _, shards := range []int{1, 4} {
		c := newCache(64, shards)
		for round := range 3 {
			for key := range 200 {
				c.Set(key, key)
				c.Get(key / 2)
			}
			for key := range 50 {
				c.Delete(key * 3)
			}

			c.Clear()

			if got := c.Len(); got != 0 {
				t.Fatalf("round %d: Len() = %d after Clear", round, got)
			}
			for key := range 200 {
				if _, ok := c.Peek(key); ok {
					t.Fatalf("round %d: Peek(%d) found a key after Clear", round, key)
				}
			}
			for k := range c.All() {
				t.Fatalf("round %d: All() yields %d after Clear", round, k)
			}
		}

		// A cleared cache has all of its capacity back.
		if shards == 1 {
			for key := range 64 {
				if _, _, displaced := c.Set(key, key); displaced {
					t.Fatalf("Set(%d) displaced an entry in a cleared cache", key)
				}
			}
			if got := c.Len(); got != 64 {
				t.Errorf("Len() = %d, want 64", got)
			}
		}
	}
}

func testAll(t *testing.T, newCache Factory[int, int]) {
	t.Helper()

	for _, shards := range []int{1, 8} {
		c := newCache(1000, shards)
		want := map[int]int{}
		for key := range 300 {
			c.Set(key, -key)
			want[key] = -key
		}
		for key := range 300 {
			if key%3 == 0 {
				c.Delete(key)
				delete(want, key)
			}
		}

		got := map[int]int{}
		for k, v := range c.All() {
			if _, seen := got[k]; seen {
				t.Fatalf("%d shards: All() yields %d twice", shards, k)
			}
			got[k] = v
		}
		if !maps.Equal(got, want) {
			t.Fatalf("%d shards: All() yields %d entries, want %d: %v", shards, len(got), len(want), got)
		}

		// The loop can stop early.
		n := 0
		for range c.All() {
			n++
			if n == 10 {
				break
			}
		}
		if n != 10 {
			t.Errorf("%d shards: stopped after %d entries, want 10", shards, n)
		}

		// The loop body can use the cache, and what it does is not undone
		// by the iteration.
		for k := range c.All() {
			c.Get(k)
			c.Set(k+1000, k)
			c.Delete(k)
			c.Len()
		}
		for k := range want {
			if _, ok := c.Peek(k); ok {
				t.Fatalf("%d shards: key %d survived being deleted during iteration", shards, k)
			}
		}
	}
}

func testGetMany(t *testing.T, newCache Factory[int, int]) {
	t.Helper()

	for _, tt := range []struct{ capacity, shards int }{{0, 1}, {100, 1}, {100, 8}, {1000, 0}} {
		c := newCache(tt.capacity, tt.shards)
		for key := range tt.capacity {
			c.Set(key, -key)
		}
		// Keys that are there and keys that are not, in an order that
		// goes from shard to shard, more than a batch of the caches
		// that look keys up in batches.
		keys := rand.New(rand.NewPCG(1, 2)).Perm(tt.capacity + 100)
		want := make([]int, len(keys))
		wantFound := make([]bool, len(keys))
		for i, key := range keys {
			want[i], wantFound[i] = c.Peek(key)
		}

		values, found := make([]int, len(keys)), make([]bool, len(keys))
		for i := range values {
			values[i] = 7 // GetMany has to overwrite what it does not find.
		}
		c.GetMany(keys, values, found)
		for i, key := range keys {
			if values[i] != want[i] || found[i] != wantFound[i] {
				t.Fatalf("capacity %d, %d shards: GetMany found %d, %t for %d, want %d, %t",
					tt.capacity, tt.shards, values[i], found[i], key, want[i], wantFound[i])
			}
		}

		// values may be keys itself.
		aliased := slices.Clone(keys)
		c.GetMany(aliased, aliased, found)
		if !slices.Equal(aliased, want) {
			t.Fatalf("capacity %d, %d shards: GetMany into its keys found %v, want %v", tt.capacity, tt.shards, aliased, want)
		}

		// Slices that are too short make GetMany panic before it writes
		// anything, keys that it does not find included.
		c.GetMany(nil, nil, nil)
		for i := range found {
			found[i] = true
		}
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("capacity %d, %d shards: GetMany with too few values did not panic", tt.capacity, tt.shards)
				}
			}()
			c.GetMany(keys, values[:len(keys)-1], found)
		}()
		if slices.Contains(found, false) {
			t.Errorf("capacity %d, %d shards: GetMany wrote found before it panicked", tt.capacity, tt.shards)
		}
	}
}

func testDeleteFunc(t *testing.T, newCache Factory[int, int]) {
	t.Helper()

	for _, shards := range []int{1, 8} {
		c := newCache(80, shards)
		// Fill the queues and give policies that keep ghosts some keys to
		// remember. Only resident entries should be passed to del.
		for key := range 300 {
			c.Set(key, -key)
		}
		want := maps.Collect(c.All())
		seen := map[int]int{}
		n := c.DeleteFunc(func(k, v int) bool {
			if _, ok := seen[k]; ok {
				t.Errorf("DeleteFunc visited %d twice", k)
			}
			seen[k] = v
			return k%2 == 0
		})
		if !maps.Equal(seen, want) {
			t.Fatalf("DeleteFunc visited %v, want %v", seen, want)
		}
		maps.DeleteFunc(want, func(k, _ int) bool { return k%2 == 0 })
		if removed := len(seen) - len(want); n != removed {
			t.Errorf("DeleteFunc = %d, want %d", n, removed)
		}
		if c.Len() != len(want) || !maps.Equal(maps.Collect(c.All()), want) {
			t.Fatal("DeleteFunc did not leave just the nonmatching entries")
		}
	}

	// A panic leaves the earlier deletions in place and the shard unlocked.
	c := newCache(2, 1)
	c.Set(1, 1)
	c.Set(2, 2)
	const stop = "stop the scan"
	func() {
		defer func() {
			if p := recover(); p != stop {
				t.Errorf("DeleteFunc panic = %v, want %q", p, stop)
			}
		}()
		first := true
		c.DeleteFunc(func(_, _ int) bool {
			if !first {
				panic(stop)
			}
			first = false
			return true
		})
	}()
	if n := c.DeleteFunc(func(_, _ int) bool { return true }); n != 1 {
		t.Errorf("DeleteFunc after panic = %d, want 1", n)
	}
}

func testStrings(t *testing.T, newCache Factory[string, string]) {
	t.Helper()

	c := newCache(100, 0)

	// The empty string is a key like any other.
	keys := []string{"", "a", "b", "ab", "ba", "a\x00", "\x00a", "日本語", string(make([]byte, 1000))}
	for _, key := range keys {
		if _, _, displaced := c.Set(key, "value of "+key); displaced {
			t.Errorf("Set(%q) displaced an entry", key)
		}
	}
	if got := c.Len(); got != len(keys) {
		t.Errorf("Len() = %d, want %d", got, len(keys))
	}
	for _, key := range keys {
		if v, ok := c.Get(key); !ok || v != "value of "+key {
			t.Errorf("Get(%q) = %q, %t", key, v, ok)
		}
	}
	if _, ok := c.Get("c"); ok {
		t.Error(`Get("c") found a key that was never set`)
	}

	if k, v, displaced := c.Set("", "again"); !displaced || k != "" || v != "value of " {
		t.Errorf(`Set("") = %q, %q, %t, want the previous entry`, k, v, displaced)
	}
	if v, ok := c.Delete(""); !ok || v != "again" {
		t.Errorf(`Delete("") = %q, %t, want "again"`, v, ok)
	}
	if _, ok := c.Get(""); ok {
		t.Error(`Get("") found a deleted key`)
	}
	if v, ok := c.Get("a"); !ok || v != "value of a" {
		t.Errorf(`Get("a") = %q, %t after deleting ""`, v, ok)
	}
}

// testModel drives caches of many shapes with random operations and checks
// every answer against a map.
//
// It works for any eviction policy because a cache gives a full account of
// what it drops: an entry is in the cache if and only if it was set and has
// not been deleted, replaced, or reported as displaced since.
func testModel(t *testing.T, newCache Factory[int, int]) {
	t.Helper()

	tests := []struct {
		capacity, shards int
		keys             int // Keys are drawn from [0, keys).
		steps            int
	}{
		{capacity: 1, shards: 1, keys: 4, steps: 5_000},
		{capacity: 2, shards: 1, keys: 5, steps: 5_000},
		{capacity: 3, shards: 1, keys: 8, steps: 5_000},
		{capacity: 4, shards: 4, keys: 16, steps: 5_000},
		{capacity: 7, shards: 1, keys: 20, steps: 10_000},
		{capacity: 64, shards: 1, keys: 50, steps: 20_000}, // Never full.
		{capacity: 64, shards: 1, keys: 200, steps: 20_000},
		{capacity: 100, shards: 64, keys: 400, steps: 20_000},
		{capacity: 1000, shards: 1, keys: 1500, steps: 50_000},
		{capacity: 1000, shards: 8, keys: 5000, steps: 50_000},
		{capacity: 5000, shards: 0, keys: 20_000, steps: 100_000},
		// Enough keys for some of them to agree in the 32 bits of the hash
		// that the shards index their entries by.
		{capacity: 300_000, shards: 1, keys: 600_000, steps: 3_000_000},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("capacity=%d/shards=%d/keys=%d", tt.capacity, tt.shards, tt.keys), func(t *testing.T) {
			t.Parallel()
			if testing.Short() && tt.steps > 100_000 {
				t.Skip("long")
			}

			rng := rand.New(rand.NewPCG(uint64(i), 42))
			c := newCache(tt.capacity, tt.shards)
			model := make(map[int]int, tt.capacity)

			// Comparing every entry takes time in proportion to the
			// capacity, so a large cache is compared less often.
			checkEvery := max(1024, 16*tt.capacity)

			for step := range tt.steps {
				key := rng.IntN(tt.keys)
				want, present := model[key]

				switch op := rng.IntN(100); {
				case op < 40:
					if v, ok := c.Get(key); ok != present || v != want {
						t.Fatalf("step %d: Get(%d) = %d, %t, want %d, %t", step, key, v, ok, want, present)
					}

				case op < 50:
					if v, ok := c.Peek(key); ok != present || v != want {
						t.Fatalf("step %d: Peek(%d) = %d, %t, want %d, %t", step, key, v, ok, want, present)
					}

				case op < 60:
					actual, found, oldKey, oldValue, evicted := c.SetIfAbsent(key, step)
					if present {
						if !found || actual != want || evicted {
							t.Fatalf("step %d: SetIfAbsent(%d) = %d, %t, %d, %d, %t, want the value %d that is there",
								step, key, actual, found, oldKey, oldValue, evicted, want)
						}
						break
					}
					if found || actual != step {
						t.Fatalf("step %d: SetIfAbsent(%d) = %d, %t for a key that is not there", step, key, actual, found)
					}
					if evicted {
						if tt.shards == 1 && len(model) < tt.capacity {
							t.Fatalf("step %d: SetIfAbsent(%d) evicted %d=%d from a cache that holds %d entries of %d",
								step, key, oldKey, oldValue, len(model), tt.capacity)
						}
						gone, ok := model[oldKey]
						if !ok || gone != oldValue || oldKey == key {
							t.Fatalf("step %d: SetIfAbsent(%d) evicted %d=%d, which the cache did not hold (%d, %t)",
								step, key, oldKey, oldValue, gone, ok)
						}
						delete(model, oldKey)
					}
					model[key] = step
					if len(model) > tt.capacity {
						t.Fatalf("step %d: SetIfAbsent(%d) took the cache past its capacity", step, key)
					}

				case op < 90:
					oldKey, oldValue, displaced := c.Set(key, step)
					switch {
					case present:
						if !displaced || oldKey != key || oldValue != want {
							t.Fatalf("step %d: Set(%d) = %d, %d, %t, want the previous entry %d, %d",
								step, key, oldKey, oldValue, displaced, key, want)
						}
					case displaced && tt.shards == 1 && len(model) < tt.capacity:
						t.Fatalf("step %d: Set(%d) displaced %d=%d from a cache that holds %d entries of %d",
							step, key, oldKey, oldValue, len(model), tt.capacity)
					case displaced:
						evicted, ok := model[oldKey]
						if !ok || evicted != oldValue || oldKey == key {
							t.Fatalf("step %d: Set(%d) displaced %d=%d, which the cache did not hold (%d, %t)",
								step, key, oldKey, oldValue, evicted, ok)
						}
						delete(model, oldKey)
					}
					model[key] = step
					if len(model) > tt.capacity {
						t.Fatalf("step %d: Set(%d) took the cache past its capacity", step, key)
					}

				default:
					if v, ok := c.Delete(key); ok != present || v != want {
						t.Fatalf("step %d: Delete(%d) = %d, %t, want %d, %t", step, key, v, ok, want, present)
					}
					delete(model, key)
				}

				if got := c.Len(); got != len(model) {
					t.Fatalf("step %d: Len() = %d, want %d", step, got, len(model))
				}
				if step%checkEvery == 0 || step == tt.steps-1 {
					checkAgainstModel(t, c, model, step)
				}

				// Start over once, when the cache has seen some use.
				if step == tt.steps/2 {
					c.Clear()
					clear(model)
				}
			}
		})
	}
}

func checkAgainstModel(t *testing.T, c evict.Cache[int, int], model map[int]int, step int) {
	t.Helper()

	seen := 0
	for k, v := range c.All() {
		if want, ok := model[k]; !ok || v != want {
			t.Fatalf("step %d: All() yields %d=%d, want %d, %t", step, k, v, want, ok)
		}
		seen++
	}
	if seen != len(model) {
		t.Fatalf("step %d: All() yields %d entries, want %d", step, seen, len(model))
	}
}

// valueOf ties a value to its key, so that an entry that got mixed up with
// another one is recognized wherever it surfaces.
func valueOf(key int) int { return key*31 + 7 }

// testConcurrent hammers a cache from several goroutines. Its main purpose is
// to give the race detector something to look at; on top of that, every entry
// that comes out of the cache must be one that went in.
func testConcurrent(t *testing.T, newCache Factory[int, int]) {
	t.Helper()

	const goroutines = 8

	steps := 40_000
	if testing.Short() {
		steps = 4_000
	}

	for _, tt := range []struct{ capacity, shards int }{{1, 1}, {64, 1}, {1000, 4}, {4096, 0}} {
		t.Run(fmt.Sprintf("capacity=%d/shards=%d", tt.capacity, tt.shards), func(t *testing.T) {
			t.Parallel()

			c := newCache(tt.capacity, tt.shards)
			keys := 3*tt.capacity + 1

			check := func(op string, key, value int) {
				if value != valueOf(key) {
					t.Errorf("%s: key %d has value %d, want %d", op, key, value, valueOf(key))
				}
			}

			var wg sync.WaitGroup
			for g := range goroutines {
				wg.Go(func() {
					rng := rand.New(rand.NewPCG(uint64(g), 7))
					batch, values, found := make([]int, 8), make([]int, 8), make([]bool, 8)
					for range steps {
						key := rng.IntN(keys)

						switch op := rng.IntN(1000); {
						case op < 450:
							if v, ok := c.Get(key); ok {
								check("Get", key, v)
							}
						case op < 500:
							for i := range batch {
								batch[i] = (key + i) % keys
							}
							c.GetMany(batch, values, found)
							for i, k := range batch {
								if found[i] {
									check("GetMany", k, values[i])
								}
							}
						case op < 600:
							if v, ok := c.Peek(key); ok {
								check("Peek", key, v)
							}
						case op < 800:
							if k, v, displaced := c.Set(key, valueOf(key)); displaced {
								check("Set", k, v)
							}
						case op < 900:
							actual, _, k, v, evicted := c.SetIfAbsent(key, valueOf(key))
							check("SetIfAbsent", key, actual)
							if evicted {
								check("SetIfAbsent", k, v)
							}
						case op < 990:
							if v, ok := c.Delete(key); ok {
								check("Delete", key, v)
							}
						case op < 995:
							if n := c.Len(); n < 0 || n > tt.capacity {
								t.Errorf("Len() = %d with capacity %d", n, tt.capacity)
							}
						case op < 996:
							if n := c.DeleteFunc(func(k, v int) bool {
								check("DeleteFunc", k, v)
								return k == key
							}); n > 1 {
								t.Errorf("DeleteFunc removed %d entries of key %d", n, key)
							}
						case op < 999:
							n := 0
							for k, v := range c.All() {
								check("All", k, v)
								n++
							}
							if n > tt.capacity {
								t.Errorf("All() yields %d entries with capacity %d", n, tt.capacity)
							}
						default:
							c.Clear()
						}
					}
				})
			}
			wg.Wait()

			if n := c.Len(); n > tt.capacity {
				t.Errorf("Len() = %d with capacity %d", n, tt.capacity)
			}
			for k, v := range c.All() {
				check("All", k, v)
			}
		})
	}
}

// testKeyIdentity checks which of two keys that are equal, but not the same,
// a cache keeps when an entry is replaced: the new one, as a map does. The
// old one is handed back, so that whatever it refers to can be released.
func testKeyIdentity(t *testing.T, newCache Factory[float64, int]) {
	t.Helper()

	// Zero and negative zero are equal and can be told apart.
	zero, negative := 0.0, math.Copysign(0, -1)

	for _, shards := range []int{1, 4} {
		c := newCache(16, shards)
		c.Set(zero, 1)

		k, v, displaced := c.Set(negative, 2)
		if !displaced || math.Signbit(k) || v != 1 {
			t.Errorf("%d shards: Set(-0) = %v, %d, %t, want the entry +0, 1 that it replaced", shards, k, v, displaced)
		}
		for k, v := range c.All() {
			if !math.Signbit(k) || v != 2 {
				t.Errorf("%d shards: the cache holds %v=%d, want -0=2", shards, k, v)
			}
		}
		if v, ok := c.Get(zero); !ok || v != 2 {
			t.Errorf("%d shards: Get(+0) = %d, %t, want 2", shards, v, ok)
		}
		if got := c.Len(); got != 1 {
			t.Errorf("%d shards: Len() = %d, want 1", shards, got)
		}

		// SetIfAbsent finds an equal key, and changes nothing about it.
		if a, present, _, _, _ := c.SetIfAbsent(zero, 9); !present || a != 2 {
			t.Errorf("%d shards: SetIfAbsent(+0) = %d, %t, want 2, true", shards, a, present)
		}
		for k := range c.All() {
			if !math.Signbit(k) {
				t.Errorf("%d shards: SetIfAbsent(+0) replaced the key -0", shards)
			}
		}

		// A key that is not equal to itself is one that nobody can ask for.
		// Its entries take up room like any other until they are evicted.
		nan := math.NaN()
		c.Set(nan, 3)
		if _, _, displaced := c.Set(nan, 4); displaced {
			t.Errorf("%d shards: Set(NaN) replaced an entry", shards)
		}
		if _, ok := c.Get(nan); ok {
			t.Errorf("%d shards: Get(NaN) found an entry", shards)
		}
		if _, ok := c.Delete(nan); ok {
			t.Errorf("%d shards: Delete(NaN) found an entry", shards)
		}
		nans := 0
		for k := range c.All() {
			if math.IsNaN(k) {
				nans++
			}
		}
		if nans != 2 || c.Len() != 3 {
			t.Errorf("%d shards: the cache holds %d entries, %d of them under NaN, want 3 and 2", shards, c.Len(), nans)
		}
		if a, present, _, _, _ := c.SetIfAbsent(nan, 5); present || a != 5 {
			t.Errorf("%d shards: SetIfAbsent(NaN) = %d, %t, want 5 stored", shards, a, present)
		}
		if n := c.DeleteFunc(func(k float64, _ int) bool { return math.IsNaN(k) }); n != 3 || c.Len() != 1 {
			t.Errorf("%d shards: DeleteFunc(NaN) = %d, Len() = %d, want 3, 1", shards, n, c.Len())
		}
		for i := range 1000 {
			c.Set(float64(i), i)
			c.Set(nan, i)
		}
		if got := c.Len(); got > c.Cap() {
			t.Errorf("%d shards: Len() = %d with capacity %d", shards, got, c.Cap())
		}
		c.Clear()
		if got := c.Len(); got != 0 {
			t.Errorf("%d shards: Len() = %d after Clear", shards, got)
		}
	}
}

// letsGo checks that a cache does not keep alive what it no longer holds: not
// many of the entries it has evicted or deleted, and none at all once it has
// been cleared. The entries are made by entry, which also returns the part of
// the entry, its key or its value, that the check is about. A cache may keep
// that part of as many entries it has dropped as the share ghosts of its
// capacity.
func letsGo[K comparable, V any](t *testing.T, newCache Factory[K, V], ghosts float64, entry func(i int) (K, V, *Blob)) {
	t.Helper()

	for _, tt := range []struct{ capacity, shards int }{{1, 1}, {64, 1}, {1000, 4}, {4096, 0}} {
		c := newCache(tt.capacity, tt.shards)
		// A cache may be slow to let go of what it has dropped, but only
		// to the tune of a fraction of its capacity.
		slack := tt.capacity/8 + 8 + int(ghosts*float64(tt.capacity))

		refs := make([]weak.Pointer[Blob], 0, 10*tt.capacity)
		for i := range 10 * tt.capacity {
			key, value, blob := entry(i)
			refs = append(refs, weak.Make(blob))
			c.Set(key, value)
			if i%3 == 0 {
				c.Set(key, value) // Replacing an entry is a way of dropping one, too.
			}
			if i%7 == 0 {
				c.Delete(key)
			}
		}
		if got, limit := Alive(refs, c.Len()+slack), c.Len()+slack; got > limit {
			t.Errorf("capacity %d, %d shards: %d blobs are alive with %d entries in the cache, want at most %d",
				tt.capacity, tt.shards, got, c.Len(), limit)
		}

		// Deleting the entries one by one must not leave them behind
		// either. Nothing is added here, so there is nothing that would
		// overwrite what a cache failed to release.
		var keys []K
		for key := range c.All() {
			keys = append(keys, key)
		}
		for _, key := range keys {
			c.Delete(key)
		}
		clear(keys)
		if got := Alive(refs, slack); c.Len() != 0 || got > slack {
			t.Errorf("capacity %d, %d shards: %d blobs are alive and Len() = %d after every entry was deleted, want at most %d and 0",
				tt.capacity, tt.shards, got, c.Len(), slack)
		}

		for i := range tt.capacity {
			key, value, blob := entry(i)
			refs = append(refs, weak.Make(blob))
			c.Set(key, value)
		}
		c.Clear()
		if got := Alive(refs, 0); got != 0 {
			t.Errorf("capacity %d, %d shards: %d blobs are alive after Clear", tt.capacity, tt.shards, got)
		}

		// With no readers to wait for, a sweep releases even a single
		// removed entry; it need not wait for the next batch of writes.
		key, value, blob := entry(0)
		c.Set(key, value)
		ref := weak.Make(blob)
		if n := c.DeleteFunc(func(K, V) bool { return true }); n != 1 {
			t.Fatalf("DeleteFunc = %d, want 1", n)
		}
		if got := Alive([]weak.Pointer[Blob]{ref}, 0); got != 0 {
			t.Error("DeleteFunc kept a removed entry alive")
		}

		// Keep the cache itself reachable until here; otherwise there
		// would be nothing to check.
		runtime.KeepAlive(c)
	}
}

// NoAllocs checks that a cache does not allocate once it exists: not when it
// stores, replaces, finds, misses, evicts or deletes an entry, and not when
// it is cleared. Counting allocations does not work while other tests are
// running, so the calling test must not be parallel.
func NoAllocs(t *testing.T, f Factories) {
	t.Helper()

	const capacity = 1000

	// Every allocation counts, also one that happens only now and then, so
	// a long run is measured as a whole rather than averaged over its steps.
	measure := func(t *testing.T, run func()) {
		t.Helper()
		if allocs := testing.AllocsPerRun(1, run); allocs != 0 {
			t.Errorf("%v allocations, want 0", allocs)
		}
	}

	t.Run("int", func(t *testing.T) {
		c := f.Int(capacity, 0)
		key := 0
		measure(t, func() {
			for range 20 * capacity {
				key++
				c.Set(key, key)  // Stores an entry and, the cache being full, evicts one.
				c.Set(key, -key) // Replaces it.
				c.Get(key)       // Finds it.
				c.Get(-key)      // Misses.
				c.Peek(key)      // Finds it again.
				if key%8 == 0 {
					c.Delete(key - 1)         // Deletes the one before.
					c.SetIfAbsent(key-1, key) // Stores it again.
				}
				c.SetIfAbsent(key, key)   // Finds it once more.
				c.SetIfAbsent(-key, -key) // Stores an entry and evicts one.
				c.Len()
			}
			if c.Len() < capacity/2 {
				t.Errorf("the cache holds %d entries, so it has hardly evicted any", c.Len())
			}
			c.Clear()
		})
	})

	t.Run("string", func(t *testing.T) {
		keys := make([]string, 4*capacity)
		for i := range keys {
			keys[i] = "key-" + strconv.Itoa(i)
		}

		c := f.String(capacity, 0)
		measure(t, func() {
			for i := range 20 * capacity {
				key := keys[i%len(keys)]
				c.Set(key, key)
				c.Set(key, "another value")
				c.Get(key)
				c.Get("missing")
				c.Peek(key)
				c.SetIfAbsent(keys[(i+7)%len(keys)], key)
				if i%3 == 0 {
					c.Delete(key)
				}
			}
			if c.Len() < capacity/2 {
				t.Errorf("the cache holds %d entries, so it has hardly evicted any", c.Len())
			}
			c.Clear()
		})
	})
}

// Padded checks that the shards of a cache, which lie side by side in an
// array, end in at least a cache line of padding and take up whole lines.
func Padded[S any](t *testing.T, _ []S) {
	t.Helper()

	typ := reflect.TypeFor[S]()
	if typ.Kind() != reflect.Struct || typ.NumField() == 0 {
		t.Fatalf("%v is not a struct with fields", typ)
	}
	last := typ.Field(typ.NumField() - 1)
	if last.Name != "_" || last.Type.Size() < shards.Line {
		t.Errorf("%v ends with the field %q of %d bytes; want padding of %d bytes or more",
			typ, last.Name, last.Type.Size(), shards.Line)
	}
	if typ.Size()%shards.Line != 0 {
		t.Errorf("%v takes %d bytes; want a multiple of %d", typ, typ.Size(), shards.Line)
	}
}
