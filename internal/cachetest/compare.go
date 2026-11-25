package cachetest

import (
	"iter"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/floatdrop/evict"
)

// Kind is what an [Op] does.
type Kind uint8

// The operations that [Compare] understands.
const (
	Get Kind = iota
	Peek
	Set
	SetIfAbsent
	Delete
	DeleteFunc
	Clear
	GetMany
)

// Op is one operation on a cache.
type Op struct {
	Kind       Kind
	Key, Value int
}

// RandomOps returns n operations on keys in [0, keys): mostly reads and
// writes, some of them with SetIfAbsent, some deletions, and a rare Clear.
func RandomOps(rng *rand.Rand, keys, n int) iter.Seq[Op] {
	return func(yield func(Op) bool) {
		for i := range n {
			op := Op{Key: rng.IntN(keys), Value: i}
			switch p := rng.IntN(1000); {
			case p < 300:
				op.Kind = Get
			case p < 350:
				op.Kind = GetMany
			case p < 450:
				op.Kind = Peek
			case p < 800:
				op.Kind = Set
			case p < 900:
				op.Kind = SetIfAbsent
			case p < 998:
				op.Kind = Delete
			case p < 999:
				op.Kind = DeleteFunc
			default:
				op.Kind = Clear
			}
			if !yield(op) {
				return
			}
		}
	}
}

// BytesOps turns arbitrary bytes into operations on keys in [0, 32), one per
// byte. It gives a fuzzer something to mutate.
func BytesOps(data []byte) iter.Seq[Op] {
	return func(yield func(Op) bool) {
		for i, b := range data {
			op := Op{Key: int(b & 31), Value: i}
			switch b >> 5 {
			case 0:
				op.Kind = Get
			case 1:
				op.Kind = GetMany
			case 2:
				op.Kind = SetIfAbsent
			case 3:
				op.Kind = Peek
			case 4, 5:
				op.Kind = Set
			case 6:
				op.Kind = Delete
			default:
				// One byte in eight would be too many.
				op.Kind = Delete
				switch b {
				case 254:
					op.Kind = DeleteFunc
				case 255:
					op.Kind = Clear
				}
			}
			if !yield(op) {
				return
			}
		}
	}
}

// GetEach is GetMany for a model: it calls Get for each of keys. It panics if
// values or found is shorter than keys, as GetMany does.
func GetEach[K comparable, V any](c interface{ Get(K) (V, bool) }, keys []K, values []V, found []bool) {
	if len(values) < len(keys) || len(found) < len(keys) {
		panic("cachetest: GetEach: values and found must be at least as long as keys")
	}
	for i, key := range keys {
		values[i], found[i] = c.Get(key)
	}
}

// DeleteEach is DeleteFunc for a model: it goes over All and deletes the
// entries that del matches one by one, which is as good as at once for a
// cache that only one goroutine uses.
func DeleteEach[K comparable, V any](c evict.Cache[K, V], del func(K, V) bool) int {
	var keys []K
	for k, v := range c.All() {
		if del(k, v) {
			keys = append(keys, k)
		}
	}
	for _, k := range keys {
		c.Delete(k)
	}
	return len(keys)
}

// Compare applies the operations to both caches and fails the test as soon as
// they disagree: about what an operation returns, about how many entries
// there are, or about the entries that All yields and their order.
//
// It is meant for a cache with a single shard and a model of its policy that
// is simple enough to be evidently right.
func Compare(t *testing.T, got, want evict.Cache[int, int], ops iter.Seq[Op]) {
	t.Helper()

	type entry struct{ key, value int }
	entries := func(c evict.Cache[int, int]) []entry {
		var all []entry
		for k, v := range c.All() {
			all = append(all, entry{k, v})
		}
		return all
	}

	step := 0
	for op := range ops {
		switch op.Kind {
		case Get:
			gv, gok := got.Get(op.Key)
			wv, wok := want.Get(op.Key)
			if gv != wv || gok != wok {
				t.Fatalf("step %d: Get(%d) = %d, %t, want %d, %t", step, op.Key, gv, gok, wv, wok)
			}
		case GetMany:
			// Enough keys for the caches that look keys up in batches,
			// with one of them twice.
			keys := []int{op.Key, op.Key + 1, op.Key, op.Key + 2, op.Key + 3}
			gv, gf := make([]int, len(keys)), make([]bool, len(keys))
			wv, wf := make([]int, len(keys)), make([]bool, len(keys))
			got.GetMany(keys, gv, gf)
			want.GetMany(keys, wv, wf)
			if !slices.Equal(gv, wv) || !slices.Equal(gf, wf) {
				t.Fatalf("step %d: GetMany(%v) = %v, %v, want %v, %v", step, keys, gv, gf, wv, wf)
			}
		case Peek:
			gv, gok := got.Peek(op.Key)
			wv, wok := want.Peek(op.Key)
			if gv != wv || gok != wok {
				t.Fatalf("step %d: Peek(%d) = %d, %t, want %d, %t", step, op.Key, gv, gok, wv, wok)
			}
		case Set:
			gk, gv, gok := got.Set(op.Key, op.Value)
			wk, wv, wok := want.Set(op.Key, op.Value)
			if gk != wk || gv != wv || gok != wok {
				t.Fatalf("step %d: Set(%d, %d) = %d, %d, %t, want %d, %d, %t",
					step, op.Key, op.Value, gk, gv, gok, wk, wv, wok)
			}
		case SetIfAbsent:
			ga, gp, gk, gv, gok := got.SetIfAbsent(op.Key, op.Value)
			wa, wp, wk, wv, wok := want.SetIfAbsent(op.Key, op.Value)
			if ga != wa || gp != wp || gk != wk || gv != wv || gok != wok {
				t.Fatalf("step %d: SetIfAbsent(%d, %d) = %d, %t, %d, %d, %t, want %d, %t, %d, %d, %t",
					step, op.Key, op.Value, ga, gp, gk, gv, gok, wa, wp, wk, wv, wok)
			}
		case Delete:
			gv, gok := got.Delete(op.Key)
			wv, wok := want.Delete(op.Key)
			if gv != wv || gok != wok {
				t.Fatalf("step %d: Delete(%d) = %d, %t, want %d, %t", step, op.Key, gv, gok, wv, wok)
			}
		case DeleteFunc:
			del := func(k, v int) bool { return (k+v)%8 == op.Key%8 }
			if g, w := got.DeleteFunc(del), want.DeleteFunc(del); g != w {
				t.Fatalf("step %d: DeleteFunc = %d, want %d", step, g, w)
			}
		case Clear:
			got.Clear()
			want.Clear()
		}

		if g, w := got.Len(), want.Len(); g != w {
			t.Fatalf("step %d: Len() = %d, want %d", step, g, w)
		}
		// Reading everything back is slow for a large cache, so do it at
		// every step only at the start.
		if step < 256 || step%64 == 0 {
			if g, w := entries(got), entries(want); !slices.Equal(g, w) {
				t.Fatalf("step %d: All() yields %v, want %v", step, g, w)
			}
		}
		step++
	}

	if g, w := entries(got), entries(want); !slices.Equal(g, w) {
		t.Fatalf("after %d steps: All() yields %v, want %v", step, g, w)
	}
	if g, w := got.Cap(), want.Cap(); g != w {
		t.Fatalf("Cap() = %d, want %d", g, w)
	}
}
