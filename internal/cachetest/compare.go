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
	Delete
	Clear
)

// Op is one operation on a cache.
type Op struct {
	Kind       Kind
	Key, Value int
}

// RandomOps returns n operations on keys in [0, keys): mostly reads and
// writes, some deletions, and a rare Clear.
func RandomOps(rng *rand.Rand, keys, n int) iter.Seq[Op] {
	return func(yield func(Op) bool) {
		for i := range n {
			op := Op{Key: rng.IntN(keys), Value: i}
			switch p := rng.IntN(1000); {
			case p < 400:
				op.Kind = Get
			case p < 500:
				op.Kind = Peek
			case p < 900:
				op.Kind = Set
			case p < 999:
				op.Kind = Delete
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
			case 0, 1, 2:
				op.Kind = Get
			case 3:
				op.Kind = Peek
			case 4, 5:
				op.Kind = Set
			case 6:
				op.Kind = Delete
			default:
				// One byte in eight would be too many.
				if op.Kind = Delete; b == 255 {
					op.Kind = Clear
				}
			}
			if !yield(op) {
				return
			}
		}
	}
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
		case Delete:
			gv, gok := got.Delete(op.Key)
			wv, wok := want.Delete(op.Key)
			if gv != wv || gok != wok {
				t.Fatalf("step %d: Delete(%d) = %d, %t, want %d, %t", step, op.Key, gv, gok, wv, wok)
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
