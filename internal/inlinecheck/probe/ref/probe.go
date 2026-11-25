// Package probe uses counted references the way a program does: with a value
// type of its own, and without importing anything but the package that counts
// them. The test two directories up compiles it and looks at what the compiler
// made of it.
package probe

import "github.com/floatdrop/evict/ref"

// Value is a value type that only this package instantiates a Ref with.
type Value struct{ Size, Hits int }

// Use keeps the compiler from discarding the methods of the Ref.
func Use(r *ref.Ref[Value]) (Value, bool) {
	v, ok := r.Acquire()
	if ok {
		r.Release()
	}
	r.Drop()
	return v, ok
}
