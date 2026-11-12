// Package wrapuser uses the caches through package wrap, with key and value
// types of its own, and imports nothing else.
package wrapuser

import "github.com/floatdrop/evict/internal/inlinecheck/probe/wrap"

// Key is a key type that only this package instantiates the caches with.
type Key struct{ Tenant, ID string }

// Value is a value type that only this package instantiates the caches with.
type Value struct{ Size, Hits int }

// Use keeps the compiler from discarding the methods of the caches.
func Use(c wrap.Caches[Key, Value], k Key) int { return c.Read(k) }
