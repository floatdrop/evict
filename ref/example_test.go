package ref_test

import (
	"fmt"

	"github.com/floatdrop/evict/lru"
	"github.com/floatdrop/evict/ref"
)

// conn stands for something that has to be closed, such as a connection.
type conn struct{ addr string }

func (c *conn) Close() { fmt.Println("closed", c.addr) }

func Example() {
	cache := lru.New[string, *ref.Ref[*conn]](1)
	set := func(addr string) {
		r := ref.New(&conn{addr}, (*conn).Close)
		if _, old, displaced := cache.Set(addr, r); displaced {
			old.Drop() // It closes once no reader has it.
		}
	}

	set("a")
	r, c, ok := ref.Get(cache, "a") // A reader acquires a.
	fmt.Println(c.addr, ok)
	set("b") // Evicts a, which the reader still uses.
	fmt.Println("the reader is done")
	r.Release()
	// Output:
	// a true
	// the reader is done
	// closed a
}
