package stats

import "unsafe"

// StripeSize is the size of a stripe in bytes.
const StripeSize = unsafe.Sizeof(stripe{})

// StripeOf returns the stripe that the calling goroutine counts in.
func StripeOf[K comparable, V any](c *Cache[K, V]) any { return c.stripe() }
