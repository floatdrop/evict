//go:build !race

package cachetest

// Race reports whether the tests were built with the race detector, which
// changes what a test may expect of the timing of the code under test: it
// puts calls into code that has none.
const Race = false
