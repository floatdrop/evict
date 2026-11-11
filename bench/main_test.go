package bench

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"testing"
)

// TestMain says what the benchmarks were built with before it runs them, in
// the style of the lines that the testing package prints about the machine.
// cmd/benchtable passes it on to the reader of the results.
func TestMain(m *testing.M) {
	fmt.Printf("go: %s\n", runtime.Version())
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch dep.Path {
			case "github.com/maypok86/otter/v2":
				fmt.Printf("otter: %s\n", dep.Version)
			case "github.com/hashicorp/golang-lru/v2":
				fmt.Printf("golang-lru: %s\n", dep.Version)
			}
		}
	}
	os.Exit(m.Run())
}
