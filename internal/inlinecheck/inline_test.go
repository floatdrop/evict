// Package inlinecheck guards the speed of the caches against a quirk of the
// compiler.
//
// The methods of the caches are generic, so they are compiled in the package
// of whoever uses them. The compiler inlines the calls they make there, such
// as locking a mutex or loading an atomic value, only if the packages of this
// module have passed on the bodies of the functions that are called, and they
// pass on only what they have inlined themselves. That is why each of them
// instantiates its own methods once. Should one of them forget a method, or
// should a helper grow too large to be inlined, nothing would break: every
// operation would just become slower by a few function calls, in everyone's
// programs but not in the benchmarks of this module, which import enough to
// make up for it. The test here notices.
package inlinecheck

import (
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// hot matches the functions that every operation on a cache goes through.
var hot = regexp.MustCompile(`/(lru|twoq|sieve|s3fifo|tinylfu)\.\(\*(Cache|shard)\[.*\]\)\.(Get|Peek|Set|set|evict|Delete|look|victim|promote|asked)$|` +
	`/c?slab\.\(\*Slab\[.*\]\)\.(Find|Lookup|Insert|Replace|Remove)$`)

// slow matches the calls that must not show up in them: functions that are
// small enough to be inlined, and are, as long as the compiler knows them.
var slow = regexp.MustCompile(`^sync\.\(\*(RW)?Mutex\)\.|^sync/atomic\.|` +
	`slab\.\(\*Slab\[.*\]\)\.(MoveToFront|PushFront|Unlink|Len|Cap|Overdue|Hash)$|` +
	`cslab\.\(\*(Readers|Slot)\)\.(Pin|Unpin)$|/s3fifo\.(tagOf|ghostOf)$|` +
	`/tinylfu\.(tagOf|kept|keep|printOf|printIn)$|/tinylfu\.\(\*climber\)\.(hit|miss)$`)

func TestHotPathsAreInlined(t *testing.T) {
	t.Parallel()

	if testing.Short() {
		t.Skip("compiles a package")
	}
	// What the compiler can inline depends on the platform. On these two
	// an atomic operation is an instruction or two. On others, 386 among
	// them, it is a call to a few lines of assembly whatever this module
	// does, and helpers that are made of such calls are too large to be
	// inlined in turn.
	switch runtime.GOARCH {
	case "amd64", "arm64":
	default:
		t.Skipf("what is inlined on %s is not what this test expects", runtime.GOARCH)
	}
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skip("there is no go command to compile with")
	}

	// -S makes the compiler print the code it generates for the package,
	// the instantiated methods of the caches included.
	// There is a package for each cache, so that none of them can count
	// on what another one has passed on to the compiler.
	//nolint:gosec // The command is the go tool, and its arguments are fixed.
	cmd := exec.CommandContext(t.Context(), goTool, "build", "-gcflags=-S", "./probe/...")
	// A build for another platform or with other flags would say nothing
	// about this one, and little about the caches.
	cmd.Env = append(os.Environ(), "GOFLAGS=")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	function, seen := "", map[string]bool{}
	scanner := bufio.NewScanner(bytes.NewReader(out))
	scanner.Buffer(nil, 1<<20)
	for scanner.Scan() {
		line := scanner.Text()

		// A function starts with a line like
		//	pkg.(*T[go.shape.int]).Method STEXT dupok size=...
		if name, _, ok := strings.Cut(line, " STEXT "); ok && !strings.HasPrefix(line, "\t") {
			function = name
			if hot.MatchString(function) {
				seen[hot.FindStringSubmatch(function)[0]] = true
			}
			continue
		}
		// A call in it looks like
		//	0x0040 00064 (file.go:12)	CALL	sync.(*Mutex).Lock(SB)
		_, target, ok := strings.Cut(line, "\tCALL\t")
		if !ok || !hot.MatchString(function) {
			continue
		}
		target = strings.TrimSuffix(target, "(SB)")
		if slow.MatchString(target) {
			t.Errorf("%s calls %s instead of inlining it", function, target)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	// The test is only worth something if it has looked at the right code.
	for _, pkg := range []string{"lru", "twoq", "sieve", "s3fifo", "tinylfu"} {
		for _, method := range []string{"Get", "Set", "Peek", "Delete"} {
			found := false
			for name := range seen {
				found = found || strings.Contains(name, "/"+pkg+".(*Cache[") && strings.HasSuffix(name, ")."+method)
			}
			if !found {
				t.Errorf("the compiler's output has no code for %s.Cache.%s; has its format changed?", pkg, method)
			}
		}
	}
}
