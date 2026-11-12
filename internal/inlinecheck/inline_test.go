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
var hot = regexp.MustCompile(`/(lru|twoq|sieve|s3fifo|tinylfu|stats)\.\(\*(Cache|shard)\[.*\]\)\.(Get|GetMany|Peek|Set|set|evict|Delete|look|victim|promote|asked)$|` +
	`/c?slab\.\(\*Slab\[.*\]\)\.(Find|Lookup|Insert|Replace|Remove)$`)

// slow matches the calls that must not show up in them: functions that are
// small enough to be inlined, and are, as long as the compiler knows them.
var slow = regexp.MustCompile(`^sync\.\(\*(RW)?Mutex\)\.|^sync/atomic\.|` +
	`slab\.\(\*Slab\[.*\]\)\.(MoveToFront|PushFront|Unlink|Len|Cap|Overdue|Hash|pin|preferred|Touch|TouchNode)$|` +
	`cslab\.\(\*Node\[.*\]\)\.(Meta|SetMeta)$|cslab\.\(\*Slot\)\.Unpin$|/s3fifo\.(tagOf|ghostOf)$|` +
	`/tinylfu\.(tagOf|kept|keep|printOf|printIn)$|/tinylfu\.\(\*climber\)\.(hit|miss)$|` +
	`/stats\.\(\*Cache\[.*\]\)\.stripe$`)

// read matches the functions that a read without a lock goes through, and
// readSlow the calls that must not show up in them even where the compiler
// knows next to nothing: in a program that gets to the caches through a
// generic type of another package that instantiates nothing. The compiler
// inlines no function there that is not generic, so the reads have to do
// without any, this module's own included, but the atomic functions, which
// it builds in. A lock is taken only when a read cannot be sure, and calls
// to it are fine.
var (
	read     = regexp.MustCompile(`/(sieve|s3fifo|tinylfu)\.\(\*Cache\[.*\]\)\.(Get|GetMany|Peek)$|/cslab\.\(\*Slab\[.*\]\)\.Lookup$`)
	readSlow = regexp.MustCompile(`^sync/atomic\.|^github\.com/floatdrop/evict/[^\[]*$|` +
		`cslab\.\(\*Slab\[.*\]\)\.(Overdue|pin|preferred|Touch|TouchNode)$|cslab\.\(\*Node\[.*\]\)\.(Meta|SetMeta)$`)
)

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
	// on what another one has passed on to the compiler, and one that uses
	// the caches through a generic type of another package.
	//nolint:gosec // The command is the go tool, and its arguments are fixed.
	cmd := exec.CommandContext(t.Context(), goTool, "build", "-gcflags=-S", "./probe/...")
	// A build for another platform or with other flags would say nothing
	// about this one, and little about the caches.
	cmd.Env = append(os.Environ(), "GOFLAGS=")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	var (
		pkg, function     string
		seen, seenWrapped = map[string]bool{}, map[string]bool{}
	)
	scanner := bufio.NewScanner(bytes.NewReader(out))
	scanner.Buffer(nil, 1<<20)
	for scanner.Scan() {
		line := scanner.Text()

		// The output for a package starts with a line like
		//	# github.com/floatdrop/evict/internal/inlinecheck/probe/lru
		if p, ok := strings.CutPrefix(line, "# "); ok {
			pkg = p
			continue
		}
		wrapped := strings.HasSuffix(pkg, "/probe/wrapuser")

		// A function starts with a line like
		//	pkg.(*T[go.shape.int]).Method STEXT dupok size=...
		if name, _, ok := strings.Cut(line, " STEXT "); ok && !strings.HasPrefix(line, "\t") {
			function = name
			switch {
			case wrapped && read.MatchString(function):
				seenWrapped[read.FindString(function)] = true
			case !wrapped && hot.MatchString(function):
				seen[hot.FindString(function)] = true
			}
			continue
		}
		// A call in it looks like
		//	0x0040 00064 (file.go:12)	CALL	sync.(*Mutex).Lock(SB)
		_, target, ok := strings.Cut(line, "\tCALL\t")
		if !ok {
			continue
		}
		target = strings.TrimSuffix(target, "(SB)")
		switch {
		case wrapped && read.MatchString(function) && readSlow.MatchString(target):
			t.Errorf("%s calls %s when the cache is used through a generic type", function, target)
		case !wrapped && hot.MatchString(function) && slow.MatchString(target):
			t.Errorf("%s calls %s instead of inlining it", function, target)
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}

	// The test is only worth something if it has looked at the right code.
	for _, pkg := range []string{"lru", "twoq", "sieve", "s3fifo", "tinylfu", "stats"} {
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
	for _, pkg := range []string{"sieve", "s3fifo", "tinylfu", "stats"} {
		found := false
		for name := range seen {
			found = found || strings.Contains(name, "/"+pkg+".(*Cache[") && strings.HasSuffix(name, ").GetMany")
		}
		if !found {
			t.Errorf("the compiler's output has no code for %s.Cache.GetMany", pkg)
		}
	}
	for _, want := range [][2]string{
		{"sieve.(*Cache[", "Get"},
		{"sieve.(*Cache[", "Peek"},
		{"sieve.(*Cache[", "GetMany"},
		{"s3fifo.(*Cache[", "Get"},
		{"s3fifo.(*Cache[", "Peek"},
		{"s3fifo.(*Cache[", "GetMany"},
		{"tinylfu.(*Cache[", "Get"},
		{"tinylfu.(*Cache[", "Peek"},
		{"tinylfu.(*Cache[", "GetMany"},
		{"cslab.(*Slab[", "Lookup"},
	} {
		found := false
		for name := range seenWrapped {
			found = found || strings.Contains(name, "/"+want[0]) && strings.HasSuffix(name, ")."+want[1])
		}
		if !found {
			t.Errorf("the compiler's output for the generic wrapper has no code for %s%s", want[0], want[1])
		}
	}
}
