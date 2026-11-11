package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// TestRun is not parallel: it measures the heap of the process.
func TestRun(t *testing.T) {
	dir := t.TempDir()
	var stdout strings.Builder
	if err := run([]string{"-capacities", "20000,50000", "-out", dir}, &stdout); err != nil {
		t.Fatal(err)
	}

	file, err := os.ReadFile(filepath.Join(dir, "memory.md"))
	if err != nil {
		t.Fatal(err)
	}
	got := string(file)
	if got != stdout.String() {
		t.Error("the report in the file differs from the one that was printed")
	}
	for _, want := range []string{
		"# Memory\n", "## Integers\n", "## Strings\n", "## Garbage collection\n",
		"Produced by `go run ./cmd/memory -capacities 20000,50000 -out " + dir + "` with go",
		"| cache | 20,000 | 50,000 |\n|:---|---:|---:|\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("the report lacks %q:\n%s", want, got)
		}
	}

	// Three tables of bytes, each with a row for every cache, and one of
	// times. The first one has the bytes per entry for integers: more than
	// the 16 that the entry itself takes, and not absurdly more.
	rows := regexp.MustCompile(`(?m)^\| evict/sieve \| ([0-9.]+) \| ([0-9.]+) \|$`).FindAllStringSubmatch(got, -1)
	if len(rows) != 3 {
		t.Fatalf("%d rows of bytes for evict/sieve, want one in each of 3 tables:\n%s", len(rows), got)
	}
	if !regexp.MustCompile(`(?m)^\| evict/sieve \| [0-9.]+–[0-9.]+ \| [0-9.]+–[0-9.]+ \|$`).MatchString(got) {
		t.Errorf("there is no row of times for evict/sieve:\n%s", got)
	}
	for _, cell := range rows[0][1:] {
		if bytes, err := strconv.ParseFloat(cell, 64); err != nil || bytes < 16 || bytes > 400 {
			t.Errorf("evict/sieve takes %s bytes per entry of integers (%v)", cell, err)
		}
	}
	// A cache of integers has nothing in it for the collector to look
	// through; one of otter does.
	for _, cell := range rows[2][1:] {
		if bytes, err := strconv.ParseFloat(cell, 64); err != nil || bytes > 8 {
			t.Errorf("the collector looks through %s bytes per entry of evict/sieve (%v)", cell, err)
		}
	}
	otter := regexp.MustCompile(`(?m)^\| otter \| ([0-9.]+) \| ([0-9.]+) \|$`).FindAllStringSubmatch(got, -1)
	if len(otter) != 3 {
		t.Fatalf("%d rows of bytes for otter, want 3", len(otter))
	}
	if bytes, err := strconv.ParseFloat(otter[2][2], 64); err != nil || bytes < 16 {
		t.Errorf("the collector looks through %s bytes per entry of otter (%v)", otter[2][2], err)
	}
}

func TestRunErrors(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{"-capacities", "10,x"},
		{"-capacities", "0"},
		{"-no-such-flag"},
		{"-capacities", "10", "-out", "/dev/null/nowhere"}, // Found out before anything is measured.
	} {
		if err := run(args, &strings.Builder{}); err == nil {
			t.Errorf("run(%q) succeeded", args)
		}
	}
	if err := run([]string{"-help"}, &strings.Builder{}); err != nil {
		t.Errorf("run(-help): %v", err)
	}
}
