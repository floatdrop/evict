package main

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"

	"github.com/floatdrop/evict/bench/report"
)

// widest says how far apart the replays of a cache were where they were
// furthest apart, for the head of a report: "by up to 4.8 percentage points
// (otter on P8 with 20,000 entries)". It says "by nothing" if no cache varied.
func widest(tables []report.Table) string {
	var (
		most  float64
		where string
	)
	for _, t := range tables {
		for _, row := range t.Rows {
			for i, cell := range row.Cells {
				points := 100 * float64(cell.Max-cell.Min) / float64(max(t.Requests, 1))
				if points > most {
					most = points
					where = fmt.Sprintf("%s on %s with %s entries", t.Caches[i], t.Title, report.Thousands(row.Capacity))
				}
			}
		}
	}
	if where == "" {
		return "by nothing"
	}
	return fmt.Sprintf("by up to %.1f percentage points (%s)", most, where)
}

// preamble returns the head of the report on hit ratios: what the numbers
// are and what produced them.
func preamble(command string, runs int, tables []report.Table) string {
	return fmt.Sprintf(`# Hit ratios

The share of requests, in percent, that a cache of a given capacity answers when a
trace is replayed against it: every key of the trace is looked up in turn and stored if
it is missing. The column `+"`optimal`"+` is not a cache but the bound that no cache can
beat: what one would score that knew the requests to come.

The caches of this module run in one piece here, with a single shard, and otter evicts on
the spot rather than in the background, as it does in its own simulator. Most of the
caches then evict the same way every time, and their numbers are the same on every run.
Two do not. Otter admits some entries at random, and `+"`evict/tinylfu`"+` counts requests in a
sketch in which keys share counters, by a hash that is seeded at random. That moves their
hit ratios from one replay to the next, in this run %s.
Their numbers are the medians of %d replays, which is also what the charts show, and the
column next to each has the lowest and the highest of them. That is not all the range
there is: of further replays, about %d in a hundred would come out below it or above it.

A number is in bold if the cache did better in that row than every other one, on each of
their replays. Where the range of one cache reaches the best of the others, the row has no
winner.

A program does not get the caches of this module in one piece unless it asks for that: by
default they are split into shards. What difference that makes is in
[sharding.md](sharding.md). For most of the numbers here it is none to speak of, but not
for all: `+"`evict/sieve`"+` in particular does far better in shards on the traces on which it
does worst here.

%s`, widest(tables), runs, outside(runs), produced(command))
}

// outside returns how many replays in a hundred are expected to score below
// the lowest or above the highest of the given number of replays: of n+1
// replays, each is as likely as any other to be the lowest, and as likely to
// be the highest.
func outside(runs int) int {
	return (200 + (runs+1)/2) / (runs + 1)
}

// summaryHead introduces the table that has all the traces in it.
const summaryHead = `## At a glance

The hit ratio of every cache on every trace, averaged over the capacities at which the
trace was replayed. Averages hide a lot: a cache that leads here may trail at the capacity
that matters to you, so look at the trace that resembles your workload, below.

`

// shardingPreamble returns the head of the report on shards.
func shardingPreamble(command string, runs int) string {
	return fmt.Sprintf(`# Hit ratios and shards

A cache of this module is split into shards, each of which has its share of the capacity
and evicts on its own: a shard that is full evicts although another one may have room.
This is what that does to the hit ratio. The numbers are differences in percentage
points: the hit ratio of a cache with the shards it has by default, less that of the same
cache in one piece, which is the one that [hitratio.md](hitratio.md) reports. A positive
number means that the cache did better in shards.

The caches were created as on a machine with %d CPUs. That gives a cache of less than 512
entries one shard, and a larger one the largest power of two of them that leaves each with
256 entries or more, up to %d. Which key goes to which shard is up to a hash that is seeded at random,
so the hit ratio of a cache in shards differs from run to run. The number is the
difference for the median of %d replays, and the brackets hold the differences for the
worst and for the best of them.

`+"`evict/tinylfu`"+` differs from run to run in one piece as well: which keys share counters in
its sketch is up to the same hash. What its replays in shards are compared with is the
median of as many replays in one piece, so its brackets are never empty, not even where a
cache is too small to be split.

%s`, sharedCPUs, 8*sharedCPUs, runs, produced(command))
}

// produced says what a report was made with.
func produced(command string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Produced by `%s` with %s", command, runtime.Version())
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch dep.Path {
			case "github.com/maypok86/otter/v2":
				fmt.Fprintf(&b, ", otter %s", dep.Version)
			case "github.com/hashicorp/golang-lru/v2":
				fmt.Fprintf(&b, ", golang-lru %s", dep.Version)
			}
		}
	}
	b.WriteString(".\n\n")
	return b.String()
}
