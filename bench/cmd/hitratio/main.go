// Command hitratio replays public cache traces against the caches of this
// module and against others, and reports how many requests each of them
// answers at a range of capacities.
//
// The traces are downloaded on first use and checked against the SHA-256 that
// this module has on record for them, so the numbers can be reproduced:
//
//	go run ./cmd/hitratio
//
// writes results/hitratio.md and results/hitratio.csv, along with a chart for
// every trace. With -sharding, it reports instead what the hit ratios of this
// module's caches owe to their being split into shards, in
// results/sharding.md and results/sharding.csv. See -help for how to pick
// datasets and caches.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/floatdrop/evict/bench/caches"
	"github.com/floatdrop/evict/bench/dataset"
	"github.com/floatdrop/evict/bench/replay"
	"github.com/floatdrop/evict/bench/report"
	"github.com/floatdrop/evict/bench/trace"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	// The first interrupt asks the replays to wind down, which takes until
	// the ones that are running have finished. From then on the signal is
	// no longer caught, so a second one ends the program on the spot.
	go func() {
		<-ctx.Done()
		stop()
	}()

	err := run(ctx, os.Args[1:])
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "hitratio:", err)
		os.Exit(1)
	}
}

// sharedCPUs is the number of CPUs that the report on sharding pretends the
// machine has, so that the caches are split the same way wherever it is run.
const sharedCPUs = 8

func run(ctx context.Context, args []string) error {
	defaultDir := filepath.Join(os.TempDir(), "evict-bench")
	if dir, err := os.UserCacheDir(); err == nil {
		defaultDir = filepath.Join(dir, "evict-bench")
	}

	flags := flag.NewFlagSet("hitratio", flag.ContinueOnError)
	var (
		datasetNames = flags.String("datasets", "all", "comma-separated `names` of the datasets to replay; see -list")
		cacheNames   = flags.String("caches", "default", "comma-separated `names` of the caches to compare; see -list. A cache of this module can be given a number of shards, as in evict/sieve@16")
		sharding     = flags.Bool("sharding", false, "report what splitting the caches of this module into shards does to their hit ratios, instead of comparing caches")
		runs         = flags.Int("runs", 11, "`number` of times to replay a trace against a cache that does not evict the same way every time")
		dir          = flags.String("dir", defaultDir, "`directory` to keep the downloaded traces in")
		out          = flags.String("out", "results", "`directory` to write the report to")
		parallel     = flags.Int("parallel", min(runtime.GOMAXPROCS(0), 8), "`number` of replays to run at the same time; every one of them holds a cache in memory")
		list         = flags.Bool("list", false, "list the datasets and the caches, and exit")
	)
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}

	if *list {
		printChoices()
		return nil
	}
	if *runs < 1 {
		return fmt.Errorf("-runs is %d, want at least 1", *runs)
	}
	datasets, err := pickDatasets(*datasetNames)
	if err != nil {
		return err
	}
	var players []player
	if *sharding {
		if *cacheNames != "default" {
			return errors.New("-sharding picks the caches itself; leave out -caches")
		}
		// A cache decides on its shards by the number of CPUs when it is
		// created.
		runtime.GOMAXPROCS(sharedCPUs)
		players = shardingPlayers()
	} else if players, err = pickPlayers(*cacheNames); err != nil {
		return err
	}

	// Whatever might go wrong with the report or with the traces had better
	// go wrong before the replays, which take a while.
	if err := os.MkdirAll(*out, 0o750); err != nil {
		return err
	}
	for _, d := range datasets {
		if err := d.Fetch(ctx, *dir); err != nil {
			return err
		}
	}

	var tables []report.Table
	for _, d := range datasets {
		table, err := replayDataset(ctx, d, players, *dir, max(*parallel, 1), *runs)
		if err != nil {
			return err
		}
		tables = append(tables, table)
	}

	command := strings.TrimSpace("go run ./cmd/hitratio " + strings.Join(args, " "))
	if *sharding {
		return write(*out, "sharding", tables, shardingPreamble(command, *runs), report.Differences, false)
	}
	return write(*out, "hitratio", tables, preamble(command, *runs, tables), summaryAndTables, true)
}

// summaryAndTables writes the report on hit ratios: the traces at a glance,
// and then each of them in full.
func summaryAndTables(w io.Writer, tables []report.Table) error {
	if _, err := io.WriteString(w, summaryHead); err != nil {
		return err
	}
	if err := report.Summary(w, tables); err != nil {
		return err
	}
	if _, err := io.WriteString(w, "\n## Trace by trace\n\n"); err != nil {
		return err
	}
	return report.Markdown(w, tables)
}

// A player replays a trace at a capacity and returns the number of hits.
type player struct {
	name string
	// varies says that the cache does not score the same on every replay,
	// so that one replay does not say enough.
	varies bool
	play   func(t *input, capacity int) int
}

// input is a trace, and what the best possible cache knows about it.
type input struct {
	keys   []uint64
	future func() replay.Future
}

// defaultCaches are the columns of the report: the caches of this module,
// the ones they are compared with, and the best that any cache could do.
var defaultCaches = []string{"evict/lru", "evict/twoq", "evict/sieve", "evict/s3fifo", "evict/tinylfu", "otter", "golang-lru/2q", report.Bound}

func allPlayers() []player {
	var players []player
	for _, kind := range caches.All[uint64, uint64]() {
		players = append(players, player{kind.Name, kind.Varies, func(t *input, capacity int) int {
			return replay.Hits(kind.Exact(capacity), t.keys)
		}})
	}
	return append(players, player{report.Bound, false, func(t *input, capacity int) int {
		return replay.Optimal(t.keys, t.future(), capacity)
	}})
}

func pickPlayers(names string) ([]player, error) {
	all := allPlayers()

	wanted := strings.Split(names, ",")
	switch names {
	case "default":
		wanted = defaultCaches
	case "all":
		wanted = nil
		for _, p := range all {
			wanted = append(wanted, p.name)
		}
	}

	var players []player
	lines := 0
	for _, name := range wanted {
		if name != report.Bound {
			lines++
		}
		if kindName, shards, ok := strings.Cut(name, "@"); ok {
			p, err := shardedPlayer(name, kindName, shards)
			if err != nil {
				return nil, err
			}
			players = append(players, p)
			continue
		}

		i := slices.IndexFunc(all, func(p player) bool { return p.name == name })
		if i < 0 {
			return nil, fmt.Errorf("there is no cache %q; see -list", name)
		}
		players = append(players, all[i])
	}
	if lines > report.MaxSeries {
		return nil, fmt.Errorf("%d caches are more than the %d that a chart can tell apart", lines, report.MaxSeries)
	}
	return players, nil
}

// shardedPlayer returns a player for a cache of this module that is split
// into a given number of shards. Which key ends up in which shard is up to a
// hash that is seeded at random, so the result varies.
func shardedPlayer(name, kindName, shards string) (player, error) {
	kind, ok := caches.Find[uint64, uint64](kindName)
	n, err := strconv.Atoi(shards)
	// The caches round the number up to a power of two; a name that says
	// otherwise would not say what was measured.
	if !ok || kind.Shards == nil || err != nil || n < 1 || n&(n-1) != 0 {
		return player{}, fmt.Errorf("%q does not name a cache with a number of shards that is a power of two, such as evict/sieve@16", name)
	}
	return player{name, n > 1, func(t *input, capacity int) int {
		return replay.Hits(kind.Shards(capacity, n), t.keys)
	}}, nil
}

// shardingPlayers returns the caches of this module in pairs: each in one
// piece, and then with the shards it has by default.
func shardingPlayers() []player {
	var players []player
	for _, kind := range caches.All[uint64, uint64]() {
		if kind.Shards == nil {
			continue
		}
		players = append(players,
			player{kind.Name, kind.Varies, func(t *input, capacity int) int {
				return replay.Hits(kind.Exact(capacity), t.keys)
			}},
			player{kind.Name + " in shards", true, func(t *input, capacity int) int {
				return replay.Hits(kind.New(capacity), t.keys)
			}},
		)
	}
	return players
}

// catalog returns the datasets there are to choose from. The tests have
// smaller ones.
var catalog = dataset.All

func pickDatasets(names string) ([]dataset.Dataset, error) {
	all := catalog()
	if names == "all" {
		return all, nil
	}

	var datasets []dataset.Dataset
	for name := range strings.SplitSeq(names, ",") {
		i := slices.IndexFunc(all, func(d dataset.Dataset) bool { return d.Name == name })
		if i < 0 {
			return nil, fmt.Errorf("there is no dataset %q; see -list", name)
		}
		datasets = append(datasets, all[i])
	}
	return datasets, nil
}

func printChoices() {
	fmt.Println("Datasets:")
	for _, d := range catalog() {
		fmt.Printf("  %-14s %s (%s)\n", d.Name, d.Workload, size(d.Download()))
	}
	fmt.Println("Caches:")
	for _, p := range allPlayers() {
		note := ""
		if slices.Contains(defaultCaches, p.name) {
			note = " (default)"
		}
		fmt.Printf("  %s%s\n", p.name, note)
	}
}

// size formats the number of bytes that a dataset takes to download.
func size(bytes int64) string {
	switch {
	case bytes == 0:
		return "generated"
	case bytes < 1e6:
		return fmt.Sprintf("%.0f kB", float64(bytes)/1e3)
	default:
		return fmt.Sprintf("%.1f MB", float64(bytes)/1e6)
	}
}

// replayDataset replays one trace against every player at every capacity of
// the dataset, several replays at a time. Players that vary are given as many
// replays as runs says.
func replayDataset(ctx context.Context, d dataset.Dataset, players []player, dir string, parallel, runs int) (report.Table, error) {
	start := time.Now()
	fmt.Fprintf(os.Stderr, "%s: loading\n", d.Name)
	keys, err := d.Load(ctx, dir)
	if err != nil {
		return report.Table{}, err
	}
	in := &input{keys: keys, future: sync.OnceValue(func() replay.Future { return replay.Foresee(keys) })}

	table := report.Table{
		Name:     d.Name,
		Title:    d.Title,
		Workload: d.Workload,
		Origin:   d.Origin,
		Requests: len(keys),
		Unique:   trace.Unique(keys),
		Rows:     make([]report.Row, len(d.Capacities)),
	}
	for _, p := range players {
		table.Caches = append(table.Caches, p.name)
	}
	fmt.Fprintf(os.Stderr, "%s: %s requests for %s keys, loaded in %s\n",
		d.Name, report.Thousands(table.Requests), report.Thousands(table.Unique), time.Since(start).Round(time.Millisecond))

	var wg sync.WaitGroup
	slots := make(chan struct{}, parallel)
	hits := make([][][]int, len(d.Capacities))
	for i, capacity := range d.Capacities {
		hits[i] = make([][]int, len(players))
		for j, p := range players {
			n := 1
			if p.varies {
				n = runs
			}
			hits[i][j] = make([]int, n)
			for run := range n {
				wg.Go(func() {
					slots <- struct{}{}
					defer func() { <-slots }()
					if ctx.Err() != nil {
						return
					}

					start := time.Now()
					scored := p.play(in, capacity)
					hits[i][j][run] = scored
					fmt.Fprintf(os.Stderr, "%s: %-22s at %9d: %6.2f%% in %s\n",
						d.Name, p.name, capacity, 100*float64(scored)/float64(max(len(keys), 1)), time.Since(start).Round(time.Millisecond))
				})
			}
		}
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return report.Table{}, err
	}

	for i, capacity := range d.Capacities {
		table.Rows[i] = report.Row{Capacity: capacity, Cells: make([]report.Cell, len(players))}
		for j := range players {
			table.Rows[i].Cells[j] = report.Summarize(hits[i][j])
		}
	}
	return table, nil
}

// write puts the report into dir: the numbers on their own first, then the
// text that markdown makes of them, and with charts a picture of every table.
func write(dir, name string, tables []report.Table, head string, markdown func(io.Writer, []report.Table) error, charts bool) error {
	save := func(file, content string) error {
		return os.WriteFile(filepath.Join(dir, file), []byte(content), 0o644) //nolint:gosec // A report is for everyone to read.
	}

	var csv strings.Builder
	if err := report.CSV(&csv, tables); err != nil {
		return err
	}
	if err := save(name+".csv", csv.String()); err != nil {
		return err
	}

	var svgs []string
	for i := range tables {
		if !charts {
			break
		}
		svg, err := tables[i].Chart().SVG()
		if err != nil {
			return err
		}
		svgs = append(svgs, svg)
		tables[i].Image = name + "-" + tables[i].Name + ".svg"
	}

	var md strings.Builder
	md.WriteString(head)
	if err := markdown(&md, tables); err != nil {
		return err
	}
	if err := save(name+".md", md.String()); err != nil {
		return err
	}
	for i, svg := range svgs {
		if err := save(tables[i].Image, svg); err != nil {
			return err
		}
	}
	return nil
}
