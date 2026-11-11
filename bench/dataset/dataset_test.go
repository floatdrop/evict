package dataset

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"

	"github.com/floatdrop/evict/bench/trace"
)

func checksum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func gzipped(t *testing.T, text string) []byte {
	t.Helper()

	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write([]byte(text)); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// serve starts a server that hands out the given files and counts the
// requests it gets.
func serve(t *testing.T, files map[string][]byte) (url string, requests *atomic.Int32) {
	t.Helper()

	requests = new(atomic.Int32)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		data, ok := files[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(data)
	}))
	t.Cleanup(server.Close)
	return server.URL, requests
}

func TestLoad(t *testing.T) {
	t.Parallel()

	file := gzipped(t, "10 3 0 0\n7 1 0 3\n")
	url, requests := serve(t, map[string][]byte{"/arc/small.gz": file})
	d := Dataset{
		Name:        "small",
		URLs:        []string{url + "/arc/small.gz"},
		SHA256:      checksum(file),
		Compression: Gzip,
		Format:      trace.ARC,
	}
	dir := filepath.Join(t.TempDir(), "not", "there", "yet")
	want := []uint64{10, 11, 12, 7}

	keys, err := d.Load(t.Context(), dir)
	if err != nil || !slices.Equal(keys, want) {
		t.Fatalf("Load = %v, %v, want %v", keys, err, want)
	}

	// The second time, the file is at hand.
	keys, err = d.Load(t.Context(), dir)
	if err != nil || !slices.Equal(keys, want) {
		t.Fatalf("second Load = %v, %v, want %v", keys, err, want)
	}
	if got := requests.Load(); got != 1 {
		t.Errorf("the server got %d requests, want 1", got)
	}

	// A file that was damaged is fetched again.
	stored := filepath.Join(dir, "small-small.gz")
	if werr := os.WriteFile(stored, file[:len(file)/2], 0o600); werr != nil {
		t.Fatal(werr)
	}
	keys, err = d.Load(t.Context(), dir)
	if err != nil || !slices.Equal(keys, want) {
		t.Fatalf("Load over a damaged file = %v, %v, want %v", keys, err, want)
	}
	if got := requests.Load(); got != 2 {
		t.Errorf("the server got %d requests, want 2", got)
	}
	if got, err := os.ReadFile(stored); err != nil || !bytes.Equal(got, file) {
		t.Errorf("the stored file is not the one that was served: %v", err)
	}
}

func TestLoadCompressions(t *testing.T) {
	t.Parallel()

	raw := binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, 7), 1<<63)
	want := []uint64{7, 1 << 63}

	var zst, x bytes.Buffer
	zw, err := zstd.NewWriter(&zst)
	if err != nil {
		t.Fatal(err)
	}
	xw, err := xz.NewWriter(&x)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range []interface {
		Write(p []byte) (int, error)
		Close() error
	}{zw, xw} {
		if _, err := w.Write(raw); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}

	url, _ := serve(t, map[string][]byte{"/keys.zst": zst.Bytes(), "/keys.xz": x.Bytes()})
	for _, d := range []Dataset{
		{Name: "zst", URLs: []string{url + "/keys.zst"}, SHA256: checksum(zst.Bytes()), Compression: Zstd, Format: trace.BigEndian},
		{Name: "xz", URLs: []string{url + "/keys.xz"}, SHA256: checksum(x.Bytes()), Compression: XZ, Format: trace.BigEndian},
	} {
		keys, err := d.Load(t.Context(), t.TempDir())
		if err != nil || !slices.Equal(keys, want) {
			t.Errorf("%s: Load = %v, %v, want %v", d.Name, keys, err, want)
		}
	}
}

func TestLoadMirror(t *testing.T) {
	t.Parallel()

	file := gzipped(t, "1\n2\n3\n")
	bad := gzipped(t, "4\n5\n6\n")
	url, requests := serve(t, map[string][]byte{"/good/lirs.gz": file, "/bad/lirs.gz": bad})
	d := Dataset{
		Name: "mirrored",
		URLs: []string{
			url + "/gone/lirs.gz", // Not found.
			url + "/bad/lirs.gz",  // Not the file it should be.
			url + "/good/lirs.gz",
			url + "/never/lirs.gz",
		},
		SHA256:      checksum(file),
		Compression: Gzip,
		Format:      trace.LIRS,
	}

	keys, err := d.Load(t.Context(), t.TempDir())
	if err != nil || !slices.Equal(keys, []uint64{1, 2, 3}) {
		t.Fatalf("Load = %v, %v, want [1 2 3]", keys, err)
	}
	if got := requests.Load(); got != 3 {
		t.Errorf("the server got %d requests, want 3", got)
	}
}

func TestLoadErrors(t *testing.T) {
	t.Parallel()

	file := gzipped(t, "1\n2\n3\n")
	url, _ := serve(t, map[string][]byte{"/lirs.gz": file, "/plain": []byte("1\n2\n3\n")})

	tests := []struct {
		name string
		d    Dataset
		want string // Part of the error.
	}{
		{
			"wrong checksum",
			Dataset{URLs: []string{url + "/lirs.gz"}, SHA256: checksum(nil), Compression: Gzip, Format: trace.LIRS},
			"checksum mismatch",
		},
		{
			"not found anywhere",
			Dataset{URLs: []string{url + "/a.gz", url + "/b.gz"}, SHA256: checksum(file), Compression: Gzip, Format: trace.LIRS},
			"/b.gz: the server answered 404",
		},
		{
			"no URL",
			Dataset{SHA256: checksum(file), Compression: Gzip, Format: trace.LIRS},
			"no place to download it from",
		},
		{
			"not compressed as promised",
			Dataset{URLs: []string{url + "/plain"}, SHA256: checksum([]byte("1\n2\n3\n")), Compression: Gzip, Format: trace.LIRS},
			"decompressing",
		},
		{
			"not in the format as promised",
			Dataset{URLs: []string{url + "/lirs.gz"}, SHA256: checksum(file), Compression: Gzip, Format: trace.BigEndian},
			"cut short",
		},
		{
			"unknown compression",
			Dataset{URLs: []string{url + "/lirs.gz"}, SHA256: checksum(file), Format: trace.LIRS},
			"unknown compression",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.d.Name = "broken"
			dir := t.TempDir()
			keys, err := tt.d.Load(t.Context(), dir)
			if err == nil || !strings.Contains(err.Error(), tt.want) || !strings.Contains(err.Error(), "dataset broken") {
				t.Fatalf("Load = %v, %v, want an error with %q", keys, err, tt.want)
			}

			// A download that was rejected leaves nothing behind.
			if strings.Contains(tt.want, "checksum") || strings.Contains(tt.want, "404") {
				if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
					t.Errorf("the directory holds %v after a failed download (%v)", entries, err)
				}
			}
		})
	}

	// A download that is called off stops.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	d := Dataset{Name: "late", URLs: []string{url + "/lirs.gz", url + "/lirs.gz"}, SHA256: checksum(file), Compression: Gzip, Format: trace.LIRS}
	if _, err := d.Load(ctx, t.TempDir()); !errors.Is(err, context.Canceled) {
		t.Errorf("Load with a canceled context = %v, want context.Canceled", err)
	}
}

// TestAll checks the list of datasets for slips of the pen. It does not go
// near the network.
func TestAll(t *testing.T) {
	t.Parallel()

	names := map[string]bool{}
	for _, d := range All() {
		if d.Name == "" || d.Title == "" || d.Workload == "" || d.Origin == "" {
			t.Errorf("dataset %q lacks a name, a title, a workload or an origin", d.Name)
		}
		if names[d.Name] {
			t.Errorf("dataset %q is listed twice", d.Name)
		}
		names[d.Name] = true

		if found, ok := Find(d.Name); !ok || found.Title != d.Title {
			t.Errorf("Find(%q) = %q, %t", d.Name, found.Title, ok)
		}

		if len(d.Capacities) == 0 || !slices.IsSorted(d.Capacities) || d.Capacities[0] <= 0 {
			t.Errorf("dataset %q has capacities %v, want a few in ascending order", d.Name, d.Capacities)
		}

		// A dataset is generated, strung together from files, or a file.
		files := []Dataset{d}
		switch {
		case d.generate != nil:
			files = nil
			if len(d.URLs) != 0 || len(d.Parts) != 0 || d.Download() != 0 {
				t.Errorf("dataset %q is generated and has URLs or parts", d.Name)
			}
		case len(d.Parts) > 0:
			files = d.Parts
			if len(d.URLs) != 0 || d.Size != 0 {
				t.Errorf("dataset %q is made of parts and has a file of its own", d.Name)
			}
		}
		for _, f := range files {
			if len(f.URLs) == 0 || f.Size <= 0 || f.Compression == 0 || f.Format == 0 || len(f.Parts) != 0 {
				t.Errorf("dataset %q: %q lacks a URL, a size, a compression or a format", d.Name, f.Name)
			}
			for _, url := range f.URLs {
				if !strings.HasPrefix(url, "https://") {
					t.Errorf("dataset %q is fetched from %q, want https", d.Name, url)
				}
			}
			if sum, err := hex.DecodeString(f.SHA256); err != nil || len(sum) != sha256.Size {
				t.Errorf("dataset %q has the checksum %q, want a SHA-256", d.Name, f.SHA256)
			}
		}
	}

	// The parts of Mixed are two files, one of which is replayed twice.
	if mixed, _ := Find("mixed"); len(mixed.Parts) != 3 || mixed.Download() != corda.Size+loop.Size {
		t.Errorf("mixed has %d parts and takes %d bytes to download, want 3 and %d", len(mixed.Parts), mixed.Download(), corda.Size+loop.Size)
	}

	if _, ok := Find("no such dataset"); ok {
		t.Error("Find found a dataset that does not exist")
	}
}

func TestGenerated(t *testing.T) {
	t.Parallel()

	d, ok := Find("zipf")
	if !ok {
		t.Fatal("there is no zipf dataset")
	}
	// It needs neither a directory nor a network.
	keys, err := d.Load(t.Context(), "/nonexistent")
	if err != nil || len(keys) != 1_000_000 {
		t.Fatalf("Load = %d keys, %v, want a million", len(keys), err)
	}
	again, err := d.Load(t.Context(), "/nonexistent")
	if err != nil || !slices.Equal(keys, again) {
		t.Errorf("the second Load differs from the first (%v)", err)
	}
}

func TestParts(t *testing.T) {
	t.Parallel()

	first, second := gzipped(t, "1\n2\n"), gzipped(t, "7\n")
	url, requests := serve(t, map[string][]byte{"/first.gz": first, "/second.gz": second})

	a := Dataset{Name: "a", URLs: []string{url + "/first.gz"}, SHA256: checksum(first), Size: int64(len(first)), Compression: Gzip, Format: trace.LIRS}
	b := Dataset{Name: "b", URLs: []string{url + "/second.gz"}, SHA256: checksum(second), Size: int64(len(second)), Compression: Gzip, Format: trace.LIRS}
	d := Dataset{Name: "both", Parts: []Dataset{a, b, a}}

	keys, err := d.Load(t.Context(), t.TempDir())
	if err != nil || !slices.Equal(keys, []uint64{1, 2, 7, 1, 2}) {
		t.Errorf("Load = %v, %v, want the keys of the parts in a row", keys, err)
	}
	if got := requests.Load(); got != 2 {
		t.Errorf("%d downloads, want one for each of the two files", got)
	}
	if got, want := d.Download(), int64(len(first)+len(second)); got != want {
		t.Errorf("Download() = %d, want %d: a file that is used twice is fetched once", got, want)
	}

	// What is wrong with a part is wrong with the whole.
	b.SHA256 = checksum(nil)
	d = Dataset{Name: "broken", Parts: []Dataset{a, b}}
	if _, err := d.Load(t.Context(), t.TempDir()); !errors.Is(err, errChecksum) || !strings.Contains(err.Error(), "dataset broken: dataset b:") {
		t.Errorf("Load with a part that fails its checksum: %v", err)
	}
}

func TestDownloadLimits(t *testing.T) {
	t.Parallel()

	file := gzipped(t, "1\n2\n3\n")
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stalls" {
			// The beginning arrives, and then nothing.
			_, _ = w.Write(file[:4])
			w.(http.Flusher).Flush()
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		_, _ = w.Write(file)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	dir := t.TempDir()
	empty := func(what string) {
		t.Helper()
		if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
			t.Errorf("%s left %d files behind (%v)", what, len(entries), err)
		}
	}

	// A file that is longer than it should be is not read to its end, let
	// alone kept.
	err := download(t.Context(), server.URL+"/file", filepath.Join(dir, "long"), checksum(file), int64(len(file))-1, time.Minute)
	if !errors.Is(err, errChecksum) || !strings.Contains(err.Error(), "longer than") {
		t.Errorf("download of a file that is too long: %v", err)
	}
	empty("a download that was too long")

	// The right size, or none to go by, is fine.
	for _, size := range []int64{int64(len(file)), 0} {
		name := filepath.Join(dir, "fine")
		if err = download(t.Context(), server.URL+"/file", name, checksum(file), size, time.Minute); err != nil {
			t.Errorf("download with size %d: %v", size, err)
		}
		if err = os.Remove(name); err != nil {
			t.Error(err)
		}
	}

	// A server that stops sending is not waited for forever.
	start := time.Now()
	err = download(t.Context(), server.URL+"/stalls", filepath.Join(dir, "stalled"), checksum(file), int64(len(file)), 50*time.Millisecond)
	if !errors.Is(err, errStalled) {
		t.Errorf("download from a server that stalls: %v", err)
	}
	if waited := time.Since(start); waited > 10*time.Second {
		t.Errorf("it took %s to give up on a server that stalls", waited)
	}
	empty("a download that stalled")
}

func TestGeneratedDataset(t *testing.T) {
	t.Parallel()

	d := Generated("made", "Made", "requests that are made up", "nobody", []int{1, 2}, func() []uint64 { return []uint64{4, 5, 4} })
	if d.Name != "made" || d.Title != "Made" || d.Workload != "requests that are made up" || d.Origin != "nobody" || !slices.Equal(d.Capacities, []int{1, 2}) {
		t.Errorf("Generated = %+v", d)
	}
	// It takes neither a directory nor a network to load or to fetch.
	if err := d.Fetch(t.Context(), "/nonexistent"); err != nil {
		t.Errorf("Fetch: %v", err)
	}
	keys, err := d.Load(t.Context(), "/nonexistent")
	if err != nil || !slices.Equal(keys, []uint64{4, 5, 4}) || d.Download() != 0 {
		t.Errorf("Load = %v, %v with a download of %d bytes", keys, err, d.Download())
	}
}
