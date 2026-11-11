package trace

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
)

func TestRead(t *testing.T) {
	t.Parallel()

	oracle := func(keys ...uint64) string {
		var b []byte
		for i, key := range keys {
			b = binary.LittleEndian.AppendUint32(b, uint32(i))    // Timestamp.
			b = binary.LittleEndian.AppendUint64(b, key)          // Key.
			b = binary.LittleEndian.AppendUint32(b, 4096)         // Size.
			b = binary.LittleEndian.AppendUint64(b, ^uint64(0)-1) // Next request.
		}
		return string(b)
	}
	bigEndian := func(keys ...uint64) string {
		var b []byte
		for _, key := range keys {
			b = binary.BigEndian.AppendUint64(b, key)
		}
		return string(b)
	}

	tests := []struct {
		name   string
		format Format
		input  string
		want   []uint64
	}{
		{"ARC", ARC, "10 3 0 0\n7 1 0 3\n", []uint64{10, 11, 12, 7}},
		{"ARC without a final newline", ARC, "1 2 0 0", []uint64{1, 2}},
		{"ARC with other spacing", ARC, "  5\t2   9 9  \r\n\n8 1\n", []uint64{5, 6, 8}},
		{"ARC run of nothing", ARC, "5 0 0 0\n6 1 0 1\n", []uint64{6}},
		{"ARC large block", ARC, "18446744073709551614 2 0 0\n", []uint64{1<<64 - 2, 1<<64 - 1}},
		{"ARC empty", ARC, "", nil},
		{"LIRS", LIRS, "3\n1\n3\n", []uint64{3, 1, 3}},
		{"LIRS with noise", LIRS, "3\r\n\r\n*\n* checkpoint\n 4 \n\n", []uint64{3, 4}},
		{"oracleGeneral", OracleGeneral, oracle(5, 1<<63+9, 5), []uint64{5, 1<<63 + 9, 5}},
		{"oracleGeneral empty", OracleGeneral, "", nil},
		{"big-endian", BigEndian, bigEndian(1, 1<<64-1, 258), []uint64{1, 1<<64 - 1, 258}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Read(strings.NewReader(tt.input), tt.format)
			if err != nil || !slices.Equal(got, tt.want) {
				t.Errorf("Read = %v, %v, want %v", got, err, tt.want)
			}

			// Read must cope with input that arrives in small pieces.
			got, err = Read(iotest.OneByteReader(strings.NewReader(tt.input)), tt.format)
			if err != nil || !slices.Equal(got, tt.want) {
				t.Errorf("Read byte by byte = %v, %v, want %v", got, err, tt.want)
			}
		})
	}
}

func TestReadErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format Format
		input  string
		want   string // Part of the error.
	}{
		{"ARC without a count", ARC, "1 2 0 0\n17\n", "line 2: 1 fields"},
		{"ARC with too many fields", ARC, "5 2 0 0 7\n", "line 1: 5 fields"},
		{"ARC with a count that is no number", ARC, "5 2x 0 0\n", `line 1: unexpected "x" after a number`},
		{"ARC with a fraction", ARC, "5 2.9 0 0\n", `line 1: unexpected ".9" after a number`},
		{"ARC with words at the end", ARC, "5 2 foo bar\n", "line 1: expected a number"},
		{"ARC with a run past the last block", ARC, "18446744073709551614 3 0 0\n", "leaves the range of block numbers"},
		{"ARC with a line that is too long", ARC, "1 1 0 0\n" + strings.Repeat("1", 70_000) + " 1 0 0\n", "line 2: bufio.Scanner: token too long"},
		{"ARC with a word", ARC, "x 2 0 0\n", "line 1: expected a number"},
		{"ARC with a negative block", ARC, "-1 2 0 0\n", "line 1: expected a number"},
		{"ARC with a huge block", ARC, "18446744073709551616 1 0 0\n", "out of range"},
		{"ARC with a huge run", ARC, "1 99999999 0 0\n", "a run of 99999999 blocks"},
		{"LIRS with a word", LIRS, "1\nfoo\n", "line 2: expected a number"},
		{"LIRS with two numbers", LIRS, "1 2\n", `unexpected " 2"`},
		{"oracleGeneral cut short", OracleGeneral, strings.Repeat("x", 24+23), "the last of 2 records is cut short"},
		{"big-endian cut short", BigEndian, "1234567", "the last of 1 records is cut short"},
		{"unknown format", Format(0), "", "unknown format 0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Read(strings.NewReader(tt.input), tt.format)
			if err == nil || !strings.Contains(err.Error(), tt.want) || got != nil {
				t.Errorf("Read = %v, %v, want an error with %q", got, err, tt.want)
			}
		})
	}

	// An error of the reader is passed on.
	inputs := map[Format]string{
		ARC:           strings.Repeat("1 1 0 0\n", 100),
		LIRS:          strings.Repeat("1\n", 100),
		OracleGeneral: strings.Repeat("x", 240),
		BigEndian:     strings.Repeat("x", 80),
	}
	for format, input := range inputs {
		r := iotest.TimeoutReader(iotest.HalfReader(strings.NewReader(input)))
		if _, err := Read(r, format); err == nil || !strings.Contains(err.Error(), "timeout") {
			t.Errorf("%v: Read = %v, want the error of the reader", format, err)
		}
	}
}

func TestZipf(t *testing.T) {
	t.Parallel()

	keys := Zipf(100_000, 1.0001, 10, 1_000_000)
	if len(keys) != 100_000 {
		t.Fatalf("%d requests, want 100000", len(keys))
	}
	if !slices.Equal(keys, Zipf(100_000, 1.0001, 10, 1_000_000)) {
		t.Error("two traces with the same parameters differ")
	}

	// A few keys get most of the requests, and there is a long tail.
	counts := map[uint64]int{}
	for _, key := range keys {
		if key > 1_000_000 {
			t.Fatalf("key %d is out of range", key)
		}
		counts[key]++
	}
	if unique := Unique(keys); unique != len(counts) || unique < 10_000 || unique > 60_000 {
		t.Errorf("Unique = %d, counted %d, want a long tail", unique, len(counts))
	}
	if counts[0] < 20*counts[1000] || counts[0] < 500 {
		t.Errorf("key 0 has %d requests and key 1000 has %d, want a steep drop", counts[0], counts[1000])
	}
}

func FuzzRead(f *testing.F) {
	f.Add(int(ARC), []byte("10 3 0 0\n7 1 0 3\n"))
	f.Add(int(LIRS), []byte("3\n*\n\n4\n"))
	f.Add(int(OracleGeneral), make([]byte, 48))
	f.Add(int(BigEndian), make([]byte, 16))

	f.Fuzz(func(t *testing.T, format int, data []byte) {
		if len(data) > 1<<12 {
			return
		}
		keys, err := Read(bytes.NewReader(data), Format(format))
		if err != nil {
			if keys != nil {
				t.Errorf("Read returned keys along with the error %v", err)
			}
			return
		}

		// Whatever was read can be written down in the plainest way the
		// format has, and comes back the same when that is read.
		var plain []byte
		switch Format(format) {
		case ARC:
			for _, key := range keys {
				plain = fmt.Appendf(plain, "%d 1 0 0\n", key)
			}
		case LIRS:
			for _, key := range keys {
				plain = fmt.Appendf(plain, "%d\n", key)
			}
		case OracleGeneral:
			if len(keys) != len(data)/24 || len(data)%24 != 0 {
				t.Fatalf("%d keys from %d bytes", len(keys), len(data))
			}
			for _, key := range keys {
				plain = binary.LittleEndian.AppendUint64(append(plain, 0, 0, 0, 0), key)
				plain = append(plain, make([]byte, 12)...)
			}
		case BigEndian:
			if len(keys) != len(data)/8 || len(data)%8 != 0 {
				t.Fatalf("%d keys from %d bytes", len(keys), len(data))
			}
			for _, key := range keys {
				plain = binary.BigEndian.AppendUint64(plain, key)
			}
		default:
			t.Fatalf("Read made sense of the unknown format %d", format)
		}
		again, err := Read(bytes.NewReader(plain), Format(format))
		if err != nil || !slices.Equal(again, keys) {
			t.Errorf("the keys %v come back as %v, %v", keys, again, err)
		}
	})
}
