package tsdb

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

func TestConcurrentAppendAndSelect(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer db.Close()
	const writers, perWriter = 8, 500
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			l := labels.FromStrings("__name__", "work", "writer", fmt.Sprint(w))
			for i := 0; i < perWriter; i += 50 {
				batch := make([]LabeledSample, 50)
				for j := range batch {
					batch[j] = LabeledSample{l, ms(t0) + int64(i+j)*1000, float64(i + j)}
				}
				if st, err := db.Append(batch); err != nil || st.Accepted != 50 {
					t.Errorf("append %+v %v", st, err)
					return
				}
			}
		}(w)
	}
	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := db.Select(context.Background(), []*labels.Matcher{eq("__name__", "work")}, math.MinInt64, math.MaxInt64); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
	close(stop)
	readers.Wait()
	got, _ := db.Select(context.Background(), []*labels.Matcher{eq("__name__", "work")}, math.MinInt64, math.MaxInt64)
	if len(got) != writers {
		t.Fatalf("series %d", len(got))
	}
	for _, s := range got {
		if len(s.Samples) != perWriter {
			t.Fatalf("%s has %d samples", s.Labels, len(s.Samples))
		}
	}
}

func TestChunksCutAtBlockBoundariesAndSampleLimit(t *testing.T) {
	h := newHead(4, time.Hour.Milliseconds())
	s, _ := h.getOrCreate(labels.FromStrings("__name__", "x"))
	base := ms(t0)
	// 6 samples in the first hour (chunk limit 4 -> 2 chunks), then one in
	// the next hour which must open a new chunk even though it has room.
	for i := 0; i < 6; i++ {
		if _, err := s.append(base+int64(i)*1000, 1, 4, h.blockDur); err != nil {
			t.Fatal(err)
		}
	}
	s.append(base+time.Hour.Milliseconds(), 2, 4, h.blockDur)
	if len(s.chunks) != 3 || s.chunks[1].c.NumSamples() != 2 || s.chunks[2].minT != base+time.Hour.Milliseconds() {
		t.Fatalf("chunk layout: %d chunks", len(s.chunks))
	}
	h.observe(base)
	h.observe(base + time.Hour.Milliseconds())
	if removed := h.truncateBefore(base + time.Hour.Milliseconds()); removed != 0 {
		t.Fatal("series with remaining chunks must survive")
	}
	if len(s.chunks) != 1 {
		t.Fatalf("chunks after truncate %d", len(s.chunks))
	}
	if minT, _, _ := h.bounds(); minT != base+time.Hour.Milliseconds() {
		t.Fatalf("head minT %d", minT)
	}
	if h.truncateBefore(math.MaxInt64) != 1 || h.numSeries() != 0 || len(h.postings) != 0 {
		t.Fatal("empty series and postings must be removed")
	}
	if _, _, ok := h.bounds(); ok {
		t.Fatal("empty head reports bounds")
	}
}

func TestWALCheckpointRemovesOldSegments(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	fill(t, db, 2, t0, 3*time.Hour)
	before, _ := filepath.Glob(filepath.Join(dir, "wal", "*.wal"))
	if _, err := db.CutBlocks(true); err != nil {
		t.Fatal(err)
	}
	after, _ := filepath.Glob(filepath.Join(dir, "wal", "*.wal"))
	if len(after) != 1 || after[0] == before[0] {
		t.Fatalf("wal segments before=%v after=%v", before, after)
	}
	st := db.Stats()
	if st.HeadSeries != 0 || st.WALBytes > 64 {
		t.Fatalf("after full cut: %+v", st)
	}
	db.Close()
	// A corrupt record in a non-final segment is a hard error.
	os.WriteFile(filepath.Join(dir, "wal", "00000000.wal"), []byte{5, 1, 2, 3, 4, 5, 0, 0, 0, 0}, 0o644)
	if _, err := Open(Options{Dir: dir}); err == nil {
		t.Fatal("corruption in an older WAL segment must fail Open")
	}
}

func TestPostingsIntersection(t *testing.T) {
	idx := map[string]map[string][]uint64{
		"job":  {"api": {1, 2, 3, 5}, "web": {4}},
		"code": {"200": {1, 4, 5}, "500": {2, 3}},
	}
	all := func() []uint64 { return []uint64{1, 2, 3, 4, 5} }
	cases := []struct {
		ms   []*labels.Matcher
		want int
	}{
		{[]*labels.Matcher{eq("job", "api"), eq("code", "200")}, 2},
		{[]*labels.Matcher{labels.MustMatcher(labels.MatchRegexp, "code", "2..|5..")}, 5},
		{[]*labels.Matcher{eq("job", "api"), labels.MustMatcher(labels.MatchNotEqual, "code", "200")}, 4}, // != is filtered later
		{[]*labels.Matcher{eq("job", "none")}, 0},
		{[]*labels.Matcher{eq("zone", "")}, 5},
	}
	for i, c := range cases {
		if got := postingsForMatchers(idx, all, c.ms); len(got) != c.want {
			t.Errorf("case %d: got %v", i, got)
		}
	}
	if got := insertSorted([]uint64{1, 3}, 2); fmt.Sprint(got) != "[1 2 3]" {
		t.Fatal(got)
	}
	if got := insertSorted([]uint64{1, 3}, 3); len(got) != 2 {
		t.Fatal("duplicates must not be inserted")
	}
	if floorDiv(-1, 10) != -1 || floorDiv(10, 10) != 1 || floorDiv(-10, 10) != -1 {
		t.Fatal("floorDiv")
	}
}
