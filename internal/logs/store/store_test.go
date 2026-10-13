package store

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/logs"
	"github.com/robertramos/developer-observability-stack/internal/logs/query"
)

var base = time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC)

func openTest(t *testing.T, dir string, mod func(*Options)) *Store {
	t.Helper()
	opts := Options{Dir: dir, PartitionDuration: time.Hour, SegmentMaxBytes: 1 << 20, SegmentMaxEntries: 1000, CompactMinSegments: 3}
	if mod != nil {
		mod(&opts)
	}
	s, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func genEntries(n int, start time.Time, step time.Duration) []logs.Entry {
	levels := []string{"info", "info", "warn", "error"}
	svcs := []string{"api", "web", "worker"}
	out := make([]logs.Entry, n)
	for i := range out {
		out[i] = logs.Entry{
			Timestamp: start.Add(time.Duration(i) * step).UnixNano(),
			Level:     levels[i%len(levels)],
			Message:   fmt.Sprintf("request %d handled status=%d path=/v1/items", i, 200+100*(i%4)),
			Fields:    map[string]string{"service": svcs[i%len(svcs)], "seq": fmt.Sprint(i)},
		}
	}
	return out
}

func search(t *testing.T, s *Store, q string, limit int) *SearchResult {
	t.Helper()
	pq, err := query.Parse(q)
	if err != nil {
		t.Fatal(err)
	}
	start, end := ResolveRange(pq, 0, 0, base.Add(10*time.Hour))
	res, err := s.Search(context.Background(), SearchRequest{Query: pq, Start: start, End: end, Limit: limit})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestAppendSearchOrderingAndLimit(t *testing.T) {
	s := openTest(t, t.TempDir(), nil)
	defer s.Close()
	// 3 hours of data, one entry per 20s, across three partitions.
	if err := s.Append(genEntries(540, base, 20*time.Second)); err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.Partitions != 3 || st.Entries != 540 {
		t.Fatalf("stats %+v", st)
	}
	res := search(t, s, "level:error service:api", 5)
	if len(res.Entries) != 5 || !res.Truncated {
		t.Fatalf("got %d entries truncated=%v", len(res.Entries), res.Truncated)
	}
	// error = i%4==3, api = i%3==0 -> i%12==3; newest is 531.
	if res.Entries[0].Fields["seq"] != "531" || res.Entries[4].Fields["seq"] != "483" {
		t.Fatalf("desc order wrong: first=%s last=%s", res.Entries[0].Fields["seq"], res.Entries[4].Fields["seq"])
	}
	for i := 1; i < len(res.Entries); i++ {
		if res.Entries[i].Timestamp > res.Entries[i-1].Timestamp {
			t.Fatal("not sorted desc")
		}
	}
	asc := search(t, s, "level:error service:api | sort asc", 3)
	if asc.Entries[0].Fields["seq"] != "3" || asc.Entries[2].Fields["seq"] != "27" {
		t.Fatalf("asc order wrong: %v", asc.Entries)
	}
	// Top-K pruning: only the newest partition should need scanning.
	if res.Stats.SegmentsPruned == 0 {
		t.Fatalf("expected segment pruning, stats %+v", res.Stats)
	}
	full := search(t, s, `"status 500" path`, 1000)
	if len(full.Entries) != 135 || full.Truncated {
		t.Fatalf("phrase query got %d", len(full.Entries))
	}
}

func TestTimeRangeAndCount(t *testing.T) {
	s := openTest(t, t.TempDir(), nil)
	defer s.Close()
	if err := s.Append(genEntries(360, base, 10*time.Second)); err != nil {
		t.Fatal(err)
	}
	pq, _ := query.Parse("level:error")
	from, to := base.Add(10*time.Minute).UnixNano(), base.Add(20*time.Minute).UnixNano()
	res, err := s.Search(context.Background(), SearchRequest{Query: pq, Start: from, End: to, Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range res.Entries {
		if e.Timestamp < from || e.Timestamp > to {
			t.Fatal("entry outside range")
		}
	}
	if len(res.Entries) != 15 { // 61 entries in range (inclusive), i%4==3 among i in [60,120]
		t.Fatalf("range search returned %d", len(res.Entries))
	}
	cq, _ := query.Parse("| count by level, service")
	cres, err := s.Search(context.Background(), SearchRequest{Query: cq})
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, g := range cres.Groups {
		total += g.Count
	}
	if total != 360 || len(cres.Groups) != 9 {
		t.Fatalf("groups=%d total=%d", len(cres.Groups), total)
	}
	nq, _ := query.Parse("NOT level:info | count")
	nres, _ := s.Search(context.Background(), SearchRequest{Query: nq})
	if nres.Groups[0].Count != 180 || nres.Stats.EntriesRead != 0 {
		t.Fatalf("exact count should not read entries: %+v", nres.Stats)
	}
	if _, err := s.Search(context.Background(), SearchRequest{Query: cq, MaxGroups: 3}); err == nil {
		t.Fatal("group limit must be enforced")
	}
}

func TestRestartRecoversUnsealedSegmentAndTornTail(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, nil)
	if err := s.Append(genEntries(100, base, time.Second)); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash: no Close (no seal), so no .idx exists. Flush the
	// buffered writer by hand and append garbage to emulate a torn write.
	var logPath string
	for _, p := range s.partitions {
		p.active.flush()
		logPath = p.active.base + ".log"
		p.active.closeFiles()
	}
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0x20, 1, 2, 3})
	f.Close()
	if _, err := os.Stat(strings.TrimSuffix(logPath, ".log") + ".idx"); !os.IsNotExist(err) {
		t.Fatal("index should not exist before recovery")
	}

	s2 := openTest(t, dir, nil)
	defer s2.Close()
	if st := s2.Stats(); st.Entries != 100 {
		t.Fatalf("recovered %d entries", st.Entries)
	}
	if res := search(t, s2, "seq:42", 10); len(res.Entries) != 1 {
		t.Fatal("recovered segment not searchable")
	}
	// New writes after recovery go to a fresh segment.
	if err := s2.Append(genEntries(1, base.Add(time.Minute), 0)); err != nil {
		t.Fatal(err)
	}
	if st := s2.Stats(); st.Entries != 101 || st.Segments != 2 {
		t.Fatalf("after append %+v", st)
	}
}

func TestCorruptIndexIsRebuilt(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, nil)
	s.Append(genEntries(50, base, time.Second))
	s.Close()
	idx, _ := filepath.Glob(filepath.Join(dir, "*", "*.idx"))
	if len(idx) != 1 {
		t.Fatalf("idx files %v", idx)
	}
	raw, _ := os.ReadFile(idx[0])
	raw[len(raw)/2] ^= 0xff
	os.WriteFile(idx[0], raw, 0o644)
	s2 := openTest(t, dir, nil)
	defer s2.Close()
	if res := search(t, s2, "service:web", 100); len(res.Entries) != 17 {
		t.Fatalf("rebuilt index search got %d", len(res.Entries))
	}
}

func TestSegmentRollAndCompaction(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, func(o *Options) { o.SegmentMaxEntries = 40 })
	defer s.Close()
	// Interleaved timestamps so compaction has to re-sort.
	entries := genEntries(200, base, time.Second)
	for i := 0; i < len(entries); i += 2 {
		entries[i], entries[i+1] = entries[i+1], entries[i]
	}
	if err := s.Append(entries); err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.Segments != 5 {
		t.Fatalf("expected 5 segments, got %+v", st)
	}
	if n, _ := s.SealIdle(base.Add(3 * time.Hour)); n != 0 {
		t.Fatalf("partition recently written must not be sealed yet (sealed %d)", n)
	}
	for _, p := range s.partitions {
		p.lastWrite = time.Time{}
	}
	if n, err := s.SealIdle(base.Add(3 * time.Hour)); err != nil || n != 1 {
		t.Fatalf("SealIdle = %d, %v", n, err)
	}
	removed, err := s.Compact(context.Background())
	if err != nil || removed != 4 {
		t.Fatalf("Compact = %d, %v", removed, err)
	}
	st := s.Stats()
	if st.Segments != 1 || st.Entries != 200 {
		t.Fatalf("after compaction %+v", st)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*", "*.log"))
	if len(files) != 1 {
		t.Fatalf("old segment files not deleted: %v", files)
	}
	res := search(t, s, "| sort asc", 200)
	for i := 1; i < len(res.Entries); i++ {
		if res.Entries[i].Timestamp < res.Entries[i-1].Timestamp {
			t.Fatal("compacted results out of order")
		}
	}
	// Compacted data survives restart.
	s.Close()
	s2 := openTest(t, dir, nil)
	defer s2.Close()
	if st := s2.Stats(); st.Entries != 200 {
		t.Fatalf("after reopen %+v", st)
	}
}

func TestRetentionDeletesOldPartitions(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir, nil)
	defer s.Close()
	s.Append(genEntries(300, base, 30*time.Second)) // 2.5h -> 3 partitions
	removed, err := s.ApplyRetention(base.Add(2 * time.Hour).UnixNano())
	if err != nil || removed != 2 {
		t.Fatalf("ApplyRetention = %d, %v", removed, err)
	}
	st := s.Stats()
	if st.Partitions != 1 || st.Entries != 60 {
		t.Fatalf("after retention %+v", st)
	}
	dirs, _ := filepath.Glob(filepath.Join(dir, "p-*"))
	if len(dirs) != 1 {
		t.Fatalf("partition dirs left: %v", dirs)
	}
	if res := search(t, s, "seq:10", 10); len(res.Entries) != 0 {
		t.Fatal("expired data still visible")
	}
}

func TestConcurrentAppendSearchCompact(t *testing.T) {
	s := openTest(t, t.TempDir(), func(o *Options) { o.SegmentMaxEntries = 25 })
	defer s.Close()
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				if err := s.Append(genEntries(10, base.Add(time.Duration(w*1000+i*10)*time.Second), time.Second)); err != nil {
					t.Error(err)
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
		pq, _ := query.Parse("service:api OR level:error")
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := s.Search(context.Background(), SearchRequest{Query: pq, Limit: 50}); err != nil {
				t.Error(err)
				return
			}
			s.Compact(context.Background())
		}
	}()
	wg.Wait()
	close(stop)
	readers.Wait()
	if st := s.Stats(); st.Entries != 800 {
		t.Fatalf("entries %d", st.Entries)
	}
}

func TestSearchValidation(t *testing.T) {
	s := openTest(t, t.TempDir(), nil)
	defer s.Close()
	pq, _ := query.Parse("x")
	if _, err := s.Search(context.Background(), SearchRequest{Query: pq, Limit: 0}); err == nil {
		t.Fatal("zero limit must be rejected")
	}
	if _, err := s.Search(context.Background(), SearchRequest{Query: pq, Limit: 1, Start: 10, End: 5}); err == nil {
		t.Fatal("inverted range must be rejected")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.Append(genEntries(3000, base, time.Millisecond))
	all, _ := query.Parse("")
	if _, err := s.Search(ctx, SearchRequest{Query: all, Limit: 5000}); err == nil {
		t.Fatal("cancelled context must abort the scan")
	}
}
