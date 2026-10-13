package tsdb

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

var t0 = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

func openDB(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(Options{Dir: dir, BlockDuration: time.Hour, ChunkMaxSamples: 50,
		RawRetention: 24 * time.Hour, DownsampleResolution: 5 * time.Minute, DownsampledRetention: 7 * 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func ms(t time.Time) int64 { return t.UnixMilli() }

func eq(name, value string) *labels.Matcher {
	return labels.MustMatcher(labels.MatchEqual, name, value)
}

// fill appends a counter series for each instance every 15s over dur.
func fill(t *testing.T, db *DB, instances int, start time.Time, dur time.Duration) int {
	t.Helper()
	var batch []LabeledSample
	n := 0
	for ts := start; ts.Before(start.Add(dur)); ts = ts.Add(15 * time.Second) {
		for i := 0; i < instances; i++ {
			batch = append(batch, LabeledSample{
				Labels: labels.FromStrings("__name__", "http_requests_total", "instance", fmt.Sprintf("host-%d", i), "job", "api"),
				T:      ms(ts), V: float64(n),
			})
		}
		n++
	}
	st, err := db.Append(batch)
	if err != nil || st.Rejected() != 0 {
		t.Fatalf("append: %+v %v", st, err)
	}
	return len(batch)
}

func TestAppendSelectAndMatchers(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer db.Close()
	fill(t, db, 3, t0, 10*time.Minute)
	ctx := context.Background()
	got, err := db.Select(ctx, []*labels.Matcher{eq("__name__", "http_requests_total")}, ms(t0), ms(t0.Add(time.Hour)))
	if err != nil || len(got) != 3 || len(got[0].Samples) != 40 {
		t.Fatalf("select: %d series, err %v", len(got), err)
	}
	cases := []struct {
		ms   []*labels.Matcher
		want int
	}{
		{[]*labels.Matcher{eq("instance", "host-1")}, 1},
		{[]*labels.Matcher{labels.MustMatcher(labels.MatchRegexp, "instance", "host-[02]")}, 2},
		{[]*labels.Matcher{eq("job", "api"), labels.MustMatcher(labels.MatchNotEqual, "instance", "host-0")}, 2},
		{[]*labels.Matcher{eq("job", "api"), labels.MustMatcher(labels.MatchNotRegexp, "instance", "host-.*")}, 0},
		// Matches the empty string: selects series lacking the label.
		{[]*labels.Matcher{eq("job", "api"), eq("zone", "")}, 3},
		{[]*labels.Matcher{labels.MustMatcher(labels.MatchRegexp, "zone", ".*")}, 3},
		{[]*labels.Matcher{eq("job", "web")}, 0},
	}
	for i, c := range cases {
		got, err := db.Select(ctx, c.ms, ms(t0), ms(t0.Add(time.Hour)))
		if err != nil || len(got) != c.want {
			t.Errorf("case %d: got %d series (err %v), want %d", i, len(got), err, c.want)
		}
	}
	// Time range restriction is inclusive on both ends.
	got, _ = db.Select(ctx, []*labels.Matcher{eq("instance", "host-0")}, ms(t0.Add(time.Minute)), ms(t0.Add(2*time.Minute)))
	if len(got[0].Samples) != 5 {
		t.Fatalf("range samples %d", len(got[0].Samples))
	}
	if names := db.LabelNames(); len(names) != 3 {
		t.Fatalf("label names %v", names)
	}
	if vals := db.LabelValues("instance"); len(vals) != 3 || vals[2] != "host-2" {
		t.Fatalf("label values %v", vals)
	}
	if _, err := db.Select(ctx, nil, 0, 1); err == nil {
		t.Fatal("select without matchers must fail")
	}
}

func TestAppendRejections(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer db.Close()
	l := labels.FromStrings("__name__", "g")
	st, err := db.Append([]LabeledSample{
		{l, 1000, 1}, {l, 2000, 2}, {l, 1500, 9}, // out of order
		{l, 2000, 2}, // idempotent duplicate: ignored
		{l, 2000, 3}, // conflicting duplicate
		{labels.FromStrings("__name__", "bad-name"), 1, 1},
		{labels.FromStrings("job", "x"), 1, 1}, // missing name
		{l, 3000, math.NaN()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.Accepted != 3 || st.OutOfOrder != 1 || st.Duplicates != 1 || st.Invalid != 2 || st.FirstError == "" {
		t.Fatalf("stats %+v", st)
	}
}

func TestWALReplayAndTornTail(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	n := fill(t, db, 2, t0, 5*time.Minute)
	db.Close()
	// Corrupt the WAL tail as a crash mid-write would.
	segs, _ := filepath.Glob(filepath.Join(dir, "wal", "*.wal"))
	f, _ := os.OpenFile(segs[len(segs)-1], os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{0x30, recSamples, 1, 2})
	f.Close()

	db2 := openDB(t, dir)
	defer db2.Close()
	got, err := db2.Select(context.Background(), []*labels.Matcher{eq("job", "api")}, math.MinInt64, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, s := range got {
		total += len(s.Samples)
	}
	if total != n {
		t.Fatalf("replayed %d samples, want %d", total, n)
	}
	// Appending continues after the replayed data without conflicts.
	st, _ := db2.Append([]LabeledSample{{labels.FromStrings("__name__", "http_requests_total", "instance", "host-0", "job", "api"), ms(t0.Add(time.Hour)), 1}})
	if st.Accepted != 1 {
		t.Fatalf("append after replay %+v", st)
	}
}

func TestCutBlocksPersistAndReopen(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	n := fill(t, db, 2, t0, 3*time.Hour)
	cut, err := db.CutBlocks(false)
	if err != nil {
		t.Fatal(err)
	}
	// Head spans 3h with 1h blocks: windows [0,1h) and [1h,2h) are cut;
	// [2h,3h) stays because the head does not extend 30m beyond it.
	if cut != 2 {
		t.Fatalf("cut %d blocks", cut)
	}
	st := db.Stats()
	if len(st.Blocks) != 2 || st.Blocks[0].NumSamples != 2*240 || st.HeadMinT != ms(t0.Add(2*time.Hour)) {
		t.Fatalf("stats after cut: %+v", st)
	}
	// Samples older than the newest block are rejected now.
	old, _ := db.Append([]LabeledSample{{labels.FromStrings("__name__", "late"), ms(t0.Add(30 * time.Minute)), 1}})
	if old.TooOld != 1 {
		t.Fatalf("expected too-old rejection, got %+v", old)
	}
	db.Close()

	db2 := openDB(t, dir)
	defer db2.Close()
	got, _ := db2.Select(context.Background(), []*labels.Matcher{eq("__name__", "http_requests_total")}, math.MinInt64, math.MaxInt64)
	total := 0
	for _, s := range got {
		total += len(s.Samples)
		for i := 1; i < len(s.Samples); i++ {
			if s.Samples[i].T <= s.Samples[i-1].T {
				t.Fatal("merged samples not strictly increasing")
			}
		}
	}
	if total != n {
		t.Fatalf("blocks+head after reopen: %d samples, want %d", total, n)
	}
	// The WAL was checkpointed: replay must not resurrect cut samples.
	if s := db2.Stats(); s.HeadMinT != ms(t0.Add(2*time.Hour)) {
		t.Fatalf("head after reopen starts at %d", s.HeadMinT)
	}
	if forced, _ := db2.CutBlocks(true); forced != 1 {
		t.Fatalf("forced cut = %d", forced)
	}
}

func TestDownsampleAggregates(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer db.Close()
	l := labels.FromStrings("__name__", "temp", "room", "a")
	var batch []LabeledSample
	// 1h of 1-minute samples with values 0..59.
	for i := 0; i < 60; i++ {
		batch = append(batch, LabeledSample{l, ms(t0.Add(time.Duration(i) * time.Minute)), float64(i)})
	}
	batch = append(batch, LabeledSample{l, ms(t0.Add(2 * time.Hour)), 0})
	db.Append(batch)
	if _, err := db.CutBlocks(true); err != nil {
		t.Fatal(err)
	}
	created, err := db.Downsample(context.Background(), t0.Add(48*time.Hour))
	if err != nil || created != 2 {
		t.Fatalf("downsample created %d, err %v", created, err)
	}
	again, _ := db.Downsample(context.Background(), t0.Add(48*time.Hour))
	if again != 0 {
		t.Fatal("downsampling must be idempotent")
	}
	ctx := context.Background()
	sel := []*labels.Matcher{eq("room", "a")}
	check := func(aggr string, want []float64) {
		t.Helper()
		got, err := db.SelectDownsampled(ctx, sel, ms(t0), ms(t0.Add(time.Hour))-1, aggr)
		if err != nil || len(got) != 1 {
			t.Fatalf("%s: %v %d", aggr, err, len(got))
		}
		if len(got[0].Samples) != len(want) {
			t.Fatalf("%s: %d buckets, want %d", aggr, len(got[0].Samples), len(want))
		}
		for i, w := range want {
			if got[0].Samples[i].V != w {
				t.Fatalf("%s bucket %d = %v, want %v", aggr, i, got[0].Samples[i].V, w)
			}
		}
	}
	// 12 five-minute buckets; bucket k holds values 5k..5k+4.
	var counts, sums, mins, maxs, lasts, avgs []float64
	for k := 0; k < 12; k++ {
		b := float64(5 * k)
		counts = append(counts, 5)
		sums = append(sums, 5*b+10)
		mins = append(mins, b)
		maxs = append(maxs, b+4)
		lasts = append(lasts, b+4)
		avgs = append(avgs, b+2)
	}
	check(AggrCount, counts)
	check(AggrSum, sums)
	check(AggrMin, mins)
	check(AggrMax, maxs)
	check(AggrLast, lasts)
	check("avg", avgs)
	if _, err := db.SelectDownsampled(ctx, sel, 0, 1, "median"); err == nil {
		t.Fatal("unknown aggregate must fail")
	}
	// Raw retention removes raw blocks; queries fall back to downsampled
	// "last" values for the expired range.
	res, err := db.ApplyRetention(t0.Add(27 * time.Hour))
	if err != nil || res.RawBlocks != 2 {
		t.Fatalf("retention %+v %v", res, err)
	}
	got, _ := db.Select(ctx, sel, ms(t0), ms(t0.Add(time.Hour)))
	if len(got) != 1 || len(got[0].Samples) != 12 || got[0].Samples[11].V != 59 {
		t.Fatalf("fallback to downsampled data failed: %+v", got)
	}
	res, _ = db.ApplyRetention(t0.Add(30 * 24 * time.Hour))
	if res.DownsampledBlocks != 2 {
		t.Fatalf("downsampled retention %+v", res)
	}
	if st := db.Stats(); len(st.Blocks)+len(st.DownsampledBlocks) != 0 {
		t.Fatalf("blocks remain: %+v", st)
	}
	entries, _ := os.ReadDir(filepath.Join(db.opts.Dir, "blocks"))
	if len(entries) != 0 {
		t.Fatalf("block dirs remain: %d", len(entries))
	}
}

func TestHeadRetentionWithoutBlocks(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer db.Close()
	fill(t, db, 1, t0, 2*time.Hour)
	res, err := db.ApplyRetention(t0.Add(25 * time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	minT, _, _ := db.head.bounds()
	if minT < ms(t0.Add(time.Hour)) || res.HeadSeries != 0 {
		t.Fatalf("head minT %d res %+v", minT, res)
	}
}

func TestCorruptBlockIsSkipped(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	fill(t, db, 1, t0, 2*time.Hour)
	db.CutBlocks(true)
	db.Close()
	blocks, _ := filepath.Glob(filepath.Join(dir, "blocks", "*", "chunks.bin"))
	raw, _ := os.ReadFile(blocks[0])
	raw[20] ^= 0xff
	os.WriteFile(blocks[0], raw, 0o644)
	db2 := openDB(t, dir)
	defer db2.Close()
	if st := db2.Stats(); len(st.Blocks) != 1 {
		t.Fatalf("expected the corrupt block to be skipped, have %d", len(st.Blocks))
	}
}
