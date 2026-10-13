package retention

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/logs"
	"github.com/robertramos/developer-observability-stack/internal/selfmon"
	"github.com/robertramos/developer-observability-stack/internal/tenant"
	"github.com/robertramos/developer-observability-stack/internal/traces"
	"github.com/robertramos/developer-observability-stack/internal/tsdb"
	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

func TestRunOnceAppliesPoliciesPerSignal(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.Logs.Retention = config.Duration(24 * time.Hour)
	cfg.Logs.CompactMinSegments = 2
	cfg.Logs.SegmentMaxEntries = 10
	cfg.Traces.Retention = config.Duration(24 * time.Hour)
	cfg.Metrics.BlockDuration = config.Duration(time.Hour)
	cfg.Metrics.RawRetention = config.Duration(6 * time.Hour)
	cfg.Metrics.DownsampleAfter = config.Duration(2 * time.Hour)
	reg := tenant.NewRegistry(cfg, slog.New(slog.DiscardHandler))
	defer reg.Close()
	ten, err := reg.Get("acme")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	var entries []logs.Entry
	for i := 0; i < 30; i++ { // 3 small segments in a partition 3 days old
		entries = append(entries, logs.Entry{Timestamp: now.Add(-72 * time.Hour).Add(time.Duration(i) * time.Second).UnixNano(), Message: "old"})
	}
	for i := 0; i < 30; i++ { // 3 small segments in a closed partition 2 hours old
		entries = append(entries, logs.Entry{Timestamp: now.Add(-2 * time.Hour).Add(time.Duration(i) * time.Second).UnixNano(), Message: "recent"})
	}
	if err := ten.Logs.Append(entries); err != nil {
		t.Fatal(err)
	}
	var samples []tsdb.LabeledSample
	for ts := now.Add(-10 * time.Hour); ts.Before(now); ts = ts.Add(time.Minute) {
		samples = append(samples, tsdb.LabeledSample{Labels: labels.FromStrings("__name__", "load"), T: ts.UnixMilli(), V: 1})
	}
	if _, err := ten.Metrics.Append(samples); err != nil {
		t.Fatal(err)
	}
	mk := func(id string, age time.Duration) traces.Span {
		return traces.Span{TraceID: strings.Repeat(id, 32), SpanID: strings.Repeat(id, 16), Name: "op", Service: "s",
			StartUnixNano: now.Add(-age).UnixNano(), EndUnixNano: now.Add(-age).UnixNano() + 1}
	}
	ten.Traces.Append([]traces.Span{mk("1", 48*time.Hour), mk("2", time.Hour)})

	m := selfmon.New()
	r := New(reg, cfg, m, slog.New(slog.DiscardHandler))
	r.Now = func() time.Time { return now }
	reports := r.RunOnce(context.Background())
	if len(reports) != 1 {
		t.Fatalf("reports %v", reports)
	}
	rep := reports[0]
	if len(rep.Errors) != 0 {
		t.Fatalf("errors %v", rep.Errors)
	}
	// The recent partition has two sealed segments (rolled at 10 entries)
	// plus an active one that was written moments ago in wall-clock time,
	// so it is not sealed yet: compaction merges the two sealed ones.
	if rep.LogPartitionsRemoved != 1 || rep.LogSegmentsSealed != 0 || rep.LogSegmentsCompacted != 1 || rep.TracesRemoved != 1 {
		t.Fatalf("log/trace maintenance %+v", rep)
	}
	// 10h of data (02:00..11:59) with 1h blocks: windows ending by 11:29
	// are cut (9); blocks ending by 10:00 are downsampled (8); raw blocks
	// ending by 06:00 exceed the 6h retention (4).
	if rep.MetricBlocksCut != 9 || rep.MetricBlocksDownsample != 8 || rep.MetricBlocksRemoved != 4 {
		t.Fatalf("metrics maintenance %+v", rep)
	}
	if st := ten.Logs.Stats(); st.Entries != 30 || st.Segments != 2 {
		t.Fatalf("log stats after maintenance %+v", st)
	}
	if got := testutil.ToFloat64(m.MaintenanceRuns.WithLabelValues("logs_retention", "success")); got != 1 {
		t.Fatalf("maintenance metric %v", got)
	}
	if got := testutil.ToFloat64(m.MaintenanceRemove.WithLabelValues("traces")); got != 1 {
		t.Fatalf("removed metric %v", got)
	}
	// A second pass is idempotent.
	rep = r.RunOnce(context.Background())[0]
	if rep.LogPartitionsRemoved+rep.TracesRemoved+rep.MetricBlocksCut+rep.LogSegmentsCompacted != 0 {
		t.Fatalf("second pass changed state: %+v", rep)
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	reg := tenant.NewRegistry(cfg, slog.New(slog.DiscardHandler))
	defer reg.Close()
	r := New(reg, cfg, nil, slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx, time.Millisecond); close(done) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}
