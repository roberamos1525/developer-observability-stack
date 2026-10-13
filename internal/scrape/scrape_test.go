package scrape

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/selfmon"
	"github.com/robertramos/developer-observability-stack/internal/tsdb"
)

type captured struct {
	mu      sync.Mutex
	samples map[string][]tsdb.LabeledSample
}

func (c *captured) append(_ context.Context, tenant string, s []tsdb.LabeledSample) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.samples[tenant] = append(c.samples[tenant], s...)
	return nil
}

func (c *captured) find(tenant, name string) []tsdb.LabeledSample {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []tsdb.LabeledSample
	for _, s := range c.samples[tenant] {
		if s.Labels.Get("__name__") == name {
			out = append(out, s)
		}
	}
	return out
}

func TestScrapeSuccessAndFailure(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept"), "text/plain") {
			t.Error("missing Accept header")
		}
		w.Write([]byte("# TYPE jobs_done counter\njobs_done{job=\"inner\",queue=\"a\"} 42\nexplicit_ts 1 1700000000000\n"))
	}))
	defer good.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer bad.Close()

	cfg := config.ScrapeConfig{
		Interval: config.Duration(time.Minute), Timeout: config.Duration(2 * time.Second), MaxBodyBytes: 1 << 20,
		Targets: []config.ScrapeTarget{
			{Tenant: "acme", Job: "worker", URL: good.URL + "/metrics", Labels: map[string]string{"env": "test"}},
			{Tenant: "acme", Job: "broken", URL: bad.URL + "/metrics"},
		},
	}
	c := &captured{samples: map[string][]tsdb.LabeledSample{}}
	m, err := NewManager(cfg, c.append, selfmon.New(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	fixed := time.Unix(1_800_000_000, 0)
	m.now = func() time.Time { return fixed }
	m.ScrapeAll(context.Background())

	jobs := c.find("acme", "jobs_done")
	if len(jobs) != 1 {
		t.Fatalf("jobs_done samples: %v", jobs)
	}
	l := jobs[0].Labels
	if l.Get("job") != "worker" || l.Get("exported_job") != "inner" || l.Get("env") != "test" || l.Get("instance") == "" {
		t.Fatalf("target labels: %v", l)
	}
	if jobs[0].T != fixed.UnixMilli() {
		t.Fatal("samples without timestamps use the scrape time")
	}
	if ts := c.find("acme", "explicit_ts"); ts[0].T != 1700000000000 {
		t.Fatal("explicit timestamps must be honoured")
	}
	ups := c.find("acme", "up")
	if len(ups) != 2 {
		t.Fatalf("up series: %v", ups)
	}
	for _, u := range ups {
		want := 1.0
		if u.Labels.Get("job") == "broken" {
			want = 0
		}
		if u.V != want {
			t.Fatalf("up for %s = %v", u.Labels.Get("job"), u.V)
		}
	}
	st := m.Targets("acme")
	if len(st) != 2 {
		t.Fatalf("targets %v", st)
	}
	for _, s := range st {
		if s.Job == "broken" && (s.Health != "down" || !strings.Contains(s.LastError, "500")) {
			t.Fatalf("broken target status %+v", s)
		}
		if s.Job == "worker" && (s.Health != "up" || s.SamplesScraped != 2) {
			t.Fatalf("worker target status %+v", s)
		}
	}
	if len(m.Targets("other")) != 0 {
		t.Fatal("targets are filtered by tenant")
	}
}

func TestScrapeBodyLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 1000; i++ {
			w.Write([]byte("some_metric_with_a_long_name 1\n"))
		}
	}))
	defer srv.Close()
	cfg := config.ScrapeConfig{Interval: config.Duration(time.Minute), Timeout: config.Duration(time.Second), MaxBodyBytes: 1024,
		Targets: []config.ScrapeTarget{{Tenant: "t", Job: "big", URL: srv.URL}}}
	c := &captured{samples: map[string][]tsdb.LabeledSample{}}
	m, _ := NewManager(cfg, c.append, nil, slog.New(slog.DiscardHandler))
	m.ScrapeAll(context.Background())
	if st := m.Targets(""); st[0].Health != "down" || !strings.Contains(st[0].LastError, "exceeds") {
		t.Fatalf("oversized response must fail the scrape: %+v", st[0])
	}
}
