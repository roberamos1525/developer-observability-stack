// Package scrape periodically pulls Prometheus text exposition endpoints
// and appends the samples to a tenant's TSDB, with Prometheus-compatible
// target labels (job, instance) and synthetic up/scrape_* series.
package scrape

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/promtext"
	"github.com/robertramos/developer-observability-stack/internal/selfmon"
	"github.com/robertramos/developer-observability-stack/internal/tsdb"
	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

// Appender persists scraped samples for a tenant.
type Appender func(ctx context.Context, tenant string, samples []tsdb.LabeledSample) error

// TargetStatus is the observable state of one target.
type TargetStatus struct {
	Tenant         string            `json:"tenant"`
	Job            string            `json:"job"`
	URL            string            `json:"url"`
	Labels         map[string]string `json:"labels"`
	Health         string            `json:"health"`
	LastScrape     time.Time         `json:"last_scrape"`
	LastDurationMs float64           `json:"last_duration_ms"`
	LastError      string            `json:"last_error,omitempty"`
	SamplesScraped int               `json:"samples_scraped"`
}

type target struct {
	cfg      config.ScrapeTarget
	instance string
	mu       sync.Mutex
	status   TargetStatus
}

// Manager runs one scrape loop per target.
type Manager struct {
	targets  []*target
	interval time.Duration
	timeout  time.Duration
	maxBody  int64
	client   *http.Client
	app      Appender
	metrics  *selfmon.Metrics
	log      *slog.Logger
	now      func() time.Time
}

// NewManager validates targets and builds a manager.
func NewManager(cfg config.ScrapeConfig, app Appender, m *selfmon.Metrics, log *slog.Logger) (*Manager, error) {
	mgr := &Manager{
		interval: cfg.Interval.D(), timeout: cfg.Timeout.D(), maxBody: cfg.MaxBodyBytes,
		client: &http.Client{Timeout: cfg.Timeout.D()}, app: app, metrics: m, log: log, now: time.Now,
	}
	for _, tc := range cfg.Targets {
		u, err := url.Parse(tc.URL)
		if err != nil {
			return nil, fmt.Errorf("scrape target %s: %w", tc.URL, err)
		}
		mgr.targets = append(mgr.targets, &target{cfg: tc, instance: u.Host, status: TargetStatus{
			Tenant: tc.Tenant, Job: tc.Job, URL: tc.URL, Labels: tc.Labels, Health: "unknown",
		}})
	}
	return mgr, nil
}

// Run scrapes every target until ctx is cancelled. Start times are spread
// across the interval by hashing the target URL.
func (m *Manager) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, t := range m.targets {
		wg.Add(1)
		go func(t *target) {
			defer wg.Done()
			h := fnv.New32a()
			h.Write([]byte(t.cfg.URL))
			offset := time.Duration(h.Sum32()) % m.interval
			timer := time.NewTimer(offset)
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
				}
				m.ScrapeOnce(ctx, t)
				timer.Reset(m.interval)
			}
		}(t)
	}
	wg.Wait()
}

// ScrapeAll scrapes every target once (used by tests and the admin API).
func (m *Manager) ScrapeAll(ctx context.Context) {
	for _, t := range m.targets {
		m.ScrapeOnce(ctx, t)
	}
}

// ScrapeOnce performs one scrape of t.
func (m *Manager) ScrapeOnce(ctx context.Context, t *target) {
	start := m.now()
	samples, err := m.fetch(ctx, t, start)
	dur := m.now().Sub(start)
	up := 1.0
	if err != nil {
		up = 0
		samples = nil
	}
	ts := start.UnixMilli()
	for name, v := range map[string]float64{
		"up": up, "scrape_duration_seconds": dur.Seconds(), "scrape_samples_scraped": float64(len(samples)),
	} {
		samples = append(samples, tsdb.LabeledSample{Labels: m.targetLabels(t, labels.Labels{{Name: labels.MetricName, Value: name}}), T: ts, V: v})
	}
	if aerr := m.app(ctx, t.cfg.Tenant, samples); aerr != nil && err == nil {
		err = fmt.Errorf("append: %w", aerr)
	}
	t.mu.Lock()
	t.status.LastScrape = start
	t.status.LastDurationMs = float64(dur.Microseconds()) / 1000
	t.status.SamplesScraped = len(samples) - 3
	if err != nil {
		t.status.Health, t.status.LastError = "down", err.Error()
	} else {
		t.status.Health, t.status.LastError = "up", ""
	}
	t.mu.Unlock()
	if m.metrics != nil {
		m.metrics.ScrapeDuration.WithLabelValues(t.cfg.Job).Observe(dur.Seconds())
		m.metrics.ScrapeSamples.WithLabelValues(t.cfg.Job).Add(float64(len(samples)))
	}
	if err != nil {
		m.log.Warn("scrape failed", "job", t.cfg.Job, "url", t.cfg.URL, "err", err)
	}
}

func (m *Manager) fetch(ctx context.Context, t *target, now time.Time) ([]tsdb.LabeledSample, error) {
	ctx, cancel := context.WithTimeout(ctx, m.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, t.cfg.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/plain;version=0.0.4")
	req.Header.Set("User-Agent", "developer-observability-stack/scraper")
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %s", resp.Status)
	}
	families, err := promtext.Parse(&limitCheck{r: resp.Body, max: m.maxBody})
	if err != nil {
		return nil, err
	}
	return Convert(families, now, func(ls labels.Labels) labels.Labels { return m.targetLabels(t, ls) }), nil
}

type limitCheck struct {
	r   io.Reader
	n   int64
	max int64
}

func (l *limitCheck) Read(p []byte) (int, error) {
	n, err := l.r.Read(p)
	l.n += int64(n)
	if l.n > l.max {
		return n, fmt.Errorf("response exceeds %d bytes", l.max)
	}
	return n, err
}

// targetLabels attaches job/instance and configured labels. Exposed labels
// that collide with target labels are kept as exported_<name>
// (Prometheus honor_labels: false semantics).
func (m *Manager) targetLabels(t *target, ls labels.Labels) labels.Labels {
	tl := map[string]string{"job": t.cfg.Job, "instance": t.instance}
	for k, v := range t.cfg.Labels {
		tl[k] = v
	}
	out := ls.Map()
	for k, v := range tl {
		if old, ok := out[k]; ok && old != v {
			out["exported_"+k] = old
		}
		out[k] = v
	}
	return labels.FromMap(out)
}

// Convert turns parsed families into samples, applying relabel to each
// label set. Samples without explicit timestamps use now.
func Convert(families []*promtext.Family, now time.Time, relabel func(labels.Labels) labels.Labels) []tsdb.LabeledSample {
	var out []tsdb.LabeledSample
	for _, f := range families {
		for _, s := range f.Samples {
			ts := now.UnixMilli()
			if s.TimestampMs != nil {
				ts = *s.TimestampMs
			}
			ls := s.Labels
			if relabel != nil {
				ls = relabel(ls)
			}
			out = append(out, tsdb.LabeledSample{Labels: ls, T: ts, V: s.Value})
		}
	}
	return out
}

// Targets returns a snapshot of target states.
func (m *Manager) Targets(tenant string) []TargetStatus {
	out := []TargetStatus{}
	for _, t := range m.targets {
		if tenant != "" && t.cfg.Tenant != tenant {
			continue
		}
		t.mu.Lock()
		out = append(out, t.status)
		t.mu.Unlock()
	}
	sort.Slice(out, func(i, j int) bool { return out[i].URL < out[j].URL })
	return out
}
