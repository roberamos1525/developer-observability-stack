// Package loadgen generates synthetic logs, metrics and OTLP spans for a
// small fictional microservice system and sends them to the server.
package loadgen

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/otlp"
)

// Config controls a load run.
type Config struct {
	URL         string
	APIKey      string
	Duration    time.Duration
	Rate        float64 // requests per second per signal
	BatchSize   int
	Concurrency int
	Signals     []string // logs, metrics, traces
	Seed        int64
	Sync        bool // ask the server to persist before responding
}

// Stats summarises a run.
type Stats struct {
	Requests   int64          `json:"requests"`
	Errors     int64          `json:"errors"`
	Throttled  int64          `json:"throttled"`
	Logs       int64          `json:"logs"`
	Samples    int64          `json:"samples"`
	Spans      int64          `json:"spans"`
	ElapsedSec float64        `json:"elapsed_sec"`
	LatencyMs  map[string]any `json:"latency_ms"`
}

var services = []string{"frontend", "checkout", "payments", "inventory"}
var routes = []string{"/", "/cart", "/checkout", "/api/items", "/api/orders"}
var messages = []string{
	"request completed", "cache miss for key %s", "connection refused by upstream %s",
	"payment authorised for order %s", "slow query detected on table %s", "retrying request to %s",
	"user session expired", "inventory reserved for sku %s",
}

// Builder produces payloads. Counter state is kept so cumulative metrics
// stay monotonic across batches. It is safe for concurrent use.
type Builder struct {
	mu       sync.Mutex
	rng      *rand.Rand
	counters map[string]float64
}

// NewBuilder creates a deterministic builder.
func NewBuilder(seed int64) *Builder {
	return &Builder{rng: rand.New(rand.NewSource(seed)), counters: map[string]float64{}}
}

// Logs returns an NDJSON batch of n entries.
func (b *Builder) Logs(n int, now time.Time) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := 0; i < n; i++ {
		svc := services[b.rng.Intn(len(services))]
		level := "info"
		switch r := b.rng.Float64(); {
		case r < 0.05:
			level = "error"
		case r < 0.15:
			level = "warn"
		case r < 0.30:
			level = "debug"
		}
		msg := messages[b.rng.Intn(len(messages))]
		if strings.Contains(msg, "%s") {
			msg = fmt.Sprintf(msg, fmt.Sprintf("%s-%d", svc, b.rng.Intn(100)))
		}
		status := 200
		if level == "error" {
			status = []int{500, 502, 503}[b.rng.Intn(3)]
		}
		enc.Encode(map[string]any{
			"timestamp":   now.Add(-time.Duration(b.rng.Intn(1000)) * time.Millisecond).Format(time.RFC3339Nano),
			"level":       level,
			"message":     msg,
			"service":     svc,
			"route":       routes[b.rng.Intn(len(routes))],
			"status":      status,
			"latency_ms":  math.Round(b.rng.ExpFloat64()*80*100) / 100,
			"host":        fmt.Sprintf("%s-%d", svc, b.rng.Intn(3)),
			"http.method": []string{"GET", "POST"}[b.rng.Intn(2)],
		})
	}
	return buf.Bytes()
}

var latencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5}

// Metrics returns a JSON samples batch: request counters per
// service/route/code, a latency histogram per service and queue gauges.
func (b *Builder) Metrics(now time.Time) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	type sample struct {
		Labels    map[string]string `json:"labels"`
		Value     float64           `json:"value"`
		Timestamp int64             `json:"timestamp"`
	}
	ts := now.UnixMilli()
	var out []sample
	inc := func(key string, d float64) float64 {
		b.counters[key] += d
		return b.counters[key]
	}
	for _, svc := range services {
		for _, code := range []string{"200", "500"} {
			rate := 20 + b.rng.Float64()*10
			if code == "500" {
				rate = b.rng.Float64() * 2
			}
			key := "req|" + svc + "|" + code
			out = append(out, sample{map[string]string{"__name__": "http_requests_total", "service": svc, "code": code}, inc(key, math.Round(rate)), ts})
		}
		// Per-batch increments are non-decreasing in le, so the cumulative
		// buckets stay monotonic both across le and over time.
		count := 0.0
		for _, le := range latencyBuckets {
			count += float64(b.rng.Intn(5))
			bound := fmt.Sprintf("%g", le)
			out = append(out, sample{map[string]string{"__name__": "http_request_duration_seconds_bucket", "service": svc, "le": bound},
				inc("lat|"+svc+"|"+bound, count), ts})
		}
		count += float64(b.rng.Intn(2))
		out = append(out, sample{map[string]string{"__name__": "http_request_duration_seconds_bucket", "service": svc, "le": "+Inf"},
			inc("lat|"+svc+"|+Inf", count), ts})
		out = append(out, sample{map[string]string{"__name__": "queue_depth", "service": svc}, float64(b.rng.Intn(50)), ts})
	}
	body, _ := json.Marshal(map[string]any{"samples": out})
	return body
}

func (b *Builder) id(n int) string {
	buf := make([]byte, n)
	b.rng.Read(buf)
	buf[0] |= 1 // never all-zero
	return hex.EncodeToString(buf)
}

func strAttr(k, v string) otlp.KeyValue {
	return otlp.KeyValue{Key: k, Value: otlp.AnyValue{StringValue: &v}}
}

// Traces returns an OTLP/JSON export request with n traces of the shape
// frontend -> checkout -> (payments, inventory).
func (b *Builder) Traces(n int, now time.Time) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	bySvc := map[string][]otlp.Span{}
	for i := 0; i < n; i++ {
		traceID := b.id(16)
		start := now.Add(-time.Duration(b.rng.Intn(2000)) * time.Millisecond)
		failing := b.rng.Float64() < 0.1
		mk := func(svc, name string, kind int, parent string, offset, dur time.Duration, isErr bool) string {
			sid := b.id(8)
			st := otlp.Status{}
			if isErr {
				st = otlp.Status{Code: 2, Message: "upstream failure"}
			}
			bySvc[svc] = append(bySvc[svc], otlp.Span{
				TraceID: traceID, SpanID: sid, ParentSpanID: parent, Name: name, Kind: kind,
				StartTimeUnixNano: otlp.Uint64(start.Add(offset).UnixNano()),
				EndTimeUnixNano:   otlp.Uint64(start.Add(offset + dur).UnixNano()),
				Attributes:        []otlp.KeyValue{strAttr("http.route", name), strAttr("deployment.environment", "loadtest")},
				Status:            st,
			})
			return sid
		}
		total := time.Duration(20+b.rng.Intn(300)) * time.Millisecond
		root := mk("frontend", "GET /checkout", 2, "", 0, total, failing)
		co := mk("checkout", "POST /orders", 2, root, 2*time.Millisecond, total-4*time.Millisecond, failing)
		mk("payments", "Charge", 2, co, 5*time.Millisecond, total/3, failing)
		mk("inventory", "Reserve", 2, co, total/2, total/4, false)
	}
	req := otlp.ExportTraceServiceRequest{}
	names := make([]string, 0, len(bySvc))
	for svc := range bySvc {
		names = append(names, svc)
	}
	sort.Strings(names)
	for _, svc := range names {
		req.ResourceSpans = append(req.ResourceSpans, otlp.ResourceSpans{
			Resource:   otlp.Resource{Attributes: []otlp.KeyValue{strAttr("service.name", svc)}},
			ScopeSpans: []otlp.ScopeSpans{{Scope: otlp.InstrumentationScope{Name: "loadgen"}, Spans: bySvc[svc]}},
		})
	}
	body, _ := json.Marshal(req)
	return body
}

// Exposition renders a Prometheus text page for a synthetic process, used
// to exercise scraping.
func (b *Builder) Exposition() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	jobs := b.counters["exp|jobs"] + float64(1+b.rng.Intn(10))
	b.counters["exp|jobs"] = jobs
	var sb strings.Builder
	sb.WriteString("# HELP loadgen_jobs_processed_total Jobs processed by the synthetic worker.\n")
	sb.WriteString("# TYPE loadgen_jobs_processed_total counter\n")
	fmt.Fprintf(&sb, "loadgen_jobs_processed_total{queue=\"default\"} %g\n", jobs)
	sb.WriteString("# TYPE loadgen_temperature_celsius gauge\n")
	fmt.Fprintf(&sb, "loadgen_temperature_celsius %.2f\n", 20+b.rng.Float64()*5)
	return []byte(sb.String())
}

type latencies struct {
	mu sync.Mutex
	v  map[string][]float64
}

func (l *latencies) add(signal string, d time.Duration) {
	l.mu.Lock()
	l.v[signal] = append(l.v[signal], float64(d.Microseconds())/1000)
	l.mu.Unlock()
}

func (l *latencies) summary() map[string]any {
	out := map[string]any{}
	for sig, vals := range l.v {
		sort.Float64s(vals)
		q := func(p float64) float64 { return vals[min(len(vals)-1, int(p*float64(len(vals))))] }
		out[sig] = map[string]float64{"p50": q(0.5), "p95": q(0.95), "p99": q(0.99), "max": vals[len(vals)-1]}
	}
	return out
}

// Run sends load until the duration elapses or ctx is cancelled.
func Run(ctx context.Context, cfg Config, client *http.Client) (Stats, error) {
	if cfg.Rate <= 0 || cfg.BatchSize <= 0 || cfg.Concurrency <= 0 || cfg.Duration <= 0 {
		return Stats{}, fmt.Errorf("rate, batch size, concurrency and duration must be positive")
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()
	b := NewBuilder(cfg.Seed)
	var st struct{ req, errs, throttled, logs, samples, spans atomic.Int64 }
	lat := &latencies{v: map[string][]float64{}}
	type job struct {
		signal, path, ctype string
		body                []byte
		items               int
	}
	jobs := make(chan job, cfg.Concurrency*2)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				began := time.Now()
				status, err := send(ctx, client, cfg, j.path, j.ctype, j.body)
				lat.add(j.signal, time.Since(began))
				st.req.Add(1)
				switch {
				case err != nil && ctx.Err() != nil:
					st.req.Add(-1) // cancelled at the end of the run
				case err != nil || status >= 400 && status != http.StatusTooManyRequests:
					st.errs.Add(1)
				case status == http.StatusTooManyRequests:
					st.throttled.Add(1)
				default:
					switch j.signal {
					case "logs":
						st.logs.Add(int64(j.items))
					case "metrics":
						st.samples.Add(int64(j.items))
					case "traces":
						st.spans.Add(int64(j.items))
					}
				}
			}
		}()
	}
	start := time.Now()
	tick := time.NewTicker(time.Duration(float64(time.Second) / cfg.Rate))
	defer tick.Stop()
	syncQ := ""
	if cfg.Sync {
		syncQ = "?sync=true"
	}
loop:
	for {
		now := time.Now()
		for _, sig := range cfg.Signals {
			var j job
			switch sig {
			case "logs":
				j = job{sig, "/api/v1/logs" + syncQ, "application/x-ndjson", b.Logs(cfg.BatchSize, now), cfg.BatchSize}
			case "metrics":
				j = job{sig, "/api/v1/metrics" + syncQ, "application/json", b.Metrics(now), len(services) * (2 + len(latencyBuckets) + 2)}
			case "traces":
				n := max(1, cfg.BatchSize/20)
				j = job{sig, "/v1/traces" + syncQ, "application/json", b.Traces(n, now), n * 4}
			default:
				continue
			}
			select {
			case jobs <- j:
			case <-ctx.Done():
				break loop
			}
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			break loop
		}
	}
	close(jobs)
	wg.Wait()
	return Stats{Requests: st.req.Load(), Errors: st.errs.Load(), Throttled: st.throttled.Load(), Logs: st.logs.Load(),
		Samples: st.samples.Load(), Spans: st.spans.Load(), ElapsedSec: time.Since(start).Seconds(), LatencyMs: lat.summary()}, nil
}

func send(ctx context.Context, client *http.Client, cfg Config, path, ctype string, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(cfg.URL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", ctype)
	if cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	return resp.StatusCode, nil
}
