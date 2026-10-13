package loadgen

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/logs"
	"github.com/robertramos/developer-observability-stack/internal/otlp"
	"github.com/robertramos/developer-observability-stack/internal/promtext"
)

var now = time.Unix(1_800_000_000, 0)

func TestPayloadsAreValid(t *testing.T) {
	b := NewBuilder(1)
	res, rejected := logs.ParseJSON(b.Logs(50, now), now)
	if rejected != 0 || len(res.Entries) != 50 {
		t.Fatalf("logs rejected=%d entries=%d %v", rejected, len(res.Entries), res.Errors)
	}
	var req otlp.ExportTraceServiceRequest
	if err := json.Unmarshal(b.Traces(5, now), &req); err != nil {
		t.Fatal(err)
	}
	spans, rej := otlp.ToSpans(&req)
	if rej.Count != 0 || len(spans) != 20 {
		t.Fatalf("spans=%d rejected=%+v", len(spans), rej)
	}
	fams, err := promtext.Parse(bytes.NewReader(b.Exposition()))
	if err != nil || len(fams) != 2 {
		t.Fatalf("exposition: %v %d", err, len(fams))
	}
}

func TestMetricsCountersAreMonotonic(t *testing.T) {
	b := NewBuilder(2)
	type sample struct {
		Labels map[string]string `json:"labels"`
		Value  float64           `json:"value"`
	}
	prev := map[string]float64{}
	for round := 0; round < 20; round++ {
		var body struct{ Samples []sample }
		if err := json.Unmarshal(b.Metrics(now.Add(time.Duration(round)*time.Second)), &body); err != nil {
			t.Fatal(err)
		}
		buckets := map[string]map[string]float64{}
		for _, s := range body.Samples {
			name := s.Labels["__name__"]
			if name == "queue_depth" {
				continue
			}
			key := name + s.Labels["service"] + s.Labels["code"] + s.Labels["le"]
			if s.Value < prev[key] {
				t.Fatalf("counter %s decreased", key)
			}
			prev[key] = s.Value
			if le := s.Labels["le"]; le != "" {
				if buckets[s.Labels["service"]] == nil {
					buckets[s.Labels["service"]] = map[string]float64{}
				}
				buckets[s.Labels["service"]][le] = s.Value
			}
		}
		for svc, bk := range buckets {
			if bk["0.005"] > bk["2.5"] || bk["2.5"] > bk["+Inf"] {
				t.Fatalf("buckets of %s not cumulative: %v", svc, bk)
			}
		}
	}
}

func TestRunAgainstFakeServer(t *testing.T) {
	var mu sync.Mutex
	paths := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if r.Header.Get("Authorization") != "Bearer k" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		mu.Lock()
		paths[strings.Split(r.URL.Path, "?")[0]]++
		n := paths[r.URL.Path]
		mu.Unlock()
		if r.URL.Path == "/v1/traces" && n == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	st, err := Run(context.Background(), Config{URL: srv.URL, APIKey: "k", Duration: 300 * time.Millisecond, Rate: 20,
		BatchSize: 20, Concurrency: 2, Signals: []string{"logs", "metrics", "traces"}, Seed: 3}, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if st.Requests < 6 || st.Errors != 0 || st.Throttled != 1 || st.Logs == 0 || st.Samples == 0 || st.Spans == 0 {
		t.Fatalf("stats %+v", st)
	}
	if _, ok := st.LatencyMs["logs"]; !ok {
		t.Fatal("latency summary missing")
	}
	if _, err := Run(context.Background(), Config{}, srv.Client()); err == nil {
		t.Fatal("invalid config must fail")
	}
}
