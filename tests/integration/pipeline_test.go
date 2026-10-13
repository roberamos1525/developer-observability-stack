package integration

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/loadgen"
)

func TestScrapeTargetsThroughApp(t *testing.T) {
	b := loadgen.NewBuilder(5)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(b.Exposition())
	}))
	defer target.Close()
	e := newEnv(t, func(c *config.Config) {
		c.Metrics.Scrape.Targets = []config.ScrapeTarget{{Tenant: "acme", Job: "worker", URL: target.URL + "/metrics", Labels: map[string]string{"env": "test"}}}
	})
	for i := 0; i < 3; i++ {
		e.app.Scraper.ScrapeAll(context.Background())
		time.Sleep(5 * time.Millisecond)
	}
	e.flush()
	r := e.get("/api/v1/query?query="+url.QueryEscape(`loadgen_jobs_processed_total{job="worker",env="test"}`), acmeReader)
	if res := asList(t, asMap(t, r.data(t))["result"]); len(res) != 1 {
		t.Fatalf("scraped series missing: %s", r.Body)
	}
	r = e.get("/api/v1/query?query=up", acmeReader)
	if res := asList(t, asMap(t, r.data(t))["result"]); len(res) != 1 || asMap(t, res[0])["value"].([]any)[1] != "1" {
		t.Fatalf("up series: %s", r.Body)
	}
	targets := asList(t, e.get("/api/v1/scrape/targets", acmeReader).data(t))
	if len(targets) != 1 || asMap(t, targets[0])["health"] != "up" {
		t.Fatalf("targets %v", targets)
	}
	if len(asList(t, e.get("/api/v1/scrape/targets", betaAll).data(t))) != 0 {
		t.Fatal("targets are tenant-scoped")
	}
}

func TestConcurrentClientsAndLoadgen(t *testing.T) {
	e := newEnv(t, func(c *config.Config) { c.Ingest.Workers = 3 })
	st, err := loadgen.Run(context.Background(), loadgen.Config{URL: e.srv.URL, APIKey: acmeWriter, Duration: 400 * time.Millisecond,
		Rate: 40, BatchSize: 40, Concurrency: 4, Signals: []string{"logs", "metrics", "traces"}, Seed: 9}, e.srv.Client())
	if err != nil || st.Errors != 0 || st.Logs == 0 || st.Spans == 0 || st.Samples == 0 {
		t.Fatalf("loadgen: %+v %v", st, err)
	}
	var wg sync.WaitGroup
	for c := 0; c < 6; c++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			var body strings.Builder
			for i := 0; i < 50; i++ {
				fmt.Fprintf(&body, `{"message":"client %d line %d","level":"info","client":"c%d"}`+"\n", c, i, c)
			}
			r := e.do(http.MethodPost, "/api/v1/logs", acmeWriter, "application/x-ndjson", []byte(body.String()))
			if r.Status != http.StatusAccepted {
				t.Errorf("client %d: %d", c, r.Status)
			}
		}(c)
	}
	wg.Wait()
	e.flush()
	r := e.get("/api/v1/logs/query?q="+url.QueryEscape("client:* | count by client"), acmeReader)
	groups := asList(t, asMap(t, r.data(t))["groups"])
	if len(groups) != 6 {
		t.Fatalf("groups %v", groups)
	}
	for _, g := range groups {
		if asMap(t, g)["count"].(float64) != 50 {
			t.Fatalf("lost writes: %v", g)
		}
	}
	total := asList(t, asMap(t, e.get("/api/v1/logs/query?q="+url.QueryEscape("| count"), acmeReader).data(t))["groups"])
	if int64(asMap(t, total[0])["count"].(float64)) != st.Logs+300 {
		t.Fatalf("total logs %v, want %d", total, st.Logs+300)
	}
	stats := asMap(t, e.get("/api/v1/admin/stats", acmeAdmin).data(t))
	if q := asMap(t, asMap(t, stats["queues"])["logs"]); q["pending"].(float64) != 0 {
		t.Fatalf("queue not drained: %v", q)
	}
}

func TestOTLPGzipAndDisabledAuth(t *testing.T) {
	e := newEnv(t, func(c *config.Config) { c.Auth.Enabled = false })
	r := e.do(http.MethodPost, "/v1/logs", "", "application/json", gzipped(fixture(t, "logs.json")), "Content-Encoding", "gzip", "X-Tenant-ID", "team-x")
	expectStatus(t, r, http.StatusOK)
	e.flush()
	r = e.do(http.MethodGet, "/api/v1/logs/query?q=declined&start=2024-01-01T00:00:00Z", "", "", nil, "X-Tenant-ID", "team-x")
	if n := len(asList(t, asMap(t, r.data(t))["entries"])); n != 1 {
		t.Fatalf("tenant header routing: %d", n)
	}
	r = e.do(http.MethodGet, "/api/v1/logs/query?q=declined&start=2024-01-01T00:00:00Z", "", "", nil)
	if n := len(asList(t, asMap(t, r.data(t))["entries"])); n != 0 {
		t.Fatal("default tenant must not see team-x data")
	}
	expectStatus(t, e.do(http.MethodGet, "/api/v1/whoami", "", "", nil, "X-Tenant-ID", "Bad Tenant"), http.StatusBadRequest)
}
