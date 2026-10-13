package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/alerting"
	"github.com/robertramos/developer-observability-stack/internal/config"
)

type receiver struct {
	mu   sync.Mutex
	got  []alerting.Notification
	sigs []string
	srv  *httptest.Server
}

func newReceiver(t *testing.T) *receiver {
	rc := &receiver{}
	rc.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var n alerting.Notification
		if err := json.Unmarshal(body, &n); err != nil {
			t.Errorf("bad webhook payload: %v", err)
		}
		rc.mu.Lock()
		rc.got = append(rc.got, n)
		rc.sigs = append(rc.sigs, r.Header.Get(alerting.SignatureHeader))
		if r.Header.Get(alerting.SignatureHeader) != alerting.Sign([]byte("hook-secret"), body) {
			t.Errorf("webhook signature mismatch")
		}
		rc.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(rc.srv.Close)
	return rc
}

func (rc *receiver) notifications() []alerting.Notification {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return append([]alerting.Notification(nil), rc.got...)
}

func TestAlertsFireToWebhookWithSilences(t *testing.T) {
	rc := newReceiver(t)
	t.Setenv("TEST_HOOK_SECRET", "hook-secret")
	e := newEnv(t, func(c *config.Config) {
		c.Alerting.Channels = append(c.Alerting.Channels, config.ChannelConfig{Name: "hook", Type: "webhook", URL: rc.srv.URL, SecretEnv: "TEST_HOOK_SECRET"})
	})
	now := time.Now()
	sample := func(v float64, at time.Time) {
		r := e.postJSON("/api/v1/metrics?sync=true", acmeWriter, map[string]any{"samples": []map[string]any{
			{"name": "errors_total", "labels": map[string]string{"job": "api"}, "value": v, "timestamp": at.UnixMilli()},
			{"name": "errors_total", "labels": map[string]string{"job": "web"}, "value": 1, "timestamp": at.UnixMilli()},
		}})
		expectStatus(t, r, http.StatusOK)
	}
	sample(10, now.Add(-10*time.Second))
	rule := map[string]any{"name": "TooManyErrors", "type": "metric", "expr": `errors_total > 5`, "for": "0s",
		"labels": map[string]string{"severity": "page"}, "annotations": map[string]string{"summary": "{{ $labels.job }} has {{ $value }} errors"},
		"channels": []string{"hook"}}
	expectStatus(t, e.postJSON("/api/v1/alerts/rules", acmeReader, rule), http.StatusForbidden)
	expectStatus(t, e.postJSON("/api/v1/alerts/rules", acmeWriter, rule), http.StatusCreated)
	bad := map[string]any{"name": "Bad", "type": "metric", "expr": "rate(", "channels": []string{"hook"}}
	expectStatus(t, e.postJSON("/api/v1/alerts/rules", acmeWriter, bad), http.StatusBadRequest)
	unknown := map[string]any{"name": "X", "type": "metric", "expr": "up", "bogus": true}
	expectStatus(t, e.postJSON("/api/v1/alerts/rules", acmeWriter, unknown), http.StatusBadRequest)

	logs := `{"level":"error","message":"db timeout","service":"api"}` + "\n" + `{"level":"error","message":"db timeout","service":"api"}`
	expectStatus(t, e.do(http.MethodPost, "/api/v1/logs?sync=true", acmeWriter, "application/x-ndjson", []byte(logs)), http.StatusOK)
	logRule := map[string]any{"name": "ErrorLogBurst", "type": "log_count", "query": "level:error | count by service", "window": "5m",
		"op": ">=", "threshold": 2, "channels": []string{"hook"}}
	expectStatus(t, e.postJSON("/api/v1/alerts/rules", acmeWriter, logRule), http.StatusCreated)

	e.app.Alerts.Evaluate(context.Background(), time.Now())
	got := rc.notifications()
	if len(got) != 2 {
		t.Fatalf("expected 2 notifications (one per rule group), got %d", len(got))
	}
	byRule := map[string]alerting.Notification{}
	for _, n := range got {
		byRule[n.GroupLabels["alertname"]] = n
	}
	metricN := byRule["TooManyErrors"]
	if metricN.Status != "firing" || metricN.Tenant != "acme" || len(metricN.Alerts) != 1 ||
		metricN.Alerts[0].Labels["job"] != "api" || metricN.Alerts[0].Annotations["summary"] != "api has 10 errors" {
		t.Fatalf("metric notification %+v", metricN)
	}
	if n := byRule["ErrorLogBurst"]; n.Alerts[0].Labels["service"] != "api" || n.Alerts[0].Value != 2 {
		t.Fatalf("log notification %+v", n)
	}
	alerts := asList(t, e.get("/api/v1/alerts", acmeReader).data(t))
	if len(alerts) != 2 || asMap(t, alerts[0])["state"] != "firing" {
		t.Fatalf("alerts %v", alerts)
	}
	rules := asMap(t, e.get("/api/v1/alerts/rules", acmeReader).data(t))
	if len(asList(t, rules["rules"])) != 2 || len(asList(t, rules["channels"])) != 2 {
		t.Fatalf("rules %v", rules)
	}
	// Silence the metric alert; resolution of a silenced alert is not sent.
	r := e.postJSON("/api/v1/silences", acmeWriter, map[string]any{"matchers": []map[string]any{{"name": "alertname", "value": "TooManyErrors"}},
		"endsAt": time.Now().Add(time.Hour).Format(time.RFC3339), "createdBy": "oncall", "comment": "deploying fix"})
	expectStatus(t, r, http.StatusCreated)
	silenceID := asMap(t, r.data(t))["id"].(string)
	if a := asList(t, e.get("/api/v1/alerts", acmeReader).data(t)); asMap(t, a[1])["silenced"] != true {
		t.Fatalf("silenced flag %v", a)
	}
	sample(0, time.Now())
	e.app.Alerts.Evaluate(context.Background(), time.Now().Add(2*time.Second))
	for _, n := range rc.notifications()[2:] {
		if n.GroupLabels["alertname"] == "TooManyErrors" {
			t.Fatalf("silenced alert notified: %+v", n)
		}
	}
	expectStatus(t, e.do(http.MethodDelete, "/api/v1/silences/"+silenceID, acmeWriter, "", nil), http.StatusNoContent)
	sil := asList(t, e.get("/api/v1/silences", acmeReader).data(t))
	if asMap(t, sil[0])["status"] != "expired" {
		t.Fatalf("silence status %v", sil)
	}
	expectStatus(t, e.do(http.MethodDelete, "/api/v1/alerts/rules/TooManyErrors", acmeWriter, "", nil), http.StatusNoContent)
	expectStatus(t, e.do(http.MethodDelete, "/api/v1/alerts/rules/TooManyErrors", acmeWriter, "", nil), http.StatusNotFound)
	// Other tenants see none of this.
	if a := asList(t, e.get("/api/v1/alerts", betaAll).data(t)); len(a) != 0 {
		t.Fatal("alerts leaked across tenants")
	}
}

func TestAuthScopesAndTenantIsolation(t *testing.T) {
	e := newEnv(t, nil)
	body := []byte(`{"message":"acme secret","level":"info"}`)
	r := e.do(http.MethodPost, "/api/v1/logs?sync=true", "", "application/json", body)
	expectStatus(t, r, http.StatusUnauthorized)
	if r.errorCode(t) != "unauthorized" || r.Header.Get("WWW-Authenticate") == "" {
		t.Fatal("401 envelope")
	}
	expectStatus(t, e.do(http.MethodPost, "/api/v1/logs?sync=true", "not-a-real-key", "application/json", body), http.StatusUnauthorized)
	expectStatus(t, e.do(http.MethodPost, "/api/v1/logs?sync=true", acmeReader, "application/json", body), http.StatusForbidden)
	expectStatus(t, e.do(http.MethodPost, "/api/v1/logs?sync=true", acmeWriter, "application/json", body), http.StatusOK)
	expectStatus(t, e.get("/api/v1/logs/query?q=secret", acmeWriter), http.StatusForbidden)
	if n := len(asList(t, asMap(t, e.get("/api/v1/logs/query?q=secret", acmeReader).data(t))["entries"])); n != 1 {
		t.Fatalf("acme sees %d", n)
	}
	if n := len(asList(t, asMap(t, e.get("/api/v1/logs/query?q=secret", betaAll).data(t))["entries"])); n != 0 {
		t.Fatal("tenant isolation violated")
	}
	who := asMap(t, e.get("/api/v1/whoami", betaAll).data(t))
	if who["tenant"] != "beta" || len(asList(t, who["scopes"])) != 2 {
		t.Fatalf("whoami %v", who)
	}
	expectStatus(t, e.get("/api/v1/admin/stats", acmeReader), http.StatusForbidden)
	stats := asMap(t, e.get("/api/v1/admin/stats", acmeAdmin).data(t))
	if len(asList(t, stats["tenants"])) < 1 || asMap(t, stats["queues"])["logs"] == nil {
		t.Fatalf("admin stats %v", stats)
	}
	// The admin key implies read/write for its own tenant.
	expectStatus(t, e.get("/api/v1/logs/query?q=secret", acmeAdmin), http.StatusOK)
}

func TestRequestLimitsAndErrorEnvelopes(t *testing.T) {
	e := newEnv(t, nil)
	big := make([]byte, 2<<20)
	r := e.do(http.MethodPost, "/api/v1/logs", acmeWriter, "application/json", big)
	expectStatus(t, r, http.StatusRequestEntityTooLarge)
	if r.errorCode(t) != "payload_too_large" {
		t.Fatal("413 code")
	}
	// A compressed body that inflates past the decompressed limit.
	bomb := gzipped([]byte(strings.Repeat(`{"message":"aaaaaaaaaaaaaaaa"}`+"\n", 300_000)))
	if len(bomb) > 1<<20 {
		t.Fatalf("bomb too large to test (%d)", len(bomb))
	}
	expectStatus(t, e.do(http.MethodPost, "/api/v1/logs", acmeWriter, "application/json", bomb, "Content-Encoding", "gzip"), http.StatusRequestEntityTooLarge)
	expectStatus(t, e.do(http.MethodPost, "/api/v1/logs", acmeWriter, "application/json", []byte("x"), "Content-Encoding", "br"), http.StatusUnsupportedMediaType)
	expectStatus(t, e.do(http.MethodPost, "/api/v1/logs", acmeWriter, "application/json", []byte("x"), "Content-Encoding", "gzip"), http.StatusBadRequest)
	expectStatus(t, e.do(http.MethodPost, "/api/v1/logs", acmeWriter, "text/csv", []byte("a,b")), http.StatusUnsupportedMediaType)
	expectStatus(t, e.do(http.MethodPost, "/api/v1/logs", acmeWriter, "application/json", nil), http.StatusBadRequest)
	expectStatus(t, e.postJSON("/api/v1/metrics", acmeWriter, map[string]any{"samples": "nope"}), http.StatusBadRequest)
	expectStatus(t, e.do(http.MethodPost, "/api/v1/metrics/prometheus", acmeWriter, "text/plain", []byte("bad metric line")), http.StatusBadRequest)
	r = e.get("/api/v1/nope", acmeReader)
	expectStatus(t, r, http.StatusNotFound)
	r = e.do(http.MethodDelete, "/api/v1/logs", acmeWriter, "", nil)
	expectStatus(t, r, http.StatusMethodNotAllowed)
	if id := e.get("/healthz", "").Header.Get("X-Request-ID"); len(id) != 16 {
		t.Fatalf("request id %q", id)
	}
}

func TestHealthAndSelfMetrics(t *testing.T) {
	e := newEnv(t, nil)
	expectStatus(t, e.get("/healthz", ""), http.StatusOK)
	expectStatus(t, e.get("/readyz", ""), http.StatusOK)
	e.do(http.MethodPost, "/api/v1/logs?sync=true", acmeWriter, "application/json", []byte(`{"message":"hi"}`))
	r := e.get("/metrics", "")
	expectStatus(t, r, http.StatusOK)
	body := string(r.Body)
	for _, want := range []string{
		`obs_http_requests_total{code="200",method="POST",route="POST /api/v1/logs"}`,
		`obs_ingest_accepted_total{signal="logs",tenant="acme"} 1`,
		`obs_queue_items_total{outcome="persisted",queue="logs"} 1`,
		`obs_queue_depth{queue="metrics"}`, "go_goroutines", "obs_tenants_open",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %s", want)
		}
	}
	e.app.Shutdown(context.Background())
	expectStatus(t, e.get("/readyz", ""), http.StatusServiceUnavailable)
}

func TestRetentionCompactionAndRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := testConfig(t, dir)
	cfg.Logs.Retention = config.Duration(7 * 24 * time.Hour)
	cfg.Traces.Retention = config.Duration(24 * time.Hour)
	cfg.Metrics.RawRetention = config.Duration(24 * time.Hour)
	cfg.Metrics.BlockDuration = config.Duration(time.Hour)
	cfg.Metrics.DownsampleAfter = config.Duration(time.Hour)
	e := start(t, cfg)
	now := time.Now()
	var nd strings.Builder
	for _, age := range []time.Duration{10 * 24 * time.Hour, 9 * 24 * time.Hour, time.Hour, time.Minute} {
		fmt.Fprintf(&nd, `{"timestamp":%d,"message":"aged %s","level":"info"}`+"\n", now.Add(-age).UnixMilli(), age)
	}
	expectStatus(t, e.do(http.MethodPost, "/api/v1/logs?sync=true", acmeWriter, "application/x-ndjson", []byte(nd.String())), http.StatusOK)
	var samples []map[string]any
	for ts := now.Add(-30 * time.Hour); ts.Before(now); ts = ts.Add(5 * time.Minute) {
		samples = append(samples, map[string]any{"name": "temperature", "labels": map[string]string{"room": "lab"}, "value": 21.5, "timestamp": ts.UnixMilli()})
	}
	expectStatus(t, e.postJSON("/api/v1/metrics?sync=true", acmeWriter, map[string]any{"samples": samples}), http.StatusOK)
	span := func(id string, age time.Duration) map[string]any {
		st := now.Add(-age)
		return map[string]any{"traceId": strings.Repeat(id, 32), "spanId": strings.Repeat(id, 16), "name": "op", "kind": 2,
			"startTimeUnixNano": fmt.Sprint(st.UnixNano()), "endTimeUnixNano": fmt.Sprint(st.Add(time.Millisecond).UnixNano())}
	}
	traces := map[string]any{"resourceSpans": []any{map[string]any{
		"resource":   map[string]any{"attributes": []any{map[string]any{"key": "service.name", "value": map[string]any{"stringValue": "svc"}}}},
		"scopeSpans": []any{map[string]any{"spans": []any{span("a", 48*time.Hour), span("b", time.Minute)}}},
	}}}
	expectStatus(t, e.postJSON("/v1/traces?sync=true", acmeWriter, traces), http.StatusOK)
	rule := map[string]any{"name": "Hot", "type": "metric", "expr": `temperature > 30`}
	expectStatus(t, e.postJSON("/api/v1/alerts/rules", acmeWriter, rule), http.StatusCreated)

	expectStatus(t, e.do(http.MethodPost, "/api/v1/admin/maintenance", acmeWriter, "", nil), http.StatusForbidden)
	r := e.do(http.MethodPost, "/api/v1/admin/maintenance", acmeAdmin, "", nil)
	expectStatus(t, r, http.StatusOK)
	reports := asList(t, r.data(t))
	rep := asMap(t, reports[0])
	if rep["log_partitions_removed"].(float64) != 2 || rep["traces_removed"].(float64) != 1 || rep["metric_blocks_cut"].(float64) < 20 {
		t.Fatalf("maintenance report %v", rep)
	}
	if rep["metric_blocks_removed"].(float64) < 5 || rep["metric_blocks_downsampled"].(float64) < 1 {
		t.Fatalf("metrics retention/downsampling %v", rep)
	}
	entries := asList(t, asMap(t, e.get("/api/v1/logs/query?q=aged&limit=10", acmeReader).data(t))["entries"])
	if len(entries) != 2 {
		t.Fatalf("expired logs still visible: %v", entries)
	}
	if len(asList(t, e.get("/api/v1/traces?service=svc", acmeReader).data(t))) != 1 {
		t.Fatal("expired trace still searchable")
	}
	ds := e.get("/api/v1/metrics/downsampled?match[]=temperature&aggr=max&start="+fmt.Sprint(now.Add(-30*time.Hour).Unix()), acmeReader)
	if res := asList(t, asMap(t, ds.data(t))["result"]); len(res) != 1 {
		t.Fatalf("downsampled data %s", ds.Body)
	}

	// Restart on the same data directory: everything persisted comes back.
	e.stop()
	e2 := start(t, cfg)
	if len(asList(t, asMap(t, e2.get("/api/v1/logs/query?q=aged&limit=10", acmeReader).data(t))["entries"])) != 2 {
		t.Fatal("logs lost across restart")
	}
	res := asList(t, asMap(t, e2.get("/api/v1/query?query=count_over_time(temperature[2h])", acmeReader).data(t))["result"])
	if len(res) != 1 {
		t.Fatal("metrics lost across restart")
	}
	if len(asList(t, e2.get("/api/v1/traces?service=svc", acmeReader).data(t))) != 1 {
		t.Fatal("traces lost across restart")
	}
	if len(asList(t, asMap(t, e2.get("/api/v1/alerts/rules", acmeReader).data(t))["rules"])) != 1 {
		t.Fatal("alert rules lost across restart")
	}
}
