package integration

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestLogsIngestAndQuery(t *testing.T) {
	e := newEnv(t, nil)
	now := time.Now().UTC()
	var nd strings.Builder
	for i := 0; i < 30; i++ {
		level, svc := "info", "web"
		if i%3 == 0 {
			level, svc = "error", "checkout"
		}
		fmt.Fprintf(&nd, `{"timestamp":%q,"level":%q,"message":"request %d failed: connection refused by db-%d","service":%q,"status":%d}`+"\n",
			now.Add(-time.Duration(30-i)*time.Second).Format(time.RFC3339Nano), level, i, i%2, svc, 200+i)
	}
	nd.WriteString("{broken json}\n")
	r := e.do(http.MethodPost, "/api/v1/logs?sync=true", acmeWriter, "application/x-ndjson", []byte(nd.String()))
	expectStatus(t, r, http.StatusOK)
	d := asMap(t, r.data(t))
	if d["accepted"].(float64) != 30 || d["rejected"].(float64) != 1 {
		t.Fatalf("ingest result %v", d)
	}
	arr := fmt.Sprintf(`[{"ts":%d,"msg":"cache warmed","service":"web","level":"debug"}]`, now.Add(-time.Minute).UnixMilli())
	expectStatus(t, e.do(http.MethodPost, "/api/v1/logs", acmeWriter, "application/json", gzipped([]byte(arr)), "Content-Encoding", "gzip"), http.StatusAccepted)
	syslog := "<11>1 " + now.Add(-2*time.Second).Format(time.RFC3339) + " host-1 payments 77 - - card declined for order 7\n" +
		"<14>" + now.Format("Jan _2 15:04:05") + " host-2 cron[9]: nightly job done\n"
	r = e.do(http.MethodPost, "/api/v1/logs/syslog?sync=1", acmeWriter, "text/plain", []byte(syslog))
	expectStatus(t, r, http.StatusOK)
	r = e.do(http.MethodPost, "/v1/logs", acmeWriter, "application/json", fixture(t, "logs.json"))
	expectStatus(t, r, http.StatusOK)
	if strings.TrimSpace(string(r.Body)) != "{}" {
		t.Fatalf("OTLP full success must return an empty response object: %s", r.Body)
	}
	e.flush()

	query := func(q string, extra ...string) map[string]any {
		t.Helper()
		v := url.Values{"q": {q}}
		for i := 0; i+1 < len(extra); i += 2 {
			v.Set(extra[i], extra[i+1])
		}
		r := e.get("/api/v1/logs/query?"+v.Encode(), acmeReader)
		expectStatus(t, r, http.StatusOK)
		return asMap(t, r.data(t))
	}
	res := query("level:error service:checkout", "limit", "3")
	entries := asList(t, res["entries"])
	if len(entries) != 3 || res["truncated"] != true {
		t.Fatalf("limit/truncation: %v", res)
	}
	first := asMap(t, entries[0])
	if !strings.Contains(first["message"].(string), "request 27") {
		t.Fatalf("newest first expected, got %v", first)
	}
	if n := len(asList(t, query(`"connection refused" AND status>=215 AND NOT service:web`)["entries"])); n != 5 {
		t.Fatalf("phrase + numeric + NOT returned %d", n)
	}
	if n := len(asList(t, query(`app:payments OR app:cron`)["entries"])); n != 2 {
		t.Fatalf("syslog entries: %d", n)
	}
	if n := len(asList(t, query(`payment.provider:acme-pay trace_id:5b8efff798038103d269b633813fc60c`, "start", "2024-12-31T00:00:00Z")["entries"])); n != 1 {
		t.Fatalf("OTLP log fields: %d", n)
	}
	counts := asList(t, query("| count by level")["groups"])
	got := map[string]float64{}
	for _, g := range counts {
		gm := asMap(t, g)
		got[asMap(t, gm["labels"])["level"].(string)] = gm["count"].(float64)
	}
	if got["error"] != 12 || got["info"] != 22 || got["debug"] != 1 {
		t.Fatalf("count by level %v", got)
	}
	// Time range via parameters and via the since stage.
	from := strconv.FormatInt(now.Add(-10*time.Second).Unix(), 10)
	if n := len(asList(t, query("service:*", "start", from, "limit", "100")["entries"])); n < 9 || n > 14 {
		t.Fatalf("start param filter returned %d", n)
	}
	if n := len(asList(t, query("service:web | since 15s | limit 100")["entries"])); n < 5 || n > 10 {
		t.Fatalf("since stage returned %d", n)
	}
	r = e.get("/api/v1/logs/query?q="+url.QueryEscape("level:(error"), acmeReader)
	expectStatus(t, r, http.StatusBadRequest)
	if r.errorCode(t) != "invalid_argument" {
		t.Fatal("bad query must be invalid_argument")
	}
	expectStatus(t, e.get("/api/v1/logs/query?q=x&limit=999999", acmeReader), http.StatusBadRequest)
	fields := asList(t, e.get("/api/v1/logs/fields", acmeReader).data(t))
	names := map[string]bool{}
	for _, f := range fields {
		names[asMap(t, f)["name"].(string)] = true
	}
	for _, want := range []string{"service", "status", "level", "facility", "resource.host.name"} {
		if !names[want] {
			t.Errorf("field %s missing from %v", want, names)
		}
	}
}

func TestMetricsIngestAndPromQL(t *testing.T) {
	e := newEnv(t, nil)
	now := time.Now()
	var samples []map[string]any
	for i := 0; i <= 20; i++ {
		ts := now.Add(-5 * time.Minute).Add(time.Duration(i) * 15 * time.Second).UnixMilli()
		for _, c := range []struct {
			code string
			rate float64
		}{{"200", 10}, {"500", 1}} {
			samples = append(samples, map[string]any{"name": "http_requests_total", "labels": map[string]string{"job": "api", "code": c.code},
				"value": c.rate * 15 * float64(i), "timestamp": ts})
		}
		for _, b := range []struct {
			le string
			v  float64
		}{{"0.1", 50}, {"0.5", 80}, {"+Inf", 100}} {
			samples = append(samples, map[string]any{"name": "latency_seconds_bucket", "labels": map[string]string{"job": "api", "le": b.le},
				"value": b.v * float64(i), "timestamp": ts})
		}
		samples = append(samples, map[string]any{"name": "queue_depth", "labels": map[string]string{"job": "api"}, "value": float64(i % 5), "timestamp": ts})
	}
	samples = append(samples, map[string]any{"name": "bad-name", "value": 1}, map[string]any{"name": "no_value"})
	r := e.postJSON("/api/v1/metrics?sync=true", acmeWriter, map[string]any{"samples": samples})
	expectStatus(t, r, http.StatusOK)
	if d := asMap(t, r.data(t)); d["rejected"].(float64) != 2 {
		t.Fatalf("validation rejections %v", d)
	}
	text := "# TYPE pushed_jobs_total counter\npushed_jobs_total{queue=\"q1\"} 42\n"
	expectStatus(t, e.do(http.MethodPost, "/api/v1/metrics/prometheus?sync=true&label=source%3Dpush", acmeWriter, "text/plain", []byte(text)), http.StatusOK)
	r = e.do(http.MethodPost, "/v1/metrics", acmeWriter, "application/json", fixture(t, "metrics.json"))
	expectStatus(t, r, http.StatusOK)
	if ps := asMap(t, r.json(t)["partialSuccess"]); ps["rejectedDataPoints"] != "2" {
		t.Fatalf("OTLP metrics partial success %v", ps)
	}
	e.flush()

	instant := func(q string, at time.Time) []any {
		t.Helper()
		r := e.get("/api/v1/query?"+url.Values{"query": {q}, "time": {strconv.FormatInt(at.Unix(), 10)}}.Encode(), acmeReader)
		expectStatus(t, r, http.StatusOK)
		d := asMap(t, r.data(t))
		if d["resultType"] != "vector" {
			t.Fatalf("%s: result type %v", q, d["resultType"])
		}
		return asList(t, d["result"])
	}
	value := func(s any) float64 {
		v, _ := strconv.ParseFloat(asMap(t, s)["value"].([]any)[1].(string), 64)
		return v
	}
	res := instant(`sum by (job) (rate(http_requests_total[2m]))`, now)
	if len(res) != 1 || value(res[0]) < 10.9 || value(res[0]) > 11.1 {
		t.Fatalf("sum(rate): %v", res)
	}
	res = instant(`histogram_quantile(0.9, rate(latency_seconds_bucket[2m]))`, now)
	// rank 0.9*100 = 90 falls in (0.5, +Inf] -> highest finite bound 0.5
	if len(res) != 1 || value(res[0]) != 0.5 {
		t.Fatalf("histogram_quantile: %v", res)
	}
	res = instant(`histogram_quantile(0.6, rate(latency_seconds_bucket[2m]))`, now)
	if v := value(res[0]); v < 0.23 || v > 0.24 { // 0.1 + 0.4*(10/30)
		t.Fatalf("histogram_quantile interpolation: %v", v)
	}
	if res = instant(`avg_over_time(queue_depth[5m])`, now); len(res) != 1 || value(res[0]) < 1.5 || value(res[0]) > 2.5 {
		t.Fatalf("avg_over_time: %v", res)
	}
	if res = instant(`pushed_jobs_total{source="push"}`, now.Add(time.Minute)); len(res) != 1 || value(res[0]) != 42 {
		t.Fatalf("prometheus text push: %v", res)
	}
	if res = instant(`http_server_requests_total{job="checkout"}`, time.Unix(1735689600, 0)); len(res) != 1 || value(res[0]) != 1250 {
		t.Fatalf("OTLP sum: %v", res)
	}
	r = e.get("/api/v1/query_range?"+url.Values{"query": {`rate(http_requests_total{code="500"}[1m])`},
		"start": {strconv.FormatInt(now.Add(-3*time.Minute).Unix(), 10)}, "end": {strconv.FormatInt(now.Unix(), 10)}, "step": {"30s"}}.Encode(), acmeReader)
	expectStatus(t, r, http.StatusOK)
	m := asMap(t, r.data(t))
	series := asList(t, m["result"])
	if m["resultType"] != "matrix" || len(series) != 1 || len(asList(t, asMap(t, series[0])["values"])) != 7 {
		t.Fatalf("range query %v", m)
	}
	r = e.get("/api/v1/series?"+url.Values{"match[]": {`http_requests_total`, `{__name__=~"queue.*"}`}, "start": {strconv.FormatInt(now.Add(-time.Hour).Unix(), 10)}}.Encode(), acmeReader)
	if n := len(asList(t, r.data(t))); n != 3 {
		t.Fatalf("series %d: %s", n, r.Body)
	}
	if vals := asList(t, e.get("/api/v1/label/code/values", acmeReader).data(t)); len(vals) != 2 {
		t.Fatalf("label values %v", vals)
	}
	if names := asList(t, e.get("/api/v1/labels", acmeReader).data(t)); len(names) < 5 {
		t.Fatalf("label names %v", names)
	}
	for _, bad := range []string{"/api/v1/query?query=rate(", "/api/v1/query", "/api/v1/query?query=up&time=yesterday",
		"/api/v1/query_range?query=up&step=-1", "/api/v1/series", "/api/v1/label/bad-name/values",
		"/api/v1/query_range?query=up&start=0&end=100000000&step=1"} {
		r := e.get(bad, acmeReader)
		expectStatus(t, r, http.StatusBadRequest)
		r.errorCode(t)
	}
	// POST form encoding as used by Prometheus clients.
	r = e.do(http.MethodPost, "/api/v1/query", acmeReader, "application/x-www-form-urlencoded", []byte("query=vector(7)"))
	if res := asList(t, asMap(t, r.data(t))["result"]); value(res[0]) != 7 {
		t.Fatalf("form-encoded query %v", res)
	}
}

func TestTracesOTLPSearchTreeAndDependencies(t *testing.T) {
	e := newEnv(t, nil)
	r := e.do(http.MethodPost, "/v1/traces", acmeWriter, "application/json", fixture(t, "traces.json"))
	expectStatus(t, r, http.StatusOK)
	ps := asMap(t, r.json(t)["partialSuccess"])
	if ps["rejectedSpans"] != "1" || !strings.Contains(ps["errorMessage"].(string), "traceId") {
		t.Fatalf("partial success %v", ps)
	}
	e.flush()
	const traceID = "5b8efff798038103d269b633813fc60c"
	search := func(q string) []any {
		t.Helper()
		r := e.get("/api/v1/traces?"+q, acmeReader)
		expectStatus(t, r, http.StatusOK)
		return asList(t, r.data(t))
	}
	res := search("service=checkout&error=true")
	if len(res) != 1 {
		t.Fatalf("search: %v", res)
	}
	sum := asMap(t, res[0])
	if sum["traceId"] != traceID || sum["rootService"] != "frontend" || sum["spanCount"].(float64) != 4 || sum["errorCount"].(float64) != 1 {
		t.Fatalf("summary %v", sum)
	}
	if len(search("operation=ProcessOrder&minDuration=150ms")) != 1 || len(search("operation=ProcessOrder&minDuration=1s")) != 0 {
		t.Fatal("duration filter")
	}
	if len(search("attr=db.system%3Dpostgresql")) != 1 || len(search("attr=deployment.environment%3Dstaging&service=frontend")) != 1 {
		t.Fatal("attribute filter (span and resource attributes)")
	}
	if len(search("start=2030-01-01T00:00:00Z")) != 0 {
		t.Fatal("time filter")
	}
	r = e.get("/api/v1/traces/"+strings.ToUpper(traceID), acmeReader)
	expectStatus(t, r, http.StatusOK)
	tree := asMap(t, r.data(t))
	roots := asList(t, tree["roots"])
	if len(roots) != 1 || tree["spanCount"].(float64) != 4 || tree["durationMs"].(float64) != 250 {
		t.Fatalf("tree %v", tree)
	}
	client := asMap(t, asList(t, asMap(t, roots[0])["children"])[0])
	server := asMap(t, asList(t, client["children"])[0])
	if asMap(t, server["span"])["name"] != "ProcessOrder" || server["depth"].(float64) != 2 {
		t.Fatalf("tree nesting %v", server)
	}
	expectStatus(t, e.get("/api/v1/traces/0123456789abcdef0123456789abcdef", acmeReader), http.StatusNotFound)
	expectStatus(t, e.get("/api/v1/traces/xyz", acmeReader), http.StatusBadRequest)
	svcs := asMap(t, e.get("/api/v1/services", acmeReader).data(t))
	if _, ok := asMap(t, svcs["checkout"])["SELECT orders"]; !ok {
		t.Fatalf("services %v", svcs)
	}
	deps := asList(t, e.get("/api/v1/dependencies", acmeReader).data(t))
	edges := map[string]float64{}
	for _, d := range deps {
		dm := asMap(t, d)
		edges[dm["parent"].(string)+"->"+dm["child"].(string)] = dm["callCount"].(float64)
	}
	if edges["frontend->checkout"] != 1 || edges["checkout->postgres"] != 1 || len(edges) != 2 {
		t.Fatalf("dependencies %v", edges)
	}
	r = e.do(http.MethodPost, "/v1/traces", acmeWriter, "application/x-protobuf", []byte{0x0a})
	expectStatus(t, r, http.StatusUnsupportedMediaType)
	if r.json(t)["code"].(float64) != 3 {
		t.Fatalf("OTLP errors use google.rpc.Status: %s", r.Body)
	}
	expectStatus(t, e.do(http.MethodPost, "/v1/traces", acmeWriter, "application/json", []byte(`{"resourceSpans": 5}`)), http.StatusBadRequest)
}
