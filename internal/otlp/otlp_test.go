package otlp

import (
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/traces"
)

func load(t *testing.T, name string, v any) {
	t.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

func TestTracesFixture(t *testing.T) {
	var req ExportTraceServiceRequest
	load(t, "traces.json", &req)
	spans, rej := ToSpans(&req)
	if len(spans) != 4 || rej.Count != 1 || !strings.Contains(rej.Message(), "traceId") {
		t.Fatalf("spans=%d rejected=%d (%s)", len(spans), rej.Count, rej.Message())
	}
	root := spans[0]
	if root.TraceID != "5b8efff798038103d269b633813fc60c" || root.SpanID != "eee19b7ec3c1b174" {
		t.Fatalf("ids must be normalised to lowercase hex: %s/%s", root.TraceID, root.SpanID)
	}
	if root.Service != "frontend" || root.Kind != traces.KindServer || root.Scope != "io.opentelemetry.contrib.http" {
		t.Fatalf("root %+v", root)
	}
	if root.Attributes["http.response.status_code"] != int64(200) || root.Attributes["user.premium"] != true {
		t.Fatalf("typed attributes: %#v", root.Attributes)
	}
	if items, ok := root.Attributes["cart.items"].([]any); !ok || len(items) != 2 {
		t.Fatalf("array attribute: %#v", root.Attributes["cart.items"])
	}
	if root.Resource["deployment.environment"] != "staging" || len(root.Events) != 1 || root.Events[0].Name != "cache.miss" {
		t.Fatalf("resource/events: %+v", root)
	}
	if root.DurationNano() != 250*int64(time.Millisecond) {
		t.Fatalf("duration %d", root.DurationNano())
	}
	proc := spans[2]
	if proc.Service != "checkout" || !proc.IsError() || proc.Status.Message != "payment declined" {
		t.Fatalf("checkout span %+v", proc)
	}
	if proc.Attributes["order.total"] != 99.5 || proc.Attributes["payload"] != "aGVsbG8=" {
		t.Fatalf("double/bytes attributes %#v", proc.Attributes)
	}
	if meta, ok := proc.Attributes["meta"].(map[string]any); !ok || meta["region"] != "eu" {
		t.Fatalf("kvlist attribute %#v", proc.Attributes["meta"])
	}
}

func TestLogsFixture(t *testing.T) {
	var req ExportLogsServiceRequest
	load(t, "logs.json", &req)
	now := time.Unix(1_800_000_000, 0)
	entries, rej := ToLogEntries(&req, now)
	if len(entries) != 2 || rej.Count != 0 {
		t.Fatalf("entries=%d rej=%+v", len(entries), rej)
	}
	e := entries[0]
	if e.Level != "error" || e.Message != "payment declined for order 42" || e.Timestamp != 1735689600050000000 {
		t.Fatalf("entry %+v", e)
	}
	want := map[string]string{
		"service": "checkout", "resource.host.name": "node-7", "order.id": "42", "payment.provider": "acme-pay",
		"trace_id": "5b8efff798038103d269b633813fc60c", "span_id": "bbb19b7ec3c1b174", "otel.scope": "com.example.checkout.logger",
	}
	for k, v := range want {
		if e.Fields[k] != v {
			t.Errorf("field %s = %q, want %q", k, e.Fields[k], v)
		}
	}
	second := entries[1]
	if second.Level != "info" || second.Timestamp != 1735689601000000000 || second.Message != `{"event":"cart.updated"}` {
		t.Fatalf("observed-time fallback / kvlist body: %+v", second)
	}
	if second.Fields["attr.http_route"] != "/cart" {
		t.Fatalf("invalid attribute key must be sanitised: %v", second.Fields)
	}
}

func TestMetricsFixture(t *testing.T) {
	var req ExportMetricsServiceRequest
	load(t, "metrics.json", &req)
	samples, rej := ToSamples(&req)
	if rej.Count != 2 {
		t.Fatalf("expected delta sum and exponential histogram to be rejected: %+v", rej)
	}
	got := map[string]float64{}
	for _, s := range samples {
		if s.Labels.Get("job") != "checkout" || s.Labels.Get("instance") != "checkout-7f9c" {
			t.Fatalf("job/instance labels: %v", s.Labels)
		}
		if s.T != 1735689600000 {
			t.Fatalf("timestamp must be converted to ms: %d", s.T)
		}
		got[s.Labels.String()] = s.V
	}
	checks := map[string]float64{
		`http_server_requests_total{http_route="/checkout", instance="checkout-7f9c", job="checkout"}`:             1250,
		`process_memory_usage{instance="checkout-7f9c", job="checkout"}`:                                           52428800,
		`http_server_duration_bucket{http_route="/checkout", instance="checkout-7f9c", job="checkout", le="50"}`:   2,
		`http_server_duration_bucket{http_route="/checkout", instance="checkout-7f9c", job="checkout", le="100"}`:  7,
		`http_server_duration_bucket{http_route="/checkout", instance="checkout-7f9c", job="checkout", le="250"}`:  9,
		`http_server_duration_bucket{http_route="/checkout", instance="checkout-7f9c", job="checkout", le="+Inf"}`: 10,
		`http_server_duration_sum{http_route="/checkout", instance="checkout-7f9c", job="checkout"}`:               1234.5,
		`http_server_duration_count{http_route="/checkout", instance="checkout-7f9c", job="checkout"}`:             10,
		`rpc_latency{instance="checkout-7f9c", job="checkout", quantile="0.5"}`:                                    2,
		`rpc_latency_count{instance="checkout-7f9c", job="checkout"}`:                                              4,
	}
	for k, v := range checks {
		if g, ok := got[k]; !ok || g != v {
			t.Errorf("%s = %v (present %v), want %v", k, g, ok, v)
		}
	}
	if v := got[`rpc_latency{instance="checkout-7f9c", job="checkout", quantile="0.99"}`]; !math.IsInf(v, 1) {
		t.Errorf("\"Infinity\" string must decode to +Inf, got %v", v)
	}
}

func TestScalarCodecs(t *testing.T) {
	var u Uint64
	var i Int64
	var f Float64
	for _, c := range []struct {
		in  string
		dst json.Unmarshaler
	}{{`"123"`, &u}, {`-5`, &i}, {`"-Infinity"`, &f}} {
		if err := c.dst.UnmarshalJSON([]byte(c.in)); err != nil {
			t.Fatal(err)
		}
	}
	if u != 123 || i != -5 || !math.IsInf(float64(f), -1) {
		t.Fatalf("u=%d i=%d f=%v", u, i, f)
	}
	if err := u.UnmarshalJSON([]byte(`"-1"`)); err == nil {
		t.Fatal("negative uint64 must fail")
	}
	if err := f.UnmarshalJSON([]byte(`"NaN"`)); err != nil || !math.IsNaN(float64(f)) {
		t.Fatal("NaN string")
	}
	out, _ := json.Marshal(ExportTraceServiceResponse{PartialSuccess: &TracePartialSuccess{RejectedSpans: 3}})
	if string(out) != `{"partialSuccess":{"rejectedSpans":"3"}}` {
		t.Fatalf("int64 must encode as a JSON string: %s", out)
	}
	deep := AnyValue{}
	cur := &deep
	for d := 0; d < 20; d++ {
		cur.ArrayValue = &ArrayValue{Values: []AnyValue{{}}}
		cur = &cur.ArrayValue.Values[0]
	}
	if !strings.Contains(deep.String(), "nested too deep") {
		t.Fatal("deep nesting must be bounded")
	}
}
