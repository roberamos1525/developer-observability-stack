package otlp

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func spanReq(t *testing.T, spans string) *ExportTraceServiceRequest {
	t.Helper()
	var req ExportTraceServiceRequest
	body := `{"resourceSpans":[{"resource":{"attributes":[]},"scopeSpans":[{"scope":{"name":"s"},"spans":[` + spans + `]}]}]}`
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	return &req
}

func TestInvalidSpansAreRejectedIndividually(t *testing.T) {
	const tid, sid = `"traceId":"0102030405060708090a0b0c0d0e0f10"`, `"spanId":"0102030405060708"`
	cases := map[string]string{
		"short trace id":  `{"traceId":"abc",` + sid + `,"name":"x","startTimeUnixNano":"1","endTimeUnixNano":"2"}`,
		"non-hex span id": `{` + tid + `,"spanId":"zzzzzzzzzzzzzzzz","name":"x","startTimeUnixNano":"1","endTimeUnixNano":"2"}`,
		"self parent":     `{` + tid + `,` + sid + `,"parentSpanId":"0102030405060708","name":"x","startTimeUnixNano":"1","endTimeUnixNano":"2"}`,
		"end before":      `{` + tid + `,` + sid + `,"name":"x","startTimeUnixNano":"5","endTimeUnixNano":"2"}`,
		"empty name":      `{` + tid + `,` + sid + `,"name":"","startTimeUnixNano":"1","endTimeUnixNano":"2"}`,
		"bad kind":        `{` + tid + `,` + sid + `,"name":"x","kind":9,"startTimeUnixNano":"1","endTimeUnixNano":"2"}`,
		"bad status":      `{` + tid + `,` + sid + `,"name":"x","startTimeUnixNano":"1","endTimeUnixNano":"2","status":{"code":7}}`,
		"overflow time":   `{` + tid + `,` + sid + `,"name":"x","startTimeUnixNano":"18446744073709551615","endTimeUnixNano":"18446744073709551615"}`,
	}
	valid := `{` + tid + `,` + sid + `,"name":"ok","startTimeUnixNano":"1","endTimeUnixNano":"2"}`
	for name, bad := range cases {
		spans, rej := ToSpans(spanReq(t, bad+","+valid))
		if len(spans) != 1 || rej.Count != 1 || rej.Message() == "" {
			t.Errorf("%s: spans=%d rejected=%d", name, len(spans), rej.Count)
		}
		if spans[0].Service != DefaultService {
			t.Errorf("missing service.name must default to %s", DefaultService)
		}
	}
	var r Rejections
	for i := 0; i < 10; i++ {
		r.add(1, "reason %d", i)
	}
	if r.Count != 10 || len(r.Reasons) != 5 {
		t.Fatalf("reasons must be capped: %+v", r)
	}
}

func TestInvalidJSONScalars(t *testing.T) {
	var req ExportTraceServiceRequest
	err := json.Unmarshal([]byte(`{"resourceSpans":[{"scopeSpans":[{"spans":[{"startTimeUnixNano":"soon"}]}]}]}`), &req)
	if err == nil || !strings.Contains(err.Error(), "uint64") {
		t.Fatalf("expected uint64 error, got %v", err)
	}
	var f Float64
	if f.UnmarshalJSON([]byte(`"abc"`)) == nil {
		t.Fatal("bad double must fail")
	}
}

func TestMetricsEdgeCases(t *testing.T) {
	body := `{"resourceMetrics":[{"resource":{"attributes":[]},"scopeMetrics":[{"scope":{},"metrics":[
		{"name":"","gauge":{"dataPoints":[]}},
		{"name":"empty.metric"},
		{"name":"bad.hist","histogram":{"aggregationTemporality":2,"dataPoints":[{"timeUnixNano":"1000000","count":"1","bucketCounts":["1"],"explicitBounds":[1,2]}]}},
		{"name":"delta.hist","histogram":{"aggregationTemporality":1,"dataPoints":[{"timeUnixNano":"1"}]}},
		{"name":"up.down","sum":{"aggregationTemporality":2,"isMonotonic":false,"dataPoints":[{"timeUnixNano":"2000000","asDouble":-3}]}},
		{"name":"already_total","sum":{"aggregationTemporality":2,"isMonotonic":true,"dataPoints":[{"timeUnixNano":"2000000","asInt":"4"}]}}
	]}]}]}`
	var req ExportMetricsServiceRequest
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	samples, rej := ToSamples(&req)
	if rej.Count != 4 {
		t.Fatalf("rejections %+v", rej)
	}
	names := map[string]float64{}
	for _, s := range samples {
		names[s.Labels.Get("__name__")] = s.V
		if s.Labels.Get("job") != DefaultService {
			t.Fatalf("job label %v", s.Labels)
		}
	}
	if names["up_down"] != -3 || names["already_total"] != 4 || len(names) != 2 {
		t.Fatalf("non-monotonic sums keep their name, _total is not doubled: %v", names)
	}
}

func TestLogSeverityMapping(t *testing.T) {
	want := map[int]string{0: "", 1: "trace", 5: "debug", 9: "info", 13: "warn", 17: "error", 21: "fatal", 24: "fatal"}
	for n, lvl := range want {
		if got := SeverityLevel(n); got != lvl {
			t.Errorf("SeverityLevel(%d) = %q, want %q", n, got, lvl)
		}
	}
	var req ExportLogsServiceRequest
	json.Unmarshal([]byte(`{"resourceLogs":[{"resource":{"attributes":[]},"scopeLogs":[{"scope":{},"logRecords":[
		{"body":{"stringValue":"no timestamps"},"severityText":"Warning"}]}]}]}`), &req)
	now := time.Unix(1_800_000_000, 0)
	entries, _ := ToLogEntries(&req, now)
	if len(entries) != 1 || entries[0].Timestamp != now.UnixNano() || entries[0].Level != "warn" || entries[0].Fields["service"] != "" {
		t.Fatalf("defaults %+v", entries)
	}
}
