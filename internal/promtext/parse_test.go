package promtext

import (
	"errors"
	"math"
	"strings"
	"testing"
)

const exposition = `# HELP http_requests_total Total HTTP requests.\nSecond line.
# TYPE http_requests_total counter
http_requests_total{method="post",code="200"} 1027 1395066363000
http_requests_total{method="post",code="400",} 3 1395066363000

# A normal comment.
# TYPE rpc_duration_seconds histogram
rpc_duration_seconds_bucket{le="0.05"} 24054
rpc_duration_seconds_bucket{le="0.1"} 33444
rpc_duration_seconds_bucket{le="+Inf"} 144320
rpc_duration_seconds_sum 53423
rpc_duration_seconds_count 144320
# TYPE go_gc_seconds summary
go_gc_seconds{quantile="0.5"} NaN
go_gc_seconds_sum 1.5e-3
go_gc_seconds_count 7
msdos_file_access_time_seconds{path="C:\\DIR\\FILE.TXT",error="Cannot find file:\n\"FILE.TXT\""} 1.458255915e9
metric_without_timestamp_and_labels 12.47
something_weird{problem="division by zero"} +Inf -3982045
# EOF
ignored_after_eof 1
`

func TestParseExposition(t *testing.T) {
	fams, err := Parse(strings.NewReader(exposition))
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]*Family{}
	for _, f := range fams {
		byName[f.Name] = f
	}
	if len(fams) != 6 {
		names := []string{}
		for _, f := range fams {
			names = append(names, f.Name)
		}
		t.Fatalf("families: %v", names)
	}
	req := byName["http_requests_total"]
	if req.Type != Counter || len(req.Samples) != 2 || !strings.Contains(req.Help, "\n") {
		t.Fatalf("counter family %+v", req)
	}
	s := req.Samples[0]
	if s.Value != 1027 || s.TimestampMs == nil || *s.TimestampMs != 1395066363000 || s.Labels.Get("code") != "200" {
		t.Fatalf("sample %+v", s)
	}
	hist := byName["rpc_duration_seconds"]
	if hist.Type != Histogram || len(hist.Samples) != 5 {
		t.Fatalf("histogram family has %d samples", len(hist.Samples))
	}
	if !math.IsInf(hist.Samples[2].Value, 0) && hist.Samples[2].Labels.Get("le") != "+Inf" {
		t.Fatal("+Inf bucket")
	}
	sum := byName["go_gc_seconds"]
	if sum.Type != Summary || !math.IsNaN(sum.Samples[0].Value) || len(sum.Samples) != 3 {
		t.Fatalf("summary %+v", sum)
	}
	ms := byName["msdos_file_access_time_seconds"]
	if got := ms.Samples[0].Labels.Get("path"); got != `C:\DIR\FILE.TXT` {
		t.Fatalf("escaped label %q", got)
	}
	if got := ms.Samples[0].Labels.Get("error"); got != "Cannot find file:\n\"FILE.TXT\"" {
		t.Fatalf("escaped label %q", got)
	}
	weird := byName["something_weird"].Samples[0]
	if !math.IsInf(weird.Value, 1) || *weird.TimestampMs != -3982045 {
		t.Fatalf("weird %+v", weird)
	}
	if byName["ignored_after_eof"] != nil {
		t.Fatal("content after # EOF must be ignored")
	}
}

func TestParseErrors(t *testing.T) {
	bad := map[string]string{
		"bad name":        "9metric 1\n",
		"missing value":   "metric{a=\"b\"}\n",
		"bad value":       "metric abc\n",
		"bad timestamp":   "metric 1 12.5\n",
		"unquoted":        "metric{a=b} 1\n",
		"unterminated":    "metric{a=\"b} 1\n",
		"bad escape":      "metric{a=\"\\x\"} 1\n",
		"duplicate label": "metric{a=\"1\",a=\"2\"} 1\n",
		"type after data": "m 1\n# TYPE m counter\n",
		"duplicate type":  "# TYPE m counter\n# TYPE m gauge\n",
		"unknown type":    "# TYPE m widget\n",
		"bucket no le":    "# TYPE h histogram\nh_bucket 1\n",
		"bad label name":  "metric{1a=\"x\"} 1\n",
		"extra fields":    "metric 1 2 3\n",
		"missing comma":   "metric{a=\"1\" b=\"2\"} 1\n",
		"long name":       strings.Repeat("m", 3000) + " 1\n",
		"long value":      "m{a=\"" + strings.Repeat("v", 3000) + "\"} 1\n",
	}
	for name, in := range bad {
		_, err := Parse(strings.NewReader(in))
		var pe *ParseError
		if err == nil || !errors.As(err, &pe) {
			t.Errorf("%s: expected ParseError, got %v", name, err)
		}
	}
}
