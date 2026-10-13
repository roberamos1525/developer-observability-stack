package httpapi

import (
	"bytes"
	"compress/gzip"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/errs"
	"github.com/robertramos/developer-observability-stack/internal/selfmon"
)

func TestParseTimeAndDuration(t *testing.T) {
	def := time.Unix(100, 0)
	cases := map[string]time.Time{
		"":                            def,
		"1700000000":                  time.Unix(1700000000, 0),
		"1700000000.5":                time.Unix(1700000000, 5e8),
		"2026-01-02T03:04:05Z":        time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		"2026-01-02T03:04:05.1+02:00": time.Date(2026, 1, 2, 1, 4, 5, 1e8, time.UTC),
	}
	for in, want := range cases {
		got, err := parseTime(in, def)
		if err != nil || !got.Equal(want) {
			t.Errorf("parseTime(%q) = %v, %v", in, got, err)
		}
	}
	for _, bad := range []string{"NaN", "Inf", "1e20", "yesterday"} {
		if _, err := parseTime(bad, def); !errs.Is(err, errs.KindInvalid) {
			t.Errorf("parseTime(%q) should be invalid", bad)
		}
	}
	durs := map[string]time.Duration{"": time.Minute, "15s": 15 * time.Second, "1.5": 1500 * time.Millisecond, "2h": 2 * time.Hour}
	for in, want := range durs {
		if got, err := parseDuration(in, time.Minute); err != nil || got != want {
			t.Errorf("parseDuration(%q) = %v, %v", in, got, err)
		}
	}
	for _, bad := range []string{"0", "-1", "abc", "1e10"} {
		if _, err := parseDuration(bad, 0); err == nil {
			t.Errorf("parseDuration(%q) should fail", bad)
		}
	}
	if n, err := parseLimit("", 10, 100); err != nil || n != 10 {
		t.Fatal("default limit")
	}
	for _, bad := range []string{"0", "x", "101"} {
		if _, err := parseLimit(bad, 10, 100); err == nil {
			t.Errorf("parseLimit(%q) should fail", bad)
		}
	}
}

func TestFormatFloatAndStatusMapping(t *testing.T) {
	cases := map[float64]string{1.5: "1.5", 100: "100", math.Inf(1): "+Inf", math.Inf(-1): "-Inf", 1e21: "1000000000000000000000"}
	for in, want := range cases {
		if got := formatFloat(in); got != want {
			t.Errorf("formatFloat(%v) = %s", in, got)
		}
	}
	if formatFloat(math.NaN()) != "NaN" {
		t.Error("NaN")
	}
	want := map[errs.Kind]int{
		errs.KindInvalid: 400, errs.KindUnauthorized: 401, errs.KindForbidden: 403, errs.KindNotFound: 404,
		errs.KindConflict: 409, errs.KindTooLarge: 413, errs.KindUnsupported: 415, errs.KindUnavailable: 429,
		errs.KindTimeout: 503, errs.KindInternal: 500,
	}
	for k, code := range want {
		if httpStatus(k) != code {
			t.Errorf("%s -> %d, want %d", k, httpStatus(k), code)
		}
	}
}

func testServer() *Server {
	cfg := config.Default()
	cfg.MaxBodyBytes = 64
	cfg.MaxDecompressedBytes = 128
	return &Server{d: Deps{Config: cfg, Log: slog.New(slog.DiscardHandler), Metrics: selfmon.New()}}
}

func TestReadBodyLimits(t *testing.T) {
	s := testServer()
	read := func(body []byte, enc string) ([]byte, error) {
		r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		if enc != "" {
			r.Header.Set("Content-Encoding", enc)
		}
		return s.readBody(httptest.NewRecorder(), r)
	}
	if b, err := read([]byte("hello"), ""); err != nil || string(b) != "hello" {
		t.Fatalf("plain body: %q %v", b, err)
	}
	var gz bytes.Buffer
	w := gzip.NewWriter(&gz)
	w.Write([]byte(strings.Repeat("a", 100)))
	w.Close()
	if b, err := read(gz.Bytes(), "gzip"); err != nil || len(b) != 100 {
		t.Fatalf("gzip within limit: %d %v", len(b), err)
	}
	gz.Reset()
	w = gzip.NewWriter(&gz)
	w.Write([]byte(strings.Repeat("a", 1000)))
	w.Close()
	if _, err := read(gz.Bytes(), "gzip"); !errs.Is(err, errs.KindTooLarge) {
		t.Fatalf("decompressed limit: %v", err)
	}
	if _, err := read(bytes.Repeat([]byte("x"), 65), ""); !errs.Is(err, errs.KindTooLarge) {
		t.Fatalf("raw limit: %v", err)
	}
	if _, err := read(nil, ""); !errs.Is(err, errs.KindInvalid) {
		t.Fatal("empty body")
	}
	if _, err := read([]byte("x"), "zstd"); !errs.Is(err, errs.KindUnsupported) {
		t.Fatal("unknown encoding")
	}
}

func TestFailHidesInternalDetails(t *testing.T) {
	s := testServer()
	rec := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	s.fail(rec, r, errors.New("open /secret/path: permission denied"))
	if rec.Code != 500 || strings.Contains(rec.Body.String(), "/secret/path") {
		t.Fatalf("internal error leaked: %s", rec.Body)
	}
	rec = httptest.NewRecorder()
	s.fail(rec, r, errs.Unavailable("queue full"))
	if rec.Code != 429 || rec.Header().Get("Retry-After") != "1" || !strings.Contains(rec.Body.String(), "queue full") {
		t.Fatalf("backpressure response: %d %v %s", rec.Code, rec.Header(), rec.Body)
	}
	rec = httptest.NewRecorder()
	s.otlpFail(rec, errs.Unavailable("busy"))
	if rec.Code != 429 || !strings.Contains(rec.Body.String(), `"code":14`) {
		t.Fatalf("OTLP status mapping: %s", rec.Body)
	}
}

func TestRecovererAndMediaType(t *testing.T) {
	s := testServer()
	h := s.recoverer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != 500 || !strings.Contains(rec.Body.String(), `"internal"`) {
		t.Fatalf("panic not converted: %d %s", rec.Code, rec.Body)
	}
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set("Content-Type", "Application/JSON; charset=utf-8")
	if mediaType(r) != "application/json" {
		t.Fatal(mediaType(r))
	}
}
