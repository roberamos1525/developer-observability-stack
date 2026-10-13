// Package integration exercises the full server in-process: HTTP API ->
// ingestion queues -> storage -> query engines, alerting and maintenance.
package integration

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/app"
	"github.com/robertramos/developer-observability-stack/internal/config"
)

const (
	acmeWriter = "acme-writer-key-0001"
	acmeReader = "acme-reader-key-0001"
	acmeAdmin  = "acme-admin-key-00001"
	betaAll    = "beta-all-scopes-0001"
)

type env struct {
	t   *testing.T
	app *app.App
	srv *httptest.Server
	cfg config.Config
}

func testConfig(t *testing.T, dataDir string) config.Config {
	cfg := config.Default()
	cfg.DataDir = dataDir
	cfg.LogLevel = "error"
	cfg.Auth.Keys = []config.APIKey{
		{Tenant: "acme", Key: acmeWriter, Scopes: []string{"write"}},
		{Tenant: "acme", Key: acmeReader, Scopes: []string{"read"}},
		{Tenant: "acme", Key: acmeAdmin, Scopes: []string{"admin"}},
		{Tenant: "beta", Key: betaAll, Scopes: []string{"read", "write"}},
	}
	cfg.Ingest.FlushInterval = config.Duration(5 * time.Millisecond)
	cfg.Alerting.GroupWait = 0
	cfg.Alerting.GroupInterval = config.Duration(time.Second)
	cfg.MaxBodyBytes = 1 << 20
	cfg.MaxDecompressedBytes = 4 << 20
	return cfg
}

func start(t *testing.T, cfg config.Config) *env {
	t.Helper()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	a, err := app.New(cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(a.Handler)
	e := &env{t: t, app: a, srv: srv, cfg: cfg}
	t.Cleanup(e.stop)
	return e
}

func newEnv(t *testing.T, mutate func(*config.Config)) *env {
	cfg := testConfig(t, t.TempDir())
	if mutate != nil {
		mutate(&cfg)
	}
	return start(t, cfg)
}

func (e *env) stop() {
	if e.srv == nil {
		return
	}
	e.srv.Close()
	e.app.Shutdown(context.Background())
	e.srv = nil
}

type response struct {
	Status int
	Header http.Header
	Body   []byte
}

func (r response) json(t *testing.T) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(r.Body, &m); err != nil {
		t.Fatalf("invalid JSON (%d): %s", r.Status, r.Body)
	}
	return m
}

// data returns the "data" member of a success envelope.
func (r response) data(t *testing.T) any {
	t.Helper()
	m := r.json(t)
	if m["status"] != "success" {
		t.Fatalf("expected success envelope, got %d %s", r.Status, r.Body)
	}
	return m["data"]
}

func (r response) errorCode(t *testing.T) string {
	t.Helper()
	m := r.json(t)
	errObj, _ := m["error"].(map[string]any)
	if m["status"] != "error" || errObj == nil || m["requestId"] == "" {
		t.Fatalf("expected error envelope, got %s", r.Body)
	}
	return errObj["code"].(string)
}

func (e *env) do(method, path, key, ctype string, body []byte, hdr ...string) response {
	e.t.Helper()
	req, err := http.NewRequest(method, e.srv.URL+path, bytes.NewReader(body))
	if err != nil {
		e.t.Fatal(err)
	}
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return response{resp.StatusCode, resp.Header, b}
}

func (e *env) get(path, key string) response { return e.do(http.MethodGet, path, key, "", nil) }

func (e *env) postJSON(path, key string, v any) response {
	b, err := json.Marshal(v)
	if err != nil {
		e.t.Fatal(err)
	}
	return e.do(http.MethodPost, path, key, "application/json", b)
}

func (e *env) flush() {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.app.Flush(ctx); err != nil {
		e.t.Fatal(err)
	}
}

func expectStatus(t *testing.T, r response, want int) {
	t.Helper()
	if r.Status != want {
		t.Fatalf("status %d, want %d: %s", r.Status, want, r.Body)
	}
}

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../../internal/otlp/testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func gzipped(b []byte) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write(b)
	zw.Close()
	return buf.Bytes()
}

func asList(t *testing.T, v any) []any {
	t.Helper()
	l, ok := v.([]any)
	if !ok {
		t.Fatalf("expected a list, got %T %v", v, v)
	}
	return l
}

func asMap(t *testing.T, v any) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	if !ok {
		t.Fatalf("expected an object, got %T %v", v, v)
	}
	return m
}
