// Package httpapi exposes the REST API: ingestion (JSON/NDJSON/syslog
// logs, JSON and Prometheus text metrics, OTLP/HTTP JSON), queries (log
// query language, PromQL, trace search), alerting management, health and
// self-metrics.
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/robertramos/developer-observability-stack/internal/alerting"
	"github.com/robertramos/developer-observability-stack/internal/auth"
	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/errs"
	"github.com/robertramos/developer-observability-stack/internal/logs"
	"github.com/robertramos/developer-observability-stack/internal/promql"
	"github.com/robertramos/developer-observability-stack/internal/queue"
	"github.com/robertramos/developer-observability-stack/internal/scrape"
	"github.com/robertramos/developer-observability-stack/internal/selfmon"
	"github.com/robertramos/developer-observability-stack/internal/tenant"
	"github.com/robertramos/developer-observability-stack/internal/traces"
	"github.com/robertramos/developer-observability-stack/internal/tsdb"
)

// Deps are the server's collaborators.
type Deps struct {
	Config      config.Config
	Log         *slog.Logger
	Metrics     *selfmon.Metrics
	Auth        *auth.Authenticator
	Tenants     *tenant.Registry
	Engine      *promql.Engine
	LogsQ       *queue.Pipeline[logs.Entry]
	MetricsQ    *queue.Pipeline[tsdb.LabeledSample]
	SpansQ      *queue.Pipeline[traces.Span]
	Alerts      *alerting.Manager
	Scraper     *scrape.Manager
	Maintenance func(ctx context.Context) any
	Flush       func(ctx context.Context) error
	Ready       func() bool
	Now         func() time.Time
}

// Server is the HTTP API.
type Server struct {
	d   Deps
	mux *http.ServeMux
}

// New builds the server and its routes.
func New(d Deps) *Server {
	if d.Now == nil {
		d.Now = time.Now
	}
	s := &Server{d: d, mux: http.NewServeMux()}
	pub := func(pattern string, h http.HandlerFunc) { s.mux.Handle(pattern, h) }
	scoped := func(scope auth.Scope, pattern string, h func(http.ResponseWriter, *http.Request, auth.Principal)) {
		s.mux.Handle(pattern, s.authorize(scope, h))
	}
	pub("GET /healthz", s.healthz)
	pub("GET /readyz", s.readyz)
	s.mux.Handle("GET /metrics", promhttp.HandlerFor(d.Metrics.Registry, promhttp.HandlerOpts{}))

	// Ingestion.
	scoped(auth.ScopeWrite, "POST /api/v1/logs", s.ingestLogs)
	scoped(auth.ScopeWrite, "POST /api/v1/logs/syslog", s.ingestSyslog)
	scoped(auth.ScopeWrite, "POST /api/v1/metrics", s.ingestMetricsJSON)
	scoped(auth.ScopeWrite, "POST /api/v1/metrics/prometheus", s.ingestPromText)
	scoped(auth.ScopeWrite, "POST /v1/traces", s.otlpTraces)
	scoped(auth.ScopeWrite, "POST /v1/logs", s.otlpLogs)
	scoped(auth.ScopeWrite, "POST /v1/metrics", s.otlpMetrics)

	// Logs queries.
	scoped(auth.ScopeRead, "GET /api/v1/logs/query", s.queryLogs)
	scoped(auth.ScopeRead, "POST /api/v1/logs/query", s.queryLogs)
	scoped(auth.ScopeRead, "GET /api/v1/logs/fields", s.logFields)

	// Metrics queries (Prometheus-compatible response shapes).
	scoped(auth.ScopeRead, "GET /api/v1/query", s.promInstant)
	scoped(auth.ScopeRead, "POST /api/v1/query", s.promInstant)
	scoped(auth.ScopeRead, "GET /api/v1/query_range", s.promRange)
	scoped(auth.ScopeRead, "POST /api/v1/query_range", s.promRange)
	scoped(auth.ScopeRead, "GET /api/v1/series", s.series)
	scoped(auth.ScopeRead, "GET /api/v1/labels", s.labelNames)
	scoped(auth.ScopeRead, "GET /api/v1/label/{name}/values", s.labelValues)
	scoped(auth.ScopeRead, "GET /api/v1/metrics/downsampled", s.downsampled)
	scoped(auth.ScopeRead, "GET /api/v1/scrape/targets", s.scrapeTargets)

	// Traces.
	scoped(auth.ScopeRead, "GET /api/v1/traces", s.searchTraces)
	scoped(auth.ScopeRead, "GET /api/v1/traces/{traceID}", s.getTrace)
	scoped(auth.ScopeRead, "GET /api/v1/services", s.services)
	scoped(auth.ScopeRead, "GET /api/v1/dependencies", s.dependencies)

	// Alerting.
	scoped(auth.ScopeRead, "GET /api/v1/alerts", s.listAlerts)
	scoped(auth.ScopeRead, "GET /api/v1/alerts/rules", s.listRules)
	scoped(auth.ScopeWrite, "POST /api/v1/alerts/rules", s.putRule)
	scoped(auth.ScopeWrite, "DELETE /api/v1/alerts/rules/{name}", s.deleteRule)
	scoped(auth.ScopeRead, "GET /api/v1/silences", s.listSilences)
	scoped(auth.ScopeWrite, "POST /api/v1/silences", s.createSilence)
	scoped(auth.ScopeWrite, "DELETE /api/v1/silences/{id}", s.deleteSilence)

	// Administration.
	scoped(auth.ScopeRead, "GET /api/v1/whoami", s.whoami)
	scoped(auth.ScopeAdmin, "GET /api/v1/admin/stats", s.adminStats)
	scoped(auth.ScopeAdmin, "POST /api/v1/admin/maintenance", s.adminMaintenance)
	scoped(auth.ScopeAdmin, "POST /api/v1/admin/flush", s.adminFlush)
	return s
}

// Handler returns the root handler with middleware applied.
func (s *Server) Handler() http.Handler {
	return s.recoverer(s.instrument(s.mux))
}

func (s *Server) authorize(scope auth.Scope, h func(http.ResponseWriter, *http.Request, auth.Principal)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := s.d.Auth.Authenticate(r)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		if !p.Has(scope) {
			s.fail(w, r, errs.New(errs.KindForbidden, "API key lacks the %q scope", scope))
			return
		}
		h(w, r.WithContext(auth.WithPrincipal(r.Context(), p)), p)
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

type ctxKey struct{}

func requestID(ctx context.Context) string {
	id, _ := ctx.Value(ctxKey{}).(string)
	return id
}

// instrument assigns request IDs, records metrics and logs each request.
func (s *Server) instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > 64 {
			var b [8]byte
			rand.Read(b[:])
			id = hex.EncodeToString(b[:])
		}
		w.Header().Set("X-Request-ID", id)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		rec := &statusRecorder{ResponseWriter: w}
		r = r.WithContext(context.WithValue(r.Context(), ctxKey{}, id))
		next.ServeHTTP(rec, r)
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}
		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		took := time.Since(start)
		s.d.Metrics.HTTPRequests.WithLabelValues(route, r.Method, strconv.Itoa(rec.status)).Inc()
		s.d.Metrics.HTTPDuration.WithLabelValues(route).Observe(took.Seconds())
		level := slog.LevelDebug
		if rec.status >= 500 {
			level = slog.LevelError
		} else if rec.status >= 400 {
			level = slog.LevelInfo
		}
		s.d.Log.Log(r.Context(), level, "http request", "id", id, "method", r.Method, "route", route,
			"status", rec.status, "bytes", rec.bytes, "duration_ms", float64(took.Microseconds())/1000)
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				s.d.Log.Error("panic in handler", "panic", v, "stack", string(debug.Stack()))
				s.fail(w, r, errs.New(errs.KindInternal, "internal error"))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.d.Ready != nil && !s.d.Ready() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) whoami(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	s.ok(w, map[string]any{"tenant": p.Tenant, "key_id": p.KeyID, "scopes": p.Scopes()})
}

func (s *Server) adminStats(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	type tenantStats struct {
		Tenant  string `json:"tenant"`
		Logs    any    `json:"logs"`
		Metrics any    `json:"metrics"`
		Traces  any    `json:"traces"`
	}
	var out []tenantStats
	for _, t := range s.d.Tenants.List() {
		out = append(out, tenantStats{t.ID, t.Logs.Stats(), t.Metrics.Stats(), t.Traces.Stats()})
	}
	queues := map[string]any{}
	for name, q := range map[string]interface {
		Depth() int
		Capacity() int
		Pending() int64
	}{"logs": s.d.LogsQ, "metrics": s.d.MetricsQ, "spans": s.d.SpansQ} {
		queues[name] = map[string]any{"depth": q.Depth(), "capacity": q.Capacity(), "pending": q.Pending()}
	}
	s.ok(w, map[string]any{"tenants": out, "queues": queues})
}

func (s *Server) adminMaintenance(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	if err := s.d.Flush(r.Context()); err != nil {
		s.fail(w, r, errs.Wrap(errs.KindTimeout, err, "flush"))
		return
	}
	s.ok(w, s.d.Maintenance(r.Context()))
}

func (s *Server) adminFlush(w http.ResponseWriter, r *http.Request, _ auth.Principal) {
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	if err := s.d.Flush(ctx); err != nil {
		s.fail(w, r, errs.Wrap(errs.KindTimeout, err, "flush"))
		return
	}
	s.ok(w, map[string]string{"flushed": "ok"})
}
