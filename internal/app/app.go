// Package app wires configuration, storage, ingestion pipelines, the query
// engine, alerting, scraping, maintenance and the HTTP API into a runnable
// server.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/alerting"
	"github.com/robertramos/developer-observability-stack/internal/auth"
	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/httpapi"
	"github.com/robertramos/developer-observability-stack/internal/logs"
	logquery "github.com/robertramos/developer-observability-stack/internal/logs/query"
	logstore "github.com/robertramos/developer-observability-stack/internal/logs/store"
	"github.com/robertramos/developer-observability-stack/internal/promql"
	"github.com/robertramos/developer-observability-stack/internal/queue"
	"github.com/robertramos/developer-observability-stack/internal/retention"
	"github.com/robertramos/developer-observability-stack/internal/scrape"
	"github.com/robertramos/developer-observability-stack/internal/selfmon"
	"github.com/robertramos/developer-observability-stack/internal/tenant"
	"github.com/robertramos/developer-observability-stack/internal/traces"
	"github.com/robertramos/developer-observability-stack/internal/tsdb"
)

// App is a fully wired server.
type App struct {
	Config    config.Config
	Log       *slog.Logger
	Metrics   *selfmon.Metrics
	Tenants   *tenant.Registry
	Engine    *promql.Engine
	LogsQ     *queue.Pipeline[logs.Entry]
	MetricsQ  *queue.Pipeline[tsdb.LabeledSample]
	SpansQ    *queue.Pipeline[traces.Span]
	Alerts    *alerting.Manager
	Scraper   *scrape.Manager
	Retention *retention.Runner
	Handler   http.Handler

	ready  atomic.Bool
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New builds the application. Background loops start with Start.
func New(cfg config.Config, log *slog.Logger) (*App, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("data dir: %w", err)
	}
	a := &App{Config: cfg, Log: log, Metrics: selfmon.New()}
	a.Tenants = tenant.NewRegistry(cfg, log)
	if err := a.Tenants.OpenExisting(); err != nil {
		return nil, fmt.Errorf("open tenants: %w", err)
	}
	a.Engine = &promql.Engine{
		LookbackDelta: cfg.Metrics.LookbackDelta.D(), MaxSamples: cfg.Metrics.MaxSamplesPerQuery,
		Timeout: cfg.Metrics.QueryTimeout.D(), MaxSteps: cfg.Metrics.MaxRangeSteps,
	}
	obs := selfmon.QueueObserver{M: a.Metrics}
	qopts := func(name string) queue.Options {
		in := cfg.Ingest
		return queue.Options{Name: name, Workers: in.Workers, Capacity: in.QueueCapacity, BatchSize: in.BatchSize,
			FlushInterval: in.FlushInterval.D(), SubmitTimeout: in.SubmitTimeout.D(), Observer: obs}
	}
	a.LogsQ = queue.New(qopts("logs"), func(_ context.Context, tenantID string, items []logs.Entry) error {
		t, err := a.Tenants.Get(tenantID)
		if err != nil {
			return err
		}
		return t.Logs.Append(items)
	})
	a.MetricsQ = queue.New(qopts("metrics"), func(_ context.Context, tenantID string, items []tsdb.LabeledSample) error {
		t, err := a.Tenants.Get(tenantID)
		if err != nil {
			return err
		}
		st, err := t.Metrics.Append(items)
		if n := st.Rejected(); n > 0 {
			a.Metrics.IngestRejected.WithLabelValues("metrics", "storage").Add(float64(n))
			log.Debug("samples rejected by storage", "tenant", tenantID, "count", n, "first", st.FirstError)
		}
		return err
	})
	a.SpansQ = queue.New(qopts("spans"), func(_ context.Context, tenantID string, items []traces.Span) error {
		t, err := a.Tenants.Get(tenantID)
		if err != nil {
			return err
		}
		res, err := t.Traces.Append(items)
		if n := res.Duplicates + res.Dropped + res.Invalid; n > 0 {
			a.Metrics.IngestRejected.WithLabelValues("spans", "storage").Add(float64(n))
		}
		return err
	})
	for name, q := range map[string]interface{ Depth() int }{"logs": a.LogsQ, "metrics": a.MetricsQ, "spans": a.SpansQ} {
		q := q
		a.Metrics.RegisterGaugeFunc("queue_depth", "Jobs waiting in an ingestion queue.", map[string]string{"queue": name},
			func() float64 { return float64(q.Depth()) })
	}
	a.Metrics.RegisterGaugeFunc("tenants_open", "Open tenants.", nil, func() float64 { return float64(len(a.Tenants.List())) })

	channels, err := alerting.BuildChannels(cfg.Alerting.Channels, os.LookupEnv, log)
	if err != nil {
		return nil, err
	}
	a.Alerts, err = alerting.NewManager(alerting.Options{
		StatePath:         filepath.Join(cfg.DataDir, "alerting", "state.json"),
		ResolvedRetention: cfg.Alerting.ResolvedRetention.D(),
		Dispatch: alerting.DispatchOptions{GroupWait: cfg.Alerting.GroupWait.D(),
			GroupInterval: cfg.Alerting.GroupInterval.D(), RepeatInterval: cfg.Alerting.RepeatInterval.D()},
		Channels: channels, Querier: &querier{a}, Metrics: a.Metrics, Logger: log.With("component", "alerting"),
	})
	if err != nil {
		return nil, err
	}
	a.Scraper, err = scrape.NewManager(cfg.Metrics.Scrape, func(ctx context.Context, tenantID string, s []tsdb.LabeledSample) error {
		return a.MetricsQ.Submit(ctx, tenantID, s, false)
	}, a.Metrics, log.With("component", "scrape"))
	if err != nil {
		return nil, err
	}
	a.Retention = retention.New(a.Tenants, cfg, a.Metrics, log.With("component", "retention"))
	authn, err := auth.New(cfg.Auth)
	if err != nil {
		return nil, err
	}
	srv := httpapi.New(httpapi.Deps{
		Config: cfg, Log: log, Metrics: a.Metrics, Auth: authn, Tenants: a.Tenants, Engine: a.Engine,
		LogsQ: a.LogsQ, MetricsQ: a.MetricsQ, SpansQ: a.SpansQ, Alerts: a.Alerts, Scraper: a.Scraper,
		Maintenance: func(ctx context.Context) any { return a.Retention.RunOnce(ctx) },
		Flush:       a.Flush,
		Ready:       a.ready.Load,
	})
	a.Handler = srv.Handler()
	a.ready.Store(true)
	return a, nil
}

// Start launches alert evaluation, scraping and maintenance loops.
func (a *App) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	a.wg.Add(3)
	go func() { defer a.wg.Done(); a.Alerts.Run(ctx, a.Config.Alerting.EvaluationInterval.D()) }()
	go func() { defer a.wg.Done(); a.Scraper.Run(ctx) }()
	go func() { defer a.wg.Done(); a.Retention.Run(ctx, a.Config.Retention.Interval.D()) }()
}

// Flush waits until all queued ingestion has been persisted.
func (a *App) Flush(ctx context.Context) error {
	return errors.Join(a.LogsQ.Flush(ctx), a.MetricsQ.Flush(ctx), a.SpansQ.Flush(ctx))
}

// Shutdown stops background work, drains ingestion queues and closes
// storage. The HTTP server must already be shut down.
func (a *App) Shutdown(ctx context.Context) error {
	a.ready.Store(false)
	if a.cancel != nil {
		a.cancel()
	}
	a.wg.Wait()
	errs := []error{a.LogsQ.Close(ctx), a.MetricsQ.Close(ctx), a.SpansQ.Close(ctx)}
	errs = append(errs, a.Tenants.Close())
	return errors.Join(errs...)
}

// Serve runs the HTTP server on ln until ctx is cancelled, then shuts
// everything down gracefully.
func (a *App) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{Handler: a.Handler, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 60 * time.Second,
		WriteTimeout: 2 * time.Minute, IdleTimeout: 2 * time.Minute, MaxHeaderBytes: 64 << 10}
	a.Start()
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		a.Shutdown(context.Background())
		return err
	case <-ctx.Done():
	}
	a.Log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), a.Config.ShutdownTimeout.D())
	defer cancel()
	a.ready.Store(false)
	herr := srv.Shutdown(sctx)
	return errors.Join(herr, a.Shutdown(sctx))
}

// querier adapts tenant storage to alerting.Querier.
type querier struct{ a *App }

func (q *querier) QueryMetric(ctx context.Context, tenantID, expr string, ts time.Time) ([]alerting.Result, error) {
	t, err := q.a.Tenants.Get(tenantID)
	if err != nil {
		return nil, err
	}
	v, err := q.a.Engine.Instant(ctx, t.Metrics, expr, ts)
	if err != nil {
		return nil, err
	}
	vec, ok := v.(promql.Vector)
	if !ok {
		return nil, fmt.Errorf("rule expression returned %s, want vector", v.Type())
	}
	out := make([]alerting.Result, len(vec))
	for i, s := range vec {
		out[i] = alerting.Result{Labels: s.Metric.Map(), Value: s.V}
	}
	return out, nil
}

func (q *querier) CountLogs(ctx context.Context, tenantID, query string, start, end time.Time) ([]alerting.Result, error) {
	t, err := q.a.Tenants.Get(tenantID)
	if err != nil {
		return nil, err
	}
	pq, err := logquery.Parse(query)
	if err != nil {
		return nil, err
	}
	pq.Count = true
	res, err := t.Logs.Search(ctx, logstore.SearchRequest{Query: pq, Start: start.UnixNano(), End: end.UnixNano()})
	if err != nil {
		return nil, err
	}
	out := make([]alerting.Result, len(res.Groups))
	for i, g := range res.Groups {
		out[i] = alerting.Result{Labels: g.Labels, Value: float64(g.Count)}
	}
	return out, nil
}
