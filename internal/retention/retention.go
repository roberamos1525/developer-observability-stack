// Package retention runs background maintenance for every tenant: log
// segment sealing, compaction and partition expiry; metric block cutting,
// downsampling and block expiry; and trace expiry.
package retention

import (
	"context"
	"log/slog"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/selfmon"
	"github.com/robertramos/developer-observability-stack/internal/tenant"
)

// TenantReport summarises one maintenance pass for a tenant.
type TenantReport struct {
	Tenant                 string   `json:"tenant"`
	LogSegmentsSealed      int      `json:"log_segments_sealed"`
	LogSegmentsCompacted   int      `json:"log_segments_compacted"`
	LogPartitionsRemoved   int      `json:"log_partitions_removed"`
	MetricBlocksCut        int      `json:"metric_blocks_cut"`
	MetricBlocksDownsample int      `json:"metric_blocks_downsampled"`
	MetricBlocksRemoved    int      `json:"metric_blocks_removed"`
	TracesRemoved          int      `json:"traces_removed"`
	Errors                 []string `json:"errors,omitempty"`
}

// Runner executes maintenance passes.
type Runner struct {
	reg     *tenant.Registry
	cfg     config.Config
	metrics *selfmon.Metrics
	log     *slog.Logger
	Now     func() time.Time
}

// New creates a runner.
func New(reg *tenant.Registry, cfg config.Config, m *selfmon.Metrics, log *slog.Logger) *Runner {
	return &Runner{reg: reg, cfg: cfg, metrics: m, log: log, Now: time.Now}
}

// RunOnce performs one pass over all open tenants.
func (r *Runner) RunOnce(ctx context.Context) []TenantReport {
	now := r.Now()
	var reports []TenantReport
	for _, t := range r.reg.List() {
		if ctx.Err() != nil {
			break
		}
		rep := TenantReport{Tenant: t.ID}
		record := func(job string, err error) {
			result := "success"
			if err != nil {
				result = "error"
				rep.Errors = append(rep.Errors, job+": "+err.Error())
				r.log.Error("maintenance job failed", "tenant", t.ID, "job", job, "err", err)
			}
			if r.metrics != nil {
				r.metrics.MaintenanceRuns.WithLabelValues(job, result).Inc()
			}
		}
		var err error
		rep.LogSegmentsSealed, err = t.Logs.SealIdle(now)
		record("logs_seal", err)
		rep.LogPartitionsRemoved, err = t.Logs.ApplyRetention(now.Add(-r.cfg.Logs.Retention.D()).UnixNano())
		record("logs_retention", err)
		rep.LogSegmentsCompacted, err = t.Logs.Compact(ctx)
		record("logs_compaction", err)

		rep.MetricBlocksCut, err = t.Metrics.CutBlocks(false)
		record("metrics_cut", err)
		rep.MetricBlocksDownsample, err = t.Metrics.Downsample(ctx, now)
		record("metrics_downsample", err)
		res, err := t.Metrics.ApplyRetention(now)
		rep.MetricBlocksRemoved = res.RawBlocks + res.DownsampledBlocks
		record("metrics_retention", err)
		record("metrics_wal_sync", t.Metrics.Sync())

		rep.TracesRemoved, err = t.Traces.ApplyRetention(now.Add(-r.cfg.Traces.Retention.D()).UnixNano())
		record("traces_retention", err)

		if r.metrics != nil {
			r.metrics.MaintenanceRemove.WithLabelValues("logs").Add(float64(rep.LogPartitionsRemoved))
			r.metrics.MaintenanceRemove.WithLabelValues("metrics").Add(float64(rep.MetricBlocksRemoved))
			r.metrics.MaintenanceRemove.WithLabelValues("traces").Add(float64(rep.TracesRemoved))
		}
		reports = append(reports, rep)
	}
	return reports
}

// Run executes RunOnce every interval until ctx is cancelled.
func (r *Runner) Run(ctx context.Context, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			r.RunOnce(ctx)
		}
	}
}
