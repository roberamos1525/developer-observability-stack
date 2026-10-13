// Package tenant opens and caches per-tenant storage. Every tenant gets
// isolated log, metric and trace stores under <data_dir>/tenants/<id>.
package tenant

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/errs"
	logstore "github.com/robertramos/developer-observability-stack/internal/logs/store"
	"github.com/robertramos/developer-observability-stack/internal/traces"
	"github.com/robertramos/developer-observability-stack/internal/tsdb"
)

// Tenant bundles a tenant's stores.
type Tenant struct {
	ID      string
	Logs    *logstore.Store
	Metrics *tsdb.DB
	Traces  *traces.Store
}

// Close closes all stores.
func (t *Tenant) Close() error {
	return errors.Join(t.Logs.Close(), t.Metrics.Close(), t.Traces.Close())
}

// MaxTenants bounds how many tenants can be opened (each holds files and
// memory); requests for new tenants beyond it are refused.
const MaxTenants = 256

// Registry lazily opens tenants.
type Registry struct {
	cfg     config.Config
	log     *slog.Logger
	mu      sync.Mutex
	tenants map[string]*Tenant
	closed  bool
}

// NewRegistry creates a registry rooted at cfg.DataDir.
func NewRegistry(cfg config.Config, log *slog.Logger) *Registry {
	return &Registry{cfg: cfg, log: log, tenants: map[string]*Tenant{}}
}

func (r *Registry) dir(id string) string { return filepath.Join(r.cfg.DataDir, "tenants", id) }

// Get returns the tenant, opening its stores on first use.
func (r *Registry) Get(id string) (*Tenant, error) {
	if !config.ValidTenantID(id) {
		return nil, errs.Invalid("invalid tenant id %q", id)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errs.Unavailable("server is shutting down")
	}
	if t := r.tenants[id]; t != nil {
		return t, nil
	}
	if len(r.tenants) >= MaxTenants {
		return nil, errs.Unavailable("tenant limit (%d) reached", MaxTenants)
	}
	t, err := r.open(id)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err, "open tenant %s", id)
	}
	r.tenants[id] = t
	return t, nil
}

func (r *Registry) open(id string) (*Tenant, error) {
	base := r.dir(id)
	c := r.cfg
	ls, err := logstore.Open(logstore.Options{
		Dir: filepath.Join(base, "logs"), PartitionDuration: c.Logs.PartitionDuration.D(),
		SegmentMaxBytes: c.Logs.SegmentMaxBytes, SegmentMaxEntries: c.Logs.SegmentMaxEntries,
		CompactMinSegments: c.Logs.CompactMinSegments, Logger: r.log.With("tenant", id, "store", "logs"),
	})
	if err != nil {
		return nil, fmt.Errorf("logs: %w", err)
	}
	db, err := tsdb.Open(tsdb.Options{
		Dir: filepath.Join(base, "metrics"), BlockDuration: c.Metrics.BlockDuration.D(),
		ChunkMaxSamples: c.Metrics.ChunkMaxSamples, RawRetention: c.Metrics.RawRetention.D(),
		DownsampleResolution: c.Metrics.DownsampleResolution.D(), DownsampleAfter: c.Metrics.DownsampleAfter.D(),
		DownsampledRetention: c.Metrics.DownsampledRetention.D(), Logger: r.log.With("tenant", id, "store", "metrics"),
	})
	if err != nil {
		ls.Close()
		return nil, fmt.Errorf("metrics: %w", err)
	}
	ts, err := traces.Open(traces.Options{
		Dir: filepath.Join(base, "traces"), PartitionDuration: c.Traces.PartitionDuration.D(),
		MaxSpansPerTrace: c.Traces.MaxSpansPerTrace, Logger: r.log.With("tenant", id, "store", "traces"),
	})
	if err != nil {
		ls.Close()
		db.Close()
		return nil, fmt.Errorf("traces: %w", err)
	}
	r.log.Info("tenant opened", "tenant", id)
	return &Tenant{ID: id, Logs: ls, Metrics: db, Traces: ts}, nil
}

// OpenExisting opens every tenant found on disk so background retention
// and alerting see them without waiting for traffic.
func (r *Registry) OpenExisting() error {
	entries, err := os.ReadDir(filepath.Join(r.cfg.DataDir, "tenants"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() && config.ValidTenantID(e.Name()) {
			if _, err := r.Get(e.Name()); err != nil {
				return err
			}
		}
	}
	return nil
}

// List returns open tenants sorted by ID.
func (r *Registry) List() []*Tenant {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*Tenant, 0, len(r.tenants))
	for _, t := range r.tenants {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Close closes all tenants; further Get calls fail.
func (r *Registry) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	var all []error
	for _, t := range r.tenants {
		all = append(all, t.Close())
	}
	return errors.Join(all...)
}
