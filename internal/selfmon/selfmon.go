// Package selfmon exposes the server's own Prometheus metrics via
// client_golang. Every subsystem reports through the *Metrics value created
// here so that /metrics describes the whole process.
package selfmon

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const namespace = "obs"

// Metrics groups all self-monitoring instruments.
type Metrics struct {
	Registry *prometheus.Registry

	HTTPRequests      *prometheus.CounterVec
	HTTPDuration      *prometheus.HistogramVec
	IngestAccepted    *prometheus.CounterVec
	IngestRejected    *prometheus.CounterVec
	QueueItems        *prometheus.CounterVec
	BatchDuration     *prometheus.HistogramVec
	BatchErrors       *prometheus.CounterVec
	QueryDuration     *prometheus.HistogramVec
	AlertEvaluations  *prometheus.CounterVec
	AlertTransitions  *prometheus.CounterVec
	Notifications     *prometheus.CounterVec
	ScrapeDuration    *prometheus.HistogramVec
	ScrapeSamples     *prometheus.CounterVec
	MaintenanceRuns   *prometheus.CounterVec
	MaintenanceRemove *prometheus.CounterVec
}

// New creates and registers all instruments, including Go runtime and
// process collectors.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "http_requests_total",
			Help: "HTTP requests by route, method and status code.",
		}, []string{"route", "method", "code"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "http_request_duration_seconds",
			Help:    "HTTP request latency by route.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2.5, 12),
		}, []string{"route"}),
		IngestAccepted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "ingest_accepted_total",
			Help: "Items accepted at the API boundary by signal and tenant.",
		}, []string{"signal", "tenant"}),
		IngestRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "ingest_rejected_total",
			Help: "Items rejected by signal and reason.",
		}, []string{"signal", "reason"}),
		QueueItems: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "queue_items_total",
			Help: "Items passing through ingestion queues by outcome.",
		}, []string{"queue", "outcome"}),
		BatchDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "queue_batch_duration_seconds",
			Help:    "Time spent persisting one batch.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 3, 12),
		}, []string{"queue"}),
		BatchErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "queue_batch_errors_total",
			Help: "Batches whose handler returned an error.",
		}, []string{"queue"}),
		QueryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "query_duration_seconds",
			Help:    "Query execution latency by signal.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 3, 10),
		}, []string{"signal"}),
		AlertEvaluations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "alert_rule_evaluations_total",
			Help: "Alert rule evaluations by result.",
		}, []string{"result"}),
		AlertTransitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "alert_state_transitions_total",
			Help: "Alert state machine transitions by target state.",
		}, []string{"to"}),
		Notifications: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "alert_notifications_total",
			Help: "Notification deliveries by channel and result.",
		}, []string{"channel", "result"}),
		ScrapeDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "scrape_duration_seconds",
			Help:    "Prometheus target scrape duration.",
			Buckets: prometheus.ExponentialBuckets(0.001, 3, 9),
		}, []string{"job"}),
		ScrapeSamples: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "scrape_samples_total",
			Help: "Samples ingested from scrape targets.",
		}, []string{"job"}),
		MaintenanceRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "maintenance_runs_total",
			Help: "Background retention/compaction job runs by job and result.",
		}, []string{"job", "result"}),
		MaintenanceRemove: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "maintenance_removed_total",
			Help: "Storage units (partitions, blocks) removed by retention.",
		}, []string{"signal"}),
	}
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.HTTPRequests, m.HTTPDuration, m.IngestAccepted, m.IngestRejected,
		m.QueueItems, m.BatchDuration, m.BatchErrors, m.QueryDuration,
		m.AlertEvaluations, m.AlertTransitions, m.Notifications,
		m.ScrapeDuration, m.ScrapeSamples, m.MaintenanceRuns, m.MaintenanceRemove,
	)
	return m
}

// RegisterGaugeFunc registers a gauge whose value is computed on scrape.
func (m *Metrics) RegisterGaugeFunc(name, help string, labels prometheus.Labels, fn func() float64) {
	m.Registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: namespace, Name: name, Help: help, ConstLabels: labels,
	}, fn))
}

// QueueObserver adapts Metrics to queue.Observer.
type QueueObserver struct{ M *Metrics }

func (o QueueObserver) Accepted(q string, n int) {
	o.M.QueueItems.WithLabelValues(q, "enqueued").Add(float64(n))
}

func (o QueueObserver) Rejected(q string, n int) {
	o.M.QueueItems.WithLabelValues(q, "rejected").Add(float64(n))
}

func (o QueueObserver) Processed(q string, n int, took time.Duration, err error) {
	o.M.BatchDuration.WithLabelValues(q).Observe(took.Seconds())
	if err != nil {
		o.M.BatchErrors.WithLabelValues(q).Inc()
		o.M.QueueItems.WithLabelValues(q, "failed").Add(float64(n))
		return
	}
	o.M.QueueItems.WithLabelValues(q, "persisted").Add(float64(n))
}
