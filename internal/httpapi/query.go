package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/auth"
	"github.com/robertramos/developer-observability-stack/internal/errs"
	"github.com/robertramos/developer-observability-stack/internal/logs"
	logquery "github.com/robertramos/developer-observability-stack/internal/logs/query"
	logstore "github.com/robertramos/developer-observability-stack/internal/logs/store"
	"github.com/robertramos/developer-observability-stack/internal/promql"
	"github.com/robertramos/developer-observability-stack/internal/tenant"
	"github.com/robertramos/developer-observability-stack/internal/traces"
	"github.com/robertramos/developer-observability-stack/internal/tsdb"
	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

func (s *Server) tenant(w http.ResponseWriter, r *http.Request, p auth.Principal) (*tenant.Tenant, bool) {
	t, err := s.d.Tenants.Get(p.Tenant)
	if err != nil {
		s.fail(w, r, err)
		return nil, false
	}
	return t, true
}

type logEntryView struct {
	Timestamp string            `json:"timestamp"`
	Level     string            `json:"level,omitempty"`
	Message   string            `json:"message"`
	Fields    map[string]string `json:"fields,omitempty"`
}

func viewEntry(e logs.Entry) logEntryView {
	return logEntryView{time.Unix(0, e.Timestamp).UTC().Format(time.RFC3339Nano), e.Level, e.Message, e.Fields}
}

// queryLogs runs a log query: q, start, end, limit.
func (s *Server) queryLogs(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	t, ok := s.tenant(w, r, p)
	if !ok {
		return
	}
	q, err := logquery.Parse(param(r, "q"))
	if err != nil {
		s.fail(w, r, errs.Invalid("%v", err))
		return
	}
	now := s.d.Now()
	start, err := parseTime(param(r, "start"), time.Time{})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	end, err := parseTime(param(r, "end"), time.Time{})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var startNs, endNs int64
	if !start.IsZero() {
		startNs = start.UnixNano()
	}
	if !end.IsZero() {
		endNs = end.UnixNano()
	}
	startNs, endNs = logstore.ResolveRange(q, startNs, endNs, now)
	cfg := s.d.Config.Logs
	limit, err := parseLimit(param(r, "limit"), cfg.DefaultQueryLimit, cfg.MaxQueryLimit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if q.Limit > 0 {
		if q.Limit > cfg.MaxQueryLimit {
			s.fail(w, r, errs.Invalid("limit must not exceed %d", cfg.MaxQueryLimit))
			return
		}
		limit = q.Limit
	}
	began := time.Now()
	res, err := t.Logs.Search(r.Context(), logstore.SearchRequest{Query: q, Start: startNs, End: endNs, Limit: limit})
	s.d.Metrics.QueryDuration.WithLabelValues("logs").Observe(time.Since(began).Seconds())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	out := map[string]any{"stats": res.Stats}
	if q.Count {
		out["groups"] = res.Groups
	} else {
		views := make([]logEntryView, len(res.Entries))
		for i, e := range res.Entries {
			views[i] = viewEntry(e)
		}
		out["entries"], out["truncated"] = views, res.Truncated
	}
	s.ok(w, out)
}

func (s *Server) logFields(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if t, ok := s.tenant(w, r, p); ok {
		s.ok(w, t.Logs.Fields())
	}
}

// ---- PromQL ----

type promSample struct {
	Metric map[string]string `json:"metric"`
	Value  [2]any            `json:"value"`
}

type promSeries struct {
	Metric map[string]string `json:"metric"`
	Values [][2]any          `json:"values"`
}

func point(t int64, v float64) [2]any { return [2]any{float64(t) / 1000, formatFloat(v)} }

func promResult(v promql.Value) map[string]any {
	switch x := v.(type) {
	case promql.Scalar:
		return map[string]any{"resultType": "scalar", "result": point(x.T, x.V)}
	case promql.String:
		return map[string]any{"resultType": "string", "result": [2]any{float64(x.T) / 1000, x.V}}
	case promql.Vector:
		out := make([]promSample, len(x))
		for i, s := range x {
			out[i] = promSample{s.Metric.Map(), point(s.T, s.V)}
		}
		return map[string]any{"resultType": "vector", "result": out}
	case promql.Matrix:
		out := make([]promSeries, len(x))
		for i, s := range x {
			vals := make([][2]any, len(s.Points))
			for j, p := range s.Points {
				vals[j] = point(p.T, p.V)
			}
			out[i] = promSeries{s.Metric.Map(), vals}
		}
		return map[string]any{"resultType": "matrix", "result": out}
	}
	return map[string]any{"resultType": "unknown"}
}

func (s *Server) promInstant(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	t, ok := s.tenant(w, r, p)
	if !ok {
		return
	}
	q := param(r, "query")
	if q == "" {
		s.fail(w, r, errs.Invalid("query is required"))
		return
	}
	ts, err := parseTime(param(r, "time"), s.d.Now())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	began := time.Now()
	v, err := s.d.Engine.Instant(r.Context(), t.Metrics, q, ts)
	s.d.Metrics.QueryDuration.WithLabelValues("metrics").Observe(time.Since(began).Seconds())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.ok(w, promResult(v))
}

func (s *Server) promRange(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	t, ok := s.tenant(w, r, p)
	if !ok {
		return
	}
	q := param(r, "query")
	if q == "" {
		s.fail(w, r, errs.Invalid("query is required"))
		return
	}
	now := s.d.Now()
	end, err := parseTime(param(r, "end"), now)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	start, err := parseTime(param(r, "start"), end.Add(-time.Hour))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	step, err := parseDuration(param(r, "step"), time.Minute)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	began := time.Now()
	m, err := s.d.Engine.Range(r.Context(), t.Metrics, q, start, end, step)
	s.d.Metrics.QueryDuration.WithLabelValues("metrics").Observe(time.Since(began).Seconds())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	s.ok(w, promResult(m))
}

// parseMatchers parses match[] selectors (each a PromQL vector selector).
func parseMatchers(r *http.Request) ([][]*labels.Matcher, error) {
	var sets [][]*labels.Matcher
	for _, m := range r.URL.Query()["match[]"] {
		e, err := promql.Parse(m)
		if err != nil {
			return nil, errs.Invalid("match[]: %v", err)
		}
		vs, ok := e.(*promql.VectorSelector)
		if !ok {
			return nil, errs.Invalid("match[] must be a vector selector")
		}
		sets = append(sets, vs.Matchers)
	}
	if len(sets) == 0 {
		return nil, errs.Invalid("at least one match[] selector is required")
	}
	return sets, nil
}

func (s *Server) timeRange(r *http.Request) (int64, int64, error) {
	now := s.d.Now()
	end, err := parseTime(param(r, "end"), now)
	if err != nil {
		return 0, 0, err
	}
	start, err := parseTime(param(r, "start"), end.Add(-time.Hour))
	if err != nil {
		return 0, 0, err
	}
	if end.Before(start) {
		return 0, 0, errs.Invalid("end must not be before start")
	}
	return start.UnixMilli(), end.UnixMilli(), nil
}

func (s *Server) series(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	t, ok := s.tenant(w, r, p)
	if !ok {
		return
	}
	sets, err := parseMatchers(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	mint, maxt, err := s.timeRange(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	seen := map[string]bool{}
	var out []map[string]string
	for _, ms := range sets {
		ls, err := t.Metrics.SeriesLabels(r.Context(), ms, mint, maxt)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		for _, l := range ls {
			if k := l.Key(); !seen[k] {
				seen[k] = true
				out = append(out, l.Map())
			}
		}
	}
	s.ok(w, out)
}

func (s *Server) labelNames(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if t, ok := s.tenant(w, r, p); ok {
		s.ok(w, t.Metrics.LabelNames())
	}
}

func (s *Server) labelValues(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	name := r.PathValue("name")
	if !labels.ValidLabelName(name) && name != labels.MetricName {
		s.fail(w, r, errs.Invalid("invalid label name %q", name))
		return
	}
	if t, ok := s.tenant(w, r, p); ok {
		s.ok(w, t.Metrics.LabelValues(name))
	}
}

func (s *Server) downsampled(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	t, ok := s.tenant(w, r, p)
	if !ok {
		return
	}
	sets, err := parseMatchers(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	mint, maxt, err := s.timeRange(r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	aggr := param(r, "aggr")
	if aggr == "" {
		aggr = "avg"
	}
	var out []promSeries
	for _, ms := range sets {
		series, err := t.Metrics.SelectDownsampled(r.Context(), ms, mint, maxt, aggr)
		if err != nil {
			s.fail(w, r, errs.Invalid("%v", err))
			return
		}
		for _, se := range series {
			out = append(out, toPromSeries(se))
		}
	}
	s.ok(w, map[string]any{"resultType": "matrix", "aggregate": aggr, "result": out})
}

func toPromSeries(se tsdb.Series) promSeries {
	vals := make([][2]any, len(se.Samples))
	for i, smp := range se.Samples {
		vals[i] = point(smp.T, smp.V)
	}
	return promSeries{se.Labels.Map(), vals}
}

func (s *Server) scrapeTargets(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	s.ok(w, s.d.Scraper.Targets(p.Tenant))
}

// ---- traces ----

func (s *Server) searchTraces(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	t, ok := s.tenant(w, r, p)
	if !ok {
		return
	}
	qp := r.URL.Query()
	q := traces.SearchQuery{Service: qp.Get("service"), Operation: qp.Get("operation"), ErrorsOnly: qp.Get("error") == "true",
		Attributes: map[string]string{}}
	var err error
	if v := qp.Get("minDuration"); v != "" {
		if q.MinDuration, err = parseDuration(v, 0); err != nil {
			s.fail(w, r, err)
			return
		}
	}
	if v := qp.Get("maxDuration"); v != "" {
		if q.MaxDuration, err = parseDuration(v, 0); err != nil {
			s.fail(w, r, err)
			return
		}
	}
	for _, kv := range qp["attr"] {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" {
			s.fail(w, r, errs.Invalid("attr must be key=value"))
			return
		}
		q.Attributes[k] = v
	}
	start, err := parseTime(qp.Get("start"), time.Time{})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	end, err := parseTime(qp.Get("end"), time.Time{})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if !start.IsZero() {
		q.Start = start.UnixNano()
	}
	if !end.IsZero() {
		q.End = end.UnixNano()
	}
	if q.Limit, err = parseLimit(qp.Get("limit"), 20, s.d.Config.Traces.MaxSearchResults); err != nil {
		s.fail(w, r, err)
		return
	}
	began := time.Now()
	res := t.Traces.Search(q)
	s.d.Metrics.QueryDuration.WithLabelValues("traces").Observe(time.Since(began).Seconds())
	if res == nil {
		res = []traces.Summary{}
	}
	s.ok(w, res)
}

func (s *Server) getTrace(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	id := strings.ToLower(r.PathValue("traceID"))
	if !traces.ValidID(id, 16) {
		s.fail(w, r, errs.Invalid("trace id must be 32 hex characters"))
		return
	}
	t, ok := s.tenant(w, r, p)
	if !ok {
		return
	}
	spans, found := t.Traces.Trace(id)
	if !found {
		s.fail(w, r, errs.NotFound("trace %s not found", id))
		return
	}
	s.ok(w, traces.BuildTree(id, spans))
}

func (s *Server) services(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if t, ok := s.tenant(w, r, p); ok {
		s.ok(w, t.Traces.Services())
	}
}

func (s *Server) dependencies(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	t, ok := s.tenant(w, r, p)
	if !ok {
		return
	}
	start, err := parseTime(param(r, "start"), time.Time{})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	end, err := parseTime(param(r, "end"), time.Time{})
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var sn, en int64
	if !start.IsZero() {
		sn = start.UnixNano()
	}
	if !end.IsZero() {
		en = end.UnixNano()
	}
	edges := t.Traces.Dependencies(sn, en)
	if edges == nil {
		edges = []traces.Edge{}
	}
	s.ok(w, edges)
}
