package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/robertramos/developer-observability-stack/internal/auth"
	"github.com/robertramos/developer-observability-stack/internal/errs"
	"github.com/robertramos/developer-observability-stack/internal/logs"
	"github.com/robertramos/developer-observability-stack/internal/otlp"
	"github.com/robertramos/developer-observability-stack/internal/promtext"
	"github.com/robertramos/developer-observability-stack/internal/queue"
	"github.com/robertramos/developer-observability-stack/internal/scrape"
	"github.com/robertramos/developer-observability-stack/internal/tsdb"
	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

// IngestResult is returned by ingestion endpoints.
type IngestResult struct {
	Accepted int              `json:"accepted"`
	Rejected int              `json:"rejected"`
	Errors   []logs.LineError `json:"errors,omitempty"`
}

// submit enqueues items, recording self-metrics.
func submit[T any](s *Server, r *http.Request, q *queue.Pipeline[T], signal, tenant string, items []T, rejected int) error {
	if rejected > 0 {
		s.d.Metrics.IngestRejected.WithLabelValues(signal, "validation").Add(float64(rejected))
	}
	if len(items) == 0 {
		return nil
	}
	ctx := r.Context()
	if syncParam(r) {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.d.Config.ShutdownTimeout.D())
		defer cancel()
	}
	if err := q.Submit(ctx, tenant, items, syncParam(r)); err != nil {
		s.d.Metrics.IngestRejected.WithLabelValues(signal, "backpressure").Add(float64(len(items)))
		return err
	}
	s.d.Metrics.IngestAccepted.WithLabelValues(signal, tenant).Add(float64(len(items)))
	return nil
}

func (s *Server) ingestLogs(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	switch mt := mediaType(r); mt {
	case "application/json", "application/x-ndjson", "application/jsonl", "":
	default:
		s.fail(w, r, errs.New(errs.KindUnsupported, "use application/json or application/x-ndjson, got %q", mt))
		return
	}
	body, err := s.readBody(w, r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	res, rejected := logs.ParseJSON(body, s.d.Now())
	if err := submit(s, r, s.d.LogsQ, "logs", p.Tenant, res.Entries, rejected); err != nil {
		s.fail(w, r, err)
		return
	}
	s.accepted(w, syncParam(r), IngestResult{len(res.Entries), rejected, res.Errors})
}

func (s *Server) ingestSyslog(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	body, err := s.readBody(w, r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	res, rejected := logs.ParseSyslog(bytes.NewReader(body), s.d.Now())
	if err := submit(s, r, s.d.LogsQ, "logs", p.Tenant, res.Entries, rejected); err != nil {
		s.fail(w, r, err)
		return
	}
	s.accepted(w, syncParam(r), IngestResult{len(res.Entries), rejected, res.Errors})
}

// jsonValue accepts numbers and the strings "NaN", "+Inf", "-Inf".
type jsonValue float64

func (v *jsonValue) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	switch s {
	case "NaN":
		*v = jsonValue(math.NaN())
		return nil
	case "+Inf", "Inf":
		*v = jsonValue(math.Inf(1))
		return nil
	case "-Inf":
		*v = jsonValue(math.Inf(-1))
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return fmt.Errorf("invalid sample value %s", b)
	}
	*v = jsonValue(f)
	return nil
}

type jsonSample struct {
	Name      string            `json:"name"`
	Labels    map[string]string `json:"labels"`
	Value     *jsonValue        `json:"value"`
	Timestamp *int64            `json:"timestamp"` // Unix ms
}

// MaxSamplesPerRequest bounds a single metrics write.
const MaxSamplesPerRequest = 100_000

func (s *Server) ingestMetricsJSON(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	if mt := mediaType(r); mt != "application/json" && mt != "" {
		s.fail(w, r, errs.New(errs.KindUnsupported, "use application/json"))
		return
	}
	body, err := s.readBody(w, r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var req struct {
		Samples []jsonSample `json:"samples"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		s.fail(w, r, errs.Invalid("invalid JSON: %v", err))
		return
	}
	if len(req.Samples) > MaxSamplesPerRequest {
		s.fail(w, r, errs.New(errs.KindTooLarge, "at most %d samples per request", MaxSamplesPerRequest))
		return
	}
	now := s.d.Now().UnixMilli()
	res := IngestResult{}
	var out []tsdb.LabeledSample
	for i, js := range req.Samples {
		m := map[string]string{}
		for k, v := range js.Labels {
			m[k] = v
		}
		if js.Name != "" {
			m[labels.MetricName] = js.Name
		}
		ls := labels.FromMap(m)
		var verr error
		switch {
		case js.Value == nil:
			verr = fmt.Errorf("value is required")
		default:
			verr = ls.Validate()
		}
		if verr != nil {
			res.Rejected++
			if len(res.Errors) < logs.MaxReportedErrors {
				res.Errors = append(res.Errors, logs.LineError{Line: i + 1, Error: verr.Error()})
			}
			continue
		}
		ts := now
		if js.Timestamp != nil {
			ts = *js.Timestamp
		}
		out = append(out, tsdb.LabeledSample{Labels: ls, T: ts, V: float64(*js.Value)})
	}
	res.Accepted = len(out)
	if err := submit(s, r, s.d.MetricsQ, "metrics", p.Tenant, out, res.Rejected); err != nil {
		s.fail(w, r, err)
		return
	}
	s.accepted(w, syncParam(r), res)
}

func (s *Server) ingestPromText(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	body, err := s.readBody(w, r)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	fams, err := promtext.Parse(bytes.NewReader(body))
	if err != nil {
		s.fail(w, r, errs.Invalid("invalid exposition: %v", err))
		return
	}
	extra := map[string]string{}
	for _, kv := range r.URL.Query()["label"] {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || !labels.ValidLabelName(k) {
			s.fail(w, r, errs.Invalid("label parameters must be name=value"))
			return
		}
		extra[k] = v
	}
	samples := scrape.Convert(fams, s.d.Now(), func(ls labels.Labels) labels.Labels {
		for k, v := range extra {
			ls = ls.With(k, v)
		}
		return ls
	})
	if len(samples) > MaxSamplesPerRequest {
		s.fail(w, r, errs.New(errs.KindTooLarge, "at most %d samples per request", MaxSamplesPerRequest))
		return
	}
	if err := submit(s, r, s.d.MetricsQ, "metrics", p.Tenant, samples, 0); err != nil {
		s.fail(w, r, err)
		return
	}
	s.accepted(w, syncParam(r), IngestResult{Accepted: len(samples)})
}

// ---- OTLP/HTTP (JSON encoding) ----

// otlpFail writes a google.rpc.Status-shaped error as OTLP/HTTP requires.
func (s *Server) otlpFail(w http.ResponseWriter, err error) {
	kind := errs.KindOf(err)
	code := map[errs.Kind]int{errs.KindInvalid: 3, errs.KindUnauthorized: 16, errs.KindForbidden: 7,
		errs.KindTooLarge: 8, errs.KindUnavailable: 14, errs.KindUnsupported: 3, errs.KindTimeout: 4}[kind]
	msg := err.Error()
	if kind == errs.KindInternal {
		code, msg = 13, "internal error"
	}
	if kind == errs.KindUnavailable {
		w.Header().Set("Retry-After", "1")
	}
	writeJSON(w, httpStatus(kind), map[string]any{"code": code, "message": msg})
}

func (s *Server) otlpBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	if mt := mediaType(r); mt != "application/json" {
		s.otlpFail(w, errs.New(errs.KindUnsupported, "only OTLP/HTTP JSON (application/json) is supported, got %q", mt))
		return false
	}
	body, err := s.readBody(w, r)
	if err != nil {
		s.otlpFail(w, err)
		return false
	}
	if err := json.Unmarshal(body, dst); err != nil {
		s.otlpFail(w, errs.Invalid("invalid OTLP JSON: %v", err))
		return false
	}
	return true
}

func (s *Server) otlpTraces(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	var req otlp.ExportTraceServiceRequest
	if !s.otlpBody(w, r, &req) {
		return
	}
	spans, rej := otlp.ToSpans(&req)
	if err := submit(s, r, s.d.SpansQ, "spans", p.Tenant, spans, rej.Count); err != nil {
		s.otlpFail(w, err)
		return
	}
	resp := otlp.ExportTraceServiceResponse{}
	if rej.Count > 0 {
		resp.PartialSuccess = &otlp.TracePartialSuccess{RejectedSpans: otlp.Int64(rej.Count), ErrorMessage: rej.Message()}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) otlpLogs(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	var req otlp.ExportLogsServiceRequest
	if !s.otlpBody(w, r, &req) {
		return
	}
	entries, rej := otlp.ToLogEntries(&req, s.d.Now())
	if err := submit(s, r, s.d.LogsQ, "logs", p.Tenant, entries, rej.Count); err != nil {
		s.otlpFail(w, err)
		return
	}
	resp := otlp.ExportLogsServiceResponse{}
	if rej.Count > 0 {
		resp.PartialSuccess = &otlp.LogsPartialSuccess{RejectedLogRecords: otlp.Int64(rej.Count), ErrorMessage: rej.Message()}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) otlpMetrics(w http.ResponseWriter, r *http.Request, p auth.Principal) {
	var req otlp.ExportMetricsServiceRequest
	if !s.otlpBody(w, r, &req) {
		return
	}
	samples, rej := otlp.ToSamples(&req)
	if err := submit(s, r, s.d.MetricsQ, "metrics", p.Tenant, samples, rej.Count); err != nil {
		s.otlpFail(w, err)
		return
	}
	resp := otlp.ExportMetricsServiceResponse{}
	if rej.Count > 0 {
		resp.PartialSuccess = &otlp.MetricsPartialSuccess{RejectedDataPoints: otlp.Int64(rej.Count), ErrorMessage: rej.Message()}
	}
	writeJSON(w, http.StatusOK, resp)
}
