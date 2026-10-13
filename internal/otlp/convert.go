package otlp

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/logs"
	"github.com/robertramos/developer-observability-stack/internal/traces"
	"github.com/robertramos/developer-observability-stack/internal/tsdb"
	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

// DefaultService is used when a resource has no service.name.
const DefaultService = "unknown_service"

// Rejections collects rejected item counts and the first few reasons.
type Rejections struct {
	Count   int
	Reasons []string
}

func (r *Rejections) add(n int, format string, args ...any) {
	r.Count += n
	if len(r.Reasons) < 5 {
		r.Reasons = append(r.Reasons, fmt.Sprintf(format, args...))
	}
}

// Message joins the recorded reasons.
func (r *Rejections) Message() string { return strings.Join(r.Reasons, "; ") }

func attrMap(kvs []KeyValue) map[string]any {
	if len(kvs) == 0 {
		return nil
	}
	m := make(map[string]any, len(kvs))
	for _, kv := range kvs {
		if kv.Key != "" {
			m[kv.Key] = kv.Value.Native()
		}
	}
	return m
}

func serviceName(r Resource) string {
	for _, kv := range r.Attributes {
		if kv.Key == "service.name" {
			if s := kv.Value.String(); s != "" {
				return s
			}
		}
	}
	return DefaultService
}

func normalizeID(id string) string { return strings.ToLower(strings.TrimSpace(id)) }

// ToSpans converts an export request into validated spans.
func ToSpans(req *ExportTraceServiceRequest) ([]traces.Span, Rejections) {
	var out []traces.Span
	var rej Rejections
	for _, rs := range req.ResourceSpans {
		svc := serviceName(rs.Resource)
		res := attrMap(rs.Resource.Attributes)
		for _, ss := range rs.ScopeSpans {
			for _, s := range ss.Spans {
				sp := traces.Span{
					TraceID: normalizeID(s.TraceID), SpanID: normalizeID(s.SpanID), ParentSpanID: normalizeID(s.ParentSpanID),
					TraceState: s.TraceState, Name: s.Name, Kind: traces.SpanKind(s.Kind), Service: svc,
					StartUnixNano: int64(s.StartTimeUnixNano), EndUnixNano: int64(s.EndTimeUnixNano),
					Attributes: attrMap(s.Attributes), Resource: res, Scope: ss.Scope.Name,
					Status: traces.Status{Code: s.Status.Code, Message: s.Status.Message},
				}
				for _, e := range s.Events {
					sp.Events = append(sp.Events, traces.Event{TimeUnixNano: int64(e.TimeUnixNano), Name: e.Name, Attributes: attrMap(e.Attributes)})
				}
				for _, l := range s.Links {
					sp.Links = append(sp.Links, traces.Link{TraceID: normalizeID(l.TraceID), SpanID: normalizeID(l.SpanID), Attributes: attrMap(l.Attributes)})
				}
				if uint64(s.StartTimeUnixNano) > math.MaxInt64 || uint64(s.EndTimeUnixNano) > math.MaxInt64 {
					rej.add(1, "span %q: timestamp out of range", s.Name)
					continue
				}
				if s.Status.Code < 0 || s.Status.Code > traces.StatusError {
					rej.add(1, "span %q: invalid status code %d", s.Name, s.Status.Code)
					continue
				}
				if err := sp.Validate(); err != nil {
					rej.add(1, "span %q: %v", s.Name, err)
					continue
				}
				out = append(out, sp)
			}
		}
	}
	return out, rej
}

// SeverityLevel maps an OTLP SeverityNumber (1..24) to a log level.
func SeverityLevel(n int) string {
	switch {
	case n <= 0:
		return ""
	case n <= 4:
		return "trace"
	case n <= 8:
		return "debug"
	case n <= 12:
		return "info"
	case n <= 16:
		return "warn"
	case n <= 20:
		return "error"
	default:
		return "fatal"
	}
}

// ToLogEntries converts an OTLP logs request into log entries. Resource
// attributes are stored with a "resource." prefix (service.name becomes
// the "service" field); record attributes are stored as-is.
func ToLogEntries(req *ExportLogsServiceRequest, now time.Time) ([]logs.Entry, Rejections) {
	var out []logs.Entry
	var rej Rejections
	for _, rl := range req.ResourceLogs {
		svc := serviceName(rl.Resource)
		for _, sl := range rl.ScopeLogs {
			for _, r := range sl.LogRecords {
				e := logs.Entry{Fields: map[string]string{}}
				switch {
				case r.TimeUnixNano > 0:
					e.Timestamp = int64(r.TimeUnixNano)
				case r.ObservedTimeUnixNano > 0:
					e.Timestamp = int64(r.ObservedTimeUnixNano)
				default:
					e.Timestamp = now.UnixNano()
				}
				if r.SeverityText != "" {
					e.Level = logs.NormalizeLevel(r.SeverityText)
				} else {
					e.Level = SeverityLevel(r.SeverityNumber)
				}
				e.Message = r.Body.String()
				if svc != DefaultService {
					e.Fields["service"] = svc
				}
				for _, kv := range rl.Resource.Attributes {
					if kv.Key != "service.name" {
						setField(e.Fields, "resource."+kv.Key, kv.Value.String())
					}
				}
				for _, kv := range r.Attributes {
					setField(e.Fields, kv.Key, kv.Value.String())
				}
				if sl.Scope.Name != "" {
					e.Fields["otel.scope"] = sl.Scope.Name
				}
				if r.TraceID != "" {
					e.Fields["trace_id"] = normalizeID(r.TraceID)
				}
				if r.SpanID != "" {
					e.Fields["span_id"] = normalizeID(r.SpanID)
				}
				if err := e.Validate(); err != nil {
					rej.add(1, "log record: %v", err)
					continue
				}
				out = append(out, e)
			}
		}
	}
	return out, rej
}

func setField(m map[string]string, k, v string) {
	if logs.ValidateFieldKey(k) != nil {
		k = "attr." + labels.SanitizeName(k, false)
	}
	m[k] = v
}

// ToSamples converts an OTLP metrics request into TSDB samples following
// the Prometheus compatibility conventions: dots in names and attribute
// keys become underscores, monotonic cumulative sums get a _total suffix,
// histograms expand into _bucket{le}/_sum/_count and summaries into
// {quantile}/_sum/_count series. job/instance come from service.name and
// service.instance.id. Delta temporality and exponential histograms are
// rejected (counted as rejected data points).
func ToSamples(req *ExportMetricsServiceRequest) ([]tsdb.LabeledSample, Rejections) {
	var out []tsdb.LabeledSample
	var rej Rejections
	for _, rm := range req.ResourceMetrics {
		base := map[string]string{"job": serviceName(rm.Resource)}
		for _, kv := range rm.Resource.Attributes {
			if kv.Key == "service.instance.id" {
				base["instance"] = kv.Value.String()
			}
		}
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				name := labels.SanitizeName(m.Name, true)
				if m.Name == "" || !labels.ValidMetricName(name) {
					rej.add(1, "metric with invalid name %q", m.Name)
					continue
				}
				emit := func(metric string, attrs []KeyValue, extra map[string]string, ts Uint64, v float64) {
					ls := make(map[string]string, len(base)+len(attrs)+2)
					for k, v := range base {
						ls[k] = v
					}
					for _, kv := range attrs {
						ls[labels.SanitizeName(kv.Key, false)] = kv.Value.String()
					}
					for k, v := range extra {
						ls[k] = v
					}
					ls[labels.MetricName] = metric
					out = append(out, tsdb.LabeledSample{Labels: labels.FromMap(ls), T: int64(ts) / 1e6, V: v})
				}
				switch {
				case m.Gauge != nil:
					for _, dp := range m.Gauge.DataPoints {
						emit(name, dp.Attributes, nil, dp.TimeUnixNano, numberValue(dp))
					}
				case m.Sum != nil:
					if m.Sum.AggregationTemporality == TemporalityDelta {
						rej.add(len(m.Sum.DataPoints), "metric %s: delta temporality is not supported", m.Name)
						continue
					}
					metric := name
					if m.Sum.IsMonotonic && !strings.HasSuffix(metric, "_total") {
						metric += "_total"
					}
					for _, dp := range m.Sum.DataPoints {
						emit(metric, dp.Attributes, nil, dp.TimeUnixNano, numberValue(dp))
					}
				case m.Histogram != nil:
					if m.Histogram.AggregationTemporality == TemporalityDelta {
						rej.add(len(m.Histogram.DataPoints), "metric %s: delta temporality is not supported", m.Name)
						continue
					}
					for _, dp := range m.Histogram.DataPoints {
						if len(dp.BucketCounts) != len(dp.ExplicitBounds)+1 {
							rej.add(1, "metric %s: bucketCounts must have len(explicitBounds)+1 entries", m.Name)
							continue
						}
						cum := uint64(0)
						for i, c := range dp.BucketCounts {
							cum += uint64(c)
							le := "+Inf"
							if i < len(dp.ExplicitBounds) {
								le = strconv.FormatFloat(float64(dp.ExplicitBounds[i]), 'g', -1, 64)
							}
							emit(name+"_bucket", dp.Attributes, map[string]string{"le": le}, dp.TimeUnixNano, float64(cum))
						}
						if dp.Sum != nil {
							emit(name+"_sum", dp.Attributes, nil, dp.TimeUnixNano, float64(*dp.Sum))
						}
						emit(name+"_count", dp.Attributes, nil, dp.TimeUnixNano, float64(dp.Count))
					}
				case m.Summary != nil:
					for _, dp := range m.Summary.DataPoints {
						for _, q := range dp.QuantileValues {
							emit(name, dp.Attributes, map[string]string{"quantile": strconv.FormatFloat(float64(q.Quantile), 'g', -1, 64)}, dp.TimeUnixNano, float64(q.Value))
						}
						emit(name+"_sum", dp.Attributes, nil, dp.TimeUnixNano, float64(dp.Sum))
						emit(name+"_count", dp.Attributes, nil, dp.TimeUnixNano, float64(dp.Count))
					}
				case len(m.ExponentialHistogram) > 0:
					rej.add(1, "metric %s: exponential histograms are not supported", m.Name)
				default:
					rej.add(1, "metric %s has no data", m.Name)
				}
			}
		}
	}
	return out, rej
}

func numberValue(dp NumberDataPoint) float64 {
	if dp.AsDouble != nil {
		return float64(*dp.AsDouble)
	}
	if dp.AsInt != nil {
		return float64(*dp.AsInt)
	}
	return 0
}
