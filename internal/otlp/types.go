// Package otlp implements the OTLP/HTTP JSON encoding of the OpenTelemetry
// trace, log and metric export requests, and converts them into the
// stack's internal models.
//
// The structs follow the protobuf JSON mapping used by OTLP/JSON:
// lowerCamelCase field names, trace and span IDs as hex strings, 64-bit
// integers as decimal strings (numbers are accepted too), enums as
// integers, and attributes as AnyValue objects.
package otlp

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

// Uint64 accepts a JSON number or a decimal string.
type Uint64 uint64

func (u *Uint64) UnmarshalJSON(b []byte) error {
	s := string(bytes.Trim(b, `"`))
	if s == "" || s == "null" {
		*u = 0
		return nil
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid uint64 %s", b)
	}
	*u = Uint64(v)
	return nil
}

func (u Uint64) MarshalJSON() ([]byte, error) {
	return []byte(`"` + strconv.FormatUint(uint64(u), 10) + `"`), nil
}

// Int64 accepts a JSON number or a decimal string.
type Int64 int64

func (i *Int64) UnmarshalJSON(b []byte) error {
	s := string(bytes.Trim(b, `"`))
	if s == "" || s == "null" {
		*i = 0
		return nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid int64 %s", b)
	}
	*i = Int64(v)
	return nil
}

func (i Int64) MarshalJSON() ([]byte, error) {
	return []byte(`"` + strconv.FormatInt(int64(i), 10) + `"`), nil
}

// Float64 accepts a JSON number or the strings "NaN", "Infinity" and
// "-Infinity" allowed by the protobuf JSON mapping.
type Float64 float64

func (f *Float64) UnmarshalJSON(b []byte) error {
	switch string(b) {
	case `"NaN"`:
		*f = Float64(math.NaN())
	case `"Infinity"`:
		*f = Float64(math.Inf(1))
	case `"-Infinity"`:
		*f = Float64(math.Inf(-1))
	default:
		v, err := strconv.ParseFloat(string(bytes.Trim(b, `"`)), 64)
		if err != nil {
			return fmt.Errorf("invalid double %s", b)
		}
		*f = Float64(v)
	}
	return nil
}

// AnyValue is the OTLP attribute value union.
type AnyValue struct {
	StringValue *string       `json:"stringValue,omitempty"`
	BoolValue   *bool         `json:"boolValue,omitempty"`
	IntValue    *Int64        `json:"intValue,omitempty"`
	DoubleValue *Float64      `json:"doubleValue,omitempty"`
	ArrayValue  *ArrayValue   `json:"arrayValue,omitempty"`
	KvlistValue *KeyValueList `json:"kvlistValue,omitempty"`
	BytesValue  *string       `json:"bytesValue,omitempty"`
}

// ArrayValue is a list of AnyValue.
type ArrayValue struct {
	Values []AnyValue `json:"values"`
}

// KeyValueList is a nested attribute map.
type KeyValueList struct {
	Values []KeyValue `json:"values"`
}

// KeyValue is one attribute.
type KeyValue struct {
	Key   string   `json:"key"`
	Value AnyValue `json:"value"`
}

// maxAnyDepth bounds recursion through nested arrays/kvlists.
const maxAnyDepth = 8

// Native converts the value into plain Go types (string, bool, int64,
// float64, []any, map[string]any); bytes become base64 strings.
func (v AnyValue) Native() any { return v.native(0) }

func (v AnyValue) native(depth int) any {
	if depth > maxAnyDepth {
		return "<nested too deep>"
	}
	switch {
	case v.StringValue != nil:
		return *v.StringValue
	case v.BoolValue != nil:
		return *v.BoolValue
	case v.IntValue != nil:
		return int64(*v.IntValue)
	case v.DoubleValue != nil:
		return float64(*v.DoubleValue)
	case v.ArrayValue != nil:
		out := make([]any, len(v.ArrayValue.Values))
		for i, x := range v.ArrayValue.Values {
			out[i] = x.native(depth + 1)
		}
		return out
	case v.KvlistValue != nil:
		out := make(map[string]any, len(v.KvlistValue.Values))
		for _, kv := range v.KvlistValue.Values {
			out[kv.Key] = kv.Value.native(depth + 1)
		}
		return out
	case v.BytesValue != nil:
		if _, err := base64.StdEncoding.DecodeString(*v.BytesValue); err == nil {
			return *v.BytesValue
		}
		return ""
	}
	return nil
}

// String renders the value as text (JSON for composite values).
func (v AnyValue) String() string {
	switch x := v.Native().(type) {
	case string:
		return x
	case nil:
		return ""
	case []any, map[string]any:
		b, _ := json.Marshal(x)
		return string(b)
	default:
		return fmt.Sprint(x)
	}
}

// Resource describes the entity producing telemetry.
type Resource struct {
	Attributes []KeyValue `json:"attributes"`
}

// InstrumentationScope identifies the instrumentation library.
type InstrumentationScope struct {
	Name    string     `json:"name"`
	Version string     `json:"version,omitempty"`
	Attrs   []KeyValue `json:"attributes,omitempty"`
}

// ---- traces ----

// ExportTraceServiceRequest is the body of POST /v1/traces.
type ExportTraceServiceRequest struct {
	ResourceSpans []ResourceSpans `json:"resourceSpans"`
}

type ResourceSpans struct {
	Resource   Resource     `json:"resource"`
	ScopeSpans []ScopeSpans `json:"scopeSpans"`
	SchemaURL  string       `json:"schemaUrl,omitempty"`
}

type ScopeSpans struct {
	Scope     InstrumentationScope `json:"scope"`
	Spans     []Span               `json:"spans"`
	SchemaURL string               `json:"schemaUrl,omitempty"`
}

type Span struct {
	TraceID           string     `json:"traceId"`
	SpanID            string     `json:"spanId"`
	TraceState        string     `json:"traceState,omitempty"`
	ParentSpanID      string     `json:"parentSpanId,omitempty"`
	Flags             uint32     `json:"flags,omitempty"`
	Name              string     `json:"name"`
	Kind              int        `json:"kind"`
	StartTimeUnixNano Uint64     `json:"startTimeUnixNano"`
	EndTimeUnixNano   Uint64     `json:"endTimeUnixNano"`
	Attributes        []KeyValue `json:"attributes,omitempty"`
	Events            []Event    `json:"events,omitempty"`
	Links             []Link     `json:"links,omitempty"`
	Status            Status     `json:"status"`
}

type Event struct {
	TimeUnixNano Uint64     `json:"timeUnixNano"`
	Name         string     `json:"name"`
	Attributes   []KeyValue `json:"attributes,omitempty"`
}

type Link struct {
	TraceID    string     `json:"traceId"`
	SpanID     string     `json:"spanId"`
	Attributes []KeyValue `json:"attributes,omitempty"`
}

type Status struct {
	Message string `json:"message,omitempty"`
	Code    int    `json:"code,omitempty"`
}

// ExportTraceServiceResponse is the body returned for /v1/traces.
type ExportTraceServiceResponse struct {
	PartialSuccess *TracePartialSuccess `json:"partialSuccess,omitempty"`
}

type TracePartialSuccess struct {
	RejectedSpans Int64  `json:"rejectedSpans"`
	ErrorMessage  string `json:"errorMessage,omitempty"`
}

// ---- logs ----

// ExportLogsServiceRequest is the body of POST /v1/logs.
type ExportLogsServiceRequest struct {
	ResourceLogs []ResourceLogs `json:"resourceLogs"`
}

type ResourceLogs struct {
	Resource  Resource    `json:"resource"`
	ScopeLogs []ScopeLogs `json:"scopeLogs"`
}

type ScopeLogs struct {
	Scope      InstrumentationScope `json:"scope"`
	LogRecords []LogRecord          `json:"logRecords"`
}

type LogRecord struct {
	TimeUnixNano         Uint64     `json:"timeUnixNano"`
	ObservedTimeUnixNano Uint64     `json:"observedTimeUnixNano"`
	SeverityNumber       int        `json:"severityNumber,omitempty"`
	SeverityText         string     `json:"severityText,omitempty"`
	Body                 AnyValue   `json:"body"`
	Attributes           []KeyValue `json:"attributes,omitempty"`
	Flags                uint32     `json:"flags,omitempty"`
	TraceID              string     `json:"traceId,omitempty"`
	SpanID               string     `json:"spanId,omitempty"`
}

type ExportLogsServiceResponse struct {
	PartialSuccess *LogsPartialSuccess `json:"partialSuccess,omitempty"`
}

type LogsPartialSuccess struct {
	RejectedLogRecords Int64  `json:"rejectedLogRecords"`
	ErrorMessage       string `json:"errorMessage,omitempty"`
}

// ---- metrics ----

// ExportMetricsServiceRequest is the body of POST /v1/metrics.
type ExportMetricsServiceRequest struct {
	ResourceMetrics []ResourceMetrics `json:"resourceMetrics"`
}

type ResourceMetrics struct {
	Resource     Resource       `json:"resource"`
	ScopeMetrics []ScopeMetrics `json:"scopeMetrics"`
}

type ScopeMetrics struct {
	Scope   InstrumentationScope `json:"scope"`
	Metrics []Metric             `json:"metrics"`
}

type Metric struct {
	Name                 string          `json:"name"`
	Description          string          `json:"description,omitempty"`
	Unit                 string          `json:"unit,omitempty"`
	Gauge                *Gauge          `json:"gauge,omitempty"`
	Sum                  *Sum            `json:"sum,omitempty"`
	Histogram            *Histogram      `json:"histogram,omitempty"`
	Summary              *Summary        `json:"summary,omitempty"`
	ExponentialHistogram json.RawMessage `json:"exponentialHistogram,omitempty"`
}

type Gauge struct {
	DataPoints []NumberDataPoint `json:"dataPoints"`
}

// Aggregation temporality values.
const (
	TemporalityUnspecified = 0
	TemporalityDelta       = 1
	TemporalityCumulative  = 2
)

type Sum struct {
	DataPoints             []NumberDataPoint `json:"dataPoints"`
	AggregationTemporality int               `json:"aggregationTemporality"`
	IsMonotonic            bool              `json:"isMonotonic"`
}

type NumberDataPoint struct {
	Attributes        []KeyValue `json:"attributes,omitempty"`
	StartTimeUnixNano Uint64     `json:"startTimeUnixNano,omitempty"`
	TimeUnixNano      Uint64     `json:"timeUnixNano"`
	AsDouble          *Float64   `json:"asDouble,omitempty"`
	AsInt             *Int64     `json:"asInt,omitempty"`
	Flags             uint32     `json:"flags,omitempty"`
}

type Histogram struct {
	DataPoints             []HistogramDataPoint `json:"dataPoints"`
	AggregationTemporality int                  `json:"aggregationTemporality"`
}

type HistogramDataPoint struct {
	Attributes        []KeyValue `json:"attributes,omitempty"`
	StartTimeUnixNano Uint64     `json:"startTimeUnixNano,omitempty"`
	TimeUnixNano      Uint64     `json:"timeUnixNano"`
	Count             Uint64     `json:"count"`
	Sum               *Float64   `json:"sum,omitempty"`
	BucketCounts      []Uint64   `json:"bucketCounts"`
	ExplicitBounds    []Float64  `json:"explicitBounds"`
	Flags             uint32     `json:"flags,omitempty"`
}

type Summary struct {
	DataPoints []SummaryDataPoint `json:"dataPoints"`
}

type SummaryDataPoint struct {
	Attributes     []KeyValue      `json:"attributes,omitempty"`
	TimeUnixNano   Uint64          `json:"timeUnixNano"`
	Count          Uint64          `json:"count"`
	Sum            Float64         `json:"sum"`
	QuantileValues []QuantileValue `json:"quantileValues"`
}

type QuantileValue struct {
	Quantile Float64 `json:"quantile"`
	Value    Float64 `json:"value"`
}

type ExportMetricsServiceResponse struct {
	PartialSuccess *MetricsPartialSuccess `json:"partialSuccess,omitempty"`
}

type MetricsPartialSuccess struct {
	RejectedDataPoints Int64  `json:"rejectedDataPoints"`
	ErrorMessage       string `json:"errorMessage,omitempty"`
}
