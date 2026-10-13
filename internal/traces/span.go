// Package traces stores spans following the OpenTelemetry data model and
// provides trace lookup, search, span-tree assembly and a service
// dependency graph.
package traces

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// SpanKind mirrors OTLP Span.SpanKind.
type SpanKind int

const (
	KindUnspecified SpanKind = iota
	KindInternal
	KindServer
	KindClient
	KindProducer
	KindConsumer
)

func (k SpanKind) String() string {
	switch k {
	case KindInternal:
		return "internal"
	case KindServer:
		return "server"
	case KindClient:
		return "client"
	case KindProducer:
		return "producer"
	case KindConsumer:
		return "consumer"
	}
	return "unspecified"
}

// Status codes mirror OTLP Status.StatusCode.
const (
	StatusUnset = 0
	StatusOK    = 1
	StatusError = 2
)

// Status is the span status.
type Status struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}

// Event is a timestamped span event.
type Event struct {
	TimeUnixNano int64          `json:"timeUnixNano"`
	Name         string         `json:"name"`
	Attributes   map[string]any `json:"attributes,omitempty"`
}

// Link references another span.
type Link struct {
	TraceID    string         `json:"traceId"`
	SpanID     string         `json:"spanId"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// Span is a stored span. IDs are lowercase hex as in OTLP/JSON.
type Span struct {
	TraceID       string         `json:"traceId"`
	SpanID        string         `json:"spanId"`
	ParentSpanID  string         `json:"parentSpanId,omitempty"`
	TraceState    string         `json:"traceState,omitempty"`
	Name          string         `json:"name"`
	Kind          SpanKind       `json:"kind"`
	Service       string         `json:"service"`
	StartUnixNano int64          `json:"startTimeUnixNano"`
	EndUnixNano   int64          `json:"endTimeUnixNano"`
	Attributes    map[string]any `json:"attributes,omitempty"`
	Resource      map[string]any `json:"resource,omitempty"`
	Scope         string         `json:"scope,omitempty"`
	Status        Status         `json:"status"`
	Events        []Event        `json:"events,omitempty"`
	Links         []Link         `json:"links,omitempty"`
}

// DurationNano returns the span duration.
func (s *Span) DurationNano() int64 { return s.EndUnixNano - s.StartUnixNano }

// IsError reports an error status.
func (s *Span) IsError() bool { return s.Status.Code == StatusError }

// AttrString renders an attribute (span first, then resource) as a string.
func (s *Span) AttrString(key string) (string, bool) {
	v, ok := s.Attributes[key]
	if !ok {
		v, ok = s.Resource[key]
	}
	if !ok {
		return "", false
	}
	return fmt.Sprint(v), true
}

// ValidID checks a lowercase hex identifier of n bytes that is not all zero.
func ValidID(id string, n int) bool {
	if len(id) != 2*n || strings.ToLower(id) != id {
		return false
	}
	b, err := hex.DecodeString(id)
	if err != nil {
		return false
	}
	for _, x := range b {
		if x != 0 {
			return true
		}
	}
	return false
}

// Validate checks structural invariants required for storage.
func (s *Span) Validate() error {
	if !ValidID(s.TraceID, 16) {
		return fmt.Errorf("traceId must be 32 lowercase hex characters and non-zero")
	}
	if !ValidID(s.SpanID, 8) {
		return fmt.Errorf("spanId must be 16 lowercase hex characters and non-zero")
	}
	if s.ParentSpanID != "" && !ValidID(s.ParentSpanID, 8) {
		return fmt.Errorf("parentSpanId must be empty or 16 lowercase hex characters")
	}
	if s.ParentSpanID == s.SpanID {
		return fmt.Errorf("span cannot be its own parent")
	}
	if s.Name == "" || len(s.Name) > 1024 {
		return fmt.Errorf("span name must be 1..1024 bytes")
	}
	if s.StartUnixNano <= 0 || s.EndUnixNano < s.StartUnixNano {
		return fmt.Errorf("span times must be positive with end >= start")
	}
	if s.Kind < KindUnspecified || s.Kind > KindConsumer {
		return fmt.Errorf("invalid span kind %d", s.Kind)
	}
	if len(s.Attributes) > 256 || len(s.Events) > 1024 || len(s.Links) > 128 {
		return fmt.Errorf("span exceeds attribute/event/link limits")
	}
	return nil
}
