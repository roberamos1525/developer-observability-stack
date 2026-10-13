package logs

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// LineError reports a rejected record within a batch.
type LineError struct {
	Line  int    `json:"line"`
	Error string `json:"error"`
}

// ParseResult is the outcome of parsing a batch.
type ParseResult struct {
	Entries []Entry
	Errors  []LineError
}

// MaxReportedErrors caps the per-batch error list returned to clients.
const MaxReportedErrors = 20

func (r *ParseResult) fail(line int, err error) {
	if len(r.Errors) < MaxReportedErrors {
		r.Errors = append(r.Errors, LineError{Line: line, Error: err.Error()})
	}
}

// ParseJSON accepts a JSON array of objects, a single object, or NDJSON.
func ParseJSON(data []byte, now time.Time) (ParseResult, int) {
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) > 0 && trimmed[0] == '[' {
		return parseArray(trimmed, now)
	}
	return ParseNDJSON(bytes.NewReader(data), now)
}

func parseArray(data []byte, now time.Time) (ParseResult, int) {
	var res ParseResult
	var raws []json.RawMessage
	if err := json.Unmarshal(data, &raws); err != nil {
		res.fail(1, fmt.Errorf("invalid JSON array: %v", err))
		return res, 1
	}
	rejected := 0
	for i, raw := range raws {
		e, err := ParseObject(raw, now)
		if err != nil {
			rejected++
			res.fail(i+1, err)
			continue
		}
		res.Entries = append(res.Entries, e)
	}
	return res, rejected
}

// ParseNDJSON parses newline-delimited JSON objects. Blank lines are
// skipped; malformed lines are reported and do not abort the batch.
func ParseNDJSON(r io.Reader, now time.Time) (ParseResult, int) {
	var res ParseResult
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), MaxMessageBytes*4)
	line, rejected := 0, 0
	for sc.Scan() {
		line++
		b := bytes.TrimSpace(sc.Bytes())
		if len(b) == 0 {
			continue
		}
		e, err := ParseObject(b, now)
		if err != nil {
			rejected++
			res.fail(line, err)
			continue
		}
		res.Entries = append(res.Entries, e)
	}
	if err := sc.Err(); err != nil {
		rejected++
		res.fail(line+1, fmt.Errorf("read: %v", err))
	}
	return res, rejected
}

var (
	timeKeys    = []string{"timestamp", "ts", "time", "@timestamp", "date"}
	levelKeys   = []string{"level", "severity", "lvl", "log.level", "severity_text"}
	messageKeys = []string{"message", "msg", "log", "body", "text"}
)

// ParseObject converts one JSON object into an Entry. Well-known keys map
// to timestamp/level/message; everything else is flattened into fields
// using dotted paths (nested objects) and JSON text (arrays).
func ParseObject(raw []byte, now time.Time) (Entry, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var obj map[string]any
	if err := dec.Decode(&obj); err != nil {
		return Entry{}, fmt.Errorf("invalid JSON object: %v", err)
	}
	if obj == nil {
		return Entry{}, fmt.Errorf("expected a JSON object")
	}
	e := Entry{Fields: map[string]string{}}
	if k, v, ok := takeFirst(obj, timeKeys); ok {
		ts, err := ParseTimestamp(v)
		if err != nil {
			return Entry{}, fmt.Errorf("%s: %v", k, err)
		}
		e.Timestamp = ts
	} else {
		e.Timestamp = now.UnixNano()
	}
	if _, v, ok := takeFirst(obj, levelKeys); ok {
		e.Level = NormalizeLevel(scalarString(v))
	}
	if _, v, ok := takeFirst(obj, messageKeys); ok {
		e.Message = scalarString(v)
	}
	flatten("", obj, e.Fields)
	if len(e.Fields) == 0 {
		e.Fields = nil
	}
	if err := e.Validate(); err != nil {
		return Entry{}, err
	}
	return e, nil
}

func takeFirst(obj map[string]any, keys []string) (string, any, bool) {
	for _, k := range keys {
		if v, ok := obj[k]; ok {
			delete(obj, k)
			return k, v, true
		}
	}
	return "", nil, false
}

func flatten(prefix string, obj map[string]any, out map[string]string) {
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		name := k
		if prefix != "" {
			name = prefix + "." + k
		} else if reservedField(k) {
			// A second spelling of a well-known key (e.g. both "msg" and
			// "message") is kept, but namespaced so it cannot shadow it.
			name = "attr." + k
		}
		switch v := obj[k].(type) {
		case map[string]any:
			flatten(name, v, out)
		case nil:
			// null fields carry no information
		default:
			out[name] = scalarString(v)
		}
	}
}

func scalarString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case bool:
		return strconv.FormatBool(x)
	case nil:
		return ""
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

// ParseTimestamp accepts RFC 3339 strings, "YYYY-MM-DD hh:mm:ss[.fff]"
// (UTC), and numeric epochs whose unit (s, ms, µs, ns) is inferred from
// magnitude. Returns Unix nanoseconds.
func ParseTimestamp(v any) (int64, error) {
	switch x := v.(type) {
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return 0, fmt.Errorf("invalid numeric timestamp")
		}
		if i, err := x.Int64(); err == nil {
			return epochToNanos(float64(i), i)
		}
		return epochToNanos(f, 0)
	case float64:
		return epochToNanos(x, 0)
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return 0, fmt.Errorf("empty timestamp")
		}
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return epochToNanos(float64(i), i)
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return epochToNanos(f, 0)
		}
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999Z07:00", "2006-01-02 15:04:05.999999999"} {
			if t, err := time.Parse(layout, s); err == nil {
				return t.UnixNano(), nil
			}
		}
		return 0, fmt.Errorf("unrecognised timestamp %q", s)
	default:
		return 0, fmt.Errorf("timestamp must be a string or number")
	}
}

func epochToNanos(f float64, exact int64) (int64, error) {
	if math.IsNaN(f) || math.IsInf(f, 0) || f <= 0 {
		return 0, fmt.Errorf("timestamp must be a positive finite number")
	}
	switch {
	case f < 1e11: // seconds
		return int64(f * 1e9), nil
	case f < 1e14: // milliseconds
		if exact != 0 {
			return exact * 1e6, nil
		}
		return int64(f * 1e6), nil
	case f < 1e17: // microseconds
		if exact != 0 {
			return exact * 1e3, nil
		}
		return int64(f * 1e3), nil
	case f < 9.3e18:
		if exact != 0 {
			return exact, nil
		}
		return int64(f), nil
	}
	return 0, fmt.Errorf("timestamp out of range")
}

func reservedField(k string) bool {
	return k == "level" || k == "message" || k == "msg" || k == "timestamp"
}
