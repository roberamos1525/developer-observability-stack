// Package logs defines the log entry model and the parsers that turn
// external payloads (JSON, NDJSON, syslog lines) into entries.
package logs

import (
	"fmt"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Limits applied to every ingested entry.
const (
	MaxMessageBytes  = 64 << 10
	MaxFields        = 128
	MaxFieldKeyLen   = 128
	MaxFieldValueLen = 8 << 10
)

// Entry is one log record. Timestamp is Unix nanoseconds.
type Entry struct {
	Timestamp int64             `json:"timestamp"`
	Level     string            `json:"level,omitempty"`
	Message   string            `json:"message"`
	Fields    map[string]string `json:"fields,omitempty"`
}

// Field returns a named attribute; "level" and "message" are virtual fields.
func (e *Entry) Field(name string) (string, bool) {
	switch name {
	case "level":
		return e.Level, e.Level != ""
	case "message", "msg":
		return e.Message, true
	}
	v, ok := e.Fields[name]
	return v, ok
}

// SortedFieldKeys returns the field names in lexical order.
func (e *Entry) SortedFieldKeys() []string {
	keys := make([]string, 0, len(e.Fields))
	for k := range e.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Validate enforces size limits and key syntax. It truncates oversize
// values rather than rejecting the entry; structural problems are errors.
func (e *Entry) Validate() error {
	if e.Timestamp <= 0 {
		return fmt.Errorf("timestamp must be positive")
	}
	if len(e.Message) > MaxMessageBytes {
		e.Message = truncateUTF8(e.Message, MaxMessageBytes)
	}
	if !utf8.ValidString(e.Message) {
		e.Message = strings.ToValidUTF8(e.Message, "�")
	}
	if len(e.Fields) > MaxFields {
		return fmt.Errorf("too many fields (%d > %d)", len(e.Fields), MaxFields)
	}
	for k, v := range e.Fields {
		if err := ValidateFieldKey(k); err != nil {
			return err
		}
		if len(v) > MaxFieldValueLen {
			e.Fields[k] = truncateUTF8(v, MaxFieldValueLen)
		}
	}
	return nil
}

// ValidateFieldKey checks a field name: non-empty, bounded, and made of
// letters, digits, '_', '.', '-' or '/'.
func ValidateFieldKey(k string) error {
	if k == "" || len(k) > MaxFieldKeyLen {
		return fmt.Errorf("field name must be 1..%d bytes", MaxFieldKeyLen)
	}
	for _, r := range k {
		if !(r == '_' || r == '.' || r == '-' || r == '/' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return fmt.Errorf("field name %q contains invalid character %q", k, r)
		}
	}
	if reservedField(k) {
		return fmt.Errorf("field name %q is reserved", k)
	}
	return nil
}

func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// NormalizeLevel maps common severity spellings onto a small canonical set:
// trace, debug, info, warn, error, fatal. Unknown values are lowercased.
func NormalizeLevel(s string) string {
	l := strings.ToLower(strings.TrimSpace(s))
	switch l {
	case "trace", "trc", "finest":
		return "trace"
	case "debug", "dbg", "fine", "verbose":
		return "debug"
	case "info", "inf", "information", "informational", "notice":
		return "info"
	case "warn", "warning", "wrn":
		return "warn"
	case "error", "err", "eror", "severe":
		return "error"
	case "fatal", "critical", "crit", "alert", "emerg", "emergency", "panic":
		return "fatal"
	}
	return l
}

// SyslogSeverityLevel maps an RFC 5424 severity (0..7) to a level.
func SyslogSeverityLevel(sev int) string {
	switch {
	case sev <= 2:
		return "fatal"
	case sev == 3:
		return "error"
	case sev == 4:
		return "warn"
	case sev <= 6:
		return "info"
	default:
		return "debug"
	}
}

// Tokenize splits text into lowercase alphanumeric tokens used by both the
// inverted index and the query engine, so the two always agree.
func Tokenize(text string) []string {
	var out []string
	start := -1
	lower := strings.ToLower(text)
	for i, r := range lower {
		alnum := r == '_' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r > 127 && (unicode.IsLetter(r) || unicode.IsDigit(r))
		if alnum {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			out = appendToken(out, lower[start:i])
			start = -1
		}
	}
	if start >= 0 {
		out = appendToken(out, lower[start:])
	}
	return out
}

// MaxTokenLen bounds indexed token length; longer tokens are truncated.
const MaxTokenLen = 64

func appendToken(out []string, tok string) []string {
	if len(tok) > MaxTokenLen {
		tok = truncateUTF8(tok, MaxTokenLen)
	}
	return append(out, tok)
}
