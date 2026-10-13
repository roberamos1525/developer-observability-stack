package httpapi

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/errs"
)

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type envelope struct {
	Status    string     `json:"status"`
	Data      any        `json:"data,omitempty"`
	Error     *errorBody `json:"error,omitempty"`
	RequestID string     `json:"requestId,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(v)
}

func (s *Server) ok(w http.ResponseWriter, data any) {
	writeJSON(w, http.StatusOK, envelope{Status: "success", Data: data})
}

func (s *Server) accepted(w http.ResponseWriter, sync bool, data any) {
	status := http.StatusAccepted
	if sync {
		status = http.StatusOK
	}
	writeJSON(w, status, envelope{Status: "success", Data: data})
}

func httpStatus(k errs.Kind) int {
	switch k {
	case errs.KindInvalid:
		return http.StatusBadRequest
	case errs.KindNotFound:
		return http.StatusNotFound
	case errs.KindConflict:
		return http.StatusConflict
	case errs.KindUnauthorized:
		return http.StatusUnauthorized
	case errs.KindForbidden:
		return http.StatusForbidden
	case errs.KindTooLarge:
		return http.StatusRequestEntityTooLarge
	case errs.KindUnavailable:
		return http.StatusTooManyRequests
	case errs.KindUnsupported:
		return http.StatusUnsupportedMediaType
	case errs.KindTimeout:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// fail writes the uniform error envelope. Internal error details are
// logged but not returned to the client.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	kind := errs.KindOf(err)
	status := httpStatus(kind)
	msg := err.Error()
	if kind == errs.KindInternal {
		s.d.Log.Error("request failed", "id", requestID(r.Context()), "err", err)
		msg = "internal server error"
	}
	if kind == errs.KindUnavailable {
		w.Header().Set("Retry-After", "1")
	}
	if kind == errs.KindUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="observability"`)
	}
	writeJSON(w, status, envelope{Status: "error", Error: &errorBody{Code: kind.String(), Message: msg}, RequestID: requestID(r.Context())})
}

// readBody reads the request body enforcing the compressed and
// decompressed size limits; gzip Content-Encoding is supported.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	limit := s.d.Config.MaxBodyBytes
	body := http.MaxBytesReader(w, r.Body, limit)
	var rd io.Reader = body
	switch strings.ToLower(r.Header.Get("Content-Encoding")) {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(body)
		if err != nil {
			return nil, errs.Invalid("invalid gzip body: %v", err)
		}
		defer gz.Close()
		limit = s.d.Config.MaxDecompressedBytes
		rd = io.LimitReader(gz, limit+1)
	default:
		return nil, errs.New(errs.KindUnsupported, "unsupported Content-Encoding %q", r.Header.Get("Content-Encoding"))
	}
	data, err := io.ReadAll(rd)
	var mbe *http.MaxBytesError
	if errors.As(err, &mbe) {
		return nil, errs.New(errs.KindTooLarge, "request body exceeds %d bytes", s.d.Config.MaxBodyBytes)
	}
	if err != nil {
		return nil, errs.Invalid("read body: %v", err)
	}
	if int64(len(data)) > limit {
		return nil, errs.New(errs.KindTooLarge, "decompressed body exceeds %d bytes", limit)
	}
	if len(data) == 0 {
		return nil, errs.Invalid("request body is empty")
	}
	return data, nil
}

func mediaType(r *http.Request) string {
	ct := r.Header.Get("Content-Type")
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	return strings.ToLower(strings.TrimSpace(ct))
}

// param returns a query or form parameter (POST forms are supported for
// Prometheus API compatibility).
func param(r *http.Request, name string) string {
	if v := r.URL.Query().Get(name); v != "" {
		return v
	}
	if r.Method == http.MethodPost && mediaType(r) == "application/x-www-form-urlencoded" {
		r.ParseForm()
		return r.PostForm.Get(name)
	}
	return ""
}

// parseTime accepts RFC 3339 or (fractional) Unix seconds; empty yields def.
func parseTime(s string, def time.Time) (time.Time, error) {
	if s == "" {
		return def, nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		if math.IsNaN(f) || math.IsInf(f, 0) || math.Abs(f) > 1e13 {
			return time.Time{}, errs.Invalid("invalid timestamp %q", s)
		}
		sec, frac := math.Modf(f)
		return time.Unix(int64(sec), int64(frac*1e9)).UTC(), nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, errs.Invalid("invalid timestamp %q (use RFC 3339 or Unix seconds)", s)
	}
	return t, nil
}

// parseDuration accepts Go/Prometheus durations or float seconds.
func parseDuration(s string, def time.Duration) (time.Duration, error) {
	if s == "" {
		return def, nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		if f <= 0 || f > 1e9 {
			return 0, errs.Invalid("invalid duration %q", s)
		}
		return time.Duration(f * float64(time.Second)), nil
	}
	d, err := config.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, errs.Invalid("invalid duration %q", s)
	}
	return d, nil
}

func parseLimit(s string, def, max int) (int, error) {
	if s == "" {
		return def, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 {
		return 0, errs.Invalid("limit must be a positive integer")
	}
	if n > max {
		return 0, errs.Invalid("limit must not exceed %d", max)
	}
	return n, nil
}

func syncParam(r *http.Request) bool {
	v := r.URL.Query().Get("sync")
	return v == "true" || v == "1"
}

// formatFloat renders sample values the way the Prometheus API does.
func formatFloat(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}
