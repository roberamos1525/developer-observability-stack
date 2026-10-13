// Package errs defines the typed error model shared by every layer of the
// stack. Storage and query packages return *Error values with a Kind; the
// HTTP layer maps kinds onto status codes and a uniform JSON envelope.
package errs

import (
	"errors"
	"fmt"
)

// Kind classifies an error independent of its message.
type Kind int

const (
	KindInternal Kind = iota
	KindInvalid
	KindNotFound
	KindConflict
	KindUnauthorized
	KindForbidden
	KindTooLarge
	KindUnavailable
	KindUnsupported
	KindTimeout
)

// String returns the stable machine-readable code used in API responses.
func (k Kind) String() string {
	switch k {
	case KindInvalid:
		return "invalid_argument"
	case KindNotFound:
		return "not_found"
	case KindConflict:
		return "conflict"
	case KindUnauthorized:
		return "unauthorized"
	case KindForbidden:
		return "forbidden"
	case KindTooLarge:
		return "payload_too_large"
	case KindUnavailable:
		return "unavailable"
	case KindUnsupported:
		return "unsupported_media_type"
	case KindTimeout:
		return "timeout"
	default:
		return "internal"
	}
}

// Error is the concrete error type carried through the stack.
type Error struct {
	Kind Kind
	Msg  string
	Err  error
}

func (e *Error) Error() string {
	switch {
	case e.Msg != "" && e.Err != nil:
		return e.Msg + ": " + e.Err.Error()
	case e.Msg != "":
		return e.Msg
	case e.Err != nil:
		return e.Err.Error()
	default:
		return e.Kind.String()
	}
}

func (e *Error) Unwrap() error { return e.Err }

// New creates an error of the given kind with a formatted message.
func New(kind Kind, format string, args ...any) error {
	return &Error{Kind: kind, Msg: fmt.Sprintf(format, args...)}
}

// Wrap attaches a kind and context message to an underlying error.
func Wrap(kind Kind, err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	return &Error{Kind: kind, Msg: fmt.Sprintf(format, args...), Err: err}
}

// Invalid is shorthand for New(KindInvalid, ...).
func Invalid(format string, args ...any) error { return New(KindInvalid, format, args...) }

// NotFound is shorthand for New(KindNotFound, ...).
func NotFound(format string, args ...any) error { return New(KindNotFound, format, args...) }

// Unavailable is shorthand for New(KindUnavailable, ...).
func Unavailable(format string, args ...any) error { return New(KindUnavailable, format, args...) }

// KindOf extracts the Kind of err, defaulting to KindInternal.
func KindOf(err error) Kind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return KindInternal
}

// Is reports whether err carries the given kind.
func Is(err error, kind Kind) bool {
	return err != nil && KindOf(err) == kind
}
