package errs

import (
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestKindsAndWrapping(t *testing.T) {
	base := io.ErrUnexpectedEOF
	err := Wrap(KindUnavailable, base, "queue %s", "logs")
	if err.Error() != "queue logs: unexpected EOF" || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("wrap: %v", err)
	}
	outer := fmt.Errorf("handler: %w", err)
	if KindOf(outer) != KindUnavailable || !Is(outer, KindUnavailable) || Is(nil, KindInternal) {
		t.Fatal("KindOf must see through fmt wrapping")
	}
	if KindOf(errors.New("plain")) != KindInternal {
		t.Fatal("untyped errors are internal")
	}
	if Wrap(KindInvalid, nil, "x") != nil {
		t.Fatal("wrapping nil yields nil")
	}
	if (&Error{Kind: KindNotFound}).Error() != "not_found" || (&Error{Err: base}).Error() != "unexpected EOF" {
		t.Fatal("message fallbacks")
	}
	codes := map[Kind]string{KindInvalid: "invalid_argument", KindNotFound: "not_found", KindConflict: "conflict",
		KindUnauthorized: "unauthorized", KindForbidden: "forbidden", KindTooLarge: "payload_too_large",
		KindUnavailable: "unavailable", KindUnsupported: "unsupported_media_type", KindTimeout: "timeout", KindInternal: "internal"}
	for k, c := range codes {
		if k.String() != c {
			t.Errorf("%d -> %s", k, k.String())
		}
	}
	if Invalid("a %d", 1).Error() != "a 1" || !Is(NotFound("x"), KindNotFound) || !Is(Unavailable("y"), KindUnavailable) {
		t.Fatal("constructors")
	}
}
