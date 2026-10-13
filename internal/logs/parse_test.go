package logs

import (
	"strings"
	"testing"
	"time"
)

var testNow = time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)

func TestParseJSONArrayAndObject(t *testing.T) {
	body := `[
	  {"timestamp":"2026-03-10T11:59:00.5Z","level":"WARNING","message":"disk almost full","service":"db","disk":{"used_pct":91,"mount":"/data"},"tags":["a","b"]},
	  {"ts":1773143940,"msg":"epoch seconds","severity":"err"},
	  {"time":1773143940123,"log":"epoch millis"},
	  "not an object"
	]`
	res, rejected := ParseJSON([]byte(body), testNow)
	if rejected != 1 || len(res.Entries) != 3 {
		t.Fatalf("rejected=%d entries=%d errors=%v", rejected, len(res.Entries), res.Errors)
	}
	e := res.Entries[0]
	if e.Level != "warn" || e.Message != "disk almost full" {
		t.Fatalf("unexpected entry %+v", e)
	}
	if e.Fields["disk.used_pct"] != "91" || e.Fields["disk.mount"] != "/data" || e.Fields["tags"] != `["a","b"]` || e.Fields["service"] != "db" {
		t.Fatalf("flattening failed: %v", e.Fields)
	}
	if got := time.Unix(0, e.Timestamp).UTC(); !got.Equal(time.Date(2026, 3, 10, 11, 59, 0, 5e8, time.UTC)) {
		t.Fatalf("timestamp %v", got)
	}
	if res.Entries[1].Timestamp != 1773143940*int64(time.Second) || res.Entries[1].Level != "error" {
		t.Fatalf("epoch seconds entry %+v", res.Entries[1])
	}
	if res.Entries[2].Timestamp != 1773143940123*int64(time.Millisecond) {
		t.Fatalf("epoch millis entry %+v", res.Entries[2])
	}
	if res.Errors[0].Line != 4 {
		t.Fatalf("error line %d", res.Errors[0].Line)
	}
}

func TestParseNDJSONPartialFailure(t *testing.T) {
	body := "{\"message\":\"one\"}\n\n{bad json}\n{\"message\":\"three\",\"timestamp\":\"yesterday\"}\n{\"message\":\"four\",\"msg\":\"dup\"}\n"
	res, rejected := ParseJSON([]byte(body), testNow)
	if rejected != 2 || len(res.Entries) != 2 {
		t.Fatalf("rejected=%d entries=%d", rejected, len(res.Entries))
	}
	if res.Entries[0].Timestamp != testNow.UnixNano() {
		t.Fatal("missing timestamp must default to now")
	}
	if res.Entries[1].Fields["attr.msg"] != "dup" {
		t.Fatalf("duplicate reserved key not namespaced: %v", res.Entries[1].Fields)
	}
	if res.Errors[0].Line != 3 || res.Errors[1].Line != 4 {
		t.Fatalf("error lines %+v", res.Errors)
	}
}

func TestParseObjectValidation(t *testing.T) {
	big := strings.Repeat("x", MaxMessageBytes+100)
	e, err := ParseObject([]byte(`{"message":"`+big+`"}`), testNow)
	if err != nil || len(e.Message) != MaxMessageBytes {
		t.Fatalf("oversized message should be truncated, err=%v len=%d", err, len(e.Message))
	}
	if _, err := ParseObject([]byte(`{"message":"x","bad key!":"v"}`), testNow); err == nil {
		t.Fatal("invalid field name must be rejected")
	}
	if _, err := ParseObject([]byte(`{"message":"x","timestamp":-5}`), testNow); err == nil {
		t.Fatal("negative timestamp must be rejected")
	}
	if _, err := ParseObject([]byte(`null`), testNow); err == nil {
		t.Fatal("null must be rejected")
	}
}

func TestParseTimestampUnits(t *testing.T) {
	want := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).UnixNano()
	cases := []any{
		"2026-01-02T03:04:05Z", "2026-01-02 03:04:05", "1767323045", "1767323045000",
		"1767323045000000", "1767323045000000000", 1767323045.0,
	}
	for _, c := range cases {
		got, err := ParseTimestamp(c)
		if err != nil || got != want {
			t.Errorf("ParseTimestamp(%v) = %d, %v; want %d", c, got, err, want)
		}
	}
	for _, bad := range []any{"", "soon", true, "0"} {
		if _, err := ParseTimestamp(bad); err == nil {
			t.Errorf("ParseTimestamp(%v) should fail", bad)
		}
	}
}

func TestParseSyslog5424(t *testing.T) {
	line := `<165>1 2026-03-10T11:00:00.003Z host1 payments 4242 ID47 [exampleSDID@32473 iut="3" eventSource="App\"x\]"][meta seq="7"] card declined`
	e, err := ParseSyslogLine(line, testNow)
	if err != nil {
		t.Fatal(err)
	}
	// PRI 165 = facility 20 (local4), severity 5 (notice -> info)
	if e.Level != "info" || e.Fields["facility"] != "local4" || e.Fields["severity"] != "5" {
		t.Fatalf("pri decoding: %+v", e)
	}
	if e.Fields["host"] != "host1" || e.Fields["app"] != "payments" || e.Fields["service"] != "payments" ||
		e.Fields["procid"] != "4242" || e.Fields["msgid"] != "ID47" {
		t.Fatalf("header fields: %v", e.Fields)
	}
	if e.Fields["sd.exampleSDID_32473.eventSource"] != `App"x]` || e.Fields["sd.meta.seq"] != "7" {
		t.Fatalf("structured data: %v", e.Fields)
	}
	if e.Message != "card declined" {
		t.Fatalf("message %q", e.Message)
	}
	if e.Timestamp != time.Date(2026, 3, 10, 11, 0, 0, 3e6, time.UTC).UnixNano() {
		t.Fatal("timestamp")
	}
}

func TestParseSyslog3164(t *testing.T) {
	e, err := ParseSyslogLine("<11>Mar  9 08:15:30 web-1 nginx[812]: upstream timed out", testNow)
	if err != nil {
		t.Fatal(err)
	}
	if e.Level != "error" || e.Fields["host"] != "web-1" || e.Fields["app"] != "nginx" || e.Fields["procid"] != "812" {
		t.Fatalf("%+v", e)
	}
	if e.Message != "upstream timed out" {
		t.Fatalf("message %q", e.Message)
	}
	if got := time.Unix(0, e.Timestamp).UTC(); got.Year() != 2026 || got.Month() != time.March || got.Day() != 9 {
		t.Fatalf("timestamp %v", got)
	}
	// A December date seen in early January belongs to the previous year.
	jan := time.Date(2027, 1, 1, 0, 5, 0, 0, time.UTC)
	e, err = ParseSyslogLine("<14>Dec 31 23:59:59 h app: bye", jan)
	if err != nil || time.Unix(0, e.Timestamp).UTC().Year() != 2026 {
		t.Fatalf("year rollover: %v %v", err, time.Unix(0, e.Timestamp).UTC())
	}
}

func TestParseSyslogBatchAndErrors(t *testing.T) {
	input := "plain line without header\n<999>1 - - - - - - bad pri\n<34>1 not-a-time h a - - - msg\n<13>1 - h a - - [unterminated x=\"1\" msg\n<13>1 - h a - - - ok\n"
	res, rejected := ParseSyslog(strings.NewReader(input), testNow)
	if len(res.Entries) != 2 || rejected != 3 {
		t.Fatalf("entries=%d rejected=%d errs=%v", len(res.Entries), rejected, res.Errors)
	}
	if res.Entries[0].Fields["format"] != "plain" || res.Entries[1].Message != "ok" {
		t.Fatalf("%+v", res.Entries)
	}
}

func TestTokenizeAndLevels(t *testing.T) {
	got := Tokenize("GET /api/v1/users?id=42 -> 500 Internal_Error (Größe)")
	want := []string{"get", "api", "v1", "users", "id", "42", "500", "internal_error", "größe"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("got %v", got)
	}
	long := Tokenize(strings.Repeat("a", 100))
	if len(long[0]) != MaxTokenLen {
		t.Fatal("long tokens must be truncated")
	}
	for in, want := range map[string]string{"WARNING": "warn", "crit": "fatal", "Notice": "info", "custom": "custom"} {
		if NormalizeLevel(in) != want {
			t.Errorf("NormalizeLevel(%q)", in)
		}
	}
}
