package logs

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

var facilityNames = []string{
	"kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news", "uucp", "cron",
	"authpriv", "ftp", "ntp", "security", "console", "solaris-cron",
	"local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7",
}

// ParseSyslog parses one syslog line per input line. RFC 5424 and RFC 3164
// (BSD) formats are recognised by their header; lines without a <PRI>
// header are accepted as plain messages stamped with now.
func ParseSyslog(r io.Reader, now time.Time) (ParseResult, int) {
	var res ParseResult
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), MaxMessageBytes*2)
	line, rejected := 0, 0
	for sc.Scan() {
		line++
		text := strings.TrimRight(sc.Text(), "\r")
		if strings.TrimSpace(text) == "" {
			continue
		}
		e, err := ParseSyslogLine(text, now)
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

// ParseSyslogLine parses a single syslog message.
func ParseSyslogLine(line string, now time.Time) (Entry, error) {
	e := Entry{Fields: map[string]string{}}
	if !strings.HasPrefix(line, "<") {
		e.Timestamp = now.UnixNano()
		e.Level = "info"
		e.Message = line
		e.Fields["format"] = "plain"
		return e, e.Validate()
	}
	end := strings.IndexByte(line, '>')
	if end < 2 || end > 4 {
		return Entry{}, fmt.Errorf("malformed PRI header")
	}
	pri, err := strconv.Atoi(line[1:end])
	if err != nil || pri < 0 || pri > 191 {
		return Entry{}, fmt.Errorf("PRI must be an integer in [0,191]")
	}
	facility, severity := pri/8, pri%8
	e.Level = SyslogSeverityLevel(severity)
	e.Fields["facility"] = facilityNames[facility]
	e.Fields["severity"] = strconv.Itoa(severity)
	rest := line[end+1:]
	if len(rest) >= 2 && rest[0] >= '1' && rest[0] <= '9' && rest[1] == ' ' {
		err = parse5424(rest[2:], now, &e)
		e.Fields["format"] = "rfc5424"
	} else {
		err = parse3164(rest, now, &e)
		e.Fields["format"] = "rfc3164"
	}
	if err != nil {
		return Entry{}, err
	}
	if svc, ok := e.Fields["app"]; ok {
		if _, has := e.Fields["service"]; !has {
			e.Fields["service"] = svc
		}
	}
	return e, e.Validate()
}

// parse5424: TIMESTAMP SP HOSTNAME SP APP-NAME SP PROCID SP MSGID SP SD [SP MSG]
func parse5424(s string, now time.Time, e *Entry) error {
	headers := make([]string, 5)
	for i := 0; i < 5; i++ {
		sp := strings.IndexByte(s, ' ')
		if sp < 0 {
			if i == 4 {
				headers[i], s = s, ""
				break
			}
			return fmt.Errorf("rfc5424: truncated header")
		}
		headers[i], s = s[:sp], s[sp+1:]
	}
	if headers[0] == "-" {
		e.Timestamp = now.UnixNano()
	} else {
		t, err := time.Parse(time.RFC3339Nano, headers[0])
		if err != nil {
			return fmt.Errorf("rfc5424: invalid timestamp %q", headers[0])
		}
		e.Timestamp = t.UnixNano()
	}
	for i, name := range []string{"", "host", "app", "procid", "msgid"} {
		if i > 0 && headers[i] != "-" && headers[i] != "" {
			e.Fields[name] = headers[i]
		}
	}
	msg, err := parseStructuredData(s, e.Fields)
	if err != nil {
		return err
	}
	e.Message = strings.TrimPrefix(msg, string(rune(0xFEFF)))
	return nil
}

// parseStructuredData consumes "-" or one or more [id k="v" ...] elements
// and returns the remaining message.
func parseStructuredData(s string, fields map[string]string) (string, error) {
	if s == "" {
		return "", nil
	}
	if s[0] == '-' {
		return strings.TrimPrefix(s[1:], " "), nil
	}
	for len(s) > 0 && s[0] == '[' {
		i := 1
		for i < len(s) && s[i] != ' ' && s[i] != ']' {
			i++
		}
		if i >= len(s) {
			return "", fmt.Errorf("rfc5424: unterminated structured data")
		}
		id := s[1:i]
		if id == "" {
			return "", fmt.Errorf("rfc5424: empty SD-ID")
		}
		for i < len(s) && s[i] == ' ' {
			i++
			eq := strings.IndexByte(s[i:], '=')
			if eq <= 0 {
				return "", fmt.Errorf("rfc5424: malformed SD-PARAM in %q", id)
			}
			name := s[i : i+eq]
			i += eq + 1
			if i >= len(s) || s[i] != '"' {
				return "", fmt.Errorf("rfc5424: SD-PARAM value must be quoted")
			}
			i++
			var val strings.Builder
			closed := false
			for i < len(s) {
				c := s[i]
				if c == '\\' && i+1 < len(s) && (s[i+1] == '"' || s[i+1] == '\\' || s[i+1] == ']') {
					val.WriteByte(s[i+1])
					i += 2
					continue
				}
				if c == '"' {
					closed = true
					i++
					break
				}
				val.WriteByte(c)
				i++
			}
			if !closed {
				return "", fmt.Errorf("rfc5424: unterminated SD-PARAM value")
			}
			key := "sd." + sanitizeKey(id) + "." + sanitizeKey(name)
			fields[key] = val.String()
		}
		if i >= len(s) || s[i] != ']' {
			return "", fmt.Errorf("rfc5424: expected ']' after structured data")
		}
		s = s[i+1:]
	}
	return strings.TrimPrefix(s, " "), nil
}

func sanitizeKey(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r == '_' || r == '-' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

// parse3164: "Mmm dd hh:mm:ss HOST TAG[PID]: MSG". The year is inferred
// from now; dates more than a day in the future roll back one year.
func parse3164(s string, now time.Time, e *Entry) error {
	if len(s) < 16 || s[3] != ' ' || s[6] != ' ' || s[9] != ':' {
		// No usable timestamp: treat the whole remainder as the message.
		e.Timestamp = now.UnixNano()
		e.Message = strings.TrimSpace(s)
		return nil
	}
	ts, err := time.ParseInLocation("Jan _2 15:04:05", s[:15], time.UTC)
	if err != nil {
		return fmt.Errorf("rfc3164: invalid timestamp %q", s[:15])
	}
	ts = ts.AddDate(now.UTC().Year(), 0, 0)
	if ts.After(now.Add(24 * time.Hour)) {
		ts = ts.AddDate(-1, 0, 0)
	}
	e.Timestamp = ts.UnixNano()
	rest := strings.TrimLeft(s[15:], " ")
	if sp := strings.IndexByte(rest, ' '); sp > 0 {
		e.Fields["host"] = rest[:sp]
		rest = rest[sp+1:]
	}
	if colon := strings.Index(rest, ": "); colon > 0 && !strings.ContainsAny(rest[:colon], " ") {
		tag := rest[:colon]
		if lb := strings.IndexByte(tag, '['); lb > 0 && strings.HasSuffix(tag, "]") {
			e.Fields["procid"] = tag[lb+1 : len(tag)-1]
			tag = tag[:lb]
		}
		e.Fields["app"] = tag
		rest = rest[colon+2:]
	}
	e.Message = rest
	return nil
}
