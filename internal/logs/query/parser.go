package query

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/logs"
)

// Order is the result ordering by timestamp.
type Order int

const (
	Desc Order = iota
	Asc
)

// Query is a parsed log query.
type Query struct {
	Root    Node
	Limit   int // 0 means "use the server default"
	Order   Order
	Since   time.Duration
	Start   int64 // Unix ns, 0 = unbounded
	End     int64 // Unix ns, 0 = unbounded
	Count   bool
	CountBy []string
}

// Limits for parser resource usage.
const (
	MaxQueryLength = 4096
	MaxDepth       = 64
	MaxCountBy     = 8
)

type parseError struct{ msg string }

func (e *parseError) Error() string { return e.msg }

func errorf(format string, args ...any) error { return &parseError{fmt.Sprintf(format, args...)} }

type parser struct {
	toks  []token
	pos   int
	depth int
}

// Parse parses a query string.
func Parse(input string) (*Query, error) {
	if len(input) > MaxQueryLength {
		return nil, &SyntaxError{0, fmt.Sprintf("query longer than %d bytes", MaxQueryLength)}
	}
	toks, err := lex(input)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	q := &Query{Root: MatchAll{}}
	if k := p.peek().kind; k != tEOF && k != tPipe {
		if q.Root, err = p.parseOr(); err != nil {
			return nil, err
		}
	}
	for p.peek().kind == tPipe {
		p.next()
		if err := p.parseStage(q); err != nil {
			return nil, err
		}
	}
	if t := p.peek(); t.kind != tEOF {
		return nil, p.errAt(t, "unexpected %s %q", t.kind, t.text)
	}
	return q, nil
}

func (p *parser) peek() token { return p.toks[p.pos] }

func (p *parser) next() token {
	t := p.toks[p.pos]
	if t.kind != tEOF {
		p.pos++
	}
	return t
}

func (p *parser) errAt(t token, format string, args ...any) error {
	return &SyntaxError{t.pos, fmt.Sprintf(format, args...)}
}

func (p *parser) wrap(t token, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(*SyntaxError); ok {
		return err
	}
	return &SyntaxError{t.pos, err.Error()}
}

func (p *parser) parseOr() (Node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peek().kind == tOr {
		p.next()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &Or{left, right}
	}
	return left, nil
}

func startsUnary(k tokenKind) bool {
	return k == tWord || k == tString || k == tLParen || k == tNot || k == tMinus
}

func (p *parser) parseAnd() (Node, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		k := p.peek().kind
		if k == tAnd {
			p.next()
		} else if !startsUnary(k) {
			return left, nil
		}
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &And{left, right}
	}
}

func (p *parser) parseUnary() (Node, error) {
	t := p.peek()
	if t.kind == tNot || t.kind == tMinus {
		p.next()
		if p.depth++; p.depth > MaxDepth {
			return nil, p.errAt(t, "query nested too deeply")
		}
		x, err := p.parseUnary()
		p.depth--
		if err != nil {
			return nil, err
		}
		return &Not{x}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (Node, error) {
	t := p.next()
	switch t.kind {
	case tLParen:
		if p.depth++; p.depth > MaxDepth {
			return nil, p.errAt(t, "query nested too deeply")
		}
		n, err := p.parseOr()
		p.depth--
		if err != nil {
			return nil, err
		}
		if r := p.next(); r.kind != tRParen {
			return nil, p.errAt(r, "expected ')' but found %s", r.kind)
		}
		return n, nil
	case tString:
		return p.fullText(t, t.text, true)
	case tWord:
		if op := p.peek(); op.kind == tOp {
			p.next()
			return p.parseField(t, op)
		}
		return p.fullText(t, t.text, false)
	case tEOF:
		return nil, p.errAt(t, "unexpected end of query")
	default:
		return nil, p.errAt(t, "unexpected %s", t.kind)
	}
}

func (p *parser) fullText(t token, text string, quoted bool) (Node, error) {
	n, err := fullText(text, quoted)
	return n, p.wrap(t, err)
}

// fullText builds a Term or Phrase from free text.
func fullText(text string, quoted bool) (Node, error) {
	prefix := !quoted && strings.HasSuffix(text, "*")
	if prefix {
		text = strings.TrimRight(text, "*")
	}
	toks := logs.Tokenize(text)
	switch {
	case len(toks) == 0:
		return nil, errorf("search text %q contains no searchable characters", text)
	case len(toks) == 1:
		return &Term{Token: toks[0], Prefix: prefix}, nil
	case prefix:
		return nil, errorf("wildcards are only supported on single words")
	default:
		return &Phrase{Text: text, Tokens: toks}, nil
	}
}

func (p *parser) parseField(name, op token) (Node, error) {
	field := name.text
	if field != "level" && field != "message" && field != "msg" {
		if err := logs.ValidateFieldKey(field); err != nil {
			return nil, p.errAt(name, "%v", err)
		}
	}
	vt := p.next()
	if vt.kind != tWord && vt.kind != tString {
		return nil, p.errAt(vt, "expected a value after %s%s", field, op.text)
	}
	value := vt.text
	o := Op(op.text)
	if o == ":" {
		o = OpEq
	}
	if field == "message" || field == "msg" {
		if o != OpEq {
			return nil, p.errAt(op, "only ':' and '=' are supported on message")
		}
		return p.fullText(vt, value, vt.kind == tString)
	}
	prefix := false
	if vt.kind == tWord && (o == OpEq || o == OpNeq) && strings.HasSuffix(value, "*") {
		prefix = true
		value = strings.TrimSuffix(value, "*")
		if value == "" {
			// field:* means "field exists"
			return &FieldMatch{Field: field, Op: o, Prefix: true}, nil
		}
	}
	fm, err := newFieldMatch(field, o, value, prefix)
	if err != nil {
		return nil, p.wrap(vt, err)
	}
	return fm, nil
}

func (p *parser) parseStage(q *Query) error {
	kw := p.next()
	if kw.kind != tWord {
		return p.errAt(kw, "expected a pipeline stage after '|'")
	}
	switch strings.ToLower(kw.text) {
	case "limit", "head":
		t := p.next()
		n, err := strconv.Atoi(t.text)
		if t.kind != tWord || err != nil || n < 1 {
			return p.errAt(t, "limit expects a positive integer")
		}
		q.Limit = n
	case "sort":
		t := p.next()
		switch strings.ToLower(t.text) {
		case "asc":
			q.Order = Asc
		case "desc":
			q.Order = Desc
		default:
			return p.errAt(t, "sort expects asc or desc")
		}
	case "since", "last":
		t := p.next()
		d, err := config.ParseDuration(t.text)
		if t.kind != tWord || err != nil || d <= 0 {
			return p.errAt(t, "since expects a positive duration such as 15m")
		}
		q.Since = d
	case "between":
		a, b := p.next(), p.next()
		start, err1 := parseTimeArg(a.text)
		end, err2 := parseTimeArg(b.text)
		if err1 != nil {
			return p.errAt(a, "between: %v", err1)
		}
		if err2 != nil {
			return p.errAt(b, "between: %v", err2)
		}
		if end < start {
			return p.errAt(b, "between: end is before start")
		}
		q.Start, q.End = start, end
	case "count":
		q.Count = true
		if t := p.peek(); t.kind == tWord && strings.EqualFold(t.text, "by") {
			p.next()
			for {
				f := p.next()
				if f.kind != tWord {
					return p.errAt(f, "count by expects field names")
				}
				q.CountBy = append(q.CountBy, f.text)
				if len(q.CountBy) > MaxCountBy {
					return p.errAt(f, "count by supports at most %d fields", MaxCountBy)
				}
				if p.peek().kind != tComma {
					break
				}
				p.next()
			}
		}
	default:
		return p.errAt(kw, "unknown pipeline stage %q", kw.text)
	}
	return nil
}

func parseTimeArg(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("missing time")
	}
	return logs.ParseTimestamp(s)
}
