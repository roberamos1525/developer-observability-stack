package query

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/robertramos/developer-observability-stack/internal/logs"
)

// Node is a boolean predicate over log entries.
type Node interface {
	// Match evaluates the predicate against an entry. tokens is the lazily
	// computed token sequence of the entry's message.
	Match(e *logs.Entry, tokens *TokenCache) bool
	String() string
}

// TokenCache memoises message tokenisation while one entry is evaluated.
type TokenCache struct {
	msg    string
	tokens []string
	ok     bool
}

// Get returns the tokens of e.Message.
func (c *TokenCache) Get(e *logs.Entry) []string {
	if !c.ok || c.msg != e.Message {
		c.msg, c.tokens, c.ok = e.Message, logs.Tokenize(e.Message), true
	}
	return c.tokens
}

// MatchAll matches every entry (the empty query).
type MatchAll struct{}

func (MatchAll) Match(*logs.Entry, *TokenCache) bool { return true }
func (MatchAll) String() string                      { return "*" }

// And is a conjunction.
type And struct{ L, R Node }

func (n *And) Match(e *logs.Entry, c *TokenCache) bool { return n.L.Match(e, c) && n.R.Match(e, c) }
func (n *And) String() string                          { return "(" + n.L.String() + " AND " + n.R.String() + ")" }

// Or is a disjunction.
type Or struct{ L, R Node }

func (n *Or) Match(e *logs.Entry, c *TokenCache) bool { return n.L.Match(e, c) || n.R.Match(e, c) }
func (n *Or) String() string                          { return "(" + n.L.String() + " OR " + n.R.String() + ")" }

// Not negates its operand.
type Not struct{ X Node }

func (n *Not) Match(e *logs.Entry, c *TokenCache) bool { return !n.X.Match(e, c) }
func (n *Not) String() string                          { return "NOT " + n.X.String() }

// Term matches entries whose message contains the token (or a token with
// the given prefix when Prefix is set).
type Term struct {
	Token  string
	Prefix bool
}

func (n *Term) Match(e *logs.Entry, c *TokenCache) bool {
	for _, t := range c.Get(e) {
		if t == n.Token || n.Prefix && strings.HasPrefix(t, n.Token) {
			return true
		}
	}
	return false
}

func (n *Term) String() string {
	if n.Prefix {
		return n.Token + "*"
	}
	return n.Token
}

// Phrase matches entries whose message token sequence contains Tokens
// consecutively. Matching is case-insensitive and punctuation-agnostic.
type Phrase struct {
	Text   string
	Tokens []string
}

func (n *Phrase) Match(e *logs.Entry, c *TokenCache) bool {
	toks := c.Get(e)
	if len(n.Tokens) == 0 {
		return true
	}
outer:
	for i := 0; i+len(n.Tokens) <= len(toks); i++ {
		for j, want := range n.Tokens {
			if toks[i+j] != want {
				continue outer
			}
		}
		return true
	}
	return false
}

func (n *Phrase) String() string { return strconv.Quote(n.Text) }

// Op is a field comparison operator.
type Op string

const (
	OpEq       Op = "="
	OpNeq      Op = "!="
	OpGt       Op = ">"
	OpGte      Op = ">="
	OpLt       Op = "<"
	OpLte      Op = "<="
	OpRegex    Op = "=~"
	OpNotRegex Op = "!~"
)

// FieldMatch compares a field against a value.
type FieldMatch struct {
	Field  string
	Op     Op
	Value  string
	Prefix bool // OpEq/OpNeq with trailing '*'
	num    float64
	isNum  bool
	re     *regexp.Regexp
}

func newFieldMatch(field string, op Op, value string, prefix bool) (*FieldMatch, error) {
	fm := &FieldMatch{Field: field, Op: op, Value: value, Prefix: prefix}
	if field == "level" {
		fm.Value = logs.NormalizeLevel(value)
	}
	if f, err := strconv.ParseFloat(value, 64); err == nil {
		fm.num, fm.isNum = f, true
	}
	if op == OpRegex || op == OpNotRegex {
		if len(value) > 1024 {
			return nil, errorf("regular expression too long")
		}
		re, err := regexp.Compile(value)
		if err != nil {
			return nil, errorf("invalid regular expression %q: %v", value, err)
		}
		fm.re = re
	}
	return fm, nil
}

func (n *FieldMatch) Match(e *logs.Entry, _ *TokenCache) bool {
	v, ok := e.Field(n.Field)
	switch n.Op {
	case OpEq:
		return ok && n.eq(v)
	case OpNeq:
		return !ok || !n.eq(v)
	case OpRegex:
		return ok && n.re.MatchString(v)
	case OpNotRegex:
		return !ok || !n.re.MatchString(v)
	}
	if !ok {
		return false
	}
	var cmp int
	if f, err := strconv.ParseFloat(v, 64); err == nil && n.isNum {
		switch {
		case f < n.num:
			cmp = -1
		case f > n.num:
			cmp = 1
		}
	} else {
		cmp = strings.Compare(v, n.Value)
	}
	switch n.Op {
	case OpGt:
		return cmp > 0
	case OpGte:
		return cmp >= 0
	case OpLt:
		return cmp < 0
	case OpLte:
		return cmp <= 0
	}
	return false
}

func (n *FieldMatch) eq(v string) bool {
	if n.Prefix {
		return strings.HasPrefix(v, n.Value)
	}
	return v == n.Value
}

func (n *FieldMatch) String() string {
	v := n.Value
	if n.Prefix {
		v += "*"
	}
	return n.Field + string(n.Op) + strconv.Quote(v)
}
