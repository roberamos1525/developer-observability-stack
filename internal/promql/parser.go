package promql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/config"
	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

// ParseError is a syntax or type error with its position.
type ParseError struct {
	Pos int
	Msg string
}

func (e *ParseError) Error() string { return fmt.Sprintf("parse error at char %d: %s", e.Pos+1, e.Msg) }

type itemKind int

const (
	itemEOF itemKind = iota
	itemIdent
	itemNumber
	itemDuration
	itemString
	itemOp // operators and punctuation
)

type item struct {
	kind itemKind
	val  string
	pos  int
}

func isIdentStart(c byte) bool {
	return c == '_' || c == ':' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func lex(input string) ([]item, error) {
	var items []item
	i := 0
	for i < len(input) {
		c := input[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '#': // comment to end of line
			for i < len(input) && input[i] != '\n' {
				i++
			}
		case isIdentStart(c):
			start := i
			for i < len(input) && (isIdentStart(input[i]) || isDigit(input[i])) {
				i++
			}
			items = append(items, item{itemIdent, input[start:i], start})
		case isDigit(c) || c == '.' && i+1 < len(input) && isDigit(input[i+1]):
			start := i
			for i < len(input) && (isDigit(input[i]) || input[i] == '.') {
				i++
			}
			if i < len(input) && (input[i] == 'e' || input[i] == 'E') {
				j := i + 1
				if j < len(input) && (input[j] == '+' || input[j] == '-') {
					j++
				}
				if j < len(input) && isDigit(input[j]) {
					i = j
					for i < len(input) && isDigit(input[i]) {
						i++
					}
				}
			}
			// A number directly followed by unit letters is a duration.
			if i < len(input) && strings.IndexByte("smhdw", input[i]) >= 0 {
				for i < len(input) && (isDigit(input[i]) || strings.IndexByte("smhdw", input[i]) >= 0) {
					i++
				}
				items = append(items, item{itemDuration, input[start:i], start})
				continue
			}
			items = append(items, item{itemNumber, input[start:i], start})
		case c == '"' || c == '\'' || c == '`':
			s, n, err := lexString(input[i:], c)
			if err != nil {
				return nil, &ParseError{i, err.Error()}
			}
			items = append(items, item{itemString, s, i})
			i += n
		default:
			two := ""
			if i+1 < len(input) {
				two = input[i : i+2]
			}
			switch two {
			case "==", "!=", ">=", "<=", "=~", "!~":
				items = append(items, item{itemOp, two, i})
				i += 2
				continue
			}
			if strings.IndexByte("+-*/%^<>=,(){}[]", c) < 0 {
				return nil, &ParseError{i, fmt.Sprintf("unexpected character %q", c)}
			}
			items = append(items, item{itemOp, string(c), i})
			i++
		}
	}
	return append(items, item{itemEOF, "", len(input)}), nil
}

func lexString(s string, q byte) (string, int, error) {
	if q == '`' {
		end := strings.IndexByte(s[1:], '`')
		if end < 0 {
			return "", 0, fmt.Errorf("unterminated raw string")
		}
		return s[1 : end+1], end + 2, nil
	}
	for i := 1; i < len(s); i++ {
		if s[i] == '\\' {
			i++
			continue
		}
		if s[i] == q {
			raw := s[:i+1]
			if q == '\'' {
				raw = `"` + strings.ReplaceAll(strings.ReplaceAll(s[1:i], `\'`, `'`), `"`, `\"`) + `"`
			}
			v, err := strconv.Unquote(raw)
			if err != nil {
				return "", 0, fmt.Errorf("invalid string literal")
			}
			return v, i + 1, nil
		}
	}
	return "", 0, fmt.Errorf("unterminated string")
}

var aggregators = map[string]bool{
	"sum": true, "avg": true, "min": true, "max": true, "count": true,
	"stddev": true, "topk": true, "bottomk": true, "quantile": true,
}

var binPrec = map[string]int{
	"or": 1, "and": 2, "unless": 2,
	"==": 3, "!=": 3, ">": 3, "<": 3, ">=": 3, "<=": 3,
	"+": 4, "-": 4, "*": 5, "/": 5, "%": 5, "^": 6,
}

func isComparison(op string) bool {
	switch op {
	case "==", "!=", ">", "<", ">=", "<=":
		return true
	}
	return false
}

func isSetOp(op string) bool { return op == "and" || op == "or" || op == "unless" }

type parser struct {
	items []item
	pos   int
	depth int
}

// MaxQueryLength bounds the size of a query string.
const MaxQueryLength = 16 << 10

// Parse parses a PromQL expression.
func Parse(input string) (Expr, error) {
	if len(input) > MaxQueryLength {
		return nil, &ParseError{0, "query too long"}
	}
	items, err := lex(input)
	if err != nil {
		return nil, err
	}
	p := &parser{items: items}
	e, err := p.expr(0)
	if err != nil {
		return nil, err
	}
	if t := p.peek(); t.kind != itemEOF {
		return nil, p.errf(t, "unexpected %q", t.val)
	}
	return e, nil
}

func (p *parser) peek() item { return p.items[p.pos] }
func (p *parser) next() item {
	t := p.items[p.pos]
	if t.kind != itemEOF {
		p.pos++
	}
	return t
}

func (p *parser) errf(t item, format string, args ...any) error {
	return &ParseError{t.pos, fmt.Sprintf(format, args...)}
}

func (p *parser) expect(val string) error {
	if t := p.next(); t.val != val || t.kind == itemString {
		return p.errf(t, "expected %q, found %q", val, t.val)
	}
	return nil
}

func (p *parser) binaryOp() (string, bool) {
	t := p.peek()
	if t.kind != itemOp && t.kind != itemIdent {
		return "", false
	}
	_, ok := binPrec[t.val]
	return t.val, ok
}

func (p *parser) expr(minPrec int) (Expr, error) {
	if p.depth++; p.depth > 128 {
		return nil, p.errf(p.peek(), "expression nested too deeply")
	}
	defer func() { p.depth-- }()
	lhs, err := p.unary()
	if err != nil {
		return nil, err
	}
	for {
		op, ok := p.binaryOp()
		if !ok || binPrec[op] < minPrec {
			return lhs, nil
		}
		opTok := p.next()
		be := &BinaryExpr{Op: op}
		if t := p.peek(); t.kind == itemIdent && t.val == "bool" {
			if !isComparison(op) {
				return nil, p.errf(t, "bool modifier only allowed on comparison operators")
			}
			p.next()
			be.ReturnBool = true
		}
		if t := p.peek(); t.kind == itemIdent && (t.val == "on" || t.val == "ignoring") {
			p.next()
			ls, err := p.labelList()
			if err != nil {
				return nil, err
			}
			be.Matching = &VectorMatching{On: t.val == "on", Labels: ls}
		}
		if t := p.peek(); t.kind == itemIdent && (t.val == "group_left" || t.val == "group_right") {
			return nil, p.errf(t, "%s is not supported", t.val)
		}
		next := binPrec[op] + 1
		if op == "^" {
			next = binPrec[op] // right associative
		}
		rhs, err := p.expr(next)
		if err != nil {
			return nil, err
		}
		be.LHS, be.RHS = lhs, rhs
		if err := checkBinary(be); err != nil {
			return nil, p.errf(opTok, "%v", err)
		}
		lhs = be
	}
}

func checkBinary(b *BinaryExpr) error {
	lt, rt := b.LHS.Type(), b.RHS.Type()
	for _, t := range []ValueType{lt, rt} {
		if t != TypeScalar && t != TypeVector {
			return fmt.Errorf("binary %q needs scalar or instant vector operands, got %s", b.Op, t)
		}
	}
	if isSetOp(b.Op) && (lt != TypeVector || rt != TypeVector) {
		return fmt.Errorf("set operator %q requires vector operands", b.Op)
	}
	if isComparison(b.Op) && lt == TypeScalar && rt == TypeScalar && !b.ReturnBool {
		return fmt.Errorf("comparisons between scalars must use the bool modifier")
	}
	if b.Matching != nil && (lt != TypeVector || rt != TypeVector) {
		return fmt.Errorf("vector matching only allowed between instant vectors")
	}
	return nil
}

func (p *parser) unary() (Expr, error) {
	if t := p.peek(); t.kind == itemOp && (t.val == "-" || t.val == "+") {
		p.next()
		e, err := p.expr(binPrec["^"])
		if err != nil {
			return nil, err
		}
		if e.Type() != TypeScalar && e.Type() != TypeVector {
			return nil, p.errf(t, "unary %s needs a scalar or vector", t.val)
		}
		if n, ok := e.(*NumberLiteral); ok && t.val == "-" {
			return &NumberLiteral{-n.Val}, nil
		}
		if t.val == "+" {
			return e, nil
		}
		return &UnaryExpr{Op: "-", Expr: e}, nil
	}
	e, err := p.primary()
	if err != nil {
		return nil, err
	}
	return p.postfix(e)
}

func (p *parser) postfix(e Expr) (Expr, error) {
	if t := p.peek(); t.kind == itemOp && t.val == "[" {
		vs, ok := e.(*VectorSelector)
		if !ok || vs.Offset != 0 {
			return nil, p.errf(t, "ranges are only allowed on vector selectors (subqueries are not supported)")
		}
		p.next()
		d, err := p.duration()
		if err != nil {
			return nil, err
		}
		if err := p.expect("]"); err != nil {
			return nil, err
		}
		e = &MatrixSelector{VS: vs, Range: d}
	}
	if t := p.peek(); t.kind == itemIdent && t.val == "offset" {
		p.next()
		neg := false
		if n := p.peek(); n.kind == itemOp && n.val == "-" {
			p.next()
			neg = true
		}
		d, err := p.duration()
		if err != nil {
			return nil, err
		}
		if neg {
			d = -d
		}
		switch s := e.(type) {
		case *VectorSelector:
			s.Offset = d
		case *MatrixSelector:
			s.VS.Offset = d
		default:
			return nil, p.errf(t, "offset must follow a selector")
		}
	}
	return e, nil
}

func (p *parser) duration() (time.Duration, error) {
	t := p.next()
	if t.kind != itemDuration {
		return 0, p.errf(t, "expected a duration such as 5m, found %q", t.val)
	}
	d, err := config.ParseDuration(t.val)
	if err != nil || d <= 0 {
		return 0, p.errf(t, "invalid duration %q", t.val)
	}
	return d, nil
}

func (p *parser) primary() (Expr, error) {
	t := p.next()
	switch t.kind {
	case itemNumber:
		v, err := strconv.ParseFloat(t.val, 64)
		if err != nil {
			return nil, p.errf(t, "invalid number %q", t.val)
		}
		return &NumberLiteral{v}, nil
	case itemString:
		return &StringLiteral{t.val}, nil
	case itemOp:
		switch t.val {
		case "(":
			e, err := p.expr(0)
			if err != nil {
				return nil, err
			}
			if err := p.expect(")"); err != nil {
				return nil, err
			}
			return &ParenExpr{e}, nil
		case "{":
			return p.selector("", t)
		}
	case itemIdent:
		switch strings.ToLower(t.val) {
		case "inf":
			return &NumberLiteral{math.Inf(1)}, nil
		case "nan":
			return &NumberLiteral{math.NaN()}, nil
		}
		if aggregators[t.val] {
			if n := p.peek(); n.val == "(" || n.val == "by" || n.val == "without" {
				return p.aggregate(t)
			}
		}
		if n := p.peek(); n.kind == itemOp && n.val == "(" {
			return p.call(t)
		}
		if n := p.peek(); n.kind == itemOp && n.val == "{" {
			p.next()
			return p.selector(t.val, n)
		}
		return p.selector(t.val, item{})
	case itemEOF:
		return nil, p.errf(t, "unexpected end of input")
	}
	return nil, p.errf(t, "unexpected %q", t.val)
}

// selector parses matchers (after '{' when open.val == "{").
func (p *parser) selector(name string, open item) (Expr, error) {
	vs := &VectorSelector{Name: name}
	if name != "" {
		vs.Matchers = append(vs.Matchers, labels.MustMatcher(labels.MatchEqual, labels.MetricName, name))
	}
	if open.val == "{" {
		for {
			t := p.next()
			if t.val == "}" && t.kind == itemOp {
				break
			}
			if t.kind != itemIdent {
				return nil, p.errf(t, "expected label name, found %q", t.val)
			}
			op := p.next()
			var mt labels.MatchType
			switch op.val {
			case "=":
				mt = labels.MatchEqual
			case "!=":
				mt = labels.MatchNotEqual
			case "=~":
				mt = labels.MatchRegexp
			case "!~":
				mt = labels.MatchNotRegexp
			default:
				return nil, p.errf(op, "expected label matching operator, found %q", op.val)
			}
			v := p.next()
			if v.kind != itemString {
				return nil, p.errf(v, "label value must be a quoted string")
			}
			m, err := labels.NewMatcher(mt, t.val, v.val)
			if err != nil {
				return nil, p.errf(v, "%v", err)
			}
			vs.Matchers = append(vs.Matchers, m)
			if n := p.peek(); n.val == "," {
				p.next()
			}
		}
	}
	nonEmpty := false
	for _, m := range vs.Matchers {
		if !m.Matches("") {
			nonEmpty = true
		}
	}
	if !nonEmpty {
		return nil, p.errf(open, "vector selector must contain at least one non-empty matcher")
	}
	return vs, nil
}

func (p *parser) labelList() ([]string, error) {
	if err := p.expect("("); err != nil {
		return nil, err
	}
	var out []string
	for {
		t := p.next()
		if t.kind == itemOp && t.val == ")" {
			return out, nil
		}
		if t.kind != itemIdent || !labels.ValidLabelName(t.val) {
			return nil, p.errf(t, "expected label name, found %q", t.val)
		}
		out = append(out, t.val)
		if n := p.peek(); n.val == "," {
			p.next()
		}
	}
}

func (p *parser) aggregate(op item) (Expr, error) {
	agg := &AggregateExpr{Op: op.val}
	parseGrouping := func() error {
		t := p.peek()
		if t.kind == itemIdent && (t.val == "by" || t.val == "without") {
			p.next()
			ls, err := p.labelList()
			if err != nil {
				return err
			}
			agg.Grouping, agg.Without = ls, t.val == "without"
		}
		return nil
	}
	if err := parseGrouping(); err != nil {
		return nil, err
	}
	if err := p.expect("("); err != nil {
		return nil, err
	}
	first, err := p.expr(0)
	if err != nil {
		return nil, err
	}
	if n := p.peek(); n.val == "," {
		p.next()
		agg.Param = first
		if agg.Expr, err = p.expr(0); err != nil {
			return nil, err
		}
	} else {
		agg.Expr = first
	}
	if err := p.expect(")"); err != nil {
		return nil, err
	}
	if agg.Grouping == nil {
		if err := parseGrouping(); err != nil {
			return nil, err
		}
	}
	needsParam := op.val == "topk" || op.val == "bottomk" || op.val == "quantile"
	if needsParam != (agg.Param != nil) {
		return nil, p.errf(op, "wrong number of arguments for aggregation %s", op.val)
	}
	if agg.Param != nil && agg.Param.Type() != TypeScalar {
		return nil, p.errf(op, "%s parameter must be a scalar", op.val)
	}
	if agg.Expr.Type() != TypeVector {
		return nil, p.errf(op, "%s expects an instant vector, got %s", op.val, agg.Expr.Type())
	}
	return agg, nil
}

func (p *parser) call(name item) (Expr, error) {
	fn, ok := functions[name.val]
	if !ok {
		return nil, p.errf(name, "unknown function %q", name.val)
	}
	p.next() // (
	var args []Expr
	if n := p.peek(); !(n.kind == itemOp && n.val == ")") {
		for {
			a, err := p.expr(0)
			if err != nil {
				return nil, err
			}
			args = append(args, a)
			if n := p.peek(); n.val == "," {
				p.next()
				continue
			}
			break
		}
	}
	if err := p.expect(")"); err != nil {
		return nil, err
	}
	if len(args) != len(fn.Args) {
		return nil, p.errf(name, "%s expects %d argument(s), got %d", fn.Name, len(fn.Args), len(args))
	}
	for i, a := range args {
		if a.Type() != fn.Args[i] {
			return nil, p.errf(name, "%s argument %d must be %s, got %s", fn.Name, i+1, fn.Args[i], a.Type())
		}
	}
	return &Call{Func: fn, Args: args}, nil
}
