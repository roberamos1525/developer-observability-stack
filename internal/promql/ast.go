// Package promql implements a subset of the Prometheus query language:
// instant and range vector selectors (with offset), arithmetic, comparison
// (with bool) and set operators with on/ignoring matching, aggregations
// (sum, avg, min, max, count, stddev, topk, bottomk, quantile) with
// by/without, and functions including rate, irate, increase, delta,
// *_over_time, histogram_quantile, clamp_min/max, abs, ceil, floor, sqrt,
// time, vector, scalar and absent.
package promql

import (
	"fmt"
	"strings"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

// ValueType is the type of an expression result.
type ValueType string

const (
	TypeScalar ValueType = "scalar"
	TypeVector ValueType = "vector"
	TypeMatrix ValueType = "matrix"
	TypeString ValueType = "string"
)

// Expr is an AST node.
type Expr interface {
	Type() ValueType
	String() string
}

// NumberLiteral is a float constant.
type NumberLiteral struct{ Val float64 }

func (*NumberLiteral) Type() ValueType  { return TypeScalar }
func (n *NumberLiteral) String() string { return fmt.Sprint(n.Val) }

// StringLiteral is a string constant.
type StringLiteral struct{ Val string }

func (*StringLiteral) Type() ValueType  { return TypeString }
func (s *StringLiteral) String() string { return fmt.Sprintf("%q", s.Val) }

// VectorSelector selects series by matchers.
type VectorSelector struct {
	Name     string
	Matchers []*labels.Matcher
	Offset   time.Duration
}

func (*VectorSelector) Type() ValueType { return TypeVector }
func (v *VectorSelector) String() string {
	var ms []string
	for _, m := range v.Matchers {
		if m.Name != labels.MetricName || v.Name == "" {
			ms = append(ms, m.String())
		}
	}
	s := v.Name
	if len(ms) > 0 || s == "" {
		s += "{" + strings.Join(ms, ",") + "}"
	}
	if v.Offset != 0 {
		s += " offset " + v.Offset.String()
	}
	return s
}

// MatrixSelector selects a range of samples per series.
type MatrixSelector struct {
	VS    *VectorSelector
	Range time.Duration
}

func (*MatrixSelector) Type() ValueType { return TypeMatrix }
func (m *MatrixSelector) String() string {
	off := ""
	vs := *m.VS
	if vs.Offset != 0 {
		off = " offset " + vs.Offset.String()
		vs.Offset = 0
	}
	return vs.String() + "[" + m.Range.String() + "]" + off
}

// Call is a function call.
type Call struct {
	Func *Function
	Args []Expr
}

func (c *Call) Type() ValueType { return c.Func.Return }
func (c *Call) String() string {
	args := make([]string, len(c.Args))
	for i, a := range c.Args {
		args[i] = a.String()
	}
	return c.Func.Name + "(" + strings.Join(args, ", ") + ")"
}

// AggregateExpr is an aggregation over a vector.
type AggregateExpr struct {
	Op       string
	Expr     Expr
	Param    Expr
	Grouping []string
	Without  bool
}

func (*AggregateExpr) Type() ValueType { return TypeVector }
func (a *AggregateExpr) String() string {
	s := a.Op
	if a.Without {
		s += " without (" + strings.Join(a.Grouping, ", ") + ")"
	} else if len(a.Grouping) > 0 {
		s += " by (" + strings.Join(a.Grouping, ", ") + ")"
	}
	if a.Param != nil {
		return s + "(" + a.Param.String() + ", " + a.Expr.String() + ")"
	}
	return s + "(" + a.Expr.String() + ")"
}

// VectorMatching describes on/ignoring for vector-vector operations.
type VectorMatching struct {
	On     bool
	Labels []string
}

// BinaryExpr is a binary operation.
type BinaryExpr struct {
	Op         string
	LHS, RHS   Expr
	ReturnBool bool
	Matching   *VectorMatching
}

func (b *BinaryExpr) Type() ValueType {
	if b.LHS.Type() == TypeScalar && b.RHS.Type() == TypeScalar {
		return TypeScalar
	}
	return TypeVector
}

func (b *BinaryExpr) String() string {
	op := b.Op
	if b.ReturnBool {
		op += " bool"
	}
	if m := b.Matching; m != nil && len(m.Labels) > 0 || m != nil && m.On {
		kw := "ignoring"
		if m.On {
			kw = "on"
		}
		op += " " + kw + "(" + strings.Join(m.Labels, ", ") + ")"
	}
	return b.LHS.String() + " " + op + " " + b.RHS.String()
}

// UnaryExpr is unary minus or plus.
type UnaryExpr struct {
	Op   string
	Expr Expr
}

func (u *UnaryExpr) Type() ValueType { return u.Expr.Type() }
func (u *UnaryExpr) String() string  { return u.Op + u.Expr.String() }

// ParenExpr is a parenthesised expression.
type ParenExpr struct{ Expr Expr }

func (p *ParenExpr) Type() ValueType { return p.Expr.Type() }
func (p *ParenExpr) String() string  { return "(" + p.Expr.String() + ")" }

// Inspect walks the AST depth-first.
func Inspect(e Expr, fn func(Expr)) {
	fn(e)
	switch n := e.(type) {
	case *MatrixSelector:
		Inspect(n.VS, fn)
	case *Call:
		for _, a := range n.Args {
			Inspect(a, fn)
		}
	case *AggregateExpr:
		if n.Param != nil {
			Inspect(n.Param, fn)
		}
		Inspect(n.Expr, fn)
	case *BinaryExpr:
		Inspect(n.LHS, fn)
		Inspect(n.RHS, fn)
	case *UnaryExpr:
		Inspect(n.Expr, fn)
	case *ParenExpr:
		Inspect(n.Expr, fn)
	}
}
