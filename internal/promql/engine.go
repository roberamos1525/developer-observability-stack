package promql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/errs"
	"github.com/robertramos/developer-observability-stack/internal/tsdb"
	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

// Queryable is the storage the engine reads from.
type Queryable interface {
	Select(ctx context.Context, ms []*labels.Matcher, mint, maxt int64) ([]tsdb.Series, error)
}

// Value is a query result.
type Value interface{ Type() ValueType }

// Scalar is a single number.
type Scalar struct {
	T int64
	V float64
}

func (Scalar) Type() ValueType { return TypeScalar }

// String is a string result.
type String struct {
	T int64
	V string
}

func (String) Type() ValueType { return TypeString }

// Sample is one element of an instant vector.
type Sample struct {
	Metric labels.Labels
	T      int64
	V      float64
}

// Vector is an instant vector.
type Vector []Sample

func (Vector) Type() ValueType { return TypeVector }

// Series is one element of a matrix.
type Series struct {
	Metric labels.Labels
	Points []tsdb.Sample
}

// Matrix is a range vector / range query result.
type Matrix []Series

func (Matrix) Type() ValueType { return TypeMatrix }

// Engine evaluates queries.
type Engine struct {
	LookbackDelta time.Duration
	MaxSamples    int
	Timeout       time.Duration
	MaxSteps      int
}

// Errors returned by the engine.
var (
	ErrTooManySamples = errors.New("query would load too many samples")
	ErrTooManySteps   = errors.New("range query has too many steps")
)

func (e *Engine) lookback() int64 {
	if e.LookbackDelta <= 0 {
		return (5 * time.Minute).Milliseconds()
	}
	return e.LookbackDelta.Milliseconds()
}

// storageError marks failures of the underlying Queryable.
type storageError struct{ err error }

func (e storageError) Error() string { return "storage: " + e.err.Error() }
func (e storageError) Unwrap() error { return e.err }

// classify attaches an errs.Kind: cancellation and timeouts are KindTimeout,
// storage failures stay internal, and everything else (syntax, limits,
// matching errors) is the caller's fault.
func classify(err error) error {
	var se storageError
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
		return errs.Wrap(errs.KindTimeout, err, "query aborted")
	case errors.As(err, &se):
		return errs.Wrap(errs.KindInternal, err, "query")
	}
	return &errs.Error{Kind: errs.KindInvalid, Err: err}
}

// Instant evaluates query at ts.
func (e *Engine) Instant(ctx context.Context, q Queryable, query string, ts time.Time) (Value, error) {
	v, err := e.instant(ctx, q, query, ts)
	return v, classify(err)
}

// Range evaluates query at each step in [start, end].
func (e *Engine) Range(ctx context.Context, q Queryable, query string, start, end time.Time, step time.Duration) (Matrix, error) {
	m, err := e.rangeQuery(ctx, q, query, start, end, step)
	return m, classify(err)
}

func (e *Engine) instant(ctx context.Context, q Queryable, query string, ts time.Time) (Value, error) {
	expr, err := Parse(query)
	if err != nil {
		return nil, err
	}
	ctx, cancel := e.withTimeout(ctx)
	defer cancel()
	t := ts.UnixMilli()
	ev, err := e.newEvaluator(ctx, q, expr, t, t)
	if err != nil {
		return nil, err
	}
	v, err := ev.eval(expr, t)
	if err != nil {
		return nil, err
	}
	if m, ok := v.(Matrix); ok {
		return m, nil
	}
	return v, nil
}

func (e *Engine) rangeQuery(ctx context.Context, q Queryable, query string, start, end time.Time, step time.Duration) (Matrix, error) {
	if step <= 0 {
		return nil, fmt.Errorf("step must be positive")
	}
	if end.Before(start) {
		return nil, fmt.Errorf("end must not be before start")
	}
	steps := int(end.Sub(start)/step) + 1
	if e.MaxSteps > 0 && steps > e.MaxSteps {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManySteps, steps, e.MaxSteps)
	}
	expr, err := Parse(query)
	if err != nil {
		return nil, err
	}
	if expr.Type() != TypeScalar && expr.Type() != TypeVector {
		return nil, fmt.Errorf("range queries require a scalar or instant vector expression, got %s", expr.Type())
	}
	ctx, cancel := e.withTimeout(ctx)
	defer cancel()
	s, en := start.UnixMilli(), end.UnixMilli()
	ev, err := e.newEvaluator(ctx, q, expr, s, en)
	if err != nil {
		return nil, err
	}
	byKey := map[string]*Series{}
	var order []string
	for t := s; t <= en; t += step.Milliseconds() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		v, err := ev.eval(expr, t)
		if err != nil {
			return nil, err
		}
		var vec Vector
		switch x := v.(type) {
		case Scalar:
			vec = Vector{{T: t, V: x.V}}
		case Vector:
			vec = x
		}
		for _, smp := range vec {
			k := smp.Metric.Key()
			ser := byKey[k]
			if ser == nil {
				ser = &Series{Metric: smp.Metric}
				byKey[k] = ser
				order = append(order, k)
			}
			ser.Points = append(ser.Points, tsdb.Sample{T: t, V: smp.V})
		}
	}
	out := make(Matrix, 0, len(order))
	for _, k := range order {
		out = append(out, *byKey[k])
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Metric.Key() < out[j].Metric.Key() })
	return out, nil
}

func (e *Engine) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if e.Timeout > 0 {
		return context.WithTimeout(ctx, e.Timeout)
	}
	return context.WithCancel(ctx)
}

type evaluator struct {
	ctx      context.Context
	lookback int64
	data     map[*VectorSelector][]tsdb.Series
}

// newEvaluator preloads every selector's data for the whole evaluation
// window so each step only does in-memory binary searches.
func (e *Engine) newEvaluator(ctx context.Context, q Queryable, expr Expr, start, end int64) (*evaluator, error) {
	ev := &evaluator{ctx: ctx, lookback: e.lookback(), data: map[*VectorSelector][]tsdb.Series{}}
	total := 0
	var err error
	Inspect(expr, func(n Expr) {
		if err != nil {
			return
		}
		var vs *VectorSelector
		window := ev.lookback
		switch s := n.(type) {
		case *MatrixSelector:
			vs, window = s.VS, s.Range.Milliseconds()
		case *VectorSelector:
			if _, done := ev.data[s]; done {
				return
			}
			vs = s
		default:
			return
		}
		off := vs.Offset.Milliseconds()
		series, serr := q.Select(ctx, vs.Matchers, start-off-window, end-off)
		if serr != nil {
			err = storageError{serr}
			return
		}
		for _, s := range series {
			total += len(s.Samples)
		}
		if e.MaxSamples > 0 && total > e.MaxSamples {
			err = fmt.Errorf("%w (%d > %d)", ErrTooManySamples, total, e.MaxSamples)
			return
		}
		ev.data[vs] = series
	})
	return ev, err
}

func (ev *evaluator) eval(expr Expr, t int64) (Value, error) {
	if err := ev.ctx.Err(); err != nil {
		return nil, err
	}
	switch n := expr.(type) {
	case *NumberLiteral:
		return Scalar{t, n.Val}, nil
	case *StringLiteral:
		return String{t, n.Val}, nil
	case *ParenExpr:
		return ev.eval(n.Expr, t)
	case *VectorSelector:
		return ev.vectorSelect(n, t), nil
	case *MatrixSelector:
		return ev.matrixSelect(n, t), nil
	case *UnaryExpr:
		v, err := ev.eval(n.Expr, t)
		if err != nil {
			return nil, err
		}
		switch x := v.(type) {
		case Scalar:
			return Scalar{t, -x.V}, nil
		case Vector:
			out := make(Vector, len(x))
			for i, s := range x {
				out[i] = Sample{s.Metric.DropMetricName(), t, -s.V}
			}
			return out, nil
		}
		return nil, fmt.Errorf("unary minus on %s", v.Type())
	case *Call:
		return ev.call(n, t)
	case *AggregateExpr:
		return ev.aggregate(n, t)
	case *BinaryExpr:
		return ev.binary(n, t)
	}
	return nil, fmt.Errorf("unsupported expression %T", expr)
}

// vectorSelect returns the newest sample per series in (t-offset-lookback, t-offset].
func (ev *evaluator) vectorSelect(vs *VectorSelector, t int64) Vector {
	ref := t - vs.Offset.Milliseconds()
	var out Vector
	for _, s := range ev.data[vs] {
		i := sort.Search(len(s.Samples), func(i int) bool { return s.Samples[i].T > ref }) - 1
		if i < 0 || s.Samples[i].T <= ref-ev.lookback {
			continue
		}
		out = append(out, Sample{s.Labels, t, s.Samples[i].V})
	}
	return out
}

// matrixSelect returns samples per series in (t-offset-range, t-offset].
func (ev *evaluator) matrixSelect(ms *MatrixSelector, t int64) Matrix {
	ref := t - ms.VS.Offset.Milliseconds()
	lo := ref - ms.Range.Milliseconds()
	var out Matrix
	for _, s := range ev.data[ms.VS] {
		i := sort.Search(len(s.Samples), func(i int) bool { return s.Samples[i].T > lo })
		j := sort.Search(len(s.Samples), func(i int) bool { return s.Samples[i].T > ref })
		if i < j {
			out = append(out, Series{Metric: s.Labels, Points: s.Samples[i:j]})
		}
	}
	return out
}

func (ev *evaluator) aggregate(a *AggregateExpr, t int64) (Value, error) {
	v, err := ev.eval(a.Expr, t)
	if err != nil {
		return nil, err
	}
	vec := v.(Vector)
	var param float64
	if a.Param != nil {
		pv, err := ev.eval(a.Param, t)
		if err != nil {
			return nil, err
		}
		param = pv.(Scalar).V
	}
	type group struct {
		metric  labels.Labels
		samples []Sample
	}
	groups := map[string]*group{}
	var order []string
	for _, s := range vec {
		var gl labels.Labels
		if a.Without {
			gl = s.Metric.Without(append([]string{labels.MetricName}, a.Grouping...)...)
		} else {
			gl = s.Metric.Only(a.Grouping...)
		}
		k := gl.Key()
		g := groups[k]
		if g == nil {
			g = &group{metric: gl}
			groups[k] = g
			order = append(order, k)
		}
		g.samples = append(g.samples, s)
	}
	var out Vector
	for _, k := range order {
		g := groups[k]
		switch a.Op {
		case "topk", "bottomk":
			n := int(param)
			if n < 1 {
				continue
			}
			ss := append([]Sample(nil), g.samples...)
			sort.SliceStable(ss, func(i, j int) bool {
				if a.Op == "topk" {
					return ss[i].V > ss[j].V || !math.IsNaN(ss[i].V) && math.IsNaN(ss[j].V)
				}
				return ss[i].V < ss[j].V || !math.IsNaN(ss[i].V) && math.IsNaN(ss[j].V)
			})
			for _, s := range ss[:min(n, len(ss))] {
				out = append(out, Sample{s.Metric, t, s.V})
			}
			continue
		}
		vals := make([]float64, len(g.samples))
		for i, s := range g.samples {
			vals[i] = s.V
		}
		out = append(out, Sample{g.metric, t, aggregateValues(a.Op, vals, param)})
	}
	return out, nil
}

func aggregateValues(op string, vals []float64, param float64) float64 {
	switch op {
	case "sum":
		s := 0.0
		for _, v := range vals {
			s += v
		}
		return s
	case "avg":
		s := 0.0
		for _, v := range vals {
			s += v
		}
		return s / float64(len(vals))
	case "count":
		return float64(len(vals))
	case "min":
		m := math.NaN()
		for _, v := range vals {
			if math.IsNaN(m) || v < m {
				m = v
			}
		}
		return m
	case "max":
		m := math.NaN()
		for _, v := range vals {
			if math.IsNaN(m) || v > m {
				m = v
			}
		}
		return m
	case "stddev":
		mean := aggregateValues("avg", vals, 0)
		ss := 0.0
		for _, v := range vals {
			ss += (v - mean) * (v - mean)
		}
		return math.Sqrt(ss / float64(len(vals)))
	case "quantile":
		return quantile(param, vals)
	}
	return math.NaN()
}

// quantile computes the φ-quantile with linear interpolation, matching
// Prometheus' quantile aggregation.
func quantile(q float64, vals []float64) float64 {
	if len(vals) == 0 || math.IsNaN(q) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(1)
	}
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	rank := q * float64(len(s)-1)
	lo := math.Floor(rank)
	hi := math.Ceil(rank)
	w := rank - lo
	return s[int(lo)]*(1-w) + s[int(hi)]*w
}
