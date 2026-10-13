package promql

import (
	"fmt"
	"math"

	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

func (ev *evaluator) binary(b *BinaryExpr, t int64) (Value, error) {
	lv, err := ev.eval(b.LHS, t)
	if err != nil {
		return nil, err
	}
	rv, err := ev.eval(b.RHS, t)
	if err != nil {
		return nil, err
	}
	switch l := lv.(type) {
	case Scalar:
		switch r := rv.(type) {
		case Scalar:
			v, keep := applyOp(b.Op, l.V, r.V)
			if isComparison(b.Op) {
				v = boolToFloat(keep)
			}
			return Scalar{t, v}, nil
		case Vector:
			return vectorScalar(b, r, l.V, true, t), nil
		}
	case Vector:
		switch r := rv.(type) {
		case Scalar:
			return vectorScalar(b, l, r.V, false, t), nil
		case Vector:
			if isSetOp(b.Op) {
				return setOp(b, l, r), nil
			}
			return vectorVector(b, l, r, t)
		}
	}
	return nil, fmt.Errorf("invalid operands for %s", b.Op)
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// applyOp returns the arithmetic result, or for comparisons the left value
// and whether the comparison holds.
func applyOp(op string, l, r float64) (float64, bool) {
	switch op {
	case "+":
		return l + r, true
	case "-":
		return l - r, true
	case "*":
		return l * r, true
	case "/":
		return l / r, true
	case "%":
		return math.Mod(l, r), true
	case "^":
		return math.Pow(l, r), true
	case "==":
		return l, l == r
	case "!=":
		return l, l != r
	case ">":
		return l, l > r
	case "<":
		return l, l < r
	case ">=":
		return l, l >= r
	case "<=":
		return l, l <= r
	}
	return math.NaN(), false
}

func vectorScalar(b *BinaryExpr, vec Vector, s float64, scalarLeft bool, t int64) Vector {
	out := make(Vector, 0, len(vec))
	for _, smp := range vec {
		l, r := smp.V, s
		if scalarLeft {
			l, r = s, smp.V
		}
		v, keep := applyOp(b.Op, l, r)
		metric := smp.Metric
		if isComparison(b.Op) {
			if b.ReturnBool {
				v, keep = boolToFloat(keep), true
				metric = metric.DropMetricName()
			} else {
				v = smp.V // filters keep the vector's own value
			}
		} else {
			metric = metric.DropMetricName()
		}
		if keep {
			out = append(out, Sample{metric, t, v})
		}
	}
	return out
}

func signature(m *VectorMatching, ls labels.Labels) string {
	if m == nil {
		return ls.DropMetricName().Key()
	}
	if m.On {
		return ls.Only(m.Labels...).Key()
	}
	return ls.Without(append([]string{labels.MetricName}, m.Labels...)...).Key()
}

func vectorVector(b *BinaryExpr, l, r Vector, t int64) (Vector, error) {
	right := make(map[string]Sample, len(r))
	for _, s := range r {
		sig := signature(b.Matching, s.Metric)
		if _, dup := right[sig]; dup {
			return nil, fmt.Errorf("many-to-many matching not allowed: duplicate series on the right-hand side of %q (group_left/group_right are not supported)", b.Op)
		}
		right[sig] = s
	}
	seen := map[string]bool{}
	var out Vector
	for _, ls := range l {
		sig := signature(b.Matching, ls.Metric)
		rs, ok := right[sig]
		if !ok {
			continue
		}
		if seen[sig] {
			return nil, fmt.Errorf("many-to-many matching not allowed: duplicate series on the left-hand side of %q", b.Op)
		}
		seen[sig] = true
		v, keep := applyOp(b.Op, ls.V, rs.V)
		metric := ls.Metric
		if isComparison(b.Op) && !b.ReturnBool {
			if keep {
				out = append(out, Sample{metric, t, ls.V})
			}
			continue
		}
		if isComparison(b.Op) {
			v = boolToFloat(keep)
		}
		metric = metric.DropMetricName()
		if m := b.Matching; m != nil {
			if m.On {
				metric = metric.Only(m.Labels...)
			} else {
				metric = metric.Without(m.Labels...)
			}
		}
		out = append(out, Sample{metric, t, v})
	}
	return out, nil
}

func setOp(b *BinaryExpr, l, r Vector) Vector {
	rsig := make(map[string]bool, len(r))
	for _, s := range r {
		rsig[signature(b.Matching, s.Metric)] = true
	}
	var out Vector
	switch b.Op {
	case "and":
		for _, s := range l {
			if rsig[signature(b.Matching, s.Metric)] {
				out = append(out, s)
			}
		}
	case "unless":
		for _, s := range l {
			if !rsig[signature(b.Matching, s.Metric)] {
				out = append(out, s)
			}
		}
	case "or":
		lsig := map[string]bool{}
		for _, s := range l {
			lsig[signature(b.Matching, s.Metric)] = true
			out = append(out, s)
		}
		for _, s := range r {
			if !lsig[signature(b.Matching, s.Metric)] {
				out = append(out, s)
			}
		}
	}
	return out
}
