package promql

import (
	"math"
	"sort"
	"strconv"

	"github.com/robertramos/developer-observability-stack/internal/tsdb"
	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

// Function describes a callable function's signature.
type Function struct {
	Name   string
	Args   []ValueType
	Return ValueType
}

var functions = map[string]*Function{}

func init() {
	def := func(name string, ret ValueType, args ...ValueType) {
		functions[name] = &Function{Name: name, Args: args, Return: ret}
	}
	for _, f := range []string{"rate", "irate", "increase", "delta",
		"avg_over_time", "sum_over_time", "min_over_time", "max_over_time", "count_over_time", "last_over_time"} {
		def(f, TypeVector, TypeMatrix)
	}
	for _, f := range []string{"abs", "ceil", "floor", "sqrt", "exp", "ln", "absent"} {
		def(f, TypeVector, TypeVector)
	}
	def("histogram_quantile", TypeVector, TypeScalar, TypeVector)
	def("clamp_min", TypeVector, TypeVector, TypeScalar)
	def("clamp_max", TypeVector, TypeVector, TypeScalar)
	def("time", TypeScalar)
	def("vector", TypeVector, TypeScalar)
	def("scalar", TypeScalar, TypeVector)
}

// FunctionNames lists the supported functions.
func FunctionNames() []string {
	out := make([]string, 0, len(functions))
	for n := range functions {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func (ev *evaluator) call(c *Call, t int64) (Value, error) {
	args := make([]Value, len(c.Args))
	for i, a := range c.Args {
		v, err := ev.eval(a, t)
		if err != nil {
			return nil, err
		}
		args[i] = v
	}
	name := c.Func.Name
	switch name {
	case "time":
		return Scalar{t, float64(t) / 1000}, nil
	case "vector":
		return Vector{{Metric: labels.Labels{}, T: t, V: args[0].(Scalar).V}}, nil
	case "scalar":
		vec := args[0].(Vector)
		if len(vec) != 1 {
			return Scalar{t, math.NaN()}, nil
		}
		return Scalar{t, vec[0].V}, nil
	case "absent":
		if len(args[0].(Vector)) > 0 {
			return Vector{}, nil
		}
		return Vector{{Metric: absentLabels(c.Args[0]), T: t, V: 1}}, nil
	case "histogram_quantile":
		return histogramQuantile(args[0].(Scalar).V, args[1].(Vector), t), nil
	case "clamp_min", "clamp_max":
		bound := args[1].(Scalar).V
		return mapVector(args[0].(Vector), t, func(v float64) float64 {
			if name == "clamp_min" {
				return math.Max(v, bound)
			}
			return math.Min(v, bound)
		}), nil
	}
	if fn, ok := mathFuncs[name]; ok {
		return mapVector(args[0].(Vector), t, fn), nil
	}
	ms := c.Args[0].(*MatrixSelector)
	rangeStart := t - ms.VS.Offset.Milliseconds() - ms.Range.Milliseconds()
	rangeEnd := t - ms.VS.Offset.Milliseconds()
	var out Vector
	for _, s := range args[0].(Matrix) {
		v, ok := rangeFunc(name, s.Points, rangeStart, rangeEnd)
		if ok {
			metric := s.Metric.DropMetricName()
			if name == "last_over_time" {
				metric = s.Metric
			}
			out = append(out, Sample{metric, t, v})
		}
	}
	return out, nil
}

var mathFuncs = map[string]func(float64) float64{
	"abs": math.Abs, "ceil": math.Ceil, "floor": math.Floor, "sqrt": math.Sqrt, "exp": math.Exp, "ln": math.Log,
}

func mapVector(vec Vector, t int64, fn func(float64) float64) Vector {
	out := make(Vector, len(vec))
	for i, s := range vec {
		out[i] = Sample{s.Metric.DropMetricName(), t, fn(s.V)}
	}
	return out
}

// absentLabels derives the output labels of absent() from equality
// matchers of a plain selector argument.
func absentLabels(e Expr) labels.Labels {
	vs, ok := e.(*VectorSelector)
	if !ok {
		return labels.Labels{}
	}
	var ls []labels.Label
	for _, m := range vs.Matchers {
		if m.Type == labels.MatchEqual && m.Name != labels.MetricName {
			ls = append(ls, labels.Label{Name: m.Name, Value: m.Value})
		}
	}
	return labels.New(ls...)
}

func rangeFunc(name string, pts []tsdb.Sample, rangeStart, rangeEnd int64) (float64, bool) {
	if len(pts) == 0 {
		return 0, false
	}
	switch name {
	case "rate":
		return extrapolatedDelta(pts, rangeStart, rangeEnd, true, true)
	case "increase":
		return extrapolatedDelta(pts, rangeStart, rangeEnd, true, false)
	case "delta":
		return extrapolatedDelta(pts, rangeStart, rangeEnd, false, false)
	case "irate":
		if len(pts) < 2 {
			return 0, false
		}
		last, prev := pts[len(pts)-1], pts[len(pts)-2]
		d := last.V - prev.V
		if last.V < prev.V {
			d = last.V // counter reset
		}
		dt := float64(last.T-prev.T) / 1000
		if dt == 0 {
			return 0, false
		}
		return d / dt, true
	case "count_over_time":
		return float64(len(pts)), true
	case "last_over_time":
		return pts[len(pts)-1].V, true
	}
	vals := make([]float64, len(pts))
	for i, p := range pts {
		vals[i] = p.V
	}
	switch name {
	case "avg_over_time":
		return aggregateValues("avg", vals, 0), true
	case "sum_over_time":
		return aggregateValues("sum", vals, 0), true
	case "min_over_time":
		return aggregateValues("min", vals, 0), true
	case "max_over_time":
		return aggregateValues("max", vals, 0), true
	}
	return 0, false
}

// extrapolatedDelta implements Prometheus' rate/increase/delta: the raw
// difference (corrected for counter resets) is extrapolated towards the
// range boundaries, but by no more than half an average sample interval,
// and never below zero for counters.
func extrapolatedDelta(pts []tsdb.Sample, rangeStart, rangeEnd int64, isCounter, isRate bool) (float64, bool) {
	if len(pts) < 2 {
		return 0, false
	}
	first, last := pts[0], pts[len(pts)-1]
	result := last.V - first.V
	if isCounter {
		prev := first.V
		for _, p := range pts[1:] {
			if p.V < prev {
				result += prev
			}
			prev = p.V
		}
	}
	durationToStart := float64(first.T-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-last.T) / 1000
	sampled := float64(last.T-first.T) / 1000
	if sampled <= 0 {
		return 0, false
	}
	avgInterval := sampled / float64(len(pts)-1)
	if isCounter && result > 0 && first.V >= 0 {
		// Do not extrapolate below the point where the counter would be zero.
		if durationToZero := sampled * (first.V / result); durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}
	threshold := avgInterval * 1.1
	extrapolateTo := sampled
	if durationToStart < threshold {
		extrapolateTo += durationToStart
	} else {
		extrapolateTo += avgInterval / 2
	}
	if durationToEnd < threshold {
		extrapolateTo += durationToEnd
	} else {
		extrapolateTo += avgInterval / 2
	}
	result *= extrapolateTo / sampled
	if isRate {
		result /= float64(rangeEnd-rangeStart) / 1000
	}
	return result, true
}

type bucket struct {
	upper float64
	count float64
}

// histogramQuantile groups classic histogram buckets (series with an "le"
// label) by their remaining labels and interpolates the φ-quantile.
func histogramQuantile(q float64, vec Vector, t int64) Vector {
	type group struct {
		metric  labels.Labels
		buckets []bucket
	}
	groups := map[string]*group{}
	var order []string
	for _, s := range vec {
		le := s.Metric.Get("le")
		if le == "" {
			continue
		}
		upper, err := strconv.ParseFloat(le, 64)
		if err != nil {
			if le == "+Inf" {
				upper = math.Inf(1)
			} else {
				continue
			}
		}
		m := s.Metric.Without(labels.MetricName, "le")
		k := m.Key()
		g := groups[k]
		if g == nil {
			g = &group{metric: m}
			groups[k] = g
			order = append(order, k)
		}
		g.buckets = append(g.buckets, bucket{upper, s.V})
	}
	out := make(Vector, 0, len(order))
	for _, k := range order {
		g := groups[k]
		out = append(out, Sample{g.metric, t, bucketQuantile(q, g.buckets)})
	}
	return out
}

func bucketQuantile(q float64, buckets []bucket) float64 {
	if math.IsNaN(q) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(1)
	}
	sort.Slice(buckets, func(i, j int) bool { return buckets[i].upper < buckets[j].upper })
	if len(buckets) < 2 || !math.IsInf(buckets[len(buckets)-1].upper, 1) {
		return math.NaN()
	}
	// Merge duplicate bounds and force monotonic counts (scrapes of a
	// histogram are not atomic, so small inversions happen).
	merged := buckets[:1]
	for _, b := range buckets[1:] {
		if b.upper == merged[len(merged)-1].upper {
			merged[len(merged)-1].count += b.count
			continue
		}
		merged = append(merged, b)
	}
	buckets = merged
	for i := 1; i < len(buckets); i++ {
		if buckets[i].count < buckets[i-1].count {
			buckets[i].count = buckets[i-1].count
		}
	}
	observations := buckets[len(buckets)-1].count
	if observations == 0 {
		return math.NaN()
	}
	rank := q * observations
	b := sort.Search(len(buckets)-1, func(i int) bool { return buckets[i].count >= rank })
	if b == len(buckets)-1 {
		return buckets[len(buckets)-2].upper
	}
	if b == 0 && buckets[0].upper <= 0 {
		return buckets[0].upper
	}
	start, end, count := 0.0, buckets[b].upper, buckets[b].count
	if b > 0 {
		start = buckets[b-1].upper
		count -= buckets[b-1].count
		rank -= buckets[b-1].count
	}
	return start + (end-start)*(rank/count)
}
