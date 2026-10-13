package promql

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/tsdb"
	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

// memStore is an in-memory Queryable for engine tests.
type memStore []tsdb.Series

func (m memStore) Select(_ context.Context, ms []*labels.Matcher, mint, maxt int64) ([]tsdb.Series, error) {
	var out []tsdb.Series
	for _, s := range m {
		if !labels.MatchesAll(s.Labels, ms) {
			continue
		}
		var smp []tsdb.Sample
		for _, p := range s.Samples {
			if p.T >= mint && p.T <= maxt {
				smp = append(smp, p)
			}
		}
		if len(smp) > 0 {
			out = append(out, tsdb.Series{Labels: s.Labels, Samples: smp})
		}
	}
	return out, nil
}

var base = time.Unix(1_800_000_000, 0)

// series generates samples every 15s for 10 minutes from fn(i).
func series(fn func(i int) float64, kv ...string) tsdb.Series {
	s := tsdb.Series{Labels: labels.FromStrings(kv...)}
	for i := 0; i <= 40; i++ {
		s.Samples = append(s.Samples, tsdb.Sample{T: base.Add(time.Duration(i) * 15 * time.Second).UnixMilli(), V: fn(i)})
	}
	return s
}

func testStore() memStore {
	return memStore{
		// 2 req/s on api/200, 1 req/s on api/500, 0.5 req/s on web/200.
		series(func(i int) float64 { return float64(i * 30) }, "__name__", "http_requests_total", "job", "api", "code", "200"),
		series(func(i int) float64 { return float64(i * 15) }, "__name__", "http_requests_total", "job", "api", "code", "500"),
		series(func(i int) float64 { return float64(i*7) + 0.5*float64(i%2) }, "__name__", "http_requests_total", "job", "web", "code", "200"),
		series(func(i int) float64 { return float64(i % 10) }, "__name__", "queue_depth", "job", "api"),
		series(func(i int) float64 { return 10 }, "__name__", "lat_bucket", "job", "api", "le", "0.1"),
		series(func(i int) float64 { return 60 }, "__name__", "lat_bucket", "job", "api", "le", "0.5"),
		series(func(i int) float64 { return 90 }, "__name__", "lat_bucket", "job", "api", "le", "1"),
		series(func(i int) float64 { return 100 }, "__name__", "lat_bucket", "job", "api", "le", "+Inf"),
	}
}

var at = base.Add(10 * time.Minute)

func instant(t *testing.T, q string) Value {
	t.Helper()
	e := &Engine{LookbackDelta: 5 * time.Minute}
	v, err := e.Instant(context.Background(), testStore(), q, at)
	if err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return v
}

func vec(t *testing.T, q string) map[string]float64 {
	t.Helper()
	v, ok := instant(t, q).(Vector)
	if !ok {
		t.Fatalf("%s: not a vector", q)
	}
	out := map[string]float64{}
	for _, s := range v {
		out[s.Metric.String()] = s.V
	}
	return out
}

func near(a, b float64) bool { return a == b || math.Abs(a-b) < 1e-9 || math.IsNaN(a) && math.IsNaN(b) }

func TestRateAndIncrease(t *testing.T) {
	got := vec(t, `rate(http_requests_total{job="api"}[5m])`)
	if len(got) != 2 || !near(got[`{code="200", job="api"}`], 2) || !near(got[`{code="500", job="api"}`], 1) {
		t.Fatalf("rate: %v", got)
	}
	inc := vec(t, `increase(http_requests_total{code="500"}[5m])`)
	if !near(inc[`{code="500", job="api"}`], 300) {
		t.Fatalf("increase: %v", inc)
	}
	ir := vec(t, `irate(http_requests_total{job="api",code="200"}[1m])`)
	if !near(ir[`{code="200", job="api"}`], 2) {
		t.Fatalf("irate: %v", ir)
	}
}

func TestRateHandlesCounterReset(t *testing.T) {
	s := series(func(i int) float64 {
		if i < 20 {
			return float64(i * 10)
		}
		return float64((i - 20) * 10) // reset at i=20
	}, "__name__", "c")
	e := &Engine{}
	v, err := e.Instant(context.Background(), memStore{s}, `increase(c[10m])`, at)
	if err != nil {
		t.Fatal(err)
	}
	// The window (base, base+10m] holds samples 1..40: raw increase is
	// 190-10 plus 190 lost at the reset = 380, extrapolated by 15s at the
	// start over a 585s sampled interval: 380*600/585.
	got := v.(Vector)[0].V
	if !near(got, 380*600.0/585) {
		t.Fatalf("increase across reset = %v", got)
	}
}

func TestAggregations(t *testing.T) {
	sum := vec(t, `sum by (job) (rate(http_requests_total[5m]))`)
	if !near(sum[`{job="api"}`], 3) || len(sum) != 2 {
		t.Fatalf("sum by: %v", sum)
	}
	without := vec(t, `sum without (code) (rate(http_requests_total[5m]))`)
	if !near(without[`{job="api"}`], 3) {
		t.Fatalf("sum without: %v", without)
	}
	total := vec(t, `sum(rate(http_requests_total[5m])) by (code)`)
	if len(total) != 2 {
		t.Fatalf("trailing by: %v", total)
	}
	cnt := vec(t, `count(http_requests_total)`)
	if cnt["{}"] != 3 {
		t.Fatalf("count: %v", cnt)
	}
	mx := vec(t, `max(http_requests_total)`)
	mn := vec(t, `min(http_requests_total)`)
	avg := vec(t, `avg(http_requests_total)`)
	if mx["{}"] != 1200 || mn["{}"] != 280 || !near(avg["{}"], (1200+600+280)/3.0) {
		t.Fatalf("max/min/avg: %v %v %v", mx, mn, avg)
	}
	top := vec(t, `topk(2, http_requests_total)`)
	if len(top) != 2 || top[`http_requests_total{code="200", job="api"}`] != 1200 {
		t.Fatalf("topk: %v", top)
	}
	bottom := vec(t, `bottomk(1, http_requests_total)`)
	if len(bottom) != 1 || bottom[`http_requests_total{code="200", job="web"}`] != 280 {
		t.Fatalf("bottomk: %v", bottom)
	}
	q := vec(t, `quantile(0.5, http_requests_total)`)
	if q["{}"] != 600 {
		t.Fatalf("quantile: %v", q)
	}
	sd := vec(t, `stddev(vector(4))`)
	if sd["{}"] != 0 {
		t.Fatalf("stddev: %v", sd)
	}
}

func TestOverTimeFunctions(t *testing.T) {
	// queue_depth cycles 0..9; the last 5m window holds samples 21..40.
	cases := map[string]float64{
		`avg_over_time(queue_depth[5m])`:   4.5,
		`sum_over_time(queue_depth[5m])`:   90,
		`min_over_time(queue_depth[5m])`:   0,
		`max_over_time(queue_depth[5m])`:   9,
		`count_over_time(queue_depth[5m])`: 20,
	}
	for q, want := range cases {
		got := vec(t, q)
		if !near(got[`{job="api"}`], want) {
			t.Errorf("%s = %v, want %v", q, got, want)
		}
	}
	last := vec(t, `last_over_time(queue_depth[5m])`)
	if last[`queue_depth{job="api"}`] != 0 {
		t.Fatalf("last_over_time keeps the metric name: %v", last)
	}
}

func TestHistogramQuantile(t *testing.T) {
	got := vec(t, `histogram_quantile(0.5, lat_bucket)`)
	// rank 50 falls in (0.1, 0.5] holding 50 of 100: 0.1 + 0.4*(40/50)
	if !near(got[`{job="api"}`], 0.42) {
		t.Fatalf("p50: %v", got)
	}
	p99 := vec(t, `histogram_quantile(0.99, sum by (le) (lat_bucket))`)
	if !near(p99["{}"], 1) {
		t.Fatalf("p99 in +Inf bucket returns the highest finite bound: %v", p99)
	}
	cases := []struct {
		q    float64
		b    []bucket
		want float64
	}{
		{-1, []bucket{{1, 1}, {math.Inf(1), 1}}, math.Inf(-1)},
		{2, []bucket{{1, 1}, {math.Inf(1), 1}}, math.Inf(1)},
		{0.5, []bucket{{1, 1}}, math.NaN()},                      // no +Inf bucket
		{0.5, []bucket{{1, 0}, {math.Inf(1), 0}}, math.NaN()},    // no observations
		{0.5, []bucket{{1, 10}, {2, 8}, {math.Inf(1), 10}}, 0.5}, // non-monotonic fixed
		{0.5, []bucket{{-1, 10}, {math.Inf(1), 10}}, -1},         // negative first bound
		{0.25, []bucket{{1, 2}, {1, 2}, {math.Inf(1), 4}}, 0.25}, // duplicate bounds merged
		{math.NaN(), []bucket{{1, 1}, {math.Inf(1), 1}}, math.NaN()},
	}
	for i, c := range cases {
		if got := bucketQuantile(c.q, c.b); !near(got, c.want) {
			t.Errorf("case %d: got %v want %v", i, got, c.want)
		}
	}
}

func TestBinaryOperators(t *testing.T) {
	ratio := vec(t, `sum by (job) (rate(http_requests_total{code="500"}[5m])) / sum by (job) (rate(http_requests_total[5m]))`)
	if !near(ratio[`{job="api"}`], 1.0/3) || len(ratio) != 1 {
		t.Fatalf("error ratio: %v", ratio)
	}
	filtered := vec(t, `rate(http_requests_total[5m]) > 1`)
	if len(filtered) != 1 || filtered[`{code="200", job="api"}`] != 2 {
		t.Fatalf("comparison filter: %v", filtered)
	}
	b := vec(t, `rate(http_requests_total[5m]) > bool 1`)
	if len(b) != 3 || b[`{code="500", job="api"}`] != 0 {
		t.Fatalf("bool comparison: %v", b)
	}
	keep := vec(t, `http_requests_total > 1000`)
	if keep[`http_requests_total{code="200", job="api"}`] != 1200 {
		t.Fatalf("filter keeps metric name: %v", keep)
	}
	on := vec(t, `http_requests_total{code="500"} / on(job) queue_depth`)
	if len(on) != 1 || !math.IsInf(on[`{job="api"}`], 1) {
		t.Fatalf("on() matching: %v", on)
	}
	ign := vec(t, `http_requests_total{code="200"} - ignoring(code) http_requests_total{code="500"}`)
	if ign[`{job="api"}`] != 600 {
		t.Fatalf("ignoring() matching: %v", ign)
	}
	if s := instant(t, `2 ^ 3 ^ 2 - -1`).(Scalar); s.V != 513 {
		t.Fatalf("precedence/right-assoc: %v", s.V)
	}
	if s := instant(t, `-2 ^ 2`).(Scalar); s.V != -4 {
		t.Fatalf("unary minus binds looser than ^: %v", s.V)
	}
	if s := instant(t, `10 % 4 * 2 + 1 >= bool 5`).(Scalar); s.V != 1 {
		t.Fatalf("scalar bool comparison: %v", s.V)
	}
	sc := vec(t, `100 - queue_depth`)
	if sc[`{job="api"}`] != 100 {
		t.Fatalf("scalar-vector: %v", sc)
	}
	neg := vec(t, `-queue_depth`)
	if len(neg) != 1 {
		t.Fatalf("unary vector: %v", neg)
	}
}

func TestSetOperators(t *testing.T) {
	and := vec(t, `http_requests_total and http_requests_total{code="500"}`)
	if len(and) != 1 {
		t.Fatalf("and: %v", and)
	}
	unless := vec(t, `http_requests_total unless http_requests_total{code="500"}`)
	if len(unless) != 2 {
		t.Fatalf("unless: %v", unless)
	}
	or := vec(t, `http_requests_total{job="web"} or on(job) queue_depth`)
	if len(or) != 2 {
		t.Fatalf("or: %v", or)
	}
}

func TestMiscFunctions(t *testing.T) {
	if s := instant(t, `time()`).(Scalar); s.V != float64(at.Unix()) {
		t.Fatalf("time(): %v", s.V)
	}
	if s := instant(t, `scalar(queue_depth)`).(Scalar); s.V != 0 {
		t.Fatalf("scalar(): %v", s.V)
	}
	if s := instant(t, `scalar(http_requests_total)`).(Scalar); !math.IsNaN(s.V) {
		t.Fatal("scalar() of multiple series is NaN")
	}
	ab := vec(t, `absent(nope{job="x"})`)
	if ab[`{job="x"}`] != 1 {
		t.Fatalf("absent: %v", ab)
	}
	if len(vec(t, `absent(queue_depth)`)) != 0 {
		t.Fatal("absent of present series must be empty")
	}
	cl := vec(t, `clamp_max(http_requests_total, 500)`)
	for _, v := range cl {
		if v > 500 {
			t.Fatalf("clamp_max: %v", cl)
		}
	}
	if v := vec(t, `clamp_min(queue_depth, 3)`); v[`{job="api"}`] != 3 {
		t.Fatalf("clamp_min: %v", v)
	}
	if v := vec(t, `abs(-queue_depth - 2)`); v[`{job="api"}`] != 2 {
		t.Fatalf("abs: %v", v)
	}
	if v := vec(t, `sqrt(vector(16)) + ceil(vector(0.2)) + floor(vector(1.8))`); v["{}"] != 6 {
		t.Fatalf("math funcs: %v", v)
	}
	off := vec(t, `http_requests_total{code="500"} offset 5m`)
	if off[`http_requests_total{code="500", job="api"}`] != 300 {
		t.Fatalf("offset: %v", off)
	}
	if s, ok := instant(t, `"hello"`).(String); !ok || s.V != "hello" {
		t.Fatal("string literal")
	}
	m, ok := instant(t, `queue_depth[1m]`).(Matrix)
	if !ok || len(m) != 1 || len(m[0].Points) != 4 {
		t.Fatalf("matrix selector: %v", m)
	}
}

func TestLookbackAndStaleness(t *testing.T) {
	e := &Engine{LookbackDelta: time.Minute}
	v, _ := e.Instant(context.Background(), testStore(), `queue_depth`, at.Add(2*time.Minute))
	if len(v.(Vector)) != 0 {
		t.Fatal("samples older than the lookback delta must not be returned")
	}
}

func TestRangeQuery(t *testing.T) {
	e := &Engine{MaxSteps: 100}
	m, err := e.Range(context.Background(), testStore(), `sum(rate(http_requests_total[1m]))`, base.Add(2*time.Minute), at, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 1 || len(m[0].Points) != 9 {
		t.Fatalf("range result %+v", m)
	}
	for _, p := range m[0].Points {
		if !near(p.V, 3+0.5) && !(p.V > 3.4 && p.V < 3.6) {
			t.Fatalf("unexpected rate %v", p.V)
		}
	}
	sc, err := e.Range(context.Background(), testStore(), `1 + 1`, base, at, 5*time.Minute)
	if err != nil || len(sc) != 1 || len(sc[0].Points) != 3 {
		t.Fatalf("scalar range: %+v %v", sc, err)
	}
	if _, err := e.Range(context.Background(), testStore(), `up`, base, base.Add(time.Hour), time.Second); !errors.Is(err, ErrTooManySteps) {
		t.Fatalf("expected too many steps, got %v", err)
	}
	if _, err := e.Range(context.Background(), testStore(), `up[5m]`, base, at, time.Minute); err == nil {
		t.Fatal("range query over a matrix must fail")
	}
	if _, err := e.Range(context.Background(), testStore(), `up`, at, base, time.Minute); err == nil {
		t.Fatal("inverted range must fail")
	}
}

func TestSampleLimit(t *testing.T) {
	e := &Engine{MaxSamples: 50}
	_, err := e.Instant(context.Background(), testStore(), `rate(http_requests_total[10m])`, at)
	if !errors.Is(err, ErrTooManySamples) {
		t.Fatalf("expected sample limit error, got %v", err)
	}
}

func TestParseErrors(t *testing.T) {
	bad := []string{
		`rate(foo)`, `sum(foo[5m])`, `foo[5m] + 1`, `{job=~".*"}`, `foo{job="x"`, `foo{job=x}`,
		`unknown_fn(foo)`, `topk(foo)`, `histogram_quantile(foo, bar)`, `1 > 2`, `foo and 1`,
		`foo / on(job) group_left bar`, `(foo)[5m]`, `foo offset`, `foo[5x]`, `rate(foo[5m], 1)`,
		`foo{a="b"} bar`, `"unterminated`, `foo $ bar`, `sum by (1) (foo)`, `1 + on(x) 2`,
		strings.Repeat("(", 200) + "1" + strings.Repeat(")", 200),
	}
	for _, q := range bad {
		if _, err := Parse(q); err == nil {
			t.Errorf("Parse(%q) should fail", q)
		}
	}
}

func TestParseRoundTripStrings(t *testing.T) {
	cases := map[string]string{
		`sum by (job) (rate(http_requests_total{code=~"5.."}[5m] offset 1m))`: `sum by (job)(rate(http_requests_total{code=~"5.."}[5m0s] offset 1m0s))`,
		`a > bool on(x) b`: `a > bool on(x) b`,
		`topk(3, foo)`:     `topk(3, foo)`,
		`{__name__="up"}`:  `{__name__="up"}`,
		`'single' `:        `"single"`,
		"`raw\\n`":         `"raw\\n"`,
	}
	for in, want := range cases {
		e, err := Parse(in)
		if err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if got := e.String(); got != want {
			t.Errorf("Parse(%q).String() = %q, want %q", in, got, want)
		}
	}
	names := FunctionNames()
	if !sort.StringsAreSorted(names) || len(names) < 20 {
		t.Fatalf("function names %v", names)
	}
}
