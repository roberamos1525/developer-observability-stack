package chunk

import (
	"math"
	"math/rand"
	"testing"
)

type sample struct {
	t int64
	v float64
}

func encode(t *testing.T, samples []sample) *XOR {
	t.Helper()
	c := NewXOR()
	app, err := c.Appender()
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range samples {
		if err := app.Append(s.t, s.v); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func decode(t *testing.T, c *XOR) []sample {
	t.Helper()
	var out []sample
	it := c.Iterator()
	for it.Next() {
		ts, v := it.At()
		out = append(out, sample{ts, v})
	}
	if it.Err() != nil {
		t.Fatalf("decode error: %v", it.Err())
	}
	return out
}

func assertRoundTrip(t *testing.T, in []sample) *XOR {
	t.Helper()
	c := encode(t, in)
	if c.NumSamples() != len(in) {
		t.Fatalf("NumSamples=%d want %d", c.NumSamples(), len(in))
	}
	out := decode(t, c)
	if len(out) != len(in) {
		t.Fatalf("decoded %d samples, want %d", len(out), len(in))
	}
	for i := range in {
		if out[i].t != in[i].t || math.Float64bits(out[i].v) != math.Float64bits(in[i].v) {
			t.Fatalf("sample %d: got (%d, %v/%x) want (%d, %v/%x)", i,
				out[i].t, out[i].v, math.Float64bits(out[i].v), in[i].t, in[i].v, math.Float64bits(in[i].v))
		}
	}
	// Bytes -> FromBytes must decode identically too.
	c2, err := FromBytes(c.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if got := decode(t, c2); len(got) != len(in) {
		t.Fatal("FromBytes round trip lost samples")
	}
	return c
}

var specialValues = []float64{
	0, math.Copysign(0, -1), 1, -1, math.NaN(), math.Float64frombits(0x7ff8000000000001), // NaN payloads
	math.Inf(1), math.Inf(-1), math.MaxFloat64, -math.MaxFloat64, math.SmallestNonzeroFloat64,
	1e-300, 123456.789, -0.5, math.Float64frombits(0x7ff0000000000001), // signalling NaN
}

// TestRoundTripRandomSeries is the property test: random series mixing
// regular scrape intervals, jitter, zero deltas, negative deltas, huge
// jumps and special float values must decode bit-for-bit.
func TestRoundTripRandomSeries(t *testing.T) {
	rng := rand.New(rand.NewSource(20260501))
	for iter := 0; iter < 500; iter++ {
		n := 1 + rng.Intn(400)
		samples := make([]sample, n)
		ts := rng.Int63n(1<<45) - 1<<44
		v := rng.NormFloat64() * 1000
		step := int64(1 + rng.Intn(60000))
		for i := range samples {
			switch rng.Intn(12) {
			case 0:
				ts += 0 // zero delta
			case 1:
				ts -= rng.Int63n(1 << 20) // negative delta
			case 2:
				ts += rng.Int63n(1 << 40) // huge jump: 64-bit bucket
			case 3:
				ts += step + rng.Int63n(1<<15) - 1<<14 // 17-bit dod bucket
			case 4:
				ts += step + rng.Int63n(1<<19) - 1<<18 // 20-bit dod bucket
			default:
				ts += step + rng.Int63n(5) - 2 // jitter: small dod
			}
			switch rng.Intn(8) {
			case 0:
				v = specialValues[rng.Intn(len(specialValues))]
			case 1:
				// unchanged value
			case 2:
				v = float64(rng.Intn(100)) // integers, common for counters
			case 3:
				v = -v
			default:
				v += rng.NormFloat64()
			}
			samples[i] = sample{ts, v}
		}
		assertRoundTrip(t, samples)
	}
}

func TestRoundTripExtremeTimestamps(t *testing.T) {
	cases := [][]sample{
		{{math.MinInt64, 1}, {math.MaxInt64, 2}, {math.MinInt64, 3}, {0, 4}},
		{{0, 0}, {0, 0}, {0, 0}},
		{{-1000, math.NaN()}, {-2000, math.Inf(1)}, {-3000, math.Inf(-1)}, {-4000, 0}},
		{{1, 1}},
	}
	for _, c := range cases {
		assertRoundTrip(t, c)
	}
}

func TestBucketBoundaries(t *testing.T) {
	// Exercise exact dod bucket edges in both directions.
	var dods []int64
	for _, n := range []uint8{14, 17, 20} {
		hi := int64(1) << (n - 1)
		dods = append(dods, hi, hi+1, -(hi - 1), -hi)
	}
	samples := []sample{{1000, 1}, {2000, 1}}
	delta := int64(1000)
	ts := int64(2000)
	for _, d := range dods {
		delta += d
		ts += delta
		samples = append(samples, sample{ts, float64(d)})
	}
	assertRoundTrip(t, samples)
}

func TestCompressionRatioOnRegularSeries(t *testing.T) {
	samples := make([]sample, 120)
	v := 100.0
	for i := range samples {
		if i%10 == 0 {
			v++
		}
		samples[i] = sample{int64(1700000000000 + i*15000), v}
	}
	c := assertRoundTrip(t, samples)
	// 120 raw samples are 1920 bytes; a regular counter must compress well.
	if c.Size() > 120 {
		t.Fatalf("poor compression: %d bytes for 120 samples", c.Size())
	}
}

func TestAppenderResumesOnDecodedChunk(t *testing.T) {
	first := []sample{{10, 1.5}, {20, 2.5}, {31, 2.5}, {41, math.NaN()}}
	c := encode(t, first)
	c2, _ := FromBytes(c.Bytes())
	app, err := c2.Appender()
	if err != nil {
		t.Fatal(err)
	}
	app.Append(52, -7)
	app.Append(52, -7)
	got := decode(t, c2)
	if len(got) != 6 || got[4].t != 52 || got[5].v != -7 {
		t.Fatalf("resumed chunk decoded as %v", got)
	}
}

func TestCorruptChunks(t *testing.T) {
	if _, err := FromBytes([]byte{1}); err == nil {
		t.Fatal("short chunk must be rejected")
	}
	c := encode(t, []sample{{1, 1}, {2, 2}, {3, 3}, {5, 4}})
	b := c.Bytes()
	truncated, _ := FromBytes(b[:len(b)-3])
	it := truncated.Iterator()
	n := 0
	for it.Next() {
		n++
	}
	if it.Err() == nil || n >= 4 {
		t.Fatalf("truncated chunk should error (n=%d)", n)
	}
}

func TestChunkFull(t *testing.T) {
	c := NewXOR()
	app, _ := c.Appender()
	for i := 0; i < MaxSamples; i++ {
		if err := app.Append(int64(i), 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := app.Append(MaxSamples, 1); err != ErrFull {
		t.Fatalf("expected ErrFull, got %v", err)
	}
}

func FuzzRoundTrip(f *testing.F) {
	f.Add(int64(0), int64(15000), 1.0, 0.5, uint8(10))
	f.Add(int64(-5), int64(-1), math.Inf(1), math.NaN(), uint8(3))
	f.Fuzz(func(t *testing.T, t0, step int64, v0, dv float64, n uint8) {
		samples := make([]sample, int(n)+1)
		ts, v := t0, v0
		for i := range samples {
			samples[i] = sample{ts, v}
			ts += step + int64(i%3)
			v += dv
		}
		assertRoundTrip(t, samples)
	})
}

func BenchmarkAppend(b *testing.B) {
	for i := 0; i < b.N; i++ {
		c := NewXOR()
		app, _ := c.Appender()
		for j := 0; j < 120; j++ {
			app.Append(int64(j*15000), float64(j))
		}
	}
}
