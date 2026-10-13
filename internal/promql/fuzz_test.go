package promql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/errs"
	"github.com/robertramos/developer-observability-stack/internal/tsdb"
	"github.com/robertramos/developer-observability-stack/internal/tsdb/labels"
)

// FuzzParseAndEval ensures arbitrary queries never panic the parser or
// the evaluator, and that every failure carries an errs.Kind.
func FuzzParseAndEval(f *testing.F) {
	for _, seed := range []string{
		`sum by (job) (rate(http_requests_total[5m]))`, `histogram_quantile(0.9, lat_bucket)`,
		`a / on(job) b`, `topk(2, x) or vector(1)`, `-1 ^ 2 > bool 0`, `foo offset -5m`,
		`count_over_time(queue_depth[1m:])`, `{job=~"a|b"}`, `quantile(2, x)`, `scalar(queue_depth) * 2`,
	} {
		f.Add(seed)
	}
	store := testStore()
	e := &Engine{MaxSamples: 100_000, MaxSteps: 50}
	f.Fuzz(func(t *testing.T, q string) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := e.Instant(ctx, store, q, at); err != nil {
			var ke *errs.Error
			if !errors.As(err, &ke) {
				t.Fatalf("untyped error for %q: %v", q, err)
			}
		}
	})
}

func TestEngineErrorKinds(t *testing.T) {
	e := &Engine{}
	_, err := e.Instant(context.Background(), testStore(), "rate(", at)
	if !errs.Is(err, errs.KindInvalid) {
		t.Fatalf("syntax errors are invalid_argument: %v", err)
	}
	_, err = e.Instant(context.Background(), failingStore{}, "up", at)
	if !errs.Is(err, errs.KindInternal) {
		t.Fatalf("storage errors are internal: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = e.Instant(ctx, testStore(), "up", at)
	if !errs.Is(err, errs.KindTimeout) {
		t.Fatalf("cancellation is a timeout: %v", err)
	}
	_, err = e.Instant(context.Background(), testStore(), `http_requests_total + on(job) http_requests_total`, at)
	if !errs.Is(err, errs.KindInvalid) {
		t.Fatalf("many-to-many matching is invalid_argument: %v", err)
	}
}

type failingStore struct{}

func (failingStore) Select(context.Context, []*labels.Matcher, int64, int64) ([]tsdb.Series, error) {
	return nil, errors.New("disk on fire")
}
