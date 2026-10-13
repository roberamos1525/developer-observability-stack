package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/errs"
)

type countingObserver struct{ accepted, rejected, processed, failed atomic.Int64 }

func (o *countingObserver) Accepted(_ string, n int) { o.accepted.Add(int64(n)) }
func (o *countingObserver) Rejected(_ string, n int) { o.rejected.Add(int64(n)) }
func (o *countingObserver) Processed(_ string, n int, _ time.Duration, err error) {
	if err != nil {
		o.failed.Add(int64(n))
		return
	}
	o.processed.Add(int64(n))
}

func TestBatchingPerTenant(t *testing.T) {
	var mu sync.Mutex
	got := map[string][]int{}
	batches := 0
	release := make(chan struct{})
	obs := &countingObserver{}
	p := New(Options{Name: "t", Workers: 1, Capacity: 100, BatchSize: 50, FlushInterval: 50 * time.Millisecond, Observer: obs},
		func(_ context.Context, tenant string, items []int) error {
			<-release
			mu.Lock()
			defer mu.Unlock()
			got[tenant] = append(got[tenant], items...)
			batches++
			return nil
		})
	ctx := context.Background()
	// The first job blocks the single worker so the rest queue up and are
	// coalesced into per-tenant batches.
	p.Submit(ctx, "a", []int{0}, false)
	time.Sleep(20 * time.Millisecond)
	for i := 1; i <= 10; i++ {
		tenant := "a"
		if i%2 == 0 {
			tenant = "b"
		}
		if err := p.Submit(ctx, tenant, []int{i, i}, false); err != nil {
			t.Fatal(err)
		}
	}
	close(release)
	if err := p.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(got["a"]) != 11 || len(got["b"]) != 10 {
		t.Fatalf("items %v", got)
	}
	if batches > 4 {
		t.Fatalf("expected coalesced batches, got %d", batches)
	}
	if obs.accepted.Load() != 21 || obs.processed.Load() != 21 {
		t.Fatalf("observer %d/%d", obs.accepted.Load(), obs.processed.Load())
	}
	p.Close(ctx)
}

func TestBackpressureAndSyncErrors(t *testing.T) {
	block := make(chan struct{})
	obs := &countingObserver{}
	p := New(Options{Name: "bp", Workers: 1, Capacity: 1, BatchSize: 1, SubmitTimeout: 10 * time.Millisecond, Observer: obs},
		func(_ context.Context, tenant string, items []string) error {
			<-block
			if items[0] == "bad" {
				return errors.New("disk full")
			}
			return nil
		})
	ctx := context.Background()
	p.Submit(ctx, "t", []string{"x"}, false) // picked up by the worker, blocks
	time.Sleep(20 * time.Millisecond)
	p.Submit(ctx, "t", []string{"y"}, false) // fills the queue
	err := p.Submit(ctx, "t", []string{"z"}, false)
	if !errs.Is(err, errs.KindUnavailable) || obs.rejected.Load() != 1 {
		t.Fatalf("expected backpressure, got %v", err)
	}
	if p.Depth() != 1 || p.Capacity() != 1 || p.Pending() != 2 {
		t.Fatalf("depth=%d pending=%d", p.Depth(), p.Pending())
	}
	close(block)
	if err := p.Submit(ctx, "t", []string{"bad"}, true); err == nil || err.Error() != "disk full" {
		t.Fatalf("sync submit must return handler error, got %v", err)
	}
	if obs.failed.Load() != 1 {
		t.Fatal("failure not observed")
	}
	if err := p.Submit(ctx, "t", nil, true); err != nil {
		t.Fatal("empty submit is a no-op")
	}
	p.Close(ctx)
	if err := p.Submit(ctx, "t", []string{"late"}, false); !errs.Is(err, errs.KindUnavailable) {
		t.Fatalf("submit after close: %v", err)
	}
	if err := p.Close(ctx); err != nil {
		t.Fatal("double close")
	}
}

func TestPanicIsContained(t *testing.T) {
	p := New(Options{Name: "p", Workers: 2, Capacity: 4, BatchSize: 1},
		func(_ context.Context, _ string, items []int) error {
			if items[0] < 0 {
				panic("boom")
			}
			return nil
		})
	defer p.Close(context.Background())
	if err := p.Submit(context.Background(), "t", []int{-1}, true); !errs.Is(err, errs.KindInternal) {
		t.Fatalf("panic must become an internal error: %v", err)
	}
	if err := p.Submit(context.Background(), "t", []int{1}, true); err != nil {
		t.Fatal("worker must survive a panic")
	}
}

func TestCloseDrainsQueue(t *testing.T) {
	var processed atomic.Int64
	p := New(Options{Name: "d", Workers: 2, Capacity: 1000, BatchSize: 10, FlushInterval: time.Millisecond},
		func(_ context.Context, _ string, items []int) error {
			time.Sleep(time.Millisecond)
			processed.Add(int64(len(items)))
			return nil
		})
	for i := 0; i < 200; i++ {
		p.Submit(context.Background(), "t", []int{i}, false)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if processed.Load() != 200 {
		t.Fatalf("drained %d of 200", processed.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	slow := New(Options{Name: "s", Workers: 1, Capacity: 10, BatchSize: 1}, func(context.Context, string, []int) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	slow.Submit(context.Background(), "t", []int{1}, false)
	if err := slow.Flush(ctx); err == nil {
		t.Fatal("flush must honour cancellation")
	}
	slow.Close(context.Background())
}
