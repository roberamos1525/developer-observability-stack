// Package queue implements bounded, batching ingestion pipelines.
//
// Producers Submit jobs (a tenant plus a slice of items). A fixed pool of
// workers drains the queue, coalescing jobs into batches of up to BatchSize
// items (or whatever arrived within FlushInterval) and hands each tenant's
// batch to the handler. When the queue is full, Submit waits at most
// SubmitTimeout and then fails with an Unavailable error, which the HTTP
// layer turns into 429 + Retry-After: that is the backpressure signal.
package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robertramos/developer-observability-stack/internal/errs"
)

// Handler persists one tenant's batch.
type Handler[T any] func(ctx context.Context, tenant string, items []T) error

// Observer receives pipeline events; implementations must be concurrency safe.
type Observer interface {
	Accepted(queue string, items int)
	Rejected(queue string, items int)
	Processed(queue string, items int, took time.Duration, err error)
}

type nopObserver struct{}

func (nopObserver) Accepted(string, int)                        {}
func (nopObserver) Rejected(string, int)                        {}
func (nopObserver) Processed(string, int, time.Duration, error) {}

// Options configure a Pipeline.
type Options struct {
	Name          string
	Workers       int
	Capacity      int
	BatchSize     int
	FlushInterval time.Duration
	SubmitTimeout time.Duration
	Observer      Observer
}

type job[T any] struct {
	tenant string
	items  []T
	done   chan error
}

// Pipeline is a bounded multi-worker batching queue.
type Pipeline[T any] struct {
	opts    Options
	handler Handler[T]
	ch      chan *job[T]

	mu      sync.RWMutex // guards closed; senders hold the read lock
	closed  bool
	pending atomic.Int64
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	idle    chan struct{} // signalled when pending drops to zero
}

// ErrQueueFull is returned (wrapped) when backpressure rejects a submission.
var ErrQueueFull = errs.Unavailable("ingestion queue is full, retry later")

// New creates and starts a pipeline.
func New[T any](opts Options, h Handler[T]) *Pipeline[T] {
	if opts.Workers < 1 {
		opts.Workers = 1
	}
	if opts.Capacity < 1 {
		opts.Capacity = 1
	}
	if opts.BatchSize < 1 {
		opts.BatchSize = 1
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = 100 * time.Millisecond
	}
	if opts.Observer == nil {
		opts.Observer = nopObserver{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pipeline[T]{
		opts: opts, handler: h, ch: make(chan *job[T], opts.Capacity),
		ctx: ctx, cancel: cancel, idle: make(chan struct{}, 1),
	}
	p.wg.Add(opts.Workers)
	for i := 0; i < opts.Workers; i++ {
		go p.worker()
	}
	return p
}

// Name returns the pipeline name.
func (p *Pipeline[T]) Name() string { return p.opts.Name }

// Depth returns the number of queued (not yet picked up) jobs.
func (p *Pipeline[T]) Depth() int { return len(p.ch) }

// Capacity returns the queue capacity in jobs.
func (p *Pipeline[T]) Capacity() int { return cap(p.ch) }

// Pending returns the number of submitted jobs not yet processed.
func (p *Pipeline[T]) Pending() int64 { return p.pending.Load() }

// Submit enqueues items for tenant. When wait is true it blocks until the
// batch containing the items has been handled and returns its error.
func (p *Pipeline[T]) Submit(ctx context.Context, tenant string, items []T, wait bool) error {
	if len(items) == 0 {
		return nil
	}
	j := &job[T]{tenant: tenant, items: items}
	if wait {
		j.done = make(chan error, 1)
	}
	if err := p.enqueue(ctx, j); err != nil {
		p.opts.Observer.Rejected(p.opts.Name, len(items))
		return err
	}
	p.opts.Observer.Accepted(p.opts.Name, len(items))
	if !wait {
		return nil
	}
	select {
	case err := <-j.done:
		return err
	case <-ctx.Done():
		return errs.Wrap(errs.KindTimeout, ctx.Err(), "waiting for ingestion")
	}
}

func (p *Pipeline[T]) enqueue(ctx context.Context, j *job[T]) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return errs.Unavailable("pipeline %s is shutting down", p.opts.Name)
	}
	p.pending.Add(1)
	select {
	case p.ch <- j:
		return nil
	default:
	}
	if p.opts.SubmitTimeout <= 0 {
		p.done(1)
		return ErrQueueFull
	}
	t := time.NewTimer(p.opts.SubmitTimeout)
	defer t.Stop()
	select {
	case p.ch <- j:
		return nil
	case <-t.C:
		p.done(1)
		return ErrQueueFull
	case <-ctx.Done():
		p.done(1)
		return errs.Wrap(errs.KindTimeout, ctx.Err(), "enqueue")
	}
}

func (p *Pipeline[T]) done(n int64) {
	if p.pending.Add(-n) == 0 {
		select {
		case p.idle <- struct{}{}:
		default:
		}
	}
}

func (p *Pipeline[T]) worker() {
	defer p.wg.Done()
	for {
		first, ok := <-p.ch
		if !ok {
			return
		}
		batch := []*job[T]{first}
		n := len(first.items)
		timer := time.NewTimer(p.opts.FlushInterval)
	collect:
		for n < p.opts.BatchSize {
			select {
			case j, ok := <-p.ch:
				if !ok {
					break collect
				}
				batch = append(batch, j)
				n += len(j.items)
			case <-timer.C:
				break collect
			}
			// Do not hold a partially filled batch while the queue is idle:
			// latency matters more than batch size once the burst ends.
			if len(p.ch) == 0 {
				break
			}
		}
		timer.Stop()
		p.process(batch)
	}
}

func (p *Pipeline[T]) process(batch []*job[T]) {
	byTenant := map[string][]*job[T]{}
	order := []string{}
	for _, j := range batch {
		if _, ok := byTenant[j.tenant]; !ok {
			order = append(order, j.tenant)
		}
		byTenant[j.tenant] = append(byTenant[j.tenant], j)
	}
	for _, tenant := range order {
		jobs := byTenant[tenant]
		var items []T
		if len(jobs) == 1 {
			items = jobs[0].items
		} else {
			for _, j := range jobs {
				items = append(items, j.items...)
			}
		}
		start := time.Now()
		err := p.safeHandle(tenant, items)
		p.opts.Observer.Processed(p.opts.Name, len(items), time.Since(start), err)
		for _, j := range jobs {
			if j.done != nil {
				j.done <- err
			}
		}
		p.done(int64(len(jobs)))
	}
}

func (p *Pipeline[T]) safeHandle(tenant string, items []T) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errs.New(errs.KindInternal, "ingest handler panic: %v", r)
		}
	}()
	return p.handler(p.ctx, tenant, items)
}

// Flush blocks until every job submitted so far has been processed.
func (p *Pipeline[T]) Flush(ctx context.Context) error {
	for p.pending.Load() > 0 {
		select {
		case <-p.idle:
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Close stops accepting jobs, drains the queue and waits for the workers.
func (p *Pipeline[T]) Close(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.ch)
	p.mu.Unlock()
	finished := make(chan struct{})
	go func() { p.wg.Wait(); close(finished) }()
	select {
	case <-finished:
		p.cancel()
		return nil
	case <-ctx.Done():
		p.cancel()
		return ctx.Err()
	}
}
