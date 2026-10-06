// Package pool implements a local work-stealing task executor with
// adaptive load balancing, bounded queues, priorities, graceful
// shutdown and panic isolation.
package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// OverflowPolicy decides what SubmitTask does when every local queue is full.
type OverflowPolicy int

const (
	// OverflowBlock waits (honouring the submitted ctx) until a slot frees.
	OverflowBlock OverflowPolicy = iota
	// OverflowReject makes SubmitTask return ErrQueueFull immediately.
	OverflowReject
	// OverflowRunInline runs the task on the calling goroutine.
	OverflowRunInline
)

// Config tunes the executor.
type Config struct {
	Workers        int            // number of worker goroutines
	QueueCapacity  int            // per-worker, per-priority queue capacity
	Policy         OverflowPolicy // behaviour when all queues are full
	PanicHandler   func(taskID uint64, v any)
	StarveEvery    int // after N consecutive high-prio runs, serve lower prio (0 = never)
	StealBatchSize int // max tasks a thief takes per steal attempt
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.Workers <= 0 {
		out.Workers = 4
	}
	if out.QueueCapacity <= 0 {
		out.QueueCapacity = 256
	}
	if out.StealBatchSize <= 0 {
		out.StealBatchSize = 1
	}
	return out
}

var (
	// ErrClosed is returned when submitting to a shut-down pool.
	ErrClosed = errors.New("pool: executor is closed")
	// ErrQueueFull is returned under OverflowReject when no queue has room.
	ErrQueueFull = errors.New("pool: all worker queues are full")
)

// Stats exposes observability counters.
type Stats struct {
	Submitted atomic.Uint64
	Completed atomic.Uint64
	Panicked  atomic.Uint64
	Rejected  atomic.Uint64
	Stolen    atomic.Uint64
	Cancelled atomic.Uint64
	Inlined   atomic.Uint64
}

// Pool is the work-stealing executor.
type Pool struct {
	cfg     Config
	workers []*worker
	stats   Stats

	nextID atomic.Uint64
	rr     atomic.Uint64 // round-robin seed for submission and stealing

	// submitMu guards the closing flag against in-flight submissions so
	// that Drain never observes a pending count that can still grow.
	submitMu sync.RWMutex
	closing  atomic.Bool

	wg      sync.WaitGroup // running workers
	pending sync.WaitGroup // accepted tasks not yet terminal
}

// New creates and starts a Pool.
func New(cfg Config) *Pool {
	c := cfg.withDefaults()
	p := &Pool{cfg: c, workers: make([]*worker, c.Workers)}
	for i := range p.workers {
		p.workers[i] = newWorker(i, p)
	}
	for _, w := range p.workers {
		p.wg.Add(1)
		go w.run()
	}
	return p
}

// Submit enqueues fn with normal priority; see SubmitTask.
func (p *Pool) Submit(ctx context.Context, fn TaskFunc) (*Task, error) {
	return p.SubmitTask(ctx, PriorityNormal, fn)
}

// SubmitTask enqueues fn. It never silently drops a task: depending on
// the configured OverflowPolicy it blocks (ctx-cancelable), returns
// ErrQueueFull, or runs the task inline on the caller.
func (p *Pool) SubmitTask(ctx context.Context, prio Priority, fn TaskFunc) (*Task, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	p.submitMu.RLock()
	defer p.submitMu.RUnlock()
	if p.closing.Load() {
		return nil, ErrClosed
	}

	tctx, cancel := context.WithCancel(ctx)
	t := &Task{
		id:     p.nextID.Add(1),
		fn:     fn,
		prio:   clampPriority(prio),
		ctx:    tctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
	p.pending.Add(1)
	t.onTerminal = p.pending.Done

	if p.tryEnqueue(t) {
		p.stats.Submitted.Add(1)
		return t, nil
	}

	switch p.cfg.Policy {
	case OverflowReject:
		p.stats.Rejected.Add(1)
		cancel()
		t.state.Store(int32(stateCancelled))
		t.terminal()
		return nil, ErrQueueFull
	case OverflowRunInline:
		p.stats.Submitted.Add(1)
		p.executeInline(t)
		return t, nil
	default: // OverflowBlock
		for !p.tryEnqueue(t) {
			select {
			case <-ctx.Done():
				p.stats.Rejected.Add(1)
				cancel()
				t.state.Store(int32(stateCancelled))
				t.terminal()
				return nil, ctx.Err()
			case <-time.After(time.Millisecond):
			}
		}
		p.stats.Submitted.Add(1)
		return t, nil
	}
}

// tryEnqueue attempts a non-blocking push to any worker's local queue,
// starting at a round-robin offset to spread load. On success the target
// worker is signalled.
func (p *Pool) tryEnqueue(t *Task) bool {
	n := len(p.workers)
	start := int(p.rr.Add(1)) % n
	for i := 0; i < n; i++ {
		w := p.workers[(start+i)%n]
		if w.pushLocal(t) {
			w.signal()
			return true
		}
	}
	return false
}

// Shutdown stops accepting new tasks. Already-accepted tasks keep running
// and queued tasks are still drained by the workers; use Drain to wait.
func (p *Pool) Shutdown() {
	p.submitMu.Lock()
	p.closing.Store(true)
	p.submitMu.Unlock()
	for _, w := range p.workers {
		w.signal()
	}
}

// Drain waits until every accepted task has reached a terminal state and
// all workers have exited. Call Shutdown first (or rely on a prior one).
func (p *Pool) Drain() {
	p.pending.Wait()
	p.wg.Wait()
}

// Stats returns the pool counters (live, atomically updated).
func (p *Pool) Stats() *Stats { return &p.stats }
