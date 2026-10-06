package pool

import "context"

// submitToWorker enqueues a task directly onto worker idx's local queue,
// bypassing round-robin placement. Test-only helper to create load skew.
func (p *Pool) submitToWorker(idx int, prio Priority, fn TaskFunc) (*Task, error) {
	if p.closing.Load() {
		return nil, ErrClosed
	}
	tctx, cancel := context.WithCancel(context.Background())
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
	if !p.workers[idx].pushLocal(t) {
		p.pending.Done()
		return nil, ErrQueueFull
	}
	p.workers[idx].signal()
	p.stats.Submitted.Add(1)
	return t, nil
}
