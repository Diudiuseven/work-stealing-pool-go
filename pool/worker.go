package pool

import (
	"runtime"
	"time"
)

// parkTimeout bounds how long a worker sleeps before re-scanning queues,
// so a missed wakeup can never stall the pool.
const parkTimeout = 2 * time.Millisecond

// worker owns per-priority local deques and runs a loop: prefer local
// work (hot, LIFO), otherwise steal from peers (cold, FIFO from the top),
// otherwise park until notified.
type worker struct {
	id     int
	pool   *Pool
	queues [numPriorities]*deque
	notify chan struct{}

	fairCtr int // consecutive high-priority runs, for starvation avoidance
}

func newWorker(id int, p *Pool) *worker {
	w := &worker{id: id, pool: p, notify: make(chan struct{}, 1)}
	for i := range w.queues {
		w.queues[i] = newDeque(p.cfg.QueueCapacity)
	}
	return w
}

// signal wakes the worker if it is parked; never blocks.
func (w *worker) signal() {
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

// pushLocal enqueues t on this worker's queue for t.prio.
func (w *worker) pushLocal(t *Task) bool { return w.queues[t.prio].pushBottom(t) }

// popLocal takes the next local task, honouring the fairness rule:
// after StarveEvery consecutive higher-priority runs, a waiting
// lower-priority task is taken first so it cannot starve.
func (w *worker) popLocal() *Task {
	starve := w.pool.cfg.StarveEvery
	order := [numPriorities]int{int(PriorityHigh), int(PriorityNormal), int(PriorityLow)}
	if starve > 0 && w.fairCtr >= starve {
		// Scan from lowest priority up: give starved tasks a turn.
		for p := int(PriorityLow); p <= int(PriorityHigh); p++ {
			if t := w.queues[p].popBottom(); t != nil {
				if p == int(PriorityHigh) {
					w.fairCtr++
				} else {
					w.fairCtr = 0
				}
				return t
			}
		}
		return nil
	}
	for _, p := range order {
		if t := w.queues[p].popBottom(); t != nil {
			if p == int(PriorityHigh) {
				w.fairCtr++
			} else {
				w.fairCtr = 0
			}
			return t
		}
	}
	return nil
}

// steal takes up to batch tasks from peers, oldest first. The first
// stolen task is returned for immediate execution; the rest are pushed
// into the thief's own local queue (executed directly if it is full).
func (w *worker) steal() *Task {
	peers := w.pool.workers
	n := len(peers)
	if n <= 1 {
		return nil
	}
	batch := w.pool.cfg.StealBatchSize
	start := int(w.pool.rr.Add(1)) % n
	for i := 0; i < n; i++ {
		v := peers[(start+i)%n]
		if v == w {
			continue
		}
		// Steal from the victim's highest-priority non-empty queue.
		for p := int(PriorityHigh); p >= int(PriorityLow); p-- {
			first := v.queues[p].stealTop()
			if first == nil {
				continue
			}
			w.pool.stats.Stolen.Add(1)
			for k := 1; k < batch; k++ {
				extra := v.queues[p].stealTop()
				if extra == nil {
					break
				}
				w.pool.stats.Stolen.Add(1)
				if !w.pushLocal(extra) {
					// Local queue full: run it right away, never drop.
					w.pool.executeTask(extra)
				}
			}
			return first
		}
	}
	return nil
}

// empty reports whether every local queue of every worker is empty.
func (p *Pool) quiescent() bool {
	for _, w := range p.workers {
		for _, q := range w.queues {
			if q.len() > 0 {
				return false
			}
		}
	}
	return true
}

// run is the worker main loop.
func (w *worker) run() {
	defer w.pool.wg.Done()
	for {
		if t := w.popLocal(); t != nil {
			w.pool.executeTask(t)
			continue
		}
		if t := w.steal(); t != nil {
			w.pool.executeTask(t)
			continue
		}
		// No work found. After closing, quiescence means we may exit;
		// queued tasks left with peers are still drained by their owners
		// or by other thieves, and no new tasks can arrive.
		if w.pool.closing.Load() && w.pool.quiescent() {
			return
		}
		select {
		case <-w.notify:
		case <-time.After(parkTimeout):
		}
	}
}

// executeTask runs one task with panic isolation and exactly-once
// terminal handling. A task cancelled before this point is skipped.
func (p *Pool) executeTask(t *Task) {
	if !t.state.CompareAndSwap(int32(statePending), int32(stateRunning)) {
		return // cancelled before execution; terminal() already ran
	}
	defer func() {
		if v := recover(); v != nil {
			t.panicVal = v
			p.stats.Panicked.Add(1)
			if p.cfg.PanicHandler != nil {
				p.cfg.PanicHandler(t.id, v)
			}
		}
		t.state.Store(int32(stateDone))
		t.terminal()
		p.stats.Completed.Add(1)
	}()
	t.err = t.fn(t.ctx)
}

// executeInline runs a task on the caller's goroutine (OverflowRunInline),
// yielding the current goroutine first to avoid deep recursion when the
// caller is itself a pool task.
func (p *Pool) executeInline(t *Task) {
	runtime.Gosched()
	p.stats.Inlined.Add(1)
	p.executeTask(t)
}
