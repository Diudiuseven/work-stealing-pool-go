package wspool

import (
	"context"
	"errors"
	"math/rand"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// Executor is the adaptive work-stealing local task executor.
//
// Quick semantics (README.md has the full description):
//
//   - Submit never blocks the caller. Saturation is handled by the configured
//     RejectPolicy and every task is reported through OnResult exactly once.
//   - Each worker owns a three-level (priority) deque: LIFO pop for the owner
//     keeps hot tasks local; idle workers steal the oldest tasks (FIFO) from
//     busy victims.
//   - Each accepted task runs its body at most/exactly once. All terminal
//     transitions funnel through a single CAS so cancellation, stealing,
//     worker count and shutdown cannot duplicate or lose a task.
//   - Shutdown drains queued tasks; Close discards unstarted tasks; in both
//     cases running tasks are never interrupted.
type Executor struct {
	cfg      Config
	workers  []*worker
	wake     chan struct{}
	shutdown chan struct{} // closed once shutdown/close begins

	mu       sync.Mutex
	cond     *sync.Cond // broadcasts pending-count changes
	stopping bool
	discard  bool // hard close: unstarted jobs are discarded
	closed   bool
	pending  int64

	global   *globalQueue
	overflow *overflowQueue

	nextID uint64
	rr     uint64 // round-robin placement cursor

	doneCh chan struct{}
	wg     sync.WaitGroup

	// monotonic counters (atomic)
	submittedN uint64
	completedN uint64
	failedN    uint64
	discardedN uint64
	canceledN  uint64
	rejectedN  uint64
	stolenN    uint64
	runningN   int64

	idleMu sync.Mutex
	idle   []*worker
}

// New builds an Executor from cfg (nil/defaults allowed) and starts workers.
func New(cfg *Config) (*Executor, error) {
	if cfg == nil {
		cfg = NewConfig()
	}
	c := *cfg
	c.normalize()
	n := c.Workers
	e := &Executor{
		cfg:      c,
		workers:  make([]*worker, n),
		wake:     make(chan struct{}, 1),
		shutdown: make(chan struct{}),
		doneCh:   make(chan struct{}),
		global:   newGlobalQueue(c.AgeTick),
		overflow: newOverflowQueue(c.OverflowCapacity),
	}
	e.cond = sync.NewCond(&e.mu)
	for i := 0; i < n; i++ {
		w := &worker{id: i, pool: e, local: newLocalDeque(c.LocalCapacity), rng: rand.New(rand.NewSource(time.Now().UnixNano() + int64(i)))}
		w.wakeCh = make(chan struct{}, 1)
		e.workers[i] = w
	}
	for i := range e.workers {
		e.wg.Add(1)
		go e.workers[i].loop()
	}
	return e, nil
}

// Submit enqueues task with PriNormal.
func (e *Executor) Submit(task Task) (Handle, error) {
	return e.submit(task, PriNormal, nil)
}

// SubmitPri enqueues task with an explicit priority.
func (e *Executor) SubmitPri(task Task, pri Priority) (Handle, error) {
	return e.submit(task, pri, nil)
}

// SubmitCtx enqueues task bound to ctx. If ctx is canceled before the task
// starts running, the task is skipped and reported as context.Canceled; after
// the task started only cooperative cancellation through the task context
// applies.
func (e *Executor) SubmitCtx(ctx context.Context, task Task, pri Priority) (Handle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return e.submit(task, pri, ctx)
}

func (e *Executor) submit(task Task, pri Priority, parent context.Context) (Handle, error) {
	if task == nil {
		return nil, errors.New("wspool: nil task")
	}
	if parent == nil {
		parent = context.Background()
	}
	id := atomic.AddUint64(&e.nextID, 1)
	ctx, cancel := context.WithCancel(parent)
	j := &job{
		id:       id,
		task:     task,
		pri:      pri,
		enqAt:    time.Now(),
		deadline: id,
		state:    jobStateQueued,
		done:     make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
		owner:    -1,
		pool:     e,
	}
	// Parent-context cancellation races submission; AfterFunc is registered
	// only after the job is safely placed (an already-canceled ctx then fires
	// it immediately, never leaving a canceled-state zombie in a queue).
	var stopAfter func() bool

	atomic.AddUint64(&e.submittedN, 1)
	atomic.AddInt64(&e.pending, 1)

	runOnCaller := false
	e.mu.Lock()
	if e.stopping {
		e.mu.Unlock()
		atomic.AddUint64(&e.rejectedN, 1)
		e.terminalize(j, jobStateDiscarded, ErrExecutorStopped, false)
		return &jobHandle{j: j}, ErrExecutorStopped
	}
	switch {
	case e.cfg.Fairness:
		e.global.push(j)
	default:
		idx := int(atomic.AddUint64(&e.rr, 1)-1) % len(e.workers)
		j.owner = idx
		if e.workers[idx].local.pushBottom(j) {
			break
		}
		// The chosen local deque is saturated.
		switch e.cfg.Policy {
		case RejectCallerRun:
			runOnCaller = true
		case RejectOverflow:
			if e.overflow.push(j) {
				j.owner = -1
				break
			}
			// Bounded overflow is also full: explicit reject, never silent.
			atomic.AddUint64(&e.rejectedN, 1)
			e.mu.Unlock()
			e.terminalize(j, jobStateDiscarded, ErrQueueFull, false)
			return &jobHandle{j: j}, ErrQueueFull
		default: // RejectReject
			atomic.AddUint64(&e.rejectedN, 1)
			e.mu.Unlock()
			e.terminalize(j, jobStateDiscarded, ErrQueueFull, false)
			return &jobHandle{j: j}, ErrQueueFull
		}
	}
	e.mu.Unlock()
	stopAfter = context.AfterFunc(ctx, func() { e.cancelJob(j) })
	e.signal()

	if runOnCaller {
		// Caller-run back-pressure: the task is accepted (counted in pending)
		// and executed on the submitting goroutine.
		stopAfter()
		e.execute(j, -1)
	}
	return &jobHandle{j: j}, nil
}

// cancelJob implements Handle.Cancel; see the state-machine comment in
// handle.go. Safe to call after the job is already terminal.
func (e *Executor) cancelJob(j *job) bool {
	if atomic.CompareAndSwapUint32(&j.state, jobStateQueued, jobStateCanceled) {
		// Won the right to terminate. Context is canceled first so a worker
		// that popped the job in the same instant observes it regardless of
		// which queue-removal race it wins.
		j.cancel()
		e.removeFromQueues(j)
		atomic.AddUint64(&e.canceledN, 1)
		e.terminalizeCAS(j, jobStateCanceled, context.Canceled, false)
		return true
	}
	switch atomic.LoadUint32(&j.state) {
	case jobStateRunning:
		j.cancel() // cooperative: running task runs to completion
		return false
	default:
		return false
	}
}

// removeFromQueues best-effort deletes a not-running job from wherever it is
// queued. A job is in at most one queue at a time; popping thieves never
// re-enqueue, so one scan suffices.
func (e *Executor) removeFromQueues(j *job) {
	if j.owner >= 0 && j.owner < len(e.workers) {
		if x := e.workers[j.owner].local.remove(j.id); x != nil {
			return
		}
	}
	if e.global.remove(j.id) != nil {
		return
	}
	e.overflow.remove(j.id)
}

// claim is the single gateway to executing a job. Only the goroutine that
// wins queued->running runs the body.
func (e *Executor) claim(j *job) bool {
	return atomic.CompareAndSwapUint32(&j.state, jobStateQueued, jobStateRunning)
}

// execute runs a claimed job with full panic isolation, then terminalizes it
// exactly once. wID is the executing worker id or -1 for caller-run.
func (e *Executor) execute(j *job, wID int) {
	if !e.claim(j) {
		// Lost the race to cancel/discard; the winner terminalized it.
		return
	}
	atomic.AddInt64(&e.runningN, 1)
	j.startAt = time.Now()
	ranOn := wID
	stole := j.owner >= 0 && ranOn != j.owner
	if stole {
		atomic.AddUint64(&e.stolenN, 1)
	}
	j.stole = stole
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = &PanicError{Value: r, Stack: debug.Stack()}
			}
		}()
		err = j.task(j.ctx)
	}()
	j.cancel() // release context resources promptly
	atomic.AddInt64(&e.runningN, -1)
	if err != nil {
		atomic.AddUint64(&e.failedN, 1)
	} else {
		atomic.AddUint64(&e.completedN, 1)
	}
	j.endAt = time.Now()
	j.ran = true
	j.err = err
	e.terminalizeCAS(j, jobStateDone, err, true)
}

// terminalizeCAS closes the lifecycle of a job after the caller already
// performed the winning CAS (or the job never entered a queue).
func (e *Executor) terminalizeCAS(j *job, terminal uint32, err error, ran bool) {
	if !ran {
		j.cancel()
		j.endAt = time.Now()
		j.err = err
		close(j.done)
	} else {
		close(j.done)
	}
	if e.cfg.OnResult != nil {
		e.cfg.OnResult(Result{
			Task:      j.task,
			Priority:  j.pri,
			Err:       err,
			Ran:       ran,
			EnqueueAt: j.enqAt,
			StartAt:   j.startAt,
			EndAt:     j.endAt,
			Stole:     j.stole,
		})
	}
	// pending mirrors acceptance; wake Shutdown's drain waiter.
	e.mu.Lock()
	p := atomic.AddInt64(&e.pending, -1)
	e.cond.Broadcast()
	if e.stopping && p == 0 {
		e.signalAll()
	}
	e.mu.Unlock()
}

// terminalize is used by non-run paths (reject/discard) that still need the
// CAS guard; it records discarded stats.
func (e *Executor) terminalize(j *job, terminal uint32, err error, ran bool) {
	if !atomic.CompareAndSwapUint32(&j.state, jobStateQueued, terminal) {
		return
	}
	if terminal == jobStateDiscarded {
		atomic.AddUint64(&e.discardedN, 1)
	}
	e.terminalizeCAS(j, terminal, err, ran)
}

// ---------------------------------------------------------------------------
// shutdown
// ---------------------------------------------------------------------------

// beginStop flips the stopping flag exactly once and starts the reaper.
func (e *Executor) beginStop(discard bool) {
	e.mu.Lock()
	if e.stopping {
		e.mu.Unlock()
		return
	}
	e.stopping = true
	e.discard = discard
	close(e.shutdown)
	e.cond.Broadcast()
	e.mu.Unlock()
	e.signalAll()
	go func() {
		e.wg.Wait()
		e.global.stopAger()
		close(e.doneCh)
	}()
}

// Shutdown gracefully drains: new submissions are rejected; queued tasks are
// executed (with ctx as the drain deadline) and running tasks run to
// completion. If ctx expires first, still-queued tasks are discarded while
// running tasks continue to completion in the background; Shutdown returns
// ctx.Err immediately and Executor.Done closes when everything settles.
func (e *Executor) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	e.beginStop(false)
	drainDone := make(chan struct{})
	go func() {
		e.waitPending(ctx, drainDone)
	}()
	select {
	case <-drainDone:
		<-e.doneCh
		return nil
	case <-ctx.Done():
		// Drain budget exhausted: discard everything still queued; running
		// tasks are awaited by the reaper and never interrupted.
		e.discardQueued()
		<-e.doneCh
		return ctx.Err()
	}
}

// waitPending returns when pending reaches zero (closing out) or ctx expires.
func (e *Executor) waitPending(ctx context.Context, out chan<- struct{}) {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			e.mu.Lock()
			e.cond.Broadcast()
			e.mu.Unlock()
		case <-stop:
		}
	}()
	e.mu.Lock()
	for atomic.LoadInt64(&e.pending) > 0 && ctx.Err() == nil {
		e.cond.Wait()
	}
	e.mu.Unlock()
	close(out)
}

// Close discards every not-yet-started task and awaits running tasks.
func (e *Executor) Close() error { return e.CloseCtx(context.Background()) }

// CloseCtx is Close with an explicit wait bound; running tasks that exceed it
// keep running in the background (they are never killed), and Done closes
// when they finish.
func (e *Executor) CloseCtx(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	e.beginStop(true)
	e.discardQueued()
	select {
	case <-e.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// discardQueued terminalizes every still-queued job. Workers concurrently
// popping jobs either win the CAS (the job runs) or lose it (the job is
// discarded here); a job cannot end in both states.
func (e *Executor) discardQueued() {
	gather := func(get func() *job) {
		for {
			j := get()
			if j == nil {
				return
			}
			e.terminalize(j, jobStateDiscarded, ErrTaskDiscarded, false)
		}
	}
	for _, w := range e.workers {
		gather(w.local.popTop)
		gather(w.local.popBottom) // deques are independent levels; drain both ends
	}
	gather(e.global.forcePop)
	gather(e.overflow.pop)
	// Second sweep catches anything parked behind a level boundary.
	for _, w := range e.workers {
		for {
			j := w.local.popBottom()
			if j == nil {
				break
			}
			e.terminalize(j, jobStateDiscarded, ErrTaskDiscarded, false)
		}
	}
	if j := e.global.forcePop(); j != nil {
		e.terminalize(j, jobStateDiscarded, ErrTaskDiscarded, false)
	}
	e.signalAll()
}

// ---------------------------------------------------------------------------
// stats
// ---------------------------------------------------------------------------

// Stats is a point-in-time snapshot.
type Stats struct {
	Workers      int
	Pending      int64
	Running      int64
	LocalQueued  int64
	GlobalQueued int64
	Overflow     int64
	Submitted    uint64
	Completed    uint64
	Failed       uint64
	Canceled     uint64
	Discarded    uint64
	Rejected     uint64
	Stolen       uint64
	Stopping     bool
}

// Snapshot returns the current Stats.
func (e *Executor) Snapshot() Stats {
	var local int64
	for _, w := range e.workers {
		local += int64(w.local.len())
	}
	e.mu.Lock()
	stopping := e.stopping
	e.mu.Unlock()
	return Stats{
		Workers:      len(e.workers),
		Pending:      atomic.LoadInt64(&e.pending),
		Running:      atomic.LoadInt64(&e.runningN),
		LocalQueued:  local,
		GlobalQueued: int64(e.global.len()),
		Overflow:     int64(e.overflow.len()),
		Submitted:    atomic.LoadUint64(&e.submittedN),
		Completed:    atomic.LoadUint64(&e.completedN),
		Failed:       atomic.LoadUint64(&e.failedN),
		Canceled:     atomic.LoadUint64(&e.canceledN),
		Discarded:    atomic.LoadUint64(&e.discardedN),
		Rejected:     atomic.LoadUint64(&e.rejectedN),
		Stolen:       atomic.LoadUint64(&e.stolenN),
		Stopping:     stopping,
	}
}

// Done closes once all workers exited and every accepted task is terminal.
func (e *Executor) Done() <-chan struct{} { return e.doneCh }

// ---------------------------------------------------------------------------
// idle/park coordination
// ---------------------------------------------------------------------------

func (e *Executor) pushIdle(w *worker) {
	e.idleMu.Lock()
	e.idle = append(e.idle, w)
	e.idleMu.Unlock()
}

func (e *Executor) popIdle() *worker {
	e.idleMu.Lock()
	if len(e.idle) == 0 {
		e.idleMu.Unlock()
		return nil
	}
	w := e.idle[len(e.idle)-1]
	e.idle = e.idle[:len(e.idle)-1]
	e.idleMu.Unlock()
	return w
}

// signal wakes one parked idle worker, if any (non-blocking, never blocks
// submitters).
func (e *Executor) signal() {
	w := e.popIdle()
	if w == nil {
		return
	}
	select {
	case w.wakeCh <- struct{}{}:
	default:
	}
}

// signalAll wakes every parked worker (shutdown / discard).
func (e *Executor) signalAll() {
	e.idleMu.Lock()
	ws := e.idle
	e.idle = nil
	e.idleMu.Unlock()
	for _, w := range ws {
		select {
		case w.wakeCh <- struct{}{}:
		default:
		}
	}
}

// ---------------------------------------------------------------------------
// worker
// ---------------------------------------------------------------------------

type worker struct {
	id     int
	pool   *Executor
	local  *localDeque
	wakeCh chan struct{}
	rng    *rand.Rand

	runCount   uint64
	stealCount uint64
}

func (w *worker) loop() {
	defer w.pool.wg.Done()
	scan := w.pool.cfg.ScanInterval
	for {
		if j := w.findJob(); j != nil {
			w.runCount++
			w.pool.execute(j, w.id)
			continue
		}
		// No work: decide between parking and exiting.
		w.pool.mu.Lock()
		stopping := w.pool.stopping
		pending := atomic.LoadInt64(&w.pool.pending)
		w.pool.mu.Unlock()
		if stopping && pending == 0 {
			return
		}
		// Park, but recheck after enqueuing ourselves as idle so a submit
		// that happened in between cannot be missed (lost-wakeup guard).
		w.pool.pushIdle(w)
		if j := w.findJob(); j != nil {
			w.pool.popIdle()
			w.runCount++
			w.pool.execute(j, w.id)
			continue
		}
		w.pool.mu.Lock()
		stopping = w.pool.stopping
		pending = atomic.LoadInt64(&w.pool.pending)
		w.pool.mu.Unlock()
		if stopping && pending == 0 {
			w.pool.popIdle()
			return
		}
		timer := time.NewTimer(scan)
		select {
		case <-w.wakeCh:
		case <-w.pool.shutdown:
		case <-timer.C:
		}
		timer.Stop()
		w.pool.popIdle()
	}
}

// findJob acquires one job via the adaptive order:
//
//	stealing mode : local LIFO -> steal FIFO -> global -> overflow
//	fairness mode : global -> local LIFO -> steal FIFO -> overflow
func (w *worker) findJob() *job {
	e := w.pool
	pop := func(j *job) *job {
		if j != nil && e.claim(j) {
			return j
		}
		return nil
	}
	if e.cfg.Fairness {
		if j := pop(e.global.pop(time.Now())); j != nil {
			return j
		}
		if j := pop(w.local.popBottom()); j != nil {
			return j
		}
		if j := w.steal(pop); j != nil {
			return j
		}
		return pop(e.overflow.pop())
	}
	if j := pop(w.local.popBottom()); j != nil {
		return j
	}
	if j := w.steal(pop); j != nil {
		return j
	}
	if j := pop(e.global.pop(time.Now())); j != nil {
		return j
	}
	return pop(e.overflow.pop())
}

// steal probes victims from a randomized offset (no thundering herd on
// worker 0) and takes the oldest job from the busiest-looking deque it sees.
func (w *worker) steal(pop func(*job) *job) *job {
	n := len(w.pool.workers)
	if n <= 1 {
		return nil
	}
	start := w.rng.Intn(n)
	for k := 1; k < n; k++ {
		v := w.pool.workers[(start+k)%n]
		if v == w {
			continue
		}
		if j := pop(v.local.popTop()); j != nil {
			w.stealCount++
			return j
		}
	}
	return nil
}

// avoid unused import in trimmed builds
var _ = runtime.NumCPU
