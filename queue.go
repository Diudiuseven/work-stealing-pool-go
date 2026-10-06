package wspool

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// job is an executable task plus all bookkeeping shared by the queues and the
// executor. A job is owned by exactly one place at a time (one queue, one
// worker or the terminal state); queue ownership changes while holding that
// queue's lock and the queued->running terminal transition is atomic.
type job struct {
	id       uint64
	task     Task
	pri      Priority // static, submission-time priority
	enqAt    time.Time
	startAt  time.Time
	endAt    time.Time
	deadline uint64    // global FIFO tie-breaker (submission ordinal)
	promoted int       // aging promotions already applied (ageSeq in old drafts)
	cancelBy context.Context

	state uint32 // accessed atomically (jobState* in handle.go)
	done  chan struct{}
	err   error
	ran   bool

	ctx    context.Context
	cancel context.CancelFunc

	owner int // worker the job was first enqueued to (-1 = global/overflow)
	pool  *Executor
	stole bool // captured at execution for the Result callback
}

// effectivePri is pri plus the aging promotions earned by now. Promotions are
// computed lazily by the global queue's ager; callers reading this without
// the global lock see a possibly stale-but-consistent value.
func (j *job) effectivePri() Priority { return j.pri + Priority(j.promoted) }

// ---------------------------------------------------------------------------
// levelDeque: a bounded, mutex-protected Chase-Lev-style deque.
//
// The owner pushes/pops at the bottom (LIFO: a task just submitted tends to
// stay hot on the same worker), thieves pop from the top (FIFO). Three of
// these per worker give priority separation without losing the stealing
// discipline.
// ---------------------------------------------------------------------------

type levelDeque struct {
	mu sync.Mutex
	// ring stores jobs in [top, bottom); top is the thief side.
	ring   []*job
	top    int
	bottom int
	count  int
}

func newLevelDeque(cap int) *levelDeque { return &levelDeque{ring: make([]*job, cap)} }

func (d *levelDeque) len() int {
	d.mu.Lock()
	n := d.count
	d.mu.Unlock()
	return n
}

// pushBottom returns false when the level is full (caller then overflows).
func (d *levelDeque) pushBottom(j *job) bool {
	d.mu.Lock()
	if d.count == len(d.ring) {
		d.mu.Unlock()
		return false
	}
	d.ring[d.bottom] = j
	d.bottom++
	if d.bottom == len(d.ring) {
		d.bottom = 0
	}
	d.count++
	d.mu.Unlock()
	return true
}

func (d *levelDeque) popBottom() *job {
	d.mu.Lock()
	if d.count == 0 {
		d.mu.Unlock()
		return nil
	}
	if d.bottom == 0 {
		d.bottom = len(d.ring) - 1
	} else {
		d.bottom--
	}
	j := d.ring[d.bottom]
	d.ring[d.bottom] = nil
	d.count--
	d.mu.Unlock()
	return j
}

func (d *levelDeque) popTop() *job {
	d.mu.Lock()
	if d.count == 0 {
		d.mu.Unlock()
		return nil
	}
	j := d.ring[d.top]
	d.ring[d.top] = nil
	d.top++
	if d.top == len(d.ring) {
		d.top = 0
	}
	d.count--
	d.mu.Unlock()
	return j
}

// remove extracts a still-queued job by id; it is used by cancellation, which
// may race with execution (the atomic state CAS in the pool is the real
// guard; this is best-effort lookup).
func (d *levelDeque) remove(id uint64) *job {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.count == 0 {
		return nil
	}
	idx := -1
	for k := 0; k < d.count; k++ {
		p := (d.top + k) % len(d.ring)
		if d.ring[p] != nil && d.ring[p].id == id {
			idx = p
			break
		}
	}
	if idx < 0 {
		return nil
	}
	j := d.ring[idx]
	// Shift the top..idx segment one slot toward idx so the hole is at top.
	for p := idx; p != d.top; p = (p - 1 + len(d.ring)) % len(d.ring) {
		prev := (p - 1 + len(d.ring)) % len(d.ring)
		d.ring[p] = d.ring[prev]
	}
	d.ring[d.top] = nil
	d.top++
	if d.top == len(d.ring) {
		d.top = 0
	}
	d.count--
	return j
}

// localDeque groups the three priority levels.
type localDeque struct {
	hi, mid, lo *levelDeque
}

func newLocalDeque(capPerLevel int) *localDeque {
	return &localDeque{
		hi:  newLevelDeque(capPerLevel),
		mid: newLevelDeque(capPerLevel),
		lo:  newLevelDeque(capPerLevel),
	}
}

func levelOf(p Priority) int {
	switch {
	case p >= PriHigh:
		return 0
	case p <= PriLow:
		return 2
	default:
		return 1
	}
}

func (q *localDeque) level(i int) *levelDeque {
	switch i {
	case 0:
		return q.hi
	case 2:
		return q.lo
	default:
		return q.mid
	}
}

// push honors the *static* submission priority: aging happens in the global
// queue, never by mutating a job's placement in another worker's deque.
func (q *localDeque) pushBottom(j *job) bool {
	return q.level(levelOf(j.pri)).pushBottom(j)
}

// popBottom: owners drain high before normal before low (LIFO within level).
func (q *localDeque) popBottom() *job {
	if j := q.hi.popBottom(); j != nil {
		return j
	}
	if j := q.mid.popBottom(); j != nil {
		return j
	}
	return q.lo.popBottom()
}

// popTop: thieves likewise take from the highest available level, but from
// the FIFO side, so they carry away the oldest (coldest) tasks and leave hot
// LIFO tasks for the owner.
func (q *localDeque) popTop() *job {
	if j := q.hi.popTop(); j != nil {
		return j
	}
	if j := q.mid.popTop(); j != nil {
		return j
	}
	return q.lo.popTop()
}

func (q *localDeque) remove(id uint64) *job {
	if j := q.hi.remove(id); j != nil {
		return j
	}
	if j := q.mid.remove(id); j != nil {
		return j
	}
	return q.lo.remove(id)
}

func (q *localDeque) len() int { return q.hi.len() + q.mid.len() + q.lo.len() }

// ---------------------------------------------------------------------------
// globalQueue: shared priority queue with priority aging.
//
// Jobs are popped by (effective priority desc, FIFO asc). Every AgeTick a
// waiting job spends in the queue earns one promotion, capped at maxAging, so
// an old low-priority job eventually overtakes fresh normal-priority jobs and
// no workload can be starved indefinitely. A small ager goroutine restores
// heap order when promotions happen; it only runs while the queue is
// non-empty.
// ---------------------------------------------------------------------------

const maxAging = 4

type priHeap []*job

func (h priHeap) Len() int { return len(h) }
func (h priHeap) Less(i, k int) bool {
	if h[i].effectivePri() != h[k].effectivePri() {
		return h[i].effectivePri() > h[k].effectivePri()
	}
	return h[i].deadline < h[k].deadline
}
func (h priHeap) Swap(i, k int) { h[i], h[k] = h[k], h[i] }
func (h *priHeap) Push(x any)   { *h = append(*h, x.(*job)) }
func (h *priHeap) Pop() any {
	old := *h
	n := len(old)
	j := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return j
}

type globalQueue struct {
	mu      sync.Mutex
	h       priHeap
	tick    time.Duration
	agerStop chan struct{}
	agerWG   sync.WaitGroup
	running bool
}

func newGlobalQueue(tick time.Duration) *globalQueue {
	return &globalQueue{h: make(priHeap, 0, 64), tick: tick, agerStop: make(chan struct{})}
}

func (q *globalQueue) startAger() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.running || q.tick <= 0 {
		return
	}
	q.running = true
	q.agerWG.Add(1)
	go q.agerLoop()
}

func (q *globalQueue) stopAger() {
	q.mu.Lock()
	if !q.running {
		q.mu.Unlock()
		return
	}
	q.running = false
	close(q.agerStop)
	q.mu.Unlock()
	q.agerWG.Wait()
}

func (q *globalQueue) agerLoop() {
	defer q.agerWG.Done()
	t := time.NewTicker(q.tick)
	defer t.Stop()
	for {
		select {
		case <-q.agerStop:
			return
		case <-t.C:
			q.mu.Lock()
			if len(q.h) == 0 {
				q.mu.Unlock()
				continue
			}
			changed := false
			for _, j := range q.h {
				// One promotion per full AgeTick waited, bounded so aging can
				// never turn the queue into permanent priority inversion.
				waited := int(time.Since(j.enqAt) / q.tick)
				want := minInt(waited, maxAging)
				if want > j.promoted {
					j.promoted = want
					changed = true
				}
			}
			if changed {
				heap.Init(&q.h)
			}
			q.mu.Unlock()
		}
	}
}

func (q *globalQueue) push(j *job) bool {
	q.mu.Lock()
	heap.Push(&q.h, j)
	q.mu.Unlock()
	q.startAger()
	return true
}

func (q *globalQueue) pop(now time.Time) *job {
	q.mu.Lock()
	if len(q.h) == 0 {
		q.mu.Unlock()
		return nil
	}
	// Fast promotion for the head when enough wall time elapsed: keeps the
	// common case correct even if a tick is momentarily late.
	if q.tick > 0 {
		head := q.h[0]
		waited := int(now.Sub(head.enqAt) / q.tick)
		if waited > head.promoted && head.promoted < maxAging {
			head.promoted = minInt(waited, maxAging)
			heap.Init(&q.h)
		}
	}
	j := heap.Pop(&q.h).(*job)
	q.mu.Unlock()
	return j
}

func (q *globalQueue) remove(id uint64) *job {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, j := range q.h {
		if j.id == id {

			return heap.Remove(&q.h, i).(*job)
		}
	}
	return nil
}

func (q *globalQueue) len() int {
	q.mu.Lock()
	n := len(q.h)
	q.mu.Unlock()
	return n
}

// forcePop removes and returns the highest-priority queued job without aging
// adjustments; used by the shutdown/close sweeps.
func (q *globalQueue) forcePop() *job {
	q.mu.Lock()
	if len(q.h) == 0 {
		q.mu.Unlock()
		return nil
	}
	j := heap.Pop(&q.h).(*job)
	q.mu.Unlock()
	return j
}

// drain pops every job (used by Close).
func (q *globalQueue) drain() []*job {
	q.mu.Lock()
	out := make([]*job, 0, len(q.h))
	for len(q.h) > 0 {
		out = append(out, heap.Pop(&q.h).(*job))
	}
	q.mu.Unlock()
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------------------------------------------------------------------------
// overflowQueue: FIFO used by RejectOverflow; optionally bounded.
// ---------------------------------------------------------------------------

type overflowQueue struct {
	mu       sync.Mutex
	jobs     []*job
	capacity int // 0 = unbounded
}

func newOverflowQueue(capacity int) *overflowQueue {
	return &overflowQueue{jobs: make([]*job, 0, 64), capacity: capacity}
}

func (q *overflowQueue) push(j *job) bool {
	q.mu.Lock()
	if q.capacity > 0 && len(q.jobs) >= q.capacity {
		q.mu.Unlock()
		return false
	}
	q.jobs = append(q.jobs, j)
	q.mu.Unlock()
	return true
}

func (q *overflowQueue) pop() *job {
	q.mu.Lock()
	if len(q.jobs) == 0 {
		q.mu.Unlock()
		return nil
	}
	j := q.jobs[0]
	q.jobs[0] = nil
	q.jobs = q.jobs[1:]
	q.mu.Unlock()
	return j
}

func (q *overflowQueue) remove(id uint64) *job {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, j := range q.jobs {
		if j.id == id {
			q.jobs = append(q.jobs[:i], q.jobs[i+1:]...)
			return j
		}
	}
	return nil
}

func (q *overflowQueue) len() int {
	q.mu.Lock()
	n := len(q.jobs)
	q.mu.Unlock()
	return n
}

func (q *overflowQueue) drain() []*job {
	q.mu.Lock()
	out := q.jobs
	q.jobs = nil
	q.mu.Unlock()
	return out
}
