package pool

import (
	"context"
	"sync"
	"sync/atomic"
)

// Priority of a task. Higher value means the task is scheduled earlier.
type Priority int

const (
	PriorityLow    Priority = 0
	PriorityNormal Priority = 1
	PriorityHigh   Priority = 2
)

const numPriorities = 3

func clampPriority(p Priority) Priority {
	if p < 0 {
		return 0
	}
	if p > PriorityHigh {
		return PriorityHigh
	}
	return p
}

// TaskFunc is the unit of work executed by the pool.
type TaskFunc func(ctx context.Context) error

// taskState describes the lifecycle of a Task.
type taskState int32

const (
	statePending taskState = iota
	stateRunning
	stateDone
	stateCancelled
)

// Task is a submitted unit of work. It carries its own cancellation and
// completion signalling so that every task is accounted for exactly once.
type Task struct {
	id     uint64
	fn     TaskFunc
	prio   Priority
	state  atomic.Int32
	ctx    context.Context
	cancel context.CancelFunc

	done     chan struct{}
	err      error
	panicVal any

	termOnce   sync.Once
	onTerminal func() // pool bookkeeping, runs exactly once
}

// ID returns the pool-unique task identifier.
func (t *Task) ID() uint64 { return t.id }

// Context returns the task's cancellable execution context.
func (t *Task) Context() context.Context { return t.ctx }

// Done returns a channel closed when the task reaches a terminal state
// (finished, panicked, or cancelled before execution).
func (t *Task) Done() <-chan struct{} { return t.done }

// Err reports the error returned by the task function, if any.
func (t *Task) Err() error {
	<-t.done
	return t.err
}

// PanicValue reports the recovered panic value, if the task panicked.
func (t *Task) PanicValue() any {
	<-t.done
	return t.panicVal
}

// Cancel requests cancellation. If the task has not started yet it will
// never run and Cancel returns true; if it is already running (or done)
// its context is cancelled and Cancel returns false.
func (t *Task) Cancel() bool {
	if t.state.CompareAndSwap(int32(statePending), int32(stateCancelled)) {
		t.cancel()
		t.terminal()
		return true
	}
	t.cancel()
	return false
}

// terminal closes done and runs pool bookkeeping exactly once.
func (t *Task) terminal() {
	t.termOnce.Do(func() {
		close(t.done)
		if t.onTerminal != nil {
			t.onTerminal()
		}
	})
}
