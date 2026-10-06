// Package wspool implements an adaptive, work-stealing local task executor.
//
// See README.md for the scheduling semantics, configuration knobs and the
// verification procedure.
package wspool

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Priority is the scheduling priority of a task. Higher values are served
// before lower values. Aging in the global queue prevents low-priority tasks
// from being starved indefinitely (see Config.AgeTick).
type Priority int

const (
	// PriLow is scheduled after PriNormal.
	PriLow Priority = -1
	// PriNormal is the default priority.
	PriNormal Priority = 0
	// PriHigh is scheduled before PriNormal/PriLow, subject to aging.
	PriHigh Priority = 1
)

// Task is a unit of work. A task executes exactly once: it is either run by a
// worker or, if it never starts before the executor is drained/shut down,
// handed to Config.OnDiscard exactly once.
type Task func(ctx context.Context) error

// Handle is the per-submission handle returned by Executor.Submit.
//
// All methods are safe for concurrent use.
type Handle interface {
	// Done returns a channel closed once the task reached a terminal state
	// (completed, discarded before start, or failed).
	Done() <-chan struct{}
	// Err returns nil while pending/running. After Done it returns the task
	// error, context.Canceled if canceled before start, ErrTaskDiscarded when
	// discarded during shutdown/drain, or ErrExecutorStopped when the
	// executor rejected the submission.
	Err() error
	// Cancel requests cancellation. A task that has not started yet is
	// removed from its queue and reported as context.Canceled; a running task
	// is only signaled through its context and is never interrupted.
	Cancel() bool
	// Wait blocks until the task reaches a terminal state or ctx expires.
	Wait(ctx context.Context) error
}

// Result is the value delivered to Config.OnResult.
type Result struct {
	// Task is the submitted task.
	Task Task
	// Priority is the priority the task was submitted with.
	Priority Priority
	// Err is the task return value; nil on success. A panic is recovered and
	// converted to a *PanicError.
	Err error
	// Ran is true when the task body was executed, false when it was
	// discarded or canceled before it ever started.
	Ran bool
	// EnqueueAt is when the task entered the executor.
	EnqueueAt time.Time
	// StartAt is when a worker picked it up (zero if it never ran).
	StartAt time.Time
	// EndAt is when it reached a terminal state.
	EndAt time.Time
	// Stole is true when the task ran on a worker other than the one it was
	// first enqueued to.
	Stole bool
}

// PanicError wraps a value recovered from a panicking task. A panicking task
// never kills its worker or affects any other task.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	if e == nil {
		return "task panic: <nil>"
	}
	return "task panic: " + fmt.Sprint(e.Value)
}

// ErrExecutorStopped is returned (or wrapped) when Submit is called after the
// executor started shutting down.
var ErrExecutorStopped = errors.New("wspool: executor stopped")

// ErrQueueFull is returned when a bounded queue rejects a submission under the
// RejectReject policy.
var ErrQueueFull = errors.New("wspool: queue full")

// ErrTaskDiscarded is reported for tasks that never started and are drained
// away during Shutdown/GracefulShutdown.
var ErrTaskDiscarded = errors.New("wspool: task discarded during shutdown")

