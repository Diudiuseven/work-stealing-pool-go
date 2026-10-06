package wspool

import (
	"context"
	"sync/atomic"
)

// job state machine:
//
//	queued -> running -> done
//	  \_______\______> canceled   (never ran; task body never executes)
//	           \_____> discarded  (never ran; shutdown drain / hard close)
//
// Exactly one atomic CAS out of queued wins: the worker that transitions the
// job to running owns the single execution; cancel/discard transitions own
// the single non-run terminalization. done is closed on every terminal path.
const (
	jobStateQueued uint32 = iota
	jobStateRunning
	jobStateDone
	jobStateCanceled
	jobStateDiscarded
)

type jobHandle struct {
	j *job
}

func (h *jobHandle) Done() <-chan struct{} { return h.j.done }

// Err returns nil while the job is pending or running and the terminal error
// afterwards (including context.Canceled or ErrTaskDiscarded).
func (h *jobHandle) Err() error {
	if atomic.LoadUint32(&h.j.state) == jobStateQueued ||
		atomic.LoadUint32(&h.j.state) == jobStateRunning {
		return nil
	}
	return h.j.err
}

// Cancel requests cancellation.
//
//   - If the job has not started it is removed and reported as
//     context.Canceled; Cancel returns true.
//   - If the job is already running its context is canceled (cooperative
//     cancellation); the task itself runs to completion and Cancel returns
//     false.
//   - Terminal jobs are untouched and Cancel returns false.
//
// Cancel is idempotent.
func (h *jobHandle) Cancel() bool { return h.j.pool.cancelJob(h.j) }

func (h *jobHandle) Wait(ctx context.Context) error {
	select {
	case <-h.j.done:
		return h.j.err
	case <-ctx.Done():
		return ctx.Err()
	}
}
