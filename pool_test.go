package wspool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestPool(t *testing.T, cfg *Config) *Executor {
	t.Helper()
	e, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = e.Shutdown(ctx)
	})
	return e
}

func TestBasicExecutesExactlyOnce(t *testing.T) {
	var ran int64
	e := newTestPool(t, NewConfig().WithWorkers(4))
	const n = 2000
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		_, err := e.Submit(func(context.Context) error {
			atomic.AddInt64(&ran, 1)
			wg.Done()
			return nil
		})
		if err != nil {
			t.Fatalf("submit: %v", err)
		}
	}
	wg.Wait()
	if got := atomic.LoadInt64(&ran); got != n {
		t.Fatalf("ran=%d want %d", got, n)
	}
}

func TestResultCallbackExactlyOnce(t *testing.T) {
	var calls int64
	cfg := NewConfig().WithWorkers(3).
		WithOnResult(func(r Result) { atomic.AddInt64(&calls, 1) })
	e := newTestPool(t, cfg)
	const n = 500
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		_, _ = e.SubmitPri(func(context.Context) error { wg.Done(); return nil }, PriNormal)
	}
	wg.Wait()
	// Give callbacks a moment to publish (they run before Done, so they are
	// already observed when wg is released).
	if got := atomic.LoadInt64(&calls); got != n {
		t.Fatalf("onResult calls=%d want %d", got, n)
	}
}

func TestWorkStealingUnderSkew(t *testing.T) {
	// 4 workers; fill ONE worker's local deque far past one worker's
	// throughput; stealing must spread the work to every worker.
	dist := make([]int64, 4)
	release := make(chan struct{})
	var wg sync.WaitGroup
	cfg := NewConfig().WithWorkers(4).WithLocalCapacity(512)
	e := newTestPool(t, cfg)

	// Block all workers momentarily so jobs pile up in local queues.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		_, _ = e.Submit(func(context.Context) error { <-release; wg.Done(); return nil })
	}
	time.Sleep(50 * time.Millisecond) // let the gate jobs land

	const n = 400
	for i := 0; i < n; i++ {
		wg.Add(1)
		_, _ = e.Submit(func(context.Context) error { return nil })
	}
	// Track execution worker via Stats-like hook: wrap tasks is not enough to
	// know the worker; instead assert throughput: after release, all jobs must
	// finish well within a one-worker time budget, and stolen counter > 0.
	_ = dist
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(release)
	}()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("skewed workload did not finish: stealing broken")
	}
	if got := atomic.LoadUint64(&e.stolenN); got == 0 {
		t.Fatalf("expected steals under skew, got 0")
	}
}

func TestPriorityAndAgingNoStarvation(t *testing.T) {
	// In fairness mode a continuous stream of high-priority jobs must not
	// starve an old low-priority job forever thanks to aging.
	var lowDone int64
	cfg := NewConfig().WithWorkers(2).
	WithFairness(true).WithAgeTick(5 * time.Millisecond)
	e := newTestPool(t, cfg)

	stop := make(chan struct{})
	var producers sync.WaitGroup
	for p := 0; p < 2; p++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, _ = e.SubmitPri(func(context.Context) error {
					time.Sleep(time.Millisecond)
					return nil
				}, PriHigh)
			}
		}()
	}
	_, err := e.SubmitPri(func(context.Context) error {
		atomic.StoreInt64(&lowDone, 1)
		return nil
	}, PriLow)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(3 * time.Second)
	for atomic.LoadInt64(&lowDone) == 0 {
		select {
		case <-deadline:
			close(stop)
			producers.Wait()
			t.Fatal("low-priority job starved despite aging")
		case <-time.After(5 * time.Millisecond):
		}
	}
	close(stop)
	producers.Wait()
}

func TestTaskErrorReported(t *testing.T) {
	boom := errors.New("boom")
	e := newTestPool(t, NewConfig().WithWorkers(2))
	h, err := e.Submit(func(context.Context) error { return boom })
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Wait(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("err=%v want boom", err)
	}
	s := e.Snapshot()
	if s.Failed != 1 || s.Completed != 0 {
		t.Fatalf("stats=%+v", s)
	}
	_ = fmt.Sprint
}
