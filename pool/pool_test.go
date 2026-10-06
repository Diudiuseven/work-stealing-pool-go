package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestExactlyOnceHighConcurrency hammers the pool from many producers and
// verifies every task runs exactly once and nothing is lost.
func TestExactlyOnceHighConcurrency(t *testing.T) {
	p := New(Config{Workers: 8, QueueCapacity: 128, Policy: OverflowBlock})
	const producers = 16
	const perProducer = 500
	var ran atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perProducer; j++ {
				if _, err := p.Submit(context.Background(), func(context.Context) error {
					ran.Add(1)
					return nil
				}); err != nil {
					t.Errorf("submit failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	p.Shutdown()
	p.Drain()
	want := int64(producers * perProducer)
	if got := ran.Load(); got != want {
		t.Fatalf("ran %d tasks, want exactly %d", got, want)
	}
	if got := p.stats.Completed.Load(); got != uint64(want) {
		t.Fatalf("completed %d, want %d", got, want)
	}
}

// TestWorkStealingUnderSkew parks all tasks on one worker and verifies
// idle peers steal them, so a skewed distribution still finishes fast.
func TestWorkStealingUnderSkew(t *testing.T) {
	p := New(Config{Workers: 4, QueueCapacity: 512, StealBatchSize: 4})
	const n = 200
	var ran atomic.Int64
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		if _, err := p.submitToWorker(0, PriorityNormal, func(context.Context) error {
			ran.Add(1)
			done <- struct{}{}
			return nil
		}); err != nil {
			t.Fatalf("skewed submit failed: %v", err)
		}
	}
	for i := 0; i < n; i++ {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("timed out waiting for task %d/%d", i, n)
		}
	}
	p.Shutdown()
	p.Drain()
	if got := ran.Load(); got != n {
		t.Fatalf("ran %d, want %d", got, n)
	}
	if p.stats.Stolen.Load() == 0 {
		t.Fatal("expected idle workers to steal from the loaded worker")
	}
}

// TestCancellationConcurrent mixes concurrent cancels with execution and
// stealing: every task must end exactly once in ran XOR cancelled.
func TestCancellationConcurrent(t *testing.T) {
	p := New(Config{Workers: 4, QueueCapacity: 128, Policy: OverflowBlock})
	// Occupy all workers briefly so cancellations race with dequeuing.
	gate := make(chan struct{})
	for i := 0; i < 4; i++ {
		_, _ = p.Submit(context.Background(), func(ctx context.Context) error {
			<-gate
			return nil
		})
	}
	const n = 400
	tasks := make([]*Task, n)
	for i := 0; i < n; i++ {
		tk, err := p.Submit(context.Background(), func(context.Context) error {
			return nil
		})
		if err != nil {
			t.Fatalf("submit failed: %v", err)
		}
		tasks[i] = tk
	}
	// Cancel half of the tasks concurrently with execution/stealing, then
	// verify every task ends exactly once in a terminal state.
	var cw sync.WaitGroup
	for i := 0; i < n; i += 2 {
		cw.Add(1)
		go func(tk *Task) { defer cw.Done(); tk.Cancel() }(tasks[i])
	}
	close(gate)
	cw.Wait()
	p.Shutdown()
	p.Drain()
	var cancelled, completed int64
	for _, tk := range tasks {
		<-tk.Done()
		switch taskState(tk.state.Load()) {
		case stateCancelled:
			cancelled++
		case stateDone:
			completed++
		default:
			t.Fatalf("task %d in non-terminal state %d", tk.id, tk.state.Load())
		}
	}
	if cancelled+completed != n {
		t.Fatalf("lost tasks: cancelled=%d completed=%d total=%d want %d",
			cancelled, completed, cancelled+completed, n)
	}
}

// TestGracefulShutdown verifies queued-but-unstarted tasks are drained to
// completion, running tasks finish, and new submissions are rejected.
func TestGracefulShutdown(t *testing.T) {
	p := New(Config{Workers: 2, QueueCapacity: 64})
	gate := make(chan struct{})
	started := make(chan struct{}, 2)
	// Two long-running tasks occupy both workers.
	for i := 0; i < 2; i++ {
		_, _ = p.Submit(context.Background(), func(ctx context.Context) error {
			started <- struct{}{}
			<-gate
			return nil
		})
	}
	<-started
	<-started
	// Queued behind the gate: must still run during drain.
	const queued = 50
	var ran atomic.Int64
	for i := 0; i < queued; i++ {
		if _, err := p.Submit(context.Background(), func(context.Context) error {
			ran.Add(1)
			return nil
		}); err != nil {
			t.Fatalf("submit failed: %v", err)
		}
	}
	p.Shutdown()
	if _, err := p.Submit(context.Background(), func(context.Context) error { return nil }); err != ErrClosed {
		t.Fatalf("submit after shutdown: got %v, want ErrClosed", err)
	}
	drained := make(chan struct{})
	go func() { p.Drain(); close(drained) }()
	select {
	case <-drained:
		t.Fatal("Drain returned while workers were still gated")
	case <-time.After(50 * time.Millisecond):
	}
	close(gate) // release running tasks; drain must now finish the queue
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("Drain did not return after gate released")
	}
	if got := ran.Load(); got != queued {
		t.Fatalf("queued tasks ran %d, want %d", got, queued)
	}
}

// TestPanicIsolation: a panicking task must not kill its worker nor affect
// other tasks; the panic is recovered, recorded and reported.
func TestPanicIsolation(t *testing.T) {
	panicked := make(chan uint64, 1)
	p := New(Config{
		Workers:       2,
		QueueCapacity: 32,
		PanicHandler:  func(id uint64, v any) { panicked <- id },
	})
	boom, err := p.Submit(context.Background(), func(context.Context) error {
		panic("boom")
	})
	if err != nil {
		t.Fatal(err)
	}
	const n = 100
	var ran atomic.Int64
	for i := 0; i < n; i++ {
		if _, err := p.Submit(context.Background(), func(context.Context) error {
			ran.Add(1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case id := <-panicked:
		if id != boom.id {
			t.Fatalf("panic handler got task %d, want %d", id, boom.id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("panic handler not called")
	}
	<-boom.Done()
	if boom.PanicValue() != "boom" {
		t.Fatalf("PanicValue = %v, want boom", boom.PanicValue())
	}
	p.Shutdown()
	p.Drain()
	if got := ran.Load(); got != n {
		t.Fatalf("ran %d sibling tasks, want %d (panic killed work?)", got, n)
	}
	if p.stats.Panicked.Load() != 1 {
		t.Fatalf("panicked = %d, want 1", p.stats.Panicked.Load())
	}
}

// TestOverflowReject: bounded queues + reject policy must surface
// ErrQueueFull without blocking or dropping silently.
func TestOverflowReject(t *testing.T) {
	p := New(Config{Workers: 1, QueueCapacity: 2, Policy: OverflowReject})
	gate := make(chan struct{})
	started := make(chan struct{})
	// Occupy the single worker.
	_, _ = p.Submit(context.Background(), func(ctx context.Context) error { close(started); <-gate; return nil })
	<-started
	// Fill the queue.
	for i := 0; i < 2; i++ {
		if _, err := p.Submit(context.Background(), func(context.Context) error { return nil }); err != nil {
			t.Fatalf("submit %d failed: %v", i, err)
		}
	}
	if _, err := p.Submit(context.Background(), func(context.Context) error { return nil }); err != ErrQueueFull {
		t.Fatalf("got %v, want ErrQueueFull", err)
	}
	if p.stats.Rejected.Load() != 1 {
		t.Fatalf("rejected = %d, want 1", p.stats.Rejected.Load())
	}
	close(gate)
	p.Shutdown()
	p.Drain()
}

// TestOverflowRunInline: the caller executes the task itself when queues
// are full; the task still runs exactly once and is not lost.
func TestOverflowRunInline(t *testing.T) {
	p := New(Config{Workers: 1, QueueCapacity: 1, Policy: OverflowRunInline})
	gate := make(chan struct{})
	started := make(chan struct{})
	_, _ = p.Submit(context.Background(), func(ctx context.Context) error { close(started); <-gate; return nil })
	<-started
	_, _ = p.Submit(context.Background(), func(context.Context) error { return nil }) // fills queue
	var ranInline atomic.Bool
	tk, err := p.Submit(context.Background(), func(context.Context) error {
		ranInline.Store(true)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ranInline.Load() {
		t.Fatal("inline task did not run before Submit returned")
	}
	select {
	case <-tk.Done():
	default:
		t.Fatal("inline task not terminal after Submit returned")
	}
	if p.stats.Inlined.Load() != 1 {
		t.Fatalf("inlined = %d, want 1", p.stats.Inlined.Load())
	}
	close(gate)
	p.Shutdown()
	p.Drain()
}

// TestPriorityFairness: with StarveEvery=N, a low-priority task submitted
// before a flood of high-priority tasks must run within N+1 executions.
func TestPriorityFairness(t *testing.T) {
	const starve = 3
	p := New(Config{Workers: 1, QueueCapacity: 32, StarveEvery: starve})
	gate := make(chan struct{})
	started := make(chan struct{})
	_, _ = p.Submit(context.Background(), func(ctx context.Context) error { close(started); <-gate; return nil })
	<-started

	var mu sync.Mutex
	var order []string
	record := func(name string) TaskFunc {
		return func(context.Context) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}
	}
	// One low-priority task, then a flood of high-priority tasks.
	if _, err := p.SubmitTask(context.Background(), PriorityLow, record("low")); err != nil {
		t.Fatal(err)
	}
	const highs = 10
	for i := 0; i < highs; i++ {
		if _, err := p.SubmitTask(context.Background(), PriorityHigh, record("high")); err != nil {
			t.Fatal(err)
		}
	}
	close(gate)
	p.Shutdown()
	p.Drain()

	mu.Lock()
	defer mu.Unlock()
	lowPos := -1
	for i, s := range order {
		if s == "low" {
			lowPos = i
		}
	}
	if lowPos < 0 {
		t.Fatal("low-priority task never ran")
	}
	if lowPos > starve {
		t.Fatalf("low-priority task ran at position %d, want <= %d (order: %v)", lowPos, starve, order)
	}
}
