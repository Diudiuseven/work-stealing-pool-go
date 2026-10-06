package wspool

import (
	"runtime"
	"time"
)

// RejectPolicy decides what happens to a submission that cannot enter a
// saturated executor. It is never applied silently: every rejected task is
// also reported through Config.OnResult with Ran == false.
type RejectPolicy int

const (
	// RejectReject fails the submission immediately with ErrQueueFull (or
	// ErrExecutorStopped after shutdown). The caller is never blocked and the
	// task is never enqueued.
	RejectReject RejectPolicy = iota
	// RejectCallerRun runs the task synchronously on the submitting goroutine,
	// providing back-pressure. If the executor is already stopped the task is
	// not run and ErrExecutorStopped is returned instead.
	RejectCallerRun
	// RejectOverflow spills the task into the shared overflow queue served by
	// workers after local/global queues. Submit fails with ErrQueueFull only
	// when OverflowCapacity > 0 and that bound is reached; with capacity 0
	// the overflow is unbounded (memory is the only limit).
	RejectOverflow
)

// Config configures an Executor. Use NewConfig for defaults.
type Config struct {
	// Workers is the fixed number of worker goroutines. Default: runtime.NumCPU.
	Workers int
	// LocalCapacity is the capacity of each of the three priority levels of
	// every worker's local deque. Default 256.
	LocalCapacity int
	// OverflowCapacity bounds the shared overflow queue used by
	// RejectOverflow. 0 means unbounded. Default 0.
	OverflowCapacity int
	// Policy selects the saturation behavior. Default RejectOverflow.
	Policy RejectPolicy
	// Fairness, when true, routes all submissions through the global aging
	// priority queue (strict ordering, higher contention); when false, jobs
	// are placed round-robin on local deques and balanced by work stealing.
	// Default false.
	Fairness bool
	// AgeTick is the period at which waiting tasks gain effective priority
	// in the global queue. A task gains at least one priority level per
	// AgeTick spent waiting (capped, see queue.go), so no task is starved
	// forever. <=0 disables aging. Default 10ms.
	AgeTick time.Duration
	// ScanInterval bounds how long a parked idle worker waits before
	// rescanning when it missed a wakeup. Default 1ms.
	ScanInterval time.Duration
	// OnResult, if set, is invoked exactly once per submitted task when that
	// task reaches a terminal state. It runs on a worker goroutine (or on the
	// submitting goroutine for caller-run tasks); keep it cheap.
	OnResult func(Result)
}

// NewConfig returns a Config populated with defaults.
func NewConfig() *Config {
	c := &Config{}
	c.defaults()
	return c
}

func (c *Config) defaults() {
	if c.Workers <= 0 {
		c.Workers = runtime.NumCPU()
	}
	if c.LocalCapacity <= 0 {
		c.LocalCapacity = 256
	}
	if c.AgeTick == 0 {
		c.AgeTick = 10 * time.Millisecond
	}
	if c.ScanInterval <= 0 {
		c.ScanInterval = time.Millisecond
	}
}

// normalize fills defaults and clamps invalid values; called once by New.
func (c *Config) normalize() {
	c.defaults()
	if c.Workers > 1<<16 {
		c.Workers = 1 << 16
	}
	if c.Policy < RejectReject || c.Policy > RejectOverflow {
		c.Policy = RejectOverflow
	}
	if c.OverflowCapacity < 0 {
		c.OverflowCapacity = 0
	}
}

// WithWorkers sets the worker count.
func (c *Config) WithWorkers(n int) *Config { c.Workers = n; return c }

// WithLocalCapacity sets the per-level local queue capacity.
func (c *Config) WithLocalCapacity(n int) *Config { c.LocalCapacity = n; return c }

// WithOverflowCapacity bounds the shared overflow queue (0 = unbounded).
func (c *Config) WithOverflowCapacity(n int) *Config { c.OverflowCapacity = n; return c }

// WithRejectPolicy sets the saturation policy.
func (c *Config) WithRejectPolicy(p RejectPolicy) *Config { c.Policy = p; return c }

// WithFairness toggles strict global-queue fairness.
func (c *Config) WithFairness(b bool) *Config { c.Fairness = b; return c }

// WithAgeTick sets the priority aging period (<=0 disables aging).
func (c *Config) WithAgeTick(d time.Duration) *Config { c.AgeTick = d; return c }

// WithScanInterval sets the idle rescan period.
func (c *Config) WithScanInterval(d time.Duration) *Config { c.ScanInterval = d; return c }

// WithOnResult installs the terminal-state callback.
func (c *Config) WithOnResult(f func(Result)) *Config { c.OnResult = f; return c }
