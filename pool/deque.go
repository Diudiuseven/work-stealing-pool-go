package pool

import "sync"

// deque is a bounded double-ended queue used as a worker's local queue.
// The owner pushes/pops at the bottom; thieves steal from the top, so
// stealing touches the coldest tasks and local execution stays LIFO-hot.
type deque struct {
	mu    sync.Mutex
	items []*Task
	cap   int
}

func newDeque(capacity int) *deque {
	return &deque{cap: capacity, items: make([]*Task, 0, capacity)}
}

// pushBottom appends a task; reports false when full.
func (d *deque) pushBottom(t *Task) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.items) >= d.cap {
		return false
	}
	d.items = append(d.items, t)
	return true
}

// popBottom removes the most recently pushed task (owner side).
func (d *deque) popBottom() *Task {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.items)
	if n == 0 {
		return nil
	}
	t := d.items[n-1]
	d.items[n-1] = nil
	d.items = d.items[:n-1]
	return t
}

// stealTop removes the oldest task (thief side).
func (d *deque) stealTop() *Task {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.items) == 0 {
		return nil
	}
	t := d.items[0]
	d.items[0] = nil
	d.items = d.items[1:]
	return t
}

func (d *deque) len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.items)
}

// drain removes and returns all remaining tasks.
func (d *deque) drain() []*Task {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := d.items
	d.items = nil
	return out
}
