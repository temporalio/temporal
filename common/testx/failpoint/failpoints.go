// Package failpoint injects faults at named points in code under test.
package failpoint

import "sync"

// Failpoint injects a fault or alternate branch at a named key in code under test.
// A new failpoint starts off and is configured with On. Call Disable to turn it off.
type Failpoint[T any] struct {
	// mu guards all fields in this struct.
	mu sync.Mutex

	key    Key[T]
	action Action

	// totalFires counts every fire of the failpoint, so tests can check that it was reached.
	totalFires int64
}

// New creates a Failpoint with the given key.
func New[T any](key Key[T]) *Failpoint[T] {
	return &Failpoint[T]{
		key:    key,
		action: ActionNone,
	}
}

// Disable turns the failpoint off.
func (f *Failpoint[T]) Disable() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
}

func (f *Failpoint[T]) disableLocked() {
	f.action = ActionNone
}

// On turns the failpoint on. It is a boolean switch (e.g. to force an alternate code path):
// Trip returns true.
func (f *Failpoint[T]) On() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.action = ActionOn
}

// isOn reports whether the failpoint is configured with On, and counts the fire if it is.
func (f *Failpoint[T]) isOn() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.action != ActionOn {
		return false
	}
	f.totalFires++
	return true
}

// TotalFires returns how often the failpoint fired in its lifetime.
func (f *Failpoint[T]) TotalFires() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.totalFires
}

func (f *Failpoint[T]) id() keyID {
	return f.key.id()
}
