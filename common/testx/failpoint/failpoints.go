// Package failpoint injects faults at named points in code under test.
package failpoint

import "sync"

// Failpoint injects a fault or alternate branch at a named key in code under test.
// A new failpoint starts off and is configured with Return or On. Call Disable to turn it off.
type Failpoint[T any] struct {
	// mu guards all fields in this struct.
	mu sync.Mutex

	key    Key[T]
	action Action
	value  T
}

// New creates a Failpoint with the given key.
func New[T any](key Key[T]) *Failpoint[T] {
	return &Failpoint[T]{
		key:    key,
		action: ActionNone,
	}
}

// Disable turns the failpoint off and clears any configured return value.
func (f *Failpoint[T]) Disable() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
}

func (f *Failpoint[T]) disableLocked() {
	var zero T
	f.action = ActionNone
	f.value = zero
}

// Return configures the failpoint to return value on every execution.
func (f *Failpoint[T]) Return(value T) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.action = ActionReturn
	f.value = value
}

// On makes the failpoint fire without a value and replaces any configured action. It is a boolean
// switch (e.g. to force an alternate code path): ExecuteSwitch returns true; Execute returns the
// zero value of T and true.
func (f *Failpoint[T]) On() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.action = ActionOn
}

// Execute executes the failpoint. It returns the configured value and whether the failpoint set it.
// A failpoint configured with On returns the zero value of T and true.
func (f *Failpoint[T]) Execute() (value T, set bool) {
	f.mu.Lock()
	action := f.action
	configured := f.value
	f.mu.Unlock()

	switch action {
	case ActionReturn, ActionOn:
		return configured, true
	default:
		return value, false
	}
}

func (f *Failpoint[T]) id() keyID {
	return f.key.id()
}
