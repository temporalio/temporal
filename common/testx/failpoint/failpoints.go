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

// NewFailpoint creates a Failpoint with the given key.
func NewFailpoint[T any](key Key[T]) *Failpoint[T] {
	return &Failpoint[T]{
		key:    key,
		action: ActionOff,
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
	f.action = ActionOff
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

// On configures the failpoint to fire without a value. Execute reports that it fired and returns
// the zero value of T.
func (f *Failpoint[T]) On() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.action = ActionOn
}

// Execute executes the failpoint. It returns the configured value and whether the failpoint fired.
// A failpoint configured with On returns the zero value of T and true.
func (f *Failpoint[T]) Execute() (t T, fired bool) {
	f.mu.Lock()
	action := f.action
	value := f.value
	f.mu.Unlock()

	switch action {
	case ActionReturn, ActionOn:
		return value, true
	default:
		return t, false
	}
}

func (f *Failpoint[T]) id() keyID {
	return f.key.id()
}

func (f *Failpoint[T]) unwrap() entry {
	if f == nil {
		return nil
	}
	return f
}
