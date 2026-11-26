// Package failpoint injects faults at named points in code under test.
package failpoint

import (
	"fmt"
	"sync"
)

// Failpoint injects a fault or alternate branch at a named key in code under test.
// A new failpoint starts off and is configured with an action. Call Disable to turn it off.
//
// A failpoint has one configuration: an action and a Trigger that limits which executions fire.
// Action setters take the trigger as an optional last argument; without one the failpoint fires
// on every execution. To fire once, pass Trigger{MaxFires: 1}.
//
// Changing a failpoint's action replaces the trigger and resets the counts of Stats.
type Failpoint[T any] struct {
	// mu guards all fields in this struct.
	mu sync.Mutex

	key    Key[T]
	action Action
	value  T

	trigger Trigger
	// executions and fires count the executions of the configured failpoint and how many fired.
	executions int64
	fires      int64
	// totalFires counts every fire of the failpoint. SetTrigger doesn't reset it.
	totalFires int64
}

// Trigger limits when a failpoint fires. The zero value fires on every execution.
type Trigger struct {
	// SkipFirst is the number of executions to skip before the failpoint can fire.
	SkipFirst int64
	// MaxFires is the maximum number of times the failpoint fires. Zero means unlimited.
	MaxFires int64
}

func validateTrigger(trigger Trigger) error {
	if trigger.SkipFirst < 0 {
		return fmt.Errorf("skipFirst %d must be non-negative", trigger.SkipFirst)
	}
	if trigger.MaxFires < 0 {
		return fmt.Errorf("maxFires %d must be non-negative", trigger.MaxFires)
	}
	return nil
}

// NewFailpoint creates a Failpoint with the given key.
func NewFailpoint[T any](key Key[T]) *Failpoint[T] {
	return &Failpoint[T]{
		key:    key,
		action: ActionOff,
	}
}

// settings is the action configuration of a failpoint, without its trigger.
type settings[T any] struct {
	action Action
	value  T
}

// install replaces the whole configuration of the failpoint, its action and its trigger, so no
// execution sees a part of it, such as the new action with the old trigger.
func (f *Failpoint[T]) install(s settings[T], trigger Trigger) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.action = s.action
	f.value = s.value
	f.setTriggerLocked(trigger)
}

// installTrigger is install for an optional trigger argument of an action setter. It panics if
// there is more than one trigger or the trigger is invalid: that is a mistake in the test, as the
// setters have no error to return.
func (f *Failpoint[T]) installTrigger(s settings[T], trigger []Trigger) {
	var t Trigger
	switch len(trigger) {
	case 0:
	case 1:
		t = trigger[0]
		if err := validateTrigger(t); err != nil {
			//nolint:forbidigo // a setter has no error to return, and the trigger is invalid by the test's mistake
			panic(err)
		}
	default:
		//nolint:forbidigo // a setter takes at most one trigger
		panic(fmt.Sprintf("failpoint %s: at most one trigger, got %d", f.key, len(trigger)))
	}
	f.install(s, t)
}

// SetTrigger replaces the trigger of the current action, without changing the action, and resets
// the execution counts. It returns an error and changes nothing if the trigger is invalid.
func (f *Failpoint[T]) SetTrigger(trigger Trigger) error {
	if err := validateTrigger(trigger); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setTriggerLocked(trigger)
	return nil
}

func (f *Failpoint[T]) setTriggerLocked(trigger Trigger) {
	f.trigger = trigger
	f.executions, f.fires = 0, 0
}

// Stats returns how often the configured failpoint executed and how often it fired since its
// trigger was last set. A failpoint without an action (see Disable) doesn't count executions.
func (f *Failpoint[T]) Stats() (executions, fires int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.executions, f.fires
}

// TotalFires returns how often the failpoint fired in its lifetime. Unlike the fires of Stats,
// setting a trigger doesn't reset it.
func (f *Failpoint[T]) TotalFires() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.totalFires
}

// fireLocked counts an execution and reports whether it fires. The caller must hold f.mu.
func (f *Failpoint[T]) fireLocked() bool {
	f.executions++
	if f.executions <= f.trigger.SkipFirst {
		return false
	}
	if f.trigger.MaxFires > 0 && f.fires >= f.trigger.MaxFires {
		return false
	}
	f.fires++
	f.totalFires++
	return true
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

// Return configures the failpoint to return value. The optional trigger limits which executions
// fire, see Failpoint.
func (f *Failpoint[T]) Return(value T, trigger ...Trigger) {
	f.installTrigger(settings[T]{action: ActionReturn, value: value}, trigger)
}

// On activates the failpoint as a boolean switch (e.g. to force an alternate code path).
// ExecuteSwitch returns true; Execute returns the zero value of T and true. The optional trigger
// limits which executions fire, see Failpoint.
func (f *Failpoint[T]) On(trigger ...Trigger) {
	f.installTrigger(settings[T]{action: ActionOn}, trigger)
}

// Execute executes the failpoint. It returns the configured value and whether the failpoint fired.
// A failpoint configured with On returns the zero value of T and true.
func (f *Failpoint[T]) Execute() (t T, fired bool) {
	f.mu.Lock()
	if f.action <= ActionOff {
		f.mu.Unlock()
		return t, false
	}
	action := f.action
	if !f.fireLocked() {
		f.mu.Unlock()
		return t, false
	}
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
