// Package failpoint injects faults and timing control at named points in code under test.
package failpoint

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Failpoint is used to inject failures and timing control into code paths for testing.
// They begin without an action and are configured with one. To turn a failpoint off and release
// the executions it holds, call Disable. Removing it from its Registry with Registry.Delete only
// detaches it, so call Disable too.
//
// A failpoint has one configuration: an action and a Trigger that limits which executions fire.
// The action setters (Return, Panic, Call, Block, On and Pause) take the trigger as an optional
// last argument; without one the failpoint fires on every execution. To fire once, pass
// Trigger{MaxFires: 1}.
//
// Changing a failpoint's action (e.g. from Return to Panic) implicitly resets the failpoint,
// waking up any blocked or paused goroutines. It also replaces the trigger and resets the counts
// of Stats.
type Failpoint[T any] struct {
	// mu guards all fields in this struct.
	mu sync.Mutex

	key        Key[T]
	action     Action
	value      T
	panicValue any
	callback   func(arg any) T
	pauseDur   time.Duration
	// pauseCh is closed to end the pauses of the current configuration early.
	pauseCh chan struct{}

	// Block synchronization: blockCh, arrivedCh, and arrived belong to one armed block
	// configuration.
	//
	// When Execute fires a Block failpoint it:
	//   1. Sets arrived and closes arrivedCh on the first arrival of this configuration, waking
	//      any Wait callers.
	//   2. Parks on blockCh until Disable or reconfiguration closes it.
	//
	// Disable and reconfiguration close blockCh (never arrivedCh), so Wait distinguishes arrival
	// from release without extra allocations or a post-wake lock.
	blockCh   chan struct{}
	arrivedCh chan struct{}
	arrived   bool

	// trigger limits which executions fire; rng is set when trigger has a probability, and seed is
	// the seed of rng.
	trigger Trigger
	rng     *rand.Rand
	seed    int64
	// executions and fires count the executions of the configured failpoint and how many fired.
	executions int64
	fires      int64
	// totalFires counts every fire of the failpoint. SetTrigger doesn't reset it.
	totalFires int64
}

// Trigger limits when a failpoint fires. The zero value fires on every execution.
type Trigger struct {
	// Probability is the chance, in (0, 1], that an execution fires. Zero means always.
	Probability float64
	// Seed seeds the random number generator for Probability. Zero means a time-based seed, which
	// Failpoint.Seed reports.
	Seed int64
	// SkipFirst is the number of executions to skip before the failpoint can fire.
	SkipFirst int64
	// MaxFires is the maximum number of times the failpoint fires. Zero means unlimited. One fires
	// once: for a Block action, exactly one execution blocks.
	MaxFires int64
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
	action     Action
	value      T
	panicValue any
	callback   func(arg any) T
	pauseDur   time.Duration
}

// install replaces the whole configuration of the failpoint, its action and its trigger, so no
// execution sees a part of it, such as the new action with the old trigger, and releases the
// executions held by the previous one. It returns the seed of the trigger, see Seed.
func (f *Failpoint[T]) install(s settings[T], trigger Trigger) (seed int64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.action = s.action
	f.value = s.value
	f.panicValue = s.panicValue
	f.callback = s.callback
	f.pauseDur = s.pauseDur
	f.setTriggerLocked(trigger)
	if f.action == ActionPause {
		f.pauseCh = make(chan struct{})
	}
	if f.action == ActionBlock {
		f.armBlockLocked()
	}
	return f.seed
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
// the execution counts. It returns an error and changes nothing if the trigger is invalid. For a
// Block action it also releases the executions that the block holds and arms it again, so a
// trigger of MaxFires 1 holds one new execution, not a second one besides the held one.
func (f *Failpoint[T]) SetTrigger(trigger Trigger) error {
	if err := validateTrigger(trigger); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.setTriggerLocked(trigger)
	if f.action == ActionBlock {
		f.releaseLocked()
		f.armBlockLocked()
	}
	return nil
}

func (f *Failpoint[T]) setTriggerLocked(trigger Trigger) {
	f.trigger = trigger
	f.executions, f.fires = 0, 0
	f.rng, f.seed = nil, 0
	if trigger.Probability > 0 {
		f.seed = cmp.Or(trigger.Seed, time.Now().UnixNano())
		f.rng = rand.New(rand.NewSource(f.seed))
	}
}

// Seed returns the seed of the trigger's random numbers, which is the seed of the trigger or, if
// it has none, the one picked when the trigger was set. Setting a trigger with this seed repeats
// the sequence of fires. It is zero if the trigger has no probability.
func (f *Failpoint[T]) Seed() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.seed
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
	if f.rng != nil && f.rng.Float64() >= f.trigger.Probability {
		return false
	}
	f.fires++
	f.totalFires++
	return true
}

func (f *Failpoint[T]) disableLocked() {
	var zero T
	f.action = ActionOff
	f.value = zero
	f.panicValue = nil
	f.callback = nil
	f.pauseDur = 0
	f.releaseLocked()
}

// releaseLocked releases the executions blocked or paused at the failpoint and wakes callers
// waiting in Wait. The caller must hold f.mu.
func (f *Failpoint[T]) releaseLocked() {
	if f.pauseCh != nil {
		close(f.pauseCh)
		f.pauseCh = nil
	}
	if f.blockCh != nil {
		close(f.blockCh)
		f.blockCh = nil
	}
	f.arrivedCh = nil
	f.arrived = false
}

// armBlockLocked creates the channels of a block configuration. The caller must hold f.mu.
func (f *Failpoint[T]) armBlockLocked() {
	f.blockCh = make(chan struct{})
	f.arrivedCh = make(chan struct{})
	f.arrived = false
}

// Disable removes the action of the failpoint, so it doesn't fire, clears its configured payload,
// and releases the executions that it blocks or pauses and the callers of Wait. Removing the
// failpoint from its Registry with Registry.Delete doesn't disable it: call Disable to release
// what it holds.
func (f *Failpoint[T]) Disable() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
}

// Return configures the failpoint to return value. The optional trigger limits which executions
// fire, see Failpoint.
func (f *Failpoint[T]) Return(value T, trigger ...Trigger) {
	f.installTrigger(settings[T]{action: ActionReturn, value: value}, trigger)
}

// Panic configures the failpoint to panic with value. The optional trigger limits which
// executions fire, see Failpoint.
func (f *Failpoint[T]) Panic(value any, trigger ...Trigger) {
	f.installTrigger(settings[T]{action: ActionPanic, panicValue: value}, trigger)
}

// Call configures the failpoint to call fn and return its result. fn runs without holding the
// failpoint's lock: the callbacks of concurrent executions can run in parallel and must
// synchronize their own state, and fn can call into the failpoint. Disable and reconfiguration
// don't wait for callbacks that already started. The optional trigger limits which executions
// fire, see Failpoint.
func (f *Failpoint[T]) Call(fn func() T, trigger ...Trigger) {
	f.CallWithArg(func(any) T { return fn() }, trigger...)
}

// CallWithArg is Call with a callback that receives the argument of the execution, which the call
// site passes via ExecuteOptions.Arg, for example the request it is about to handle. Executions
// without an argument pass nil.
func (f *Failpoint[T]) CallWithArg(fn func(arg any) T, trigger ...Trigger) {
	f.installTrigger(settings[T]{action: ActionCall, callback: fn}, trigger)
}

// Block configures the failpoint to hold the executions that fire until Disable or
// reconfiguration releases them. Pass Trigger{MaxFires: 1} to hold exactly one execution. The
// optional trigger limits which executions fire, see Failpoint.
func (f *Failpoint[T]) Block(trigger ...Trigger) {
	f.installTrigger(settings[T]{action: ActionBlock}, trigger)
}

// On activates the failpoint as a boolean switch (e.g. to force an alternate code path).
// ExecuteSwitch returns true; Execute returns the zero value of T and true. The optional trigger
// limits which executions fire, see Failpoint.
func (f *Failpoint[T]) On(trigger ...Trigger) {
	f.installTrigger(settings[T]{action: ActionOn}, trigger)
}

// Pause configures the failpoint to sleep for dur. Concurrent executions pause in parallel.
// Disable and reconfiguration end the pauses that already started. The optional trigger limits
// which executions fire, see Failpoint.
func (f *Failpoint[T]) Pause(dur time.Duration, trigger ...Trigger) {
	f.installTrigger(settings[T]{action: ActionPause, pauseDur: dur}, trigger)
}

// Execute executes the failpoint. It returns the configured value and whether the failpoint set it.
// A failpoint configured with On returns the zero value of T and true.
func (f *Failpoint[T]) Execute() (t T, setReturnVal bool) {
	t, setReturnVal, _ = f.execute(nil)
	return t, setReturnVal
}

// execute is Execute that passes arg to a callback. It also reports whether the failpoint fired,
// with any action.
func (f *Failpoint[T]) execute(arg any) (t T, setReturnVal, fired bool) {
	f.mu.Lock()
	if f.action <= ActionOff {
		f.mu.Unlock()
		return t, false, false
	}
	action := f.action
	if !f.fireLocked() {
		f.mu.Unlock()
		return t, false, false
	}

	// Non-Block actions capture their arguments under the lock, and run without it, so concurrent
	// executions don't wait for each other's pause or callback, and a callback can call into the
	// failpoint.
	if action != ActionBlock {
		value, panicValue, callback, pauseDur, pauseCh := f.value, f.panicValue, f.callback, f.pauseDur, f.pauseCh
		f.mu.Unlock()

		switch action {
		case ActionReturn, ActionOn:
			return value, true, true
		case ActionPanic:
			//nolint:forbidigo // the Panic action panics by design
			panic(panicValue)
		case ActionCall:
			return callback(arg), true, true
		case ActionPause:
			timer := time.NewTimer(pauseDur)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-pauseCh:
			}
		default:
		}
		return t, false, true
	}

	// Mark the first arrival for this block configuration and release the lock while parked.
	if !f.arrived {
		f.arrived = true
		close(f.arrivedCh)
	}
	blockCh := f.blockCh
	f.mu.Unlock()

	<-blockCh
	return t, false, true
}

func (f *Failpoint[T]) id() keyID {
	return f.key.id()
}

// ErrNotBlocking is returned by Wait when the failpoint is not configured to block, or is disabled
// or reconfigured before an execution reaches the block.
var ErrNotBlocking = errors.New("failpoint is not configured to block")

// Wait blocks until an execution reaches the block or ctx is done. It can be called before or after
// the execution starts. It returns ErrNotBlocking if the failpoint is not configured to block, or
// if it is disabled or reconfigured before an execution arrives. A block of Trigger{MaxFires: 1}
// holds one execution: Wait returns nil while it is held.
func (f *Failpoint[T]) Wait(ctx context.Context) error {
	f.mu.Lock()
	if f.action != ActionBlock {
		f.mu.Unlock()
		return ErrNotBlocking
	}
	if f.arrived {
		f.mu.Unlock()
		return nil
	}
	arrivedCh, blockCh := f.arrivedCh, f.blockCh
	f.mu.Unlock()

	select {
	case <-arrivedCh:
		return nil
	case <-blockCh:
		// If an execution arrived just before Disable/reconfiguration closed blockCh, both
		// channels are closed; arrival wins.
		select {
		case <-arrivedCh:
			return nil
		default:
			return ErrNotBlocking
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}
