// In-process failpoints for testing. Unlike Pingcap's failpoints package[1]
// these are meant for use in injected code paths. For better performance
// (interfaces have overhead) you can use [1] below but it requires build-time
// code generation.
//
// [1]: https://pkg.go.dev/github.com/pingcap/failpoint#section-readme
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

// Recurrence is how often a failpoint executes. The values match the FailpointRecurrence proto
// enum.
type Recurrence int32

const (
	RecurrenceUnspecified Recurrence = iota
	Once
	Always
)

// Info describes the state of a failpoint.
type Info struct {
	Key        Key
	Enabled    bool
	Action     Action
	Recurrence Recurrence
}

// closedCh is a pre-closed channel returned by WaitCh when the goroutine is
// already blocked at the failpoint. It is immediately readable.
var closedCh = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// Failpoint is used to inject failures into code paths for testing.
// They begin disabled and can be enabled with a specific action.
//
// Changing a failpoint's action (e.g. from Return to Panic) will implicitly
// reset the failpoint, waking up any blocked goroutines.
type Failpoint[T any] struct {
	// mu guards all fields in this struct.
	mu sync.Mutex

	key        Key
	action     Action
	t          T
	panicValue any
	callback   func() T
	pauseDur   time.Duration
	blockCh    chan struct{}
	times      Recurrence

	// Block synchronization: blockCh, arrivedCh, and blocked work together.
	//
	// When Execute fires a Block failpoint it:
	//   1. Counts itself in blocked.
	//   2. Closes arrivedCh (waking any WaitCh/Wait callers) and replaces it
	//      with a fresh channel for the next arrival.
	//   3. Blocks on blockCh until Disable (close) or UnblockOne (send) is called.
	//   4. Uncounts itself from blocked before returning.
	//
	// WaitCh/Wait use blocked to handle the race where the goroutine
	// reaches the failpoint before the test calls WaitCh/Wait: if
	// an execution is already blocked, WaitCh returns a pre-closed channel
	// and Wait returns immediately without consulting arrivedCh at all.
	//
	// blocked counts the executions blocked at the current block configuration. It is replaced
	// with each configuration, so an execution released from an earlier configuration doesn't
	// uncount one that is blocked at the current one.
	blocked *int
	// afterRelease is called by an execution after its block is released, before it updates the
	// failpoint. Tests use it to order executions.
	afterRelease func()
	arrivedCh    chan struct{}
	// arrived is set when an execution reaching the block closes arrivedCh, as opposed to a
	// Disable or reconfiguration, so AwaitBlock can tell them apart. It is replaced together with
	// arrivedCh, so an arrival at a later block configuration doesn't affect an earlier one.
	arrived *bool

	// trigger limits which executions fire; rng is set when trigger has a probability.
	trigger Trigger
	rng     *rand.Rand
	// executions and fires count the executions of the enabled failpoint and how many fired.
	executions int64
	fires      int64
}

// Trigger limits when an enabled failpoint fires. The zero value fires on every execution.
type Trigger struct {
	// Probability is the chance, in (0, 1], that an execution fires. Zero means always.
	Probability float64
	// Seed seeds the random number generator for Probability. Zero means a time-based seed.
	Seed int64
	// After is the number of executions to skip before the failpoint can fire.
	After int64
	// Max is the maximum number of times the failpoint fires. Zero means unlimited.
	Max int64
}

// NewFailpoint creates a Failpoint with the given key.
func NewFailpoint[T any](key Key) *Failpoint[T] {
	return &Failpoint[T]{
		key:    key,
		action: Off,
	}
}

// SetTrigger limits when the failpoint fires and resets its execution counts.
func (f *Failpoint[T]) SetTrigger(trigger Trigger) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.trigger = trigger
	f.executions, f.fires = 0, 0
	f.rng = nil
	if trigger.Probability > 0 {
		f.rng = rand.New(rand.NewSource(cmp.Or(trigger.Seed, time.Now().UnixNano())))
	}
}

// Stats returns how often the enabled failpoint executed and how often it fired since its
// trigger was last set.
func (f *Failpoint[T]) Stats() (executions, fires int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.executions, f.fires
}

// fireLocked counts an execution and reports whether it fires. The caller must hold f.mu.
func (f *Failpoint[T]) fireLocked() bool {
	f.executions++
	if f.executions <= f.trigger.After {
		return false
	}
	if f.trigger.Max > 0 && f.fires >= f.trigger.Max {
		return false
	}
	if f.rng != nil && f.rng.Float64() >= f.trigger.Probability {
		return false
	}
	f.fires++
	return true
}

// Info returns the failpoint's current state.
func (f *Failpoint[T]) Info() Info {
	f.mu.Lock()
	defer f.mu.Unlock()
	return Info{
		Key:        f.key,
		Enabled:    f.action > Off,
		Action:     f.action,
		Recurrence: f.times,
	}
}

func (f *Failpoint[T]) disableLocked() {
	f.action = Off
	if f.blockCh != nil {
		close(f.blockCh)
		f.blockCh = nil
	}
	// Wake any goroutines waiting in WaitCh/Wait for the goroutine to arrive.
	if f.arrivedCh != nil {
		close(f.arrivedCh)
		f.arrivedCh = nil
		f.arrived = nil
	}
	f.blocked = nil
}

// currentlyBlocked reports whether an execution is blocked at the current block configuration.
// The caller must hold f.mu.
func (f *Failpoint[T]) currentlyBlocked() bool {
	return f.blocked != nil && *f.blocked > 0
}

func (f *Failpoint[T]) Disable() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
}

func (f *Failpoint[T]) Return(t T, times Recurrence) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.action = Return
	f.t = t
	f.times = times
}

func (f *Failpoint[T]) Panic(value any, times Recurrence) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.panicValue = value
	f.action = Panic
	f.times = times
}

func (f *Failpoint[T]) Call(fn func() T, times Recurrence) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.action = Call
	f.callback = fn
	f.times = times
}

func (f *Failpoint[T]) Block(times Recurrence) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.action = Block
	f.times = times
	// Store up to _one_ unlock. Be careful with this
	f.blockCh = make(chan struct{}, 1)
	f.arrivedCh = make(chan struct{})
	f.arrived = new(bool)
	f.blocked = new(int)
}

// UnblockOne unblocks one goroutine waiting on the failpoint.
// This is only relevant when the failpoint's action is Block.
//
// The send is performed under the lock so that a concurrent Disable cannot
// close blockCh between our snapshot of it and our send (which would panic
// with "send on closed channel"). blockCh has buffer capacity 1, and the
// select has a default case, so the send cannot block — it is safe to do
// under the lock.
func (f *Failpoint[T]) UnblockOne() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.blockCh == nil {
		return
	}
	select {
	case f.blockCh <- struct{}{}:
	default:
	}
}

func (f *Failpoint[T]) Pause(dur time.Duration, times Recurrence) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.action = Pause
	f.pauseDur = dur
	f.times = times
}

// Do runs the provided callback based on the failpoint's state.
//
// This is deprecated in favor of `Execute`.
func (f *Failpoint[T]) Do(op func() T) T {
	t1, set := f.Execute()
	if set {
		return t1
	}
	return op()
}

// Execute is an in-progress API for executing a failpoint and returning whether it set the
// return value.
//
// It will replace Do in its entirety
func (f *Failpoint[T]) Execute() (t T, setReturnVal bool) {
	return f.execute(nil)
}

// executeAny is Execute with the return value as any, for call sites of another type.
func (f *Failpoint[T]) executeAny(emit func(Event)) (any, bool) {
	t, ok := f.execute(emit)
	if !ok {
		return nil, false
	}
	return t, true
}

// execute is Execute that reports the execution to emit, if not nil. emit is called without
// holding f.mu, before a Block failpoint waits.
func (f *Failpoint[T]) execute(emit func(Event)) (t T, setReturnVal bool) {
	f.mu.Lock()

	if f.action <= Off {
		f.mu.Unlock()
		return t, false
	}

	action := f.action
	once := f.times == Once
	fired := f.fireLocked()
	event := Event{Key: f.key, Action: action, Fired: fired, Execution: f.executions}
	report := func() {
		if emit != nil {
			emit(event)
		}
	}

	if !fired {
		f.mu.Unlock()
		report()
		return t, false
	}

	// Non-Block actions hold the lock for their full execution, so a deferred
	// disableLocked is safe.
	if action != Block {
		// Deferred functions run in reverse order, so report runs after the unlock.
		defer report()
		defer f.mu.Unlock()
		if once {
			defer f.disableLocked()
		}

		switch action {
		case Return:
			return f.t, true
		case Panic:
			panic(f.panicValue)
		case Call:
			return f.callback(), true
		case Pause:
			time.Sleep(f.pauseDur)
		default:
			panic(fmt.Sprintf("unknown failpoint action: %s", action))
		}
		return t, false
	}

	// Block: we have to release the lock during the wait, so a deferred
	// disableLocked would race with concurrent Inject*/Disable calls and
	// could clobber state belonging to a freshly re-injected failpoint.
	// Disable inline (under the lock) for Once, then capture blockCh and
	// drop the lock for the wait. After re-acquiring the lock, only run
	// channel cleanup if blockCh is still ours; otherwise leave the new
	// failpoint's state alone.
	blocked := f.blocked
	*blocked++
	if f.arrivedCh != nil {
		*f.arrived = true
		close(f.arrivedCh)
	}
	f.arrivedCh = make(chan struct{}) // fresh channel for the next arrival
	f.arrived = new(bool)
	blockCh := f.blockCh
	if once {
		// Subsequent Execute calls (and any concurrent Inject) must observe
		// this failpoint as disabled. We intentionally don't close blockCh
		// here because we still need to receive on it.
		f.action = Off
	}
	f.mu.Unlock()
	report()

	<-blockCh
	if f.afterRelease != nil {
		f.afterRelease()
	}

	f.mu.Lock()
	*blocked--
	if once && f.blockCh == blockCh {
		// No one re-injected while we were waiting, so finish cleaning up.
		f.disableLocked()
	}
	f.mu.Unlock()
	return t, false
}

// WaitCh returns a channel that will be closed when the goroutine reaches the Block failpoint. It
// is safe to call before or after the goroutine is launched: if the goroutine is already blocked,
// the returned channel is immediately readable.
//
// If the failpoint is not currently configured for Block — for example,
// because a concurrent Disable raced with the caller — WaitCh returns a
// pre-closed channel rather than panicking. This makes WaitCh safe to use
// from request handlers that hold no lock against the failpoint state.
//
// The returned channel is suitable for use in a multi-case select. For a single wait, prefer Wait
// which also respects context cancellation.
func (f *Failpoint[T]) WaitCh() chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.action != Block || f.currentlyBlocked() {
		return closedCh
	}
	return f.arrivedCh
}

// Wait blocks until the goroutine reaches the Block failpoint or ctx is done.
// It is safe to call before or after the goroutine is launched. If the
// failpoint is not currently configured for Block, Wait returns nil
// immediately (mirroring WaitCh's tolerant semantics).
func (f *Failpoint[T]) Wait(ctx context.Context) error {
	f.mu.Lock()
	if f.action != Block || f.currentlyBlocked() {
		f.mu.Unlock()
		return nil
	}
	ch := f.arrivedCh
	f.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ErrNotBlocking is returned by AwaitBlock when the failpoint is not configured to block, or is
// disabled or reconfigured before an execution reaches the block.
var ErrNotBlocking = errors.New("failpoint is not configured to block")

// AwaitBlock is Wait that only succeeds when an execution actually reaches the block. Unlike
// Wait, it returns ErrNotBlocking instead of nil if the failpoint is not configured to block and
// holds no execution at the block, or if it is disabled or reconfigured during the wait.
func (f *Failpoint[T]) AwaitBlock(ctx context.Context) error {
	w, err := f.beginAwaitBlock()
	if err != nil || w == nil {
		return err
	}
	return f.finishAwaitBlock(ctx, w)
}

// blockWaiter is a wait for the next arrival at one block configuration.
type blockWaiter struct {
	ch      chan struct{}
	arrived *bool
}

// beginAwaitBlock starts AwaitBlock. It returns a nil waiter if an execution is already at the
// block.
func (f *Failpoint[T]) beginAwaitBlock() (*blockWaiter, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.currentlyBlocked() {
		return nil, nil
	}
	if f.action != Block {
		return nil, ErrNotBlocking
	}
	return &blockWaiter{ch: f.arrivedCh, arrived: f.arrived}, nil
}

// finishAwaitBlock waits for the arrival that w waits for.
func (f *Failpoint[T]) finishAwaitBlock(ctx context.Context, w *blockWaiter) error {
	select {
	case <-w.ch:
	case <-ctx.Done():
		return ctx.Err()
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if !*w.arrived {
		return ErrNotBlocking
	}
	return nil
}
