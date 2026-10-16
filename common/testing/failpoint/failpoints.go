//go:build test_dep

// In-process failpoints for testing. Unlike Pingcap's failpoints package[1]
// these are meant for use in injected code paths. For better performance
// (interfaces have overhead) you can use [1] below but it requires build-time
// code generation.
//
// [1]: https://pkg.go.dev/github.com/pingcap/failpoint#section-readme
package failpoint

import (
	"context"
	"fmt"
	"sync"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/failpointservice/v1"
)

type (
	Action     = enumsspb.FailpointAction
	Recurrence = enumsspb.FailpointRecurrence
)

const (
	Unspecified = enumsspb.FAILPOINT_ACTION_UNSPECIFIED
	Off         = enumsspb.FAILPOINT_ACTION_OFF
	Return      = enumsspb.FAILPOINT_ACTION_RETURN
	Panic       = enumsspb.FAILPOINT_ACTION_PANIC
	Block       = enumsspb.FAILPOINT_ACTION_BLOCK
	Pause       = enumsspb.FAILPOINT_ACTION_PAUSE
	Call        = enumsspb.FAILPOINT_ACTION_CALL

	Once   = enumsspb.FAILPOINT_RECURRENCE_ONCE
	Always = enumsspb.FAILPOINT_RECURRENCE_ALWAYS
)

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

	// Block synchronization: blockCh, arrivedCh, and currentlyBlocked work together.
	//
	// When Execute fires a Block failpoint it:
	//   1. Sets currentlyBlocked = true.
	//   2. Closes arrivedCh (waking any WaitCh/Wait callers) and replaces it
	//      with a fresh channel for the next arrival.
	//   3. Blocks on blockCh until Disable (close) or UnblockOne (send) is called.
	//   4. Sets currentlyBlocked = false before returning.
	//
	// WaitCh/Wait use currentlyBlocked to handle the race where the goroutine
	// reaches the failpoint before the test calls WaitCh/Wait: if
	// currentlyBlocked is already true, WaitCh returns a pre-closed channel
	// and Wait returns immediately without consulting arrivedCh at all.
	currentlyBlocked bool
	arrivedCh        chan struct{}
}

// NewFailpoint creates a Failpoint with the given key.
func NewFailpoint[T any](key Key) *Failpoint[T] {
	return &Failpoint[T]{
		key:    key,
		action: Off,
	}
}

// Info returns the failpoint's current state as a FailpointInfo proto.
func (f *Failpoint[T]) Info() *failpointservice.FailpointInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &failpointservice.FailpointInfo{
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
	}
	f.currentlyBlocked = false
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
	f.mu.Lock()

	if f.action <= Off {
		f.mu.Unlock()
		return t, false
	}

	action := f.action
	once := f.times == Once

	// Non-Block actions hold the lock for their full execution, so a deferred
	// disableLocked is safe.
	if action != Block {
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
	f.currentlyBlocked = true
	if f.arrivedCh != nil {
		close(f.arrivedCh)
	}
	f.arrivedCh = make(chan struct{}) // fresh channel for the next arrival
	blockCh := f.blockCh
	if once {
		// Subsequent Execute calls (and any concurrent Inject) must observe
		// this failpoint as disabled. We intentionally don't close blockCh
		// here because we still need to receive on it.
		f.action = Off
	}
	f.mu.Unlock()

	<-blockCh

	f.mu.Lock()
	f.currentlyBlocked = false
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

	if f.action != Block || f.currentlyBlocked {
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
	if f.action != Block || f.currentlyBlocked {
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

// Failpoint2 is a failpoint that can be triggered with two arguments.
// It behaves exactly like Failpoint[T].
type Failpoint2[T1 any, T2 any] struct {
	mu         sync.Mutex
	key        Key
	action     Action
	t1         T1
	t2         T2
	panicValue any
	callback   func() (T1, T2)
	pauseDur   time.Duration
	blockCh    chan struct{}
	times      Recurrence

	// See the Failpoint[T] struct for a description of these fields.
	currentlyBlocked bool
	arrivedCh        chan struct{}
}

// NewFailpoint2 creates a Failpoint2 with the given key.
func NewFailpoint2[T1, T2 any](key Key) *Failpoint2[T1, T2] {
	return &Failpoint2[T1, T2]{
		key:    key,
		action: Off,
	}
}

// Info returns the failpoint's current state as a FailpointInfo proto.
func (f *Failpoint2[T1, T2]) Info() *failpointservice.FailpointInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &failpointservice.FailpointInfo{
		Key:        f.key,
		Enabled:    f.action > Off,
		Action:     f.action,
		Recurrence: f.times,
	}
}

func (f *Failpoint2[T1, T2]) disableLocked() {
	f.action = Off
	if f.blockCh != nil {
		close(f.blockCh)
		f.blockCh = nil
	}
	if f.arrivedCh != nil {
		close(f.arrivedCh)
		f.arrivedCh = nil
	}
	f.currentlyBlocked = false
}

func (f *Failpoint2[T1, T2]) Disable() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
}

func (f *Failpoint2[T1, T2]) Return(t1 T1, t2 T2, times Recurrence) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.action = Return
	f.t1 = t1
	f.t2 = t2
	f.times = times
}

func (f *Failpoint2[T1, T2]) Panic(value any, times Recurrence) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.action = Panic
	f.panicValue = value
	f.times = times
}

func (f *Failpoint2[T1, T2]) Call(fn func() (T1, T2), times Recurrence) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.action = Call
	f.callback = fn
	f.times = times
}

func (f *Failpoint2[T1, T2]) Block(times Recurrence) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.action = Block
	f.times = times
	// Store up to _one_ unlock. Be careful with this
	f.blockCh = make(chan struct{}, 1)
	f.arrivedCh = make(chan struct{})
}

// UnblockOne unblocks one goroutine waiting on the failpoint.
// This is only relevant when the failpoint's action is Block.
// See Failpoint[T].UnblockOne for why the send is performed under the lock.
func (f *Failpoint2[T1, T2]) UnblockOne() {
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

func (f *Failpoint2[T1, T2]) Pause(dur time.Duration, times Recurrence) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.disableLocked()
	f.action = Pause
	f.pauseDur = dur
	f.times = times
}

func (f *Failpoint2[T1, T2]) Do(op func() (T1, T2)) (T1, T2) {
	t1, t2, set := f.Execute()
	if set {
		return t1, t2
	}
	return op()
}

// Execute is an in-progress API for executing a failpoint and returning whether it set the
// return value.
//
// It will replace Do in its entirety
func (f *Failpoint2[T1, T2]) Execute() (t1 T1, t2 T2, setReturnVal bool) {
	f.mu.Lock()

	if f.action <= Off {
		f.mu.Unlock()
		return t1, t2, false
	}

	action := f.action
	once := f.times == Once

	// See Failpoint[T].Execute for why Block is treated separately.
	if action != Block {
		defer f.mu.Unlock()
		if once {
			defer f.disableLocked()
		}

		switch action {
		case Return:
			return f.t1, f.t2, true
		case Panic:
			panic(f.panicValue)
		case Call:
			t1, t2 = f.callback()
			return t1, t2, true
		case Pause:
			time.Sleep(f.pauseDur)
		default:
			panic(fmt.Sprintf("unknown failpoint action: %s", action))
		}
		return t1, t2, false
	}

	f.currentlyBlocked = true
	if f.arrivedCh != nil {
		close(f.arrivedCh)
	}
	f.arrivedCh = make(chan struct{})
	blockCh := f.blockCh
	if once {
		f.action = Off
	}
	f.mu.Unlock()

	<-blockCh

	f.mu.Lock()
	f.currentlyBlocked = false
	if once && f.blockCh == blockCh {
		f.disableLocked()
	}
	f.mu.Unlock()
	return t1, t2, false
}

// WaitCh returns a channel that will be closed when the goroutine reaches the
// Block failpoint. Safe to call before or after the goroutine is launched.
// See Failpoint[T].WaitCh for the non-Block fallback semantics.
func (f *Failpoint2[T1, T2]) WaitCh() chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.action != Block || f.currentlyBlocked {
		return closedCh
	}
	return f.arrivedCh
}

// Wait blocks until the goroutine reaches the Block failpoint or ctx is done.
// It is safe to call before or after the goroutine is launched. See
// Failpoint[T].Wait for the non-Block fallback semantics.
func (f *Failpoint2[T1, T2]) Wait(ctx context.Context) error {
	f.mu.Lock()
	if f.action != Block || f.currentlyBlocked {
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
