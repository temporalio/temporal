// Copyright 2014 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package context defines the Context type, which carries deadlines,
// cancellation signals, and other request-scoped values across API boundaries
// and between processes.
//
// Incoming requests to a server should create a [Context], and outgoing
// calls to servers should accept a Context. The chain of function
// calls between them must propagate the Context, optionally replacing
// it with a derived Context created using [WithCancel], [WithDeadline],
// [WithTimeout], or [WithValue]. When a Context is canceled, all
// Contexts derived from it are also canceled.
//
// The [WithCancel], [WithDeadline], and [WithTimeout] functions take a
// Context (the parent) and return a derived Context (the child) and a
// [CancelFunc]. Calling the CancelFunc cancels the child and its
// children, removes the parent's reference to the child, and stops
// any associated timers. Failing to call the CancelFunc leaks the
// child and its children until the parent is canceled or the timer
// fires. The go vet tool checks that CancelFuncs are used on all
// control-flow paths.
//
// The [WithCancelCause] function returns a [CancelCauseFunc], which
// takes an error and records it as the cancellation cause. Calling
// [Cause] on the canceled context or any of its children retrieves
// the cause. If no cause is specified, Cause(ctx) returns the same
// value as ctx.Err().
//
// Programs that use Contexts should follow these rules to keep interfaces
// consistent across packages and enable static analysis tools to check context
// propagation:
//
// Do not store Contexts inside a struct type; instead, pass a Context
// explicitly to each function that needs it. The Context should be the first
// parameter, typically named ctx:
//
//	func DoSomething(ctx context.Context, arg Arg) error {
//		// ... use ctx ...
//	}
//
// Do not pass a nil [Context], even if a function permits it. Pass [context.TODO]
// if you are unsure about which Context to use.
//
// Use context Values only for request-scoped data that transits processes and
// APIs, not for passing optional parameters to functions.
//
// The same Context may be passed to functions running in different goroutines;
// Contexts are safe for simultaneous use by multiple goroutines.
//
// See https://blog.golang.org/context for example code for a server that uses
// Contexts.
package lib

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	SIMAPI "go.temporal.io/server/tools/gomad/api/lang"
)

// Canceled is the error returned by [Context.Err] when the context is canceled.
var Canceled = errors.New("context canceled")

// DeadlineExceeded is the error returned by [Context.Err] when the context's
// deadline passes.
var DeadlineExceeded error = deadlineExceededError{}

type deadlineExceededError struct{}

func (deadlineExceededError) Error() string { SIMAPI.FuncStart(); return "context deadline exceeded" }
func (deadlineExceededError) Timeout() bool { SIMAPI.FuncStart(); return true }
func (deadlineExceededError) Temporary() bool {
	SIMAPI.FuncStart(

	// An emptyCtx is never canceled, has no values, and has no deadline.
	// It is the common base of backgroundCtx and todoCtx.
	)
	return true
}

type emptyCtx struct{}

func (emptyCtx) Deadline() (deadline time.Time, ok bool) {
	SIMAPI.FuncStart()
	return
}

func (emptyCtx) Done() <-chan struct{} {
	SIMAPI.FuncStart()
	return nil
}

func (emptyCtx) Err() error {
	SIMAPI.FuncStart()
	return nil
}

func (emptyCtx) Value(key any) any {
	SIMAPI.FuncStart()
	return nil
}

type backgroundCtx struct {
	emptyCtx
}

func (backgroundCtx) String() string {
	SIMAPI.FuncStart()
	return "context.Background"
}

type todoCtx struct {
	emptyCtx
}

func (todoCtx) String() string {
	SIMAPI.FuncStart()
	return "context.TODO"
}

// Background returns a non-nil, empty [Context]. It is never canceled, has no
// values, and has no deadline. It is typically used by the main function,
// initialization, and tests, and as the top-level Context for incoming
// requests.
func Background() context.Context {
	SIMAPI.FuncStart()
	return backgroundCtx{}
}

// TODO returns a non-nil, empty [Context]. Code should use context.TODO when
// it's unclear which Context to use or it is not yet available (because the
// surrounding function has not yet been extended to accept a Context
// parameter).
func TODO() context.Context {
	SIMAPI.FuncStart()
	return todoCtx{}
}

// WithCancel returns a copy of parent with a new Done channel. The returned
// context's Done channel is closed when the returned cancel function is called
// or when the parent context's Done channel is closed, whichever happens first.
//
// Canceling this context releases resources associated with it, so code should
// call cancel as soon as the operations running in this Context complete.
func WithCancel(parent context.Context) (ctx context.Context, cancel context.CancelFunc) {
	SIMAPI.FuncStart()
	c := withCancel(parent)
	return c, func() { c.cancel(true, Canceled, nil) }
}

// A CancelCauseFunc behaves like a [CancelFunc] but additionally sets the cancellation cause.
// This cause can be retrieved by calling [Cause] on the canceled Context or on
// any of its derived Contexts.
//
// If the context has already been canceled, CancelCauseFunc does not set the cause.
// For example, if childContext is derived from parentContext:
//   - if parentContext is canceled with cause1 before childContext is canceled with cause2,
//     then Cause(parentContext) == Cause(childContext) == cause1
//   - if childContext is canceled with cause2 before parentContext is canceled with cause1,
//     then Cause(parentContext) == cause1 and Cause(childContext) == cause2
type CancelCauseFunc func(cause error)

// WithCancelCause behaves like [WithCancel] but returns a [CancelCauseFunc] instead of a [CancelFunc].
// Calling cancel with a non-nil error (the "cause") records that error in ctx;
// it can then be retrieved using Cause(ctx).
// Calling cancel with nil sets the cause to Canceled.
//
// Example use:
//
//	ctx, cancel := context.WithCancelCause(parent)
//	cancel(myError)
//	ctx.Err() // returns context.Canceled
//	context.Cause(ctx) // returns myError
func WithCancelCause(parent context.Context) (ctx context.Context, cancel CancelCauseFunc) {
	SIMAPI.FuncStart()
	c := withCancel(parent)
	return c, func(cause error) { c.cancel(true, Canceled, cause) }
}

func withCancel(parent context.Context) *cancelCtx {
	SIMAPI.FuncStart()
	if parent == nil {
		panic("cannot create context from nil parent")
	}
	c := &cancelCtx{}
	c.propagateCancel(parent, c)
	return c
}

// Cause returns a non-nil error explaining why c was canceled.
// The first cancellation of c or one of its parents sets the cause.
// If that cancellation happened via a call to CancelCauseFunc(err),
// then [Cause] returns err.
// Otherwise Cause(c) returns the same value as c.Err().
// Cause returns nil if c has not been canceled yet.
func Cause(c context.Context) error {
	SIMAPI.FuncStart()
	if cc, ok := c.Value(&cancelCtxKey).(*cancelCtx); ok {
		cc.mu.Lock()
		defer cc.mu.Unlock()
		return cc.cause
	}
	return nil
}

// AfterFunc arranges to call f in its own goroutine after ctx is done
// (cancelled or timed out).
// If ctx is already done, AfterFunc calls f immediately in its own goroutine.
//
// Multiple calls to AfterFunc on a context operate independently;
// one does not replace another.
//
// Calling the returned stop function stops the association of ctx with f.
// It returns true if the call stopped f from being run.
// If stop returns false,
// either the context is done and f has been started in its own goroutine;
// or f was already stopped.
// The stop function does not wait for f to complete before returning.
// If the caller needs to know whether f is completed,
// it must coordinate with f explicitly.
//
// If ctx has a "AfterFunc(func()) func() bool" method,
// AfterFunc will use it to schedule the call.
func CtxAfterFunc(ctx context.Context, f func()) (stop func() bool) {
	SIMAPI.FuncStart()
	a := &afterFuncCtx{f: f}

	a.cancelCtx.propagateCancel(ctx, a)
	return func() bool {
		stopped := false
		a.once.Do(func() {
			stopped = true
		})
		if stopped {
			a.cancel(true, Canceled, nil)
		}
		return stopped
	}
}

type afterFuncer interface {
	AfterFunc(func()) func() bool
}

type afterFuncCtx struct {
	cancelCtx
	once Once // either starts running f or stops f from running
	f    func()
}

func (a *afterFuncCtx) cancel(removeFromParent bool, err, cause error) {
	SIMAPI.FuncStart()
	a.cancelCtx.cancel(false, err, cause)
	if removeFromParent {
		removeChild(a.Context, a)
	}
	a.once.Do(func() {
		SIMAPI.Go(a.f)
	})
}

// A stopCtx is used as the parent context of a cancelCtx when
// an AfterFunc has been registered with the parent.
// It holds the stop function used to unregister the AfterFunc.
type stopCtx struct {
	context.Context
	stop func() bool
}

// goroutines counts the number of goroutines ever created; for testing.
var goroutines atomic.Int32

// &cancelCtxKey is the key that a cancelCtx returns itself for.
var cancelCtxKey int

// parentCancelCtx returns the underlying *cancelCtx for parent.
// It does this by looking up parent.Value(&cancelCtxKey) to find
// the innermost enclosing *cancelCtx and then checking whether
// parent.Done() matches that *cancelCtx. (If not, the *cancelCtx
// has been wrapped in a custom implementation providing a
// different done channel, in which case we should not bypass it.)
func parentCancelCtx(parent context.Context) (*cancelCtx, bool) {
	SIMAPI.FuncStart()
	done := parent.Done()
	if done == closedchan || done == nil {
		return nil, false
	}
	p, ok := parent.Value(&cancelCtxKey).(*cancelCtx)
	if !ok {
		return nil, false
	}
	pdone, _ := p.done.Load().(chan struct{})
	if pdone != done {
		return nil, false
	}
	return p, true
}

// removeChild removes a context from its parent.
func removeChild(parent context.Context, child canceler) {
	SIMAPI.FuncStart()
	if s, ok := parent.(stopCtx); ok {
		s.stop()
		return
	}
	p, ok := parentCancelCtx(parent)
	if !ok {
		return
	}
	p.mu.Lock()
	if p.children != nil {
		delete(p.children, child)
	}
	p.mu.Unlock()
}

// A canceler is a context type that can be canceled directly. The
// implementations are *cancelCtx and *timerCtx.
type canceler interface {
	cancel(removeFromParent bool, err, cause error)
	Done() <-chan struct{}
}

// closedchan is a reusable closed channel.
var closedchan = make(chan struct{})

func init() {
	SIMAPI.FuncStart()
	SIMAPI.

		// A cancelCtx can be canceled. When canceled, it also cancels any children
		// that implement canceler.
		ChanClose(closedchan)
}

type cancelCtx struct {
	context.Context

	mu       Mutex                 // protects following fields
	done     atomic.Value          // of chan struct{}, created lazily, closed by first cancel call
	children map[canceler]struct{} // set to nil by the first cancel call
	err      error                 // set to non-nil by the first cancel call
	cause    error
} // set to non-nil by the first cancel call

func (c *cancelCtx) Value(key any) any {
	SIMAPI.FuncStart()
	if key == &cancelCtxKey {
		return c
	}
	return value(c.Context, key)
}

func (c *cancelCtx) Done() <-chan struct{} {
	SIMAPI.FuncStart()
	d := c.done.Load()
	if d != nil {
		return d.(chan struct{})
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	d = c.done.Load()
	if d == nil {
		d = make(chan struct{})
		c.done.Store(d)
	}
	return d.(chan struct{})
}

func (c *cancelCtx) Err() error {
	SIMAPI.FuncStart()
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	return err
}

// propagateCancel arranges for child to be canceled when parent is.
// It sets the parent context of cancelCtx.
func (c *cancelCtx) propagateCancel(parent context.Context, child canceler) {
	SIMAPI.FuncStart()
	c.Context = parent

	done := parent.Done()
	if done == nil {
		return // parent is never canceled
	}
	switch selector := SIMAPI.Select(

		// parent is already canceled
		0, SIMAPI.RcvChan(done), nil, nil); selector.Case {
	case 0:

		child.cancel(false, parent.Err(), Cause(parent))
		return
	default:
	}

	if p, ok := parentCancelCtx(parent); ok {
		// parent is a *cancelCtx, or derives from one.
		p.mu.Lock()
		if p.err != nil {
			// parent has already been canceled
			child.cancel(false, p.err, p.cause)
		} else {
			if p.children == nil {
				p.children = make(map[canceler]struct{})
			}
			p.children[child] = struct{}{}
			SIMAPI.MapKey(child)
		}
		p.mu.Unlock()
		return
	}

	if a, ok := parent.(afterFuncer); ok {
		// parent implements an AfterFunc method.
		c.mu.Lock()
		stop := a.AfterFunc(func() {
			child.cancel(false, parent.Err(), Cause(parent))
		})
		c.Context = stopCtx{Context: parent,
			stop: stop}

		c.mu.Unlock()
		return
	}

	goroutines.Add(1)
	SIMAPI.Go(func() {
		switch selector := SIMAPI.Select(0, SIMAPI.RcvChan(parent.Done()), nil, 0, SIMAPI.RcvChan(child.Done()), nil); selector.Case {
		case 0:
			child.cancel(false, parent.Err(), Cause(parent))
		case 1:
		default:
			panic("unreachable")
		}

	})
}

type stringer interface {
	String() string
}

func contextName(c context.Context) string {
	SIMAPI.FuncStart()
	if s, ok := c.(stringer); ok {
		return s.String()
	}
	return reflect.TypeOf(c).String()
}

func (c *cancelCtx) String() string {
	SIMAPI.FuncStart()
	return contextName(c.Context) + ".WithCancel"
}

// cancel closes c.done, cancels each of c's children, and, if
// removeFromParent is true, removes c from its parent's children.
// cancel sets c.cause to cause if this is the first time c is canceled.
func (c *cancelCtx) cancel(removeFromParent bool, err, cause error) {
	SIMAPI.FuncStart()
	if err == nil {
		panic("context: internal error: missing cancel error")
	}
	if cause == nil {
		cause = err
	}
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return // already canceled
	}
	c.err = err
	c.cause = cause
	d, _ := c.done.Load().(chan struct{})
	if d == nil {
		c.done.Store(closedchan)
	} else {
		SIMAPI.ChanClose(d)
	}
	for _, child := range SIMAPI.MapKeys(c.children) {
		// NOTE: acquiring the child's lock while holding parent's lock.
		child.cancel(false, err, cause)
	}
	c.children = nil
	c.mu.Unlock()

	if removeFromParent {
		removeChild(c.Context, c)
	}
}

// WithoutCancel returns a copy of parent that is not canceled when parent is canceled.
// The returned context returns no Deadline or Err, and its Done channel is nil.
// Calling [Cause] on the returned context returns nil.
func WithoutCancel(parent context.Context) context.Context {
	SIMAPI.FuncStart()
	if parent == nil {
		panic("cannot create context from nil parent")
	}
	return withoutCancelCtx{parent}
}

type withoutCancelCtx struct {
	c context.Context
}

func (withoutCancelCtx) Deadline() (deadline time.Time, ok bool) {
	SIMAPI.FuncStart()
	return
}

func (withoutCancelCtx) Done() <-chan struct{} {
	SIMAPI.FuncStart()
	return nil
}

func (withoutCancelCtx) Err() error {
	SIMAPI.FuncStart()
	return nil
}

func (c withoutCancelCtx) Value(key any) any {
	SIMAPI.FuncStart()
	return value(c, key)
}

func (c withoutCancelCtx) String() string {
	SIMAPI.FuncStart()
	return contextName(c.c) + ".WithoutCancel"
}

// WithDeadline returns a copy of the parent context with the deadline adjusted
// to be no later than d. If the parent's deadline is already earlier than d,
// WithDeadline(parent, d) is semantically equivalent to parent. The returned
// [Context.Done] channel is closed when the deadline expires, when the returned
// cancel function is called, or when the parent context's Done channel is
// closed, whichever happens first.
//
// Canceling this context releases resources associated with it, so code should
// call cancel as soon as the operations running in this [Context] complete.
func WithDeadline(parent context.Context, d time.Time) (context.Context, context.CancelFunc) {
	SIMAPI.FuncStart()
	return WithDeadlineCause(parent, d, nil)
}

// WithDeadlineCause behaves like [WithDeadline] but also sets the cause of the
// returned Context when the deadline is exceeded. The returned [CancelFunc] does
// not set the cause.
func WithDeadlineCause(parent context.Context, d time.Time, cause error) (context.Context, context.CancelFunc) {
	SIMAPI.FuncStart()
	if parent == nil {
		panic("cannot create context from nil parent")
	}
	if cur, ok := parent.Deadline(); ok && cur.Before(d) {
		// The current deadline is already sooner than the new one.
		return WithCancel(parent)
	}
	c := &timerCtx{deadline: d}

	c.cancelCtx.propagateCancel(parent, c)
	dur := Until(d)
	if dur <= 0 {
		c.cancel(true, DeadlineExceeded, cause) // deadline has already passed
		return c, func() { c.cancel(false, Canceled, nil) }
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err == nil {
		c.timer = AfterFunc(dur, func() {
			c.cancel(true, DeadlineExceeded, cause)
		})
	}
	return c, func() { c.cancel(true, Canceled, nil) }
}

// A timerCtx carries a timer and a deadline. It embeds a cancelCtx to
// implement Done and Err. It implements cancel by stopping its timer then
// delegating to cancelCtx.cancel.
type timerCtx struct {
	cancelCtx
	timer *Timer // Under cancelCtx.mu.

	deadline time.Time
}

func (c *timerCtx) Deadline() (deadline time.Time, ok bool) {
	SIMAPI.FuncStart()
	return c.deadline, true
}

func (c *timerCtx) String() string {
	SIMAPI.FuncStart()
	return contextName(c.cancelCtx.Context) + ".WithDeadline(" +
		c.deadline.String() + " [" + Until(c.deadline).String() + "])"
}

func (c *timerCtx) cancel(removeFromParent bool, err, cause error) {
	SIMAPI.FuncStart()
	c.cancelCtx.cancel(false, err, cause)
	if removeFromParent {
		// Remove this timerCtx from its parent cancelCtx's children.
		removeChild(c.cancelCtx.Context, c)
	}
	c.mu.Lock()
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	c.mu.Unlock()
}

// WithTimeout returns WithDeadline(parent, time.Now().Add(timeout)).
//
// Canceling this context releases resources associated with it, so code should
// call cancel as soon as the operations running in this [Context] complete:
//
//	func slowOperationWithTimeout(ctx context.Context) (Result, error) {
//		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
//		defer cancel()  // releases resources if slowOperation completes before timeout elapses
//		return slowOperation(ctx)
//	}
func WithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	SIMAPI.FuncStart()
	return WithDeadline(parent, Now().Add(timeout))
}

// WithTimeoutCause behaves like [WithTimeout] but also sets the cause of the
// returned Context when the timeout expires. The returned [CancelFunc] does
// not set the cause.
func WithTimeoutCause(parent context.Context, timeout time.Duration, cause error) (context.Context, context.CancelFunc) {
	SIMAPI.FuncStart()
	return WithDeadlineCause(parent, Now().Add(timeout), cause)
}

// WithValue returns a copy of parent in which the value associated with key is
// val.
//
// Use context Values only for request-scoped data that transits processes and
// APIs, not for passing optional parameters to functions.
//
// The provided key must be comparable and should not be of type
// string or any other built-in type to avoid collisions between
// packages using context. Users of WithValue should define their own
// types for keys. To avoid allocating when assigning to an
// interface{}, context keys often have concrete type
// struct{}. Alternatively, exported context key variables' static
// type should be a pointer or interface.
func WithValue(parent context.Context, key, val any) context.Context {
	SIMAPI.FuncStart()
	if parent == nil {
		panic("cannot create context from nil parent")
	}
	if key == nil {
		panic("nil key")
	}
	if !reflect.TypeOf(key).Comparable() {
		panic("key is not comparable")
	}
	return &valueCtx{parent, key, val}
}

// A valueCtx carries a key-value pair. It implements Value for that key and
// delegates all other calls to the embedded Context.
type valueCtx struct {
	context.Context
	key, val any
}

// stringify tries a bit to stringify v, without using fmt, since we don't
// want context depending on the unicode tables. This is only used by
// *valueCtx.String().
func stringify(v any) string {
	SIMAPI.FuncStart()
	switch s := v.(type) {
	case stringer:
		return s.String()
	case string:
		return s
	}
	return "<not Stringer>"
}

func (c *valueCtx) String() string {
	SIMAPI.FuncStart()
	return contextName(c.Context) + ".WithValue(type " +
		reflect.TypeOf(c.key).String() +
		", val " + stringify(c.val) + ")"
}

func (c *valueCtx) Value(key any) any {
	SIMAPI.FuncStart()
	if c.key == key {
		return c.val
	}
	return value(c.Context, key)
}

func value(c context.Context, key any) any {
	SIMAPI.FuncStart()
	for {
		switch ctx := c.(type) {
		case *valueCtx:
			if key == ctx.key {
				return ctx.val
			}
			c = ctx.Context
		case *cancelCtx:
			if key == &cancelCtxKey {
				return c
			}
			c = ctx.Context
		case withoutCancelCtx:
			if key == &cancelCtxKey {
				// This implements Cause(ctx) == nil
				// when ctx is created using WithoutCancel.
				return nil
			}
			c = ctx.c
		case *timerCtx:
			if key == &cancelCtxKey {
				return &ctx.cancelCtx
			}
			c = ctx.Context
		case backgroundCtx, todoCtx:
			return nil
		default:
			return c.Value(key)
		}
	}
}

type _ = sync.Mutex
type _ = sync.Once

var _ = time.AfterFunc
var _ = time.Now

type _ = time.Timer

var _ = time.Until
