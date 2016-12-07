package coroutine

import (
	"time"

	"golang.org/x/net/context"
)

// Channel must be used instead of native go channel by coroutine code.
// Use Context.NewChannel method to create an instance.
type Channel interface {
	Recv(ctx Context) (v interface{}, more bool)    // more is false when channel is closed
	RecvAsync() (v interface{}, ok bool, more bool) // ok is true when value was returned, more is false when channel is closed

	Send(ctx Context, v interface{})
	SendAsync(v interface{}) (ok bool) // ok when value was sent
	Close()                            // prohibit sends
}

// RecvCaseFunc is executed when a value is received from the corresponding channel
type RecvCaseFunc func(v interface{}, more bool)

// SendCaseFunc is executed when value was sent to a correspondent channel
type SendCaseFunc func()

// DefaultCaseFunc is executed when none of the channel cases executed
type DefaultCaseFunc func()

// Selector must be used instead of native go select by coroutine code
// Use Context.NewSelector method to create an instance.
type Selector interface {
	AddRecv(c Channel, f RecvCaseFunc) Selector
	AddSend(c Channel, v interface{}, f SendCaseFunc) Selector
	AddDefault(f DefaultCaseFunc)
	Select(ctx Context)
}

// Func is a body of a coroutine
type Func func(ctx Context)

// Context is coroutine specific context that extends context.Context.
// Use coroutine.Use... methods to create subcontexts.
// Provides factory methods to create Channel and Selector.
// Provides method to create new coroutine.
type Context interface {
	context.Context

	NewChannel() Channel
	NewNamedChannel(name string) Channel

	NewBufferedChannel(size int) Channel
	NewNamedBufferedChannel(name string, size int) Channel

	NewSelector() Selector
	NewNamedSelector(name string) Selector

	NewCoroutine(f Func)
	NewNamedCoroutine(name string, f Func)
}

// WithCancel returns a copy of parent with a new Done channel. The returned
// context's Done channel is closed when the returned cancel function is called
// or when the parent context's Done channel is closed, whichever happens first.
//
// Canceling this context releases resources associated with it, so code should
// call cancel as soon as the operations running in this Context complete.
func WithCancel(parent Context) (ctx Context, cancel context.CancelFunc) {
	pImpl := parent.(*contextImpl)
	pImpl.Context, cancel = context.WithCancel(pImpl.Context)
	return pImpl, cancel
}

// WithDeadline returns a copy of the parent context with the deadline adjusted
// to be no later than d.  If the parent's deadline is already earlier than d,
// WithDeadline(parent, d) is semantically equivalent to parent.  The returned
// context's Done channel is closed when the deadline expires, when the returned
// cancel function is called, or when the parent context's Done channel is
// closed, whichever happens first.
//
// Canceling this context releases resources associated with it, so code should
// call cancel as soon as the operations running in this Context complete.
func WithDeadline(parent Context, deadline time.Time) (ctx Context, cancel context.CancelFunc) {
	pImpl := parent.(*contextImpl)
	pImpl.Context, cancel = context.WithDeadline(pImpl.Context, deadline)
	return pImpl, cancel
}

// WithTimeout returns WithDeadline(parent, time.Now().Add(timeout)).
//
// Canceling this context releases resources associated with it, so code should
// call cancel as soon as the operations running in this Context complete:
//
// 	func slowOperationWithTimeout(ctx Context) (Result, error) {
// 		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
// 		defer cancel()  // releases resources if slowOperation completes before timeout elapses
// 		return slowOperation(ctx)
// 	}
func WithTimeout(parent Context, timeout time.Duration) (ctx Context, cancel context.CancelFunc) {
	pImpl := parent.(*contextImpl)
	pImpl.Context, cancel = context.WithTimeout(pImpl.Context, timeout)
	return pImpl, cancel
}

// WithValue returns a copy of parent in which the value associated with key is
// val.
//
// Use context Values only for request-scoped data that transits processes and
// APIs, not for passing optional parameters to functions.
func WithValue(parent Context, key interface{}, val interface{}) Context {
	pImpl := parent.(*contextImpl)
	pImpl.Context = context.WithValue(pImpl.Context, key, val)
	return pImpl
}

// PanicError contains information about panicked coroutine
type PanicError interface {
	error
	Value() interface{} // Value passed to panic call
	StackTrace() string // Stack trace of a panicked coroutine
}

// Dispatcher is a container of a set of coroutines.
type Dispatcher interface {
	// ExecuteUntilAllBlocked executes coroutines one by one in deterministic order
	// until all of them are completed or blocked on Channel or Selector
	ExecuteUntilAllBlocked() (err PanicError)
	// IsDone returns true when all of coroutines are completed
	IsDone() bool
	Close()             // Destroys all coroutines without waiting for their completion
	StackTrace() string // Stack trace of all coroutines owned by the Dispatcher instance
}

// NewDispatcher creates a new Dispatcher instance with a root coroutine function.
func NewDispatcher(root Func) Dispatcher {
	result := &dispatcherImpl{}
	result.newCoroutine(root)
	return result
}
