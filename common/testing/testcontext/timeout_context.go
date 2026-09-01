package testcontext

import (
	"context"
	"sync"
	"time"
)

// timeoutContext separates two clocks so its identity stays stable: ceiling is
// the fixed hard limit reported by Deadline, while activeExpiration controls
// when Done closes and only moves forward through extend.
type timeoutContext struct {
	context.Context
	parent            context.Context
	parentDeadline    time.Time
	hasParentDeadline bool
	ceiling           time.Time
	done              chan struct{}
	owner             *contextState

	mu                 sync.Mutex
	activeExpiration   time.Time
	timer              *time.Timer
	stopParentCallback func() bool
	cancelCause        context.CancelCauseFunc
	err                error
}

func newTimeoutContext(parent context.Context, ceiling, activeExpiration time.Time, owner *contextState) *timeoutContext {
	parentDeadline, hasParentDeadline := parent.Deadline()
	if hasParentDeadline && parentDeadline.Before(ceiling) {
		ceiling = parentDeadline
	}
	if ceiling.Before(activeExpiration) {
		activeExpiration = ceiling
	}
	causeContext, cancelCause := context.WithCancelCause(context.WithoutCancel(parent))
	ctx := &timeoutContext{
		Context:           causeContext,
		parent:            parent,
		parentDeadline:    parentDeadline,
		hasParentDeadline: hasParentDeadline,
		ceiling:           ceiling,
		done:              make(chan struct{}),
		owner:             owner,
		activeExpiration:  activeExpiration,
		cancelCause:       cancelCause,
	}

	// Either callback may run immediately, and finishLocked needs both handles
	// initialized. Hold the lock until both are installed.
	ctx.mu.Lock()
	ctx.timer = time.AfterFunc(time.Until(activeExpiration), ctx.expire)
	ctx.stopParentCallback = context.AfterFunc(parent, func() {
		ctx.finish(parent.Err(), context.Cause(parent))
	})
	ctx.mu.Unlock()
	return ctx
}

// Deadline reports the ceiling, not the active expiration: the context's
// identity is stable across [timeoutContext.extend], so the reported deadline
// must not shrink or grow with it. Done may therefore close well before the
// reported deadline. Watch Done; do not compute a budget from Deadline.
func (c *timeoutContext) Deadline() (time.Time, bool) {
	return c.ceiling, true
}

func (c *timeoutContext) Done() <-chan struct{} {
	return c.done
}

func (c *timeoutContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *timeoutContext) extend(expiration time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.err != nil {
		return
	}
	if c.ceiling.Before(expiration) {
		expiration = c.ceiling
	}
	if !expiration.After(c.activeExpiration) {
		return
	}

	c.activeExpiration = expiration
	c.timer.Reset(time.Until(expiration))
}

func (c *timeoutContext) effectiveExpiration() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.activeExpiration
}

func (c *timeoutContext) activeExpirationOwnership() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	owned := !c.hasParentDeadline || c.activeExpiration.Before(c.parentDeadline)
	return c.activeExpiration, owned
}

func (c *timeoutContext) cancel() {
	c.finish(context.Canceled, context.Canceled)
}

func (c *timeoutContext) finish(err, cause error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.finishLocked(err, cause)
}

func (c *timeoutContext) expire() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.err != nil {
		return
	}
	// Reset may race a callback that already started. Re-check the active
	// expiration so that stale callbacks cannot cancel an extended context.
	if remaining := time.Until(c.activeExpiration); remaining > 0 {
		c.timer.Reset(remaining)
		return
	}
	err := error(context.DeadlineExceeded)
	cause := err
	if !c.hasParentDeadline || c.activeExpiration.Before(c.parentDeadline) {
		cause = newDeadlineExceededCause(c.activeExpiration, c.owner)
	} else if parentErr := c.parent.Err(); parentErr != nil {
		err = parentErr
		cause = context.Cause(c.parent)
	}
	c.finishLocked(err, cause)
}

func (c *timeoutContext) finishLocked(err, cause error) {
	if c.err != nil {
		return
	}
	c.err = err
	c.cancelCause(cause)
	close(c.done)
	c.timer.Stop()
	c.stopParentCallback()
}
