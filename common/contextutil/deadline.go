package contextutil

import (
	"context"
	"time"
)

var noop = func() {}

// WithDeadlineBuffer creates a child context with desired timeout.
// If buffer is non-zero, then child context timeout will be
// the minOf(parentCtx.Deadline()-buffer, maxTimeout). Use this
// method to create child context when childContext cannot use
// all of parent's deadline but instead there is a need to leave
// some time for parent to do some post-work
func WithDeadlineBuffer(
	parent context.Context,
	timeout time.Duration,
	buffer time.Duration,
) (context.Context, context.CancelFunc) {
	if parent.Err() != nil {
		return parent, noop
	}

	deadline, hasDeadline := parent.Deadline()

	if !hasDeadline {
		return context.WithTimeout(parent, timeout)
	}

	remaining := time.Until(deadline) - buffer
	if remaining < timeout {
		// Cap the timeout to the remaining time minus buffer.
		timeout = max(0, remaining)
	}
	return context.WithTimeout(parent, timeout)
}
