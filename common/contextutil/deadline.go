package contextutil

import (
	"context"
	"time"
)

var noop = func() {}

// WithDeadlineBuffer returns a child context with a deadline that ensures that at least buffer
// amount of time remains after the child deadline expires and before the parent deadline expires.
// The returned context timeout is therefore <= the requested timeout. If the parent deadline itself
// does not allow buffer amount of time, then the returned context deadline expires immediately. Use
// this method to create child context when the child cannot use all of parent's deadline but
// instead there is a need to leave some time for parent to do some post-work.
func WithDeadlineBuffer(
	parent context.Context,
	timeout time.Duration,
	buffer time.Duration,
) (context.Context, context.CancelFunc) {
	if parent.Err() != nil {
		return parent, noop
	}

	parentDeadline, parentHasDeadline := parent.Deadline()

	if !parentHasDeadline {
		// No parent deadline, so buffer is available to parent after child deadline expiry.
		return context.WithTimeout(parent, timeout)
	}

	// If parent deadline itself does not allow buffer then set child timeout to zero. Otherwise
	// compute child deadline such that at least buffer remains after it and before parent deadline.
	remaining := time.Until(parentDeadline) - buffer
	if remaining < timeout {
		timeout = max(0, remaining)
	}
	return context.WithTimeout(parent, timeout)
}
