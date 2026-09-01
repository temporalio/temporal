package testcontext

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// DeadlineExceeded identifies a deadline owned by testcontext. It wraps
// context.DeadlineExceeded, so either error matches through errors.Is.
var DeadlineExceeded = fmt.Errorf("testcontext deadline exceeded: %w", context.DeadlineExceeded)

type deadlineExceededCause struct {
	deadline time.Time
	owner    *contextState
	reported *atomic.Bool
}

// WithDeadline derives a deadline from parent. When parent belongs to a test
// context, expiry carries DeadlineExceeded as its cause. Foreign contexts keep
// the standard context deadline cause.
func WithDeadline(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	if parent == nil {
		// Preserve context.WithDeadline's nil-parent panic and message.
		return context.WithDeadline(parent, deadline)
	}
	owner, _ := parent.Value(ownerKey{}).(*contextState)
	if owner == nil {
		return context.WithDeadline(parent, deadline)
	}
	if parentDeadline, ok := parent.Deadline(); ok &&
		!deadline.Before(parentDeadline) &&
		parentDeadline.Before(owner.timeoutContext.effectiveExpiration()) {
		// A tighter deadline added by the caller remains caller-owned even
		// when this derived context requests that exact deadline.
		return context.WithDeadline(parent, deadline)
	}
	return context.WithDeadlineCause(parent, deadline, newDeadlineExceededCause(deadline, owner))
}

// FailIfDeadlineExceeded reports and stops the test when ctx ended at a
// testcontext-owned deadline. It returns false without touching tb for every
// other termination. supplementalDetails is optional caller-formatted text
// appended to the canonical testcontext report.
func FailIfDeadlineExceeded(ctx context.Context, tb testing.TB, supplementalDetails string) bool {
	if ctx == nil {
		return false
	}
	cause := internalDeadlineExceededCause(ctx)
	if cause == nil || cause.owner == nil {
		return false
	}

	tb.Helper()
	if !cause.reported.CompareAndSwap(false, true) {
		tb.FailNow()
		return true
	}

	tb.Fatalf("%s", cause.owner.deadlineExceededMessage(cause.deadline, supplementalDetails))
	return true
}

func internalDeadlineExceededCause(ctx context.Context) *deadlineExceededCause {
	var cause *deadlineExceededCause
	if !errors.As(context.Cause(ctx), &cause) {
		return nil
	}
	return cause
}

func appendDeadlineDetails(message string, details ...string) string {
	var nonEmpty []string
	for _, detail := range details {
		if detail = strings.TrimSpace(detail); detail != "" {
			nonEmpty = append(nonEmpty, detail)
		}
	}
	if len(nonEmpty) == 0 {
		return message
	}
	return message + "\ndetails:\n  " + strings.ReplaceAll(strings.Join(nonEmpty, "\n"), "\n", "\n  ")
}

func (c *deadlineExceededCause) Error() string {
	return DeadlineExceeded.Error()
}

func (c *deadlineExceededCause) Unwrap() error {
	return DeadlineExceeded
}

func newDeadlineExceededCause(deadline time.Time, owner *contextState) *deadlineExceededCause {
	reported := &atomic.Bool{}
	if owner != nil {
		actual, _ := owner.deadlineClaims.LoadOrStore(deadline, reported)
		reported = actual.(*atomic.Bool)
	}
	return &deadlineExceededCause{
		deadline: deadline,
		owner:    owner,
		reported: reported,
	}
}
