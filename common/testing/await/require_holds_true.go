package await

import (
	"fmt"
	"testing"
	"time"

	"go.temporal.io/server/common/testing/testcontext"
)

const requireHoldsTrueMisuseHint = "do not use test assertions inside the predicate - return false to fail or use await.RequireHolds for assertions"

// RequireHoldsTrue requires condition to return true on every poll for the
// full duration, with at least one completed, passing poll. It follows
// [RequireHolds] when an attempt is still running at the end of the duration.
// Use it for simple local predicates only. Do not use assertions or blocking
// operations in the predicate; use [RequireHolds] for these.
func RequireHoldsTrue(tb testing.TB, condition func() bool, duration, pollInterval time.Duration) {
	tb.Helper()
	runHolds(testcontext.For(tb), tb, func(t *T) {
		if !condition() {
			t.Fail()
		}
	}, legacyConfig(duration, pollInterval, ""), "RequireHoldsTrue", requireHoldsTrueMisuseHint, false)
}

// RequireHoldsTruef is like [RequireHoldsTrue] but adds a formatted failure message.
func RequireHoldsTruef(tb testing.TB, condition func() bool, duration, pollInterval time.Duration, msg string, args ...any) {
	tb.Helper()
	runHolds(testcontext.For(tb), tb, func(t *T) {
		if !condition() {
			t.Fail()
		}
	}, legacyConfig(duration, pollInterval, fmt.Sprintf(msg, args...)), "RequireHoldsTruef", requireHoldsTrueMisuseHint, false)
}
