package await

import (
	"fmt"
	"testing"
	"time"

	"go.temporal.io/server/common/testing/testcontext"
)

const requireTrueMisuseHint = "do not use test assertions inside the predicate - return false to retry or use await.Require for assertions"

// RequireTrue runs `condition` repeatedly until it returns true, or until the
// timeout expires. The timeout is capped at the test's deadline, if one is set.
//
// Use [RequireTrue] for simple local predicates only. Do not use assertions or
// side effects in the predicate - use [Require] for these.
func RequireTrue(tb testing.TB, condition func() bool, timeout, pollInterval time.Duration) {
	tb.Helper()
	run(testcontext.New(tb), tb, func(t *T) {
		if !condition() {
			t.Fail()
		}
	}, timeout, pollInterval, "", "RequireTrue", requireTrueMisuseHint, false)
}

// RequireTruef is like [RequireTrue] but accepts a format string that is included
// in the failure message when the condition is not satisfied before the timeout.
func RequireTruef(tb testing.TB, condition func() bool, timeout, pollInterval time.Duration, msg string, args ...any) {
	tb.Helper()
	run(testcontext.New(tb), tb, func(t *T) {
		if !condition() {
			t.Fail()
		}
	}, timeout, pollInterval, fmt.Sprintf(msg, args...), "RequireTruef", requireTrueMisuseHint, false)
}
