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
	}, legacyConfig(timeout, pollInterval, ""), "RequireTrue", requireTrueMisuseHint, false, true)
}

// RequireTruef is like [RequireTrue] but accepts a format string that is included
// in the failure message when the condition is not satisfied before the timeout.
func RequireTruef(tb testing.TB, condition func() bool, timeout, pollInterval time.Duration, msg string, args ...any) {
	tb.Helper()
	run(testcontext.New(tb), tb, func(t *T) {
		if !condition() {
			t.Fail()
		}
	}, legacyConfig(timeout, pollInterval, fmt.Sprintf(msg, args...)), "RequireTruef", requireTrueMisuseHint, false, true)
}

// RequireTrue2 polls condition until it returns true. See [Require2] for options.
// Use only for simple local predicates; for assertions use [Require2].
func RequireTrue2(tb testing.TB, condition func() bool, opts ...Option) {
	tb.Helper()
	cfg := newConfig(opts)
	run(testcontext.New(tb), tb, func(t *T) {
		if !condition() {
			t.Fail()
		}
	}, cfg, "RequireTrue2", requireTrueMisuseHint, false, true)
}
