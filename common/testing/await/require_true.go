package await

import (
	"testing"

	"go.temporal.io/server/common/testing/testcontext"
)

const requireTrueMisuseHint = "do not use test assertions inside the predicate - return false to retry or use await.Require for assertions"

// RequireTrue runs condition repeatedly until it returns true, or until the
// configured timeout expires.
//
// Use [RequireTrue] for simple local predicates only. Do not use assertions or
// side effects in the predicate - use [Require] for these.
func RequireTrue(tb testing.TB, condition func() bool, opts ...Option) {
	tb.Helper()
	cfg := newConfig(opts)
	run(testcontext.New(tb), tb, func(t *T) {
		if !condition() {
			t.Fail()
		}
	}, cfg, "RequireTrue", requireTrueMisuseHint, false, true)
}
