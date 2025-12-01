//go:build !test_dep

package testhooks

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/softassert"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewTestHooks),
)

// TestHooks (in production mode) is an empty struct just so the build works.
// See TestHooks in test_impl.go.
//
// TestHooks are an inherently unclean way of writing tests. They require mixing test-only
// concerns into production code. In general you should prefer other ways of writing tests
// wherever possible, and only use TestHooks sparingly, as a last resort.
type TestHooks struct {
	logger log.Logger
}

func NewTestHooks(logger log.Logger) TestHooks {
	return TestHooks{logger: logger}
}

// Get gets the value of a test hook. In production mode it always returns the zero value and
// false, which hopefully the compiler will inline and remove the hook as dead code.
//
// TestHooks should be used very sparingly, see comment on TestHooks.
func Get[T any](_ TestHooks, key Key) (T, bool) {
	var zero T
	return zero, false
}

// Call calls a func() hook if present.
//
// TestHooks should be used very sparingly, see comment on TestHooks.
func Call(_ TestHooks, key Key) {
}

// Set is only to be used by test code together with the test_dep build tag.
func Set[T any](th TestHooks, key Key, val T) func() {
	softassert.Fail(th.logger, "testhooks.Set called without test_dep build tag")
	return func() {}
}
