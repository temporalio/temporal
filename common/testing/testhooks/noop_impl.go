//go:build !test_dep

package testhooks

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewTestHooks),
)

// noopTestHooks is a noop implementation of TestHooks for production builds.
type noopTestHooks struct{}

func NewTestHooks() TestHooks {
	return noopTestHooks{}
}

func (noopTestHooks) testHooks() {}

// Get gets the value of a test hook. In production mode it always returns the zero value and
// false, which hopefully the compiler will inline and remove the hook as dead code.
//
// TestHooks should be used very sparingly, see comment on TestHooks.
func Get[T any](_ TestHooks, _ Key) (T, bool) {
	var zero T
	return zero, false
}

// Call calls a func() hook if present.
//
// TestHooks should be used very sparingly, see comment on TestHooks.
func Call(_ TestHooks, _ Key) {
}

// Set is only to be used by test code together with the test_dep build tag.
func Set[T any](_ TestHooks, _ Key, _ T) func() {
	panic("testhooks.Set called but TestHooks are not enabled: use -tags=test_dep when running `go test`")
}
