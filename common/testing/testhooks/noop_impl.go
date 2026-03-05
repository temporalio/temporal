//go:build !test_dep

package testhooks

import (
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
type TestHooks struct{}

func NewTestHooks() TestHooks {
	return TestHooks{}
}

// Get gets the value of a test hook. In production mode it always returns the zero value and
// false, which hopefully the compiler will inline and remove the hook as dead code.
//
// TestHooks should be used very sparingly, see comment on TestHooks.
func Get[T any, S any](_ TestHooks, _ Key[T, S], _ S) (T, bool) {
	var zero T
	return zero, false
}

// Call calls a func() hook if present.
//
// TestHooks should be used very sparingly, see comment on TestHooks.
func Call[S any](_ TestHooks, _ Key[func(), S], _ S) {}

// Hook is an empty stub in production mode. NewHook and its methods are only available with -tags=test_dep.
type Hook struct{}

func (h Hook) Scope() ScopeType {
	panic("testhooks.Hook used but TestHooks are not enabled: use -tags=test_dep when running `go test`")
}

func (h Hook) Apply(_ TestHooks, _ any) func() {
	panic("testhooks.Hook used but TestHooks are not enabled: use -tags=test_dep when running `go test`")
}

// Set is only to be used by test code together with the test_dep build tag.
func Set[T any, S any](_ TestHooks, _ Key[T, S], _ T, _ any) func() {
	panic("testhooks.Set called but TestHooks are not enabled: use -tags=test_dep when running `go test`")
}

func newKey[T any, S any]() Key[T, S] {
	return Key[T, S]{}
}
