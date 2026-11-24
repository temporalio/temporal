//go:build !test_dep

package testhooks

// TestHooks (in production mode) is an empty struct just so the build works.
// See TestHooks in test_impl.go.
//
// TestHooks are an inherently unclean way of writing tests. They require mixing test-only
// concerns into production code. In general you should prefer other ways of writing tests
// wherever possible, and only use TestHooks sparingly, as a last resort.
type TestHooks struct{}

// NewTestHooks returns an empty registry.
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

// Set is only to be used by test code together with the test_dep build tag.
func Set[T any, S any](_ TestHooks, _ Key[T, S], _ T, _ any) func() {
	//nolint:forbidigo // Setting a hook without test_dep is a test configuration error.
	panic("testhooks.Set called but TestHooks are not enabled: use -tags=test_dep when running `go test`")
}

// NewKey returns a key that is ignored in production mode.
func NewKey[T any, S any]() Key[T, S] {
	return Key[T, S]{}
}
