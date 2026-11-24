package testhooks

import testxhooks "go.temporal.io/testx/testhooks"

// TestHooks is the registry of active test hooks used by the server.
// It should be obtained through Fx and used with [Get] and [Set].
type TestHooks = testxhooks.TestHooks

func NewTestHooks() TestHooks {
	return testxhooks.NewTestHooks()
}

// Get gets the value of a test hook from the registry.
func Get[T any, S any](th TestHooks, key Key[T, S], scope S) (T, bool) {
	return testxhooks.Get(th, key.key, scope)
}

// Call calls a func() hook if present.
func Call[S any](th TestHooks, key Key[func(), S], scope S) {
	testxhooks.Call(th, key.key, scope)
}

// Set sets a test hook to a value with the given scope and returns a cleanup function to unset it.
func Set[T any, S any](th TestHooks, key Key[T, S], val T, scope any) func() {
	return testxhooks.Set(th, key.key, val, scope)
}
