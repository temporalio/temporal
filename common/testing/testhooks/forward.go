// TODO: Remove this forwarding file once all callers use go.temporal.io/testx/testhooks.
package testhooks

import testxhooks "go.temporal.io/testx/testhooks"

// TestHooks is the registry of active test hooks used by the server.
// It should be obtained through Fx and used with [Get] and [Set].
type TestHooks = testxhooks.TestHooks

type Key[T any, S any] = testxhooks.Key[T, S]
type Hook = testxhooks.Hook
type ScopeType = testxhooks.ScopeType

var GlobalScope = testxhooks.GlobalScope

func NewHook[T any, S any](key Key[T, S], value T) Hook {
	return testxhooks.NewHook(key, value)
}

func NewTestHooks() TestHooks {
	return testxhooks.NewTestHooks()
}

// Get gets the value of a test hook from the registry.
func Get[T any, S any](th TestHooks, key Key[T, S], scope S) (T, bool) {
	return testxhooks.Get(th, key, scope)
}

// Call calls a func() hook if present.
func Call[S any](th TestHooks, key Key[func(), S], scope S) {
	testxhooks.Call(th, key, scope)
}

// Set sets a test hook to a value with the given scope and returns a cleanup function to unset it.
func Set[T any, S any](th TestHooks, key Key[T, S], val T, scope any) func() {
	typedScope, ok := scope.(S)
	if !ok {
		panic("testhooks: scope type mismatch")
	}
	return testxhooks.Set(th, key, val, typedScope)
}
