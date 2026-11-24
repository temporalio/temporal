//go:build test_dep

package testhooks

import (
	"sync"
	"sync/atomic"
)

type hookKey struct {
	id    keyID
	scope any
}

// TestHooks holds a registry of active test hooks. Create it with [NewTestHooks]
// and use it with [Get] and [Set].
//
// TestHooks are an inherently unclean way of writing tests. They require mixing test-only
// concerns into production code. In general you should prefer other ways of writing tests
// wherever possible, and only use TestHooks sparingly, as a last resort.
type TestHooks struct {
	data *sync.Map
}

// NewTestHooks returns an empty registry.
func NewTestHooks() TestHooks {
	return TestHooks{data: &sync.Map{}}
}

// Get gets the value of a test hook from the registry.
//
// TestHooks should be used sparingly, see comment on TestHooks.
func Get[T any, S any](th TestHooks, key Key[T, S], scope S) (T, bool) {
	var zero T
	if th.data == nil {
		// A zero-value TestHooks has no registered hooks.
		return zero, false
	}
	if val, ok := th.data.Load(hookKey{key.id, scope}); ok {
		return val.(T), true //nolint:revive
	}
	return zero, false
}

// Call calls a func() hook if present.
//
// TestHooks should be used sparingly, see comment on TestHooks.
func Call[S any](th TestHooks, key Key[func(), S], scope S) {
	if hook, ok := Get(th, key, scope); ok {
		hook()
	}
}

// Set sets a test hook to a value with the given scope and returns a cleanup function to unset it.
// The scope must be comparable.
func Set[T any, S any](th TestHooks, key Key[T, S], val T, scope any) func() {
	mk := hookKey{key.id, scope}
	th.data.Store(mk, val)
	return func() { th.data.Delete(mk) }
}

// keyCounter provides unique IDs for keys.
var keyCounter atomic.Int64

// NewKey returns a new key for a hook.
func NewKey[T any, S any]() Key[T, S] {
	return Key[T, S]{id: keyCounter.Add(1)}
}
