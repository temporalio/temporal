//go:build test_dep

package testhooks

import (
	"sync"

	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewTestHooks),
)

// TestHooks holds a registry of active test hooks. It should be obtained through fx and
// used with Get and Set.
//
// TestHooks are an inherently unclean way of writing tests. They require mixing test-only
// concerns into production code. In general you should prefer other ways of writing tests
// wherever possible, and only use TestHooks sparingly, as a last resort.
type TestHooks struct {
	data *sync.Map
}

func NewTestHooks() TestHooks {
	return TestHooks{data: &sync.Map{}}
}

// Get gets the value of a test hook from the registry.
//
// TestHooks should be used sparingly, see comment on TestHooks.
func Get[T any](th TestHooks, key Key) (T, bool) {
	var zero T
	if th.data == nil {
		return zero, false
	}
	if val, ok := th.data.Load(key); ok {
		// this is only used in test so we want to panic on type mismatch:
		return val.(T), ok //nolint:revive
	}
	return zero, false
}

// Call calls a func() hook if present.
//
// TestHooks should be used sparingly, see comment on TestHooks.
func Call(th TestHooks, key Key) {
	if hook, ok := Get[func()](th, key); ok {
		hook()
	}
}

// Set sets a test hook to a value and returns a cleanup function to unset it.
// Calls to Set and the cleanup function should form a stack.
func Set[T any](th TestHooks, key Key, val T) func() {
	th.data.Store(key, val)
	return func() { th.data.Delete(key) }
}
