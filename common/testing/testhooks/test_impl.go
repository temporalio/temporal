//go:build test_dep

package testhooks

import (
	"sync"

	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewTestHooks),
)

// testHooksImpl is an implementation of TestHooks.
type testHooksImpl struct {
	m sync.Map
}

func NewTestHooks() TestHooks {
	return &testHooksImpl{}
}

func (*testHooksImpl) testHooks() {}

// Get gets the value of a test hook from the registry.
//
// TestHooks should be used sparingly, see comment on TestHooks.
func Get[T any](th TestHooks, key Key) (T, bool) {
	var zero T
	if th == nil {
		return zero, false
	}
	impl, ok := th.(*testHooksImpl)
	if !ok {
		return zero, false
	}
	if val, ok := impl.m.Load(key); ok {
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
	impl := th.(*testHooksImpl)
	impl.m.Store(key, val)
	return func() { impl.m.Delete(key) }
}
