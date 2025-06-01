//go:build test_dep

package testhooks

import (
	"sync"

	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewTestHooksImpl),
)

type (
	// TestHooks holds a registry of active test hooks. It should be obtained through fx and
	// used with Get and Set.
	//
	// TestHooks are an inherently unclean way of writing tests. They require mixing test-only
	// concerns into production code. In general you should prefer other ways of writing tests
	// wherever possible, and only use TestHooks sparingly, as a last resort.
	TestHooks interface {
		// private accessors; access must go through package-level Get/Set
		get(Key) (any, bool)
		set(Key, any)
		del(Key)
	}

	// testHooksImpl is an implementation of TestHooks.
	testHooksImpl struct {
		m sync.Map
	}
)

// Get gets the value of a test hook from the registry.
//
// TestHooks should be used very sparingly, see comment on TestHooks.
func Get[T any](th TestHooks, key Key) (T, bool) {
	var zero T
	if th == nil {
		return zero, false
	}
	if val, ok := th.get(key); ok {
		// this is only used in test so we want to panic on type mismatch:
		return val.(T), ok // nolint:revive
	}
	return zero, false
}

// Call calls a func() hook if present.
//
// TestHooks should be used very sparingly, see comment on TestHooks.
func Call(th TestHooks, key Key) {
	if hook, ok := Get[func()](th, key); ok {
		hook()
	}
}

// Set sets a test hook to a value and returns a cleanup function to unset it.
// Calls to Set and the cleanup functions should form a stack.
func Set[T any](th TestHooks, key Key, val T) func() {
	th.set(key, val)
	return func() { th.del(key) }
}

// NewTestHooksImpl returns a new instance of a test hook registry. This is provided and used
// in the main "resource" module as a default, but in functional tests, it's overridden by an
// explicitly constructed instance.
func NewTestHooksImpl() TestHooks {
	return &testHooksImpl{}
}

func (th *testHooksImpl) get(key Key) (any, bool) {
	return th.m.Load(key)
}

func (th *testHooksImpl) set(key Key, val any) {
	th.m.Store(key, val)
}

func (th *testHooksImpl) del(key Key) {
	th.m.Delete(key)
}
