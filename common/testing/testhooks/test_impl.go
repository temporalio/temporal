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
	lock  *sync.Mutex
	hooks map[keyID]map[any]any // id -> scope -> value
}

func NewTestHooks() TestHooks {
	return TestHooks{
		lock:  &sync.Mutex{},
		hooks: make(map[keyID]map[any]any),
	}
}

// Get gets the value of a test hook from the registry.
//
// TestHooks should be used sparingly, see comment on TestHooks.
func Get[T any, S any](th TestHooks, key Key[T, S], scope S) (T, bool) {
	var zero T
	if th.lock == nil {
		// This means TestHooks wasn't created via NewTestHooks. Ignore.
		return zero, false
	}
	th.lock.Lock()
	defer th.lock.Unlock()

	if th.hooks == nil {
		return zero, false
	}
	scopes, ok := th.hooks[key.id]
	if !ok {
		return zero, false
	}
	val, ok := scopes[scope]
	if !ok {
		return zero, false
	}
	return val.(T), true //nolint:revive
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
// Calls to Set and the cleanup function should form a stack.
func Set[T any, S any](th TestHooks, key Key[T, S], val T, scope S) func() {
	th.lock.Lock()
	defer th.lock.Unlock()

	scopes := th.hooks[key.id]
	if scopes == nil {
		scopes = make(map[any]any)
		th.hooks[key.id] = scopes
	}
	scopes[scope] = val

	return func() {
		th.lock.Lock()
		defer th.lock.Unlock()
		delete(scopes, scope)
	}
}
