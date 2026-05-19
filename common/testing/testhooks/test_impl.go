//go:build test_dep

package testhooks

import (
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common/namespace"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewTestHooks),
)

type hookKey struct {
	id    keyID
	scope any
}

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
func Get[T any, S any](th TestHooks, key Key[T, S], scope S) (T, bool) {
	var zero T
	if th.data == nil {
		// This means TestHooks wasn't created via NewTestHooks. Ignore.
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

// Hook bundles a key and value for type-erased use in InjectHook.
type Hook struct {
	scopeType ScopeType
	apply     func(TestHooks, any) func()
}

func (h Hook) Scope() ScopeType                     { return h.scopeType }
func (h Hook) Apply(th TestHooks, scope any) func() { return h.apply(th, scope) }

func NewHook[T any, S any](key Key[T, S], value T) Hook {
	return Hook{
		scopeType: key.scopeType,
		apply: func(th TestHooks, scope any) func() {
			if _, ok := scope.(S); !ok {
				panic("testhooks: scope type mismatch")
			}
			return Set(th, key, value, scope)
		},
	}
}

// Set sets a test hook to a value with the given scope and returns a cleanup function to unset it.
func Set[T any, S any](th TestHooks, key Key[T, S], val T, scope any) func() {
	mk := hookKey{key.id, scope}
	th.data.Store(mk, val)
	return func() { th.data.Delete(mk) }
}

// keyCounter provides unique IDs for keys.
var keyCounter atomic.Int64

func newKey[T any, S any]() Key[T, S] {
	var zero S
	var s ScopeType
	switch any(zero).(type) {
	case namespace.ID:
		s = ScopeNamespace
	case global:
		s = ScopeGlobal
	default:
		panic("testhooks: unknown scope type")
	}
	return Key[T, S]{id: keyCounter.Add(1), scopeType: s}
}
