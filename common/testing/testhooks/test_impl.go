//go:build test_dep

package testhooks

import (
	"go.temporal.io/server/common/namespace"
	testxhooks "go.temporal.io/testx/testhooks"
)

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

func newKey[T any, S any]() Key[T, S] {
	var zero S
	var s ScopeType
	switch any(zero).(type) {
	case namespace.ID, namespace.Name:
		s = ScopeNamespace
	case global:
		s = ScopeGlobal
	default:
		panic("testhooks: unknown scope type")
	}
	return Key[T, S]{key: testxhooks.NewKey[T, S](), scopeType: s}
}
