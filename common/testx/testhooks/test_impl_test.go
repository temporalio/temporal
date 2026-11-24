//go:build test_dep

package testhooks

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type scopeID string

var (
	boolKey   = NewKey[bool, scopeID]()
	intKey    = NewKey[int, scopeID]()
	funcKey   = NewKey[func(), scopeID]()
	globalKey = NewGlobalKey[int]()
)

func TestGet(t *testing.T) {
	th := NewTestHooks()

	t.Run("returns false for unset key", func(t *testing.T) {
		_, ok := Get(th, boolKey, scopeID("scope1"))
		require.False(t, ok)
	})

	t.Run("returns set value", func(t *testing.T) {
		cleanup := Set(th, intKey, 42, scopeID("scope1"))
		defer cleanup()

		v, ok := Get(th, intKey, scopeID("scope1"))
		require.True(t, ok)
		require.Equal(t, 42, v)
	})

	t.Run("returns a set nil interface value", func(t *testing.T) {
		key := NewKey[error, scopeID]()
		scope := scopeID("scope1")
		t.Cleanup(Set(th, key, nil, scope))

		value, ok := Get(th, key, scope)
		require.True(t, ok)
		require.NoError(t, value)
	})

	t.Run("cleanup removes value", func(t *testing.T) {
		cleanup := Set(th, intKey, 42, scopeID("scope1"))
		cleanup()

		_, ok := Get(th, intKey, scopeID("scope1"))
		require.False(t, ok)
	})
}

func TestCall(t *testing.T) {
	th := NewTestHooks()

	t.Run("does nothing for unset key", func(t *testing.T) {
		Call(th, funcKey, scopeID("scope1")) // should not panic
	})

	t.Run("calls set function", func(t *testing.T) {
		called := false
		cleanup := Set(th, funcKey, func() { called = true }, scopeID("scope1"))
		defer cleanup()

		Call(th, funcKey, scopeID("scope1"))
		require.True(t, called)
	})
}

func TestSet(t *testing.T) {
	th := NewTestHooks()

	t.Run("values are isolated by scope", func(t *testing.T) {
		cleanup1 := Set(th, boolKey, true, scopeID("scope1"))
		defer cleanup1()

		cleanup2 := Set(th, boolKey, false, scopeID("scope2"))

		v, ok := Get(th, boolKey, scopeID("scope1"))
		require.True(t, ok)
		require.True(t, v)

		v, ok = Get(th, boolKey, scopeID("scope2"))
		require.True(t, ok)
		require.False(t, v)

		_, ok = Get(th, boolKey, scopeID("other"))
		require.False(t, ok)

		// Cleanup only affects the selected scope.
		cleanup2()
		_, ok = Get(th, boolKey, scopeID("scope2"))
		require.False(t, ok)

		v, ok = Get(th, boolKey, scopeID("scope1"))
		require.True(t, ok)
		require.True(t, v)
	})

	t.Run("global-scoped keys", func(t *testing.T) {
		cleanup := Set(th, globalKey, 42, GlobalScope)
		defer cleanup()

		v, ok := Get(th, globalKey, GlobalScope)
		require.True(t, ok)
		require.Equal(t, 42, v)
	})

	t.Run("overwrites previous value for same scope", func(t *testing.T) {
		cleanup1 := Set(th, intKey, 1, scopeID("scope1"))
		cleanup2 := Set(th, intKey, 2, scopeID("scope1"))

		v, ok := Get(th, intKey, scopeID("scope1"))
		require.True(t, ok)
		require.Equal(t, 2, v)

		cleanup2()
		_, ok = Get(th, intKey, scopeID("scope1"))
		require.False(t, ok)

		cleanup1() // should not panic
	})

	t.Run("old cleanup leaves the replacement registered", func(t *testing.T) {
		key := NewKey[struct{}, scopeID]()
		scope := scopeID("scope1")
		first := Set(th, key, struct{}{}, scope)
		second := Set(th, key, struct{}{}, scope)
		t.Cleanup(second)

		first()
		_, ok := Get(th, key, scope)
		require.True(t, ok)

		second()
		_, ok = Get(th, key, scope)
		require.False(t, ok)
	})
}

func TestHookScopes(t *testing.T) {
	t.Parallel()

	hooks := NewTestHooks()

	t.Run("value scope", func(t *testing.T) {
		key := NewKey[int, scopeID]()
		hook := NewHook(key, 42)
		scope := scopeID("scope")
		require.Equal(t, ScopeValue, hook.Scope())
		require.Panics(t, func() { hook.Apply(hooks, string(scope)) })

		cleanup := hook.Apply(hooks, scope)
		value, ok := Get(hooks, key, scope)
		require.True(t, ok)
		require.Equal(t, 42, value)

		cleanup()
		_, ok = Get(hooks, key, scope)
		require.False(t, ok)
	})

	t.Run("global scope", func(t *testing.T) {
		key := NewGlobalKey[int]()
		hook := NewHook(key, 42)
		require.Equal(t, ScopeGlobal, hook.Scope())
		t.Cleanup(hook.Apply(hooks, GlobalScope))

		value, ok := Get(hooks, key, GlobalScope)
		require.True(t, ok)
		require.Equal(t, 42, value)
	})
}
