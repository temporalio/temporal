//go:build test_dep

package testhooks

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type namespaceScope string

var (
	boolKey   = NewKey[bool, namespaceScope]()
	intKey    = NewKey[int, namespaceScope]()
	funcKey   = NewKey[func(), namespaceScope]()
	globalKey = NewKey[int, struct{}]()
)

func TestGet(t *testing.T) {
	th := NewTestHooks()

	t.Run("returns false for unset key", func(t *testing.T) {
		_, ok := Get(th, boolKey, namespaceScope("ns1"))
		require.False(t, ok)
	})

	t.Run("returns set value", func(t *testing.T) {
		cleanup := Set(th, intKey, 42, namespaceScope("ns1"))
		defer cleanup()

		v, ok := Get(th, intKey, namespaceScope("ns1"))
		require.True(t, ok)
		require.Equal(t, 42, v)
	})

	t.Run("cleanup removes value", func(t *testing.T) {
		cleanup := Set(th, intKey, 42, namespaceScope("ns1"))
		cleanup()

		_, ok := Get(th, intKey, namespaceScope("ns1"))
		require.False(t, ok)
	})
}

func TestCall(t *testing.T) {
	th := NewTestHooks()

	t.Run("does nothing for unset key", func(t *testing.T) {
		Call(th, funcKey, namespaceScope("ns1")) // should not panic
	})

	t.Run("calls set function", func(t *testing.T) {
		called := false
		cleanup := Set(th, funcKey, func() { called = true }, namespaceScope("ns1"))
		defer cleanup()

		Call(th, funcKey, namespaceScope("ns1"))
		require.True(t, called)
	})
}

func TestSet(t *testing.T) {
	th := NewTestHooks()

	t.Run("namespace-scoped keys are isolated", func(t *testing.T) {
		cleanup1 := Set(th, boolKey, true, namespaceScope("ns1"))
		defer cleanup1()

		cleanup2 := Set(th, boolKey, false, namespaceScope("ns2"))

		v, ok := Get(th, boolKey, namespaceScope("ns1"))
		require.True(t, ok)
		require.True(t, v)

		v, ok = Get(th, boolKey, namespaceScope("ns2"))
		require.True(t, ok)
		require.False(t, v)

		_, ok = Get(th, boolKey, namespaceScope("other"))
		require.False(t, ok)

		// Cleanup only affects specific namespace
		cleanup2()
		_, ok = Get(th, boolKey, namespaceScope("ns2"))
		require.False(t, ok)

		v, ok = Get(th, boolKey, namespaceScope("ns1"))
		require.True(t, ok)
		require.True(t, v)
	})

	t.Run("global-scoped keys", func(t *testing.T) {
		cleanup := Set(th, globalKey, 42, struct{}{})
		defer cleanup()

		v, ok := Get(th, globalKey, struct{}{})
		require.True(t, ok)
		require.Equal(t, 42, v)
	})

	t.Run("overwrites previous value for same scope", func(t *testing.T) {
		cleanup1 := Set(th, intKey, 1, namespaceScope("ns1"))
		cleanup2 := Set(th, intKey, 2, namespaceScope("ns1"))

		v, ok := Get(th, intKey, namespaceScope("ns1"))
		require.True(t, ok)
		require.Equal(t, 2, v)

		cleanup2()
		_, ok = Get(th, intKey, namespaceScope("ns1"))
		require.False(t, ok)

		cleanup1() // should not panic
	})
}
