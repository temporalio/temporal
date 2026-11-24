//go:build !test_dep

package testhooks

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHooksDisabled(t *testing.T) {
	t.Parallel()

	hooks := NewTestHooks()
	key := NewKey[int, string]()
	value, ok := Get(hooks, key, "scope")
	require.Zero(t, value)
	require.False(t, ok)
	require.Panics(t, func() { Set(hooks, key, 42, "scope") })
}

func TestHookDisabled(t *testing.T) {
	t.Parallel()

	var hook Hook
	require.Panics(t, func() { NewHook(NewGlobalKey[int](), 42) })
	require.Panics(t, func() { hook.Scope() })
	require.Panics(t, func() { hook.Apply(NewTestHooks(), GlobalScope) })
}
