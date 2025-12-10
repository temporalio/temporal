//go:build test_dep

package testhooks

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) {
	th := NewTestHooks()

	t.Run("returns false for unset key", func(t *testing.T) {
		_, ok := Get[int](th, MatchingDisableSyncMatch)
		require.False(t, ok)
	})

	t.Run("returns set value", func(t *testing.T) {
		cleanup := Set(th, MatchingDisableSyncMatch, 42)
		defer cleanup()

		v, ok := Get[int](th, MatchingDisableSyncMatch)
		require.True(t, ok)
		require.Equal(t, 42, v)
	})

	t.Run("cleanup removes value", func(t *testing.T) {
		cleanup := Set(th, MatchingDisableSyncMatch, 42)
		cleanup()

		_, ok := Get[int](th, MatchingDisableSyncMatch)
		require.False(t, ok)
	})
}

func TestCall(t *testing.T) {
	th := NewTestHooks()

	t.Run("does nothing for unset key", func(t *testing.T) {
		Call(th, MatchingDisableSyncMatch) // should not panic
	})

	t.Run("calls set function", func(t *testing.T) {
		called := false
		cleanup := Set(th, MatchingDisableSyncMatch, func() { called = true })
		defer cleanup()

		Call(th, MatchingDisableSyncMatch)
		require.True(t, called)
	})
}
