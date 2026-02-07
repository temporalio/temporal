//go:build test_dep

package testhooks

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/namespace"
)

func TestGet(t *testing.T) {
	th := NewTestHooks()

	t.Run("returns false for unset key", func(t *testing.T) {
		_, ok := Get(th, MatchingDisableSyncMatch, namespace.ID("ns1"))
		require.False(t, ok)
	})

	t.Run("returns set value", func(t *testing.T) {
		cleanup := Set(th, MatchingLBForceReadPartition, 42, namespace.ID("ns1"))
		defer cleanup()

		v, ok := Get(th, MatchingLBForceReadPartition, namespace.ID("ns1"))
		require.True(t, ok)
		require.Equal(t, 42, v)
	})

	t.Run("cleanup removes value", func(t *testing.T) {
		cleanup := Set(th, MatchingLBForceReadPartition, 42, namespace.ID("ns1"))
		cleanup()

		_, ok := Get(th, MatchingLBForceReadPartition, namespace.ID("ns1"))
		require.False(t, ok)
	})
}

func TestCall(t *testing.T) {
	th := NewTestHooks()

	t.Run("does nothing for unset key", func(t *testing.T) {
		Call(th, UpdateWithStartInBetweenLockAndStart, namespace.ID("ns1")) // should not panic
	})

	t.Run("calls set function", func(t *testing.T) {
		called := false
		cleanup := Set(th, UpdateWithStartInBetweenLockAndStart, func() { called = true }, namespace.ID("ns1"))
		defer cleanup()

		Call(th, UpdateWithStartInBetweenLockAndStart, namespace.ID("ns1"))
		require.True(t, called)
	})
}

func TestSet(t *testing.T) {
	th := NewTestHooks()

	t.Run("namespace-scoped keys are isolated", func(t *testing.T) {
		cleanup1 := Set(th, MatchingDisableSyncMatch, true, namespace.ID("ns1"))
		defer cleanup1()

		cleanup2 := Set(th, MatchingDisableSyncMatch, false, namespace.ID("ns2"))

		v, ok := Get(th, MatchingDisableSyncMatch, namespace.ID("ns1"))
		require.True(t, ok)
		require.True(t, v)

		v, ok = Get(th, MatchingDisableSyncMatch, namespace.ID("ns2"))
		require.True(t, ok)
		require.False(t, v)

		_, ok = Get(th, MatchingDisableSyncMatch, namespace.ID("other"))
		require.False(t, ok)

		// Cleanup only affects specific namespace
		cleanup2()
		_, ok = Get(th, MatchingDisableSyncMatch, namespace.ID("ns2"))
		require.False(t, ok)

		v, ok = Get(th, MatchingDisableSyncMatch, namespace.ID("ns1"))
		require.True(t, ok)
		require.True(t, v)
	})

	t.Run("global-scoped keys", func(t *testing.T) {
		cleanup := Set(th, TaskQueuesInDeploymentSyncBatchSize, 42, GlobalScope)
		defer cleanup()

		v, ok := Get(th, TaskQueuesInDeploymentSyncBatchSize, GlobalScope)
		require.True(t, ok)
		require.Equal(t, 42, v)
	})

	t.Run("overwrites previous value for same scope", func(t *testing.T) {
		cleanup1 := Set(th, MatchingLBForceReadPartition, 1, namespace.ID("ns1"))
		cleanup2 := Set(th, MatchingLBForceReadPartition, 2, namespace.ID("ns1"))

		v, ok := Get(th, MatchingLBForceReadPartition, namespace.ID("ns1"))
		require.True(t, ok)
		require.Equal(t, 2, v)

		cleanup2()
		_, ok = Get(th, MatchingLBForceReadPartition, namespace.ID("ns1"))
		require.False(t, ok)

		cleanup1() // should not panic
	})
}
