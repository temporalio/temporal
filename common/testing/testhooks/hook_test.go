//go:build test_dep

package testhooks

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/namespace"
)

func TestHookNamespaceScope(t *testing.T) {
	t.Parallel()

	hooks := NewTestHooks()
	scope := namespace.ID("ns")
	hook := NewHook(MatchingLBForceReadPartition, 42)
	require.Equal(t, ScopeNamespace, hook.Scope())
	require.Panics(t, func() { hook.Apply(hooks, namespace.Name(scope)) })

	cleanup := hook.Apply(hooks, scope)
	value, ok := Get(hooks, MatchingLBForceReadPartition, scope)
	require.True(t, ok)
	require.Equal(t, 42, value)

	cleanup()
	_, ok = Get(hooks, MatchingLBForceReadPartition, scope)
	require.False(t, ok)
}
