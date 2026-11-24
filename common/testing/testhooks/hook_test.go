//go:build test_dep

package testhooks

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/namespace"
)

func TestSetRejectsMismatchedScope(t *testing.T) {
	t.Parallel()

	hooks := NewTestHooks()
	require.Panics(t, func() {
		Set(hooks, MatchingLBForceReadPartition, 42, namespace.Name("ns"))
	})
}
