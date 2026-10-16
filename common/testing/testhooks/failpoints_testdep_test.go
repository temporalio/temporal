//go:build test_dep

package testhooks

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFailpoints(t *testing.T) {
	th1 := NewTestHooks()
	th2 := NewTestHooks()

	registry := th1.Failpoints()
	require.NotNil(t, registry)
	require.Same(t, registry, th1.Failpoints())
	require.NotSame(t, registry, th2.Failpoints())
}
