package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/protoassert"
)

// This file intentionally contains lint violations to verify that the
// forbidigo rule in .golangci.yml correctly flags assert.X and protoassert.X
// calls while allowing assert.CollectT for EventuallyWithT callbacks.
//
// Expected violations: assert.NoError, assert.Equal, assert.True, assert.Len, protoassert.ProtoEqual
// Expected allowed:   assert.CollectT, require.NoError, require.Equal

func TestAssertBanLintVerification(t *testing.T) {
	t.Skip("lint-only: this test exists to verify the assert ban lint rule")

	// These should be caught by the linter:
	assert.NoError(t, nil)
	assert.Equal(t, 1, 1)
	assert.True(t, true)
	assert.Len(t, []int{1}, 1)
	protoassert.ProtoEqual(t, nil, nil)

	// These should NOT be caught:
	require.NoError(t, nil)
	require.Equal(t, 1, 1)

	// assert.CollectT should NOT be caught — needed for EventuallyWithT.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		assert.NoError(collect, nil)
	}, 1*time.Second, 10*time.Millisecond)
}
