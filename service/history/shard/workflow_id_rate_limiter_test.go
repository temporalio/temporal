package shard

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/configs"
	"go.uber.org/mock/gomock"
)

func newTestShardForRateLimiter(t *testing.T) *ContextTest {
	controller := gomock.NewController(t)
	t.Cleanup(controller.Finish)
	return NewTestContext(
		controller,
		&persistencespb.ShardInfo{ShardId: 1},
		configs.NewConfig(dynamicconfig.NewNoopCollection(), 1),
	)
}

func TestWorkflowIDReuseRateLimiter_Disabled(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.WorkflowIDReuseRate = func(_ string) int { return 0 }
	nsID := namespace.ID("test-ns-id")
	require.Nil(t, shard.WorkflowIDReuseRateLimiter(nsID, "wf-id", chasm.WorkflowArchetypeID))
}

func TestWorkflowIDReuseRateLimiter_AllowsUnderLimit(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.WorkflowIDReuseRate = func(_ string) int { return 10 }
	nsID := namespace.ID("test-ns-id")
	rl := shard.WorkflowIDReuseRateLimiter(nsID, "wf-id", chasm.WorkflowArchetypeID)
	require.NotNil(t, rl)
	for range 10 {
		require.True(t, rl.Allow())
	}
}

func TestWorkflowIDReuseRateLimiter_BlocksOverLimit(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.WorkflowIDReuseRate = func(_ string) int { return 1 }
	nsID := namespace.ID("test-ns-id")
	rl := shard.WorkflowIDReuseRateLimiter(nsID, "wf-id", chasm.WorkflowArchetypeID)
	require.NotNil(t, rl)
	require.True(t, rl.Allow())
	require.False(t, rl.Allow())
}

func TestWorkflowIDReuseRateLimiter_IndependentWorkflowIDs(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.WorkflowIDReuseRate = func(_ string) int { return 1 }
	nsID := namespace.ID("test-ns-id")
	rl1 := shard.WorkflowIDReuseRateLimiter(nsID, "wf-id-1", chasm.WorkflowArchetypeID)
	rl2 := shard.WorkflowIDReuseRateLimiter(nsID, "wf-id-2", chasm.WorkflowArchetypeID)
	require.True(t, rl1.Allow())
	require.True(t, rl2.Allow())
}

func TestWorkflowIDReuseRateLimiter_IndependentNamespaces(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.WorkflowIDReuseRate = func(_ string) int { return 1 }
	ns1 := namespace.ID("ns-1")
	ns2 := namespace.ID("ns-2")
	rl1 := shard.WorkflowIDReuseRateLimiter(ns1, "wf-id", chasm.WorkflowArchetypeID)
	rl2 := shard.WorkflowIDReuseRateLimiter(ns2, "wf-id", chasm.WorkflowArchetypeID)
	require.True(t, rl1.Allow())
	require.True(t, rl2.Allow())
}

func TestWorkflowIDReuseRateLimiter_IndependentArchetypes(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.WorkflowIDReuseRate = func(_ string) int { return 1 }
	nsID := namespace.ID("test-ns-id")
	rl1 := shard.WorkflowIDReuseRateLimiter(nsID, "wf-id", chasm.WorkflowArchetypeID)
	rl2 := shard.WorkflowIDReuseRateLimiter(nsID, "wf-id", chasm.ArchetypeID(99999))
	require.True(t, rl1.Allow())
	require.True(t, rl2.Allow())
}

func TestWorkflowIDReuseRateLimiter_BurstRatio(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.WorkflowIDReuseRate = func(_ string) int { return 2 }
	shard.config.WorkflowIDReuseBurstRatio = func(_ string) float64 { return 3.0 }
	nsID := namespace.ID("test-ns-id")
	rl := shard.WorkflowIDReuseRateLimiter(nsID, "wf-id", chasm.WorkflowArchetypeID)
	require.NotNil(t, rl)
	for i := range 6 {
		require.True(t, rl.Allow(), "expected Allow() on call %d", i+1)
	}
	require.False(t, rl.Allow(), "expected Allow() to be false after burst exhausted")
}

func TestWorkflowIDReuseRateLimiter_BurstUpdatesOnConfigChange(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.WorkflowIDReuseRate = func(_ string) int { return 2 }
	shard.config.WorkflowIDReuseBurstRatio = func(_ string) float64 { return 1.0 }
	nsID := namespace.ID("test-ns-id")
	shard.WorkflowIDReuseRateLimiter(nsID, "wf-id", chasm.WorkflowArchetypeID)

	shard.config.WorkflowIDReuseBurstRatio = func(_ string) float64 { return 3.0 }
	rl := shard.WorkflowIDReuseRateLimiter(nsID, "wf-id", chasm.WorkflowArchetypeID)
	require.Equal(t, 6, rl.Burst())
}
