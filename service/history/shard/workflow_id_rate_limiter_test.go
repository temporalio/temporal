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

func TestBusinessIDReuseRateLimiter_Disabled(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.BusinessIDReuseRate = func(_ string) int { return 0 }
	nsID := namespace.ID("test-ns-id")
	require.Nil(t, shard.BusinessIDReuseRateLimiter(nsID, "wf-id", chasm.WorkflowArchetypeID))
}

func TestBusinessIDReuseRateLimiter_AllowsUnderLimit(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.BusinessIDReuseRate = func(_ string) int { return 10 }
	nsID := namespace.ID("test-ns-id")
	rl := shard.BusinessIDReuseRateLimiter(nsID, "wf-id", chasm.WorkflowArchetypeID)
	require.NotNil(t, rl)
	for range 10 {
		require.True(t, rl.Allow())
	}
}

func TestBusinessIDReuseRateLimiter_BlocksOverLimit(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.BusinessIDReuseRate = func(_ string) int { return 1 }
	nsID := namespace.ID("test-ns-id")
	rl := shard.BusinessIDReuseRateLimiter(nsID, "wf-id", chasm.WorkflowArchetypeID)
	require.NotNil(t, rl)
	require.True(t, rl.Allow())
	require.False(t, rl.Allow())
}

func TestBusinessIDReuseRateLimiter_IndependentBusinessIDs(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.BusinessIDReuseRate = func(_ string) int { return 1 }
	nsID := namespace.ID("test-ns-id")
	rl1 := shard.BusinessIDReuseRateLimiter(nsID, "wf-id-1", chasm.WorkflowArchetypeID)
	rl2 := shard.BusinessIDReuseRateLimiter(nsID, "wf-id-2", chasm.WorkflowArchetypeID)
	require.True(t, rl1.Allow())
	require.True(t, rl2.Allow())
}

func TestBusinessIDReuseRateLimiter_IndependentNamespaces(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.BusinessIDReuseRate = func(_ string) int { return 1 }
	ns1 := namespace.ID("ns-1")
	ns2 := namespace.ID("ns-2")
	rl1 := shard.BusinessIDReuseRateLimiter(ns1, "wf-id", chasm.WorkflowArchetypeID)
	rl2 := shard.BusinessIDReuseRateLimiter(ns2, "wf-id", chasm.WorkflowArchetypeID)
	require.True(t, rl1.Allow())
	require.True(t, rl2.Allow())
}

func TestBusinessIDReuseRateLimiter_IndependentArchetypes(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.BusinessIDReuseRate = func(_ string) int { return 1 }
	nsID := namespace.ID("test-ns-id")
	rl1 := shard.BusinessIDReuseRateLimiter(nsID, "wf-id", chasm.WorkflowArchetypeID)
	rl2 := shard.BusinessIDReuseRateLimiter(nsID, "wf-id", chasm.ArchetypeID(99999))
	require.True(t, rl1.Allow())
	require.True(t, rl2.Allow())
}

func TestBusinessIDReuseRateLimiter_BurstRatio(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.BusinessIDReuseRate = func(_ string) int { return 2 }
	shard.config.BusinessIDReuseBurstRatio = func(_ string) float64 { return 3.0 }
	nsID := namespace.ID("test-ns-id")
	rl := shard.BusinessIDReuseRateLimiter(nsID, "wf-id", chasm.WorkflowArchetypeID)
	require.NotNil(t, rl)
	for i := range 6 {
		require.True(t, rl.Allow(), "expected Allow() on call %d", i+1)
	}
	require.False(t, rl.Allow(), "expected Allow() to be false after burst exhausted")
}

func TestBusinessIDReuseRateLimiter_BurstUpdatesOnConfigChange(t *testing.T) {
	shard := newTestShardForRateLimiter(t)
	shard.config.BusinessIDReuseRate = func(_ string) int { return 2 }
	shard.config.BusinessIDReuseBurstRatio = func(_ string) float64 { return 1.0 }
	nsID := namespace.ID("test-ns-id")
	shard.BusinessIDReuseRateLimiter(nsID, "wf-id", chasm.WorkflowArchetypeID)

	shard.config.BusinessIDReuseBurstRatio = func(_ string) float64 { return 3.0 }
	rl := shard.BusinessIDReuseRateLimiter(nsID, "wf-id", chasm.WorkflowArchetypeID)
	require.Equal(t, 6, rl.Burst())
}
