package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestEmptyWorkflowContextsAreEvictedOnRelease(t *testing.T) {
	t.Parallel()
	for _, sizeBased := range []bool{false, true} {
		name := "entry_count"
		if sizeBased {
			name = "byte_size"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			controller := gomock.NewController(t)
			shardContext := shard.NewTestContext(controller, &persistencespb.ShardInfo{ShardId: 1, RangeId: 1}, tests.NewDynamicConfig())
			t.Cleanup(shardContext.StopForTest)
			shardContext.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
			config := shardContext.GetConfig()
			config.HistoryCacheLimitSizeBased = sizeBased
			c := NewHostLevelCache(config, shardContext.GetLogger(), metrics.NoopMetricsHandler, testhooks.TestHooks{}).(*cacheImpl)
			t.Cleanup(c.stop)
			tv := testvars.New(t).WithRunID("00000000-0000-4000-8000-000000000001")
			for i := range 1000 {
				_, release, err := c.GetOrCreateWorkflowExecution(t.Context(), shardContext, tv.NamespaceID(), tv.WithWorkflowIDNumber(i).WorkflowExecution(), locks.PriorityHigh)
				require.NoError(t, err)
				release(serviceerror.NewNotFound("execution already deleted"))
			}
			iterator := c.Iterator()
			hasEntries := iterator.HasNext()
			iterator.Close()
			require.False(t, hasEntries, "failed loads must not retain empty workflow contexts")
			require.Zero(t, c.Size())
		})
	}
}
