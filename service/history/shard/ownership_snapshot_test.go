package shard

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func ownershipSnapshotContext(t *testing.T) *ContextImpl {
	t.Helper()
	now := time.Now()
	result := &ContextImpl{state: contextStateAcquired, ownershipRevision: 7,
		ownershipChangedAt: now.Add(-2 * time.Second), rangeAcquiredAt: now.Add(-time.Second),
		shardInfo: &persistencespb.ShardInfo{RangeId: 11}, engineFuture: future.NewFuture[historyi.Engine]()}
	result.engineFuture.Set(historyi.NewMockEngine(gomock.NewController(t)), nil)
	return result
}

func ownershipSnapshotController(contexts map[int32]historyi.ControllableContext) *ControllerImpl {
	result := &ControllerImpl{historyShards: contexts, ownershipRevision: 1,
		ownershipChangedAt: time.Now().Add(-3 * time.Second), contextTaggedLogger: log.NewNoopLogger(), taggedMetricsHandler: metrics.NoopMetricsHandler}
	result.status.Store(common.DaemonStatusStarted)
	return result
}

func TestOwnershipSnapshotOnlyReportsAcquiredRanges(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		state contextState
	}{{"initialized", contextStateInitialized}, {"acquiring", contextStateAcquiring}, {"acquired", contextStateAcquired}, {"stopping", contextStateStopping}, {"stopped", contextStateStopped}} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := ownershipSnapshotContext(t)
			ctx.state = test.state
			controller := ownershipSnapshotController(map[int32]historyi.ControllableContext{1: ctx})
			result, available := controller.OwnershipSnapshot(1)
			require.True(t, available)
			expected := ShardOwnership{Revision: 7, Since: ctx.ownershipChangedAt}
			if test.state == contextStateAcquired {
				expected.RangeID, expected.Since = 11, ctx.rangeAcquiredAt
			}
			require.Equal(t, OwnershipSnapshot{Revision: 1, ChangedAt: controller.ownershipChangedAt,
				Contexts: map[int32]ShardOwnership{1: expected}}, result)
			result.Contexts[1] = ShardOwnership{}
			again, available := controller.OwnershipSnapshot(1)
			require.True(t, available)
			require.Equal(t, expected, again.Contexts[1])
		})
	}
}

func TestOwnershipSnapshotDeclinesUnavailableAuthority(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"controller-busy", "state-busy", "range-busy", "stopped", "limit", "zero-limit", "unknown-context", "missing-engine", "pending-engine", "missing-range", "nonpositive-range", "unacquired-range", "context-overflow", "controller-overflow"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			ctx := ownershipSnapshotContext(t)
			controller := ownershipSnapshotController(map[int32]historyi.ControllableContext{1: ctx})
			limit := 1
			switch mode {
			case "controller-busy":
				controller.Lock()
				defer controller.Unlock()
			case "state-busy":
				ctx.stateLock.Lock()
				defer ctx.stateLock.Unlock()
			case "range-busy":
				ctx.rwLock.Lock()
				defer ctx.rwLock.Unlock()
			case "stopped":
				controller.status.Store(common.DaemonStatusStopped)
			case "limit":
				controller.historyShards[2] = ctx
			case "zero-limit":
				limit = 0
			case "unknown-context":
				controller.historyShards[1] = historyi.NewMockControllableContext(gomock.NewController(t))
			case "missing-engine":
				ctx.engineFuture = nil
			case "pending-engine":
				ctx.engineFuture = future.NewFuture[historyi.Engine]()
			case "nonpositive-range":
				ctx.shardInfo.RangeId = 0
			case "missing-range":
				ctx.shardInfo = nil
			case "unacquired-range":
				ctx.rangeAcquiredAt = time.Time{}
			case "context-overflow":
				ctx.ownershipRevision = math.MaxUint64
			case "controller-overflow":
				controller.ownershipRevision = math.MaxUint64
			default:
				t.Fatalf("unknown snapshot case %q", mode)
			}
			type response struct {
				value     OwnershipSnapshot
				available bool
			}
			done := make(chan response, 1)
			go func() {
				value, available := controller.OwnershipSnapshot(limit)
				done <- response{value: value, available: available}
			}()
			select {
			case result := <-done:
				require.Equal(t, response{}, result)
			case <-time.After(time.Second):
				t.Fatal("ownership snapshot waited for a held lock")
			}
		})
	}
}

type changingOwnershipContext struct {
	historyi.ControllableContext
	reads int
}

func (c *changingOwnershipContext) ownershipSnapshot() (ShardOwnership, bool) {
	c.reads++
	return ShardOwnership{Revision: uint64(c.reads), RangeID: 11}, true
}

func TestOwnershipSnapshotRejectsChangesBetweenPasses(t *testing.T) {
	t.Parallel()
	controller := ownershipSnapshotController(map[int32]historyi.ControllableContext{1: &changingOwnershipContext{}})
	result, available := controller.OwnershipSnapshot(1)
	require.False(t, available)
	require.Equal(t, OwnershipSnapshot{}, result)
}

func TestOwnershipSnapshotRetainsLifecycleAndRangeChanges(t *testing.T) {
	t.Parallel()
	ctx := ownershipSnapshotContext(t)
	controller := ownershipSnapshotController(map[int32]historyi.ControllableContext{1: ctx})
	first, available := controller.OwnershipSnapshot(1)
	require.True(t, available)
	ctx.stateLock.Lock()
	ctx.setOwnershipStateLocked(contextStateAcquiring)
	ctx.setOwnershipStateLocked(contextStateAcquired)
	ctx.stateLock.Unlock()
	second, available := controller.OwnershipSnapshot(1)
	require.True(t, available)
	require.Equal(t, first.Contexts[1].Revision+2, second.Contexts[1].Revision)
	require.False(t, second.Contexts[1].Since.Before(first.Contexts[1].Since))
	ctx.rwLock.Lock()
	ctx.shardInfo.RangeId++
	ctx.rangeAcquiredAt = time.Now()
	ctx.rwLock.Unlock()
	third, available := controller.OwnershipSnapshot(1)
	require.True(t, available)
	require.Equal(t, second.Contexts[1].RangeID+1, third.Contexts[1].RangeID)
	require.False(t, third.Contexts[1].Since.Before(second.Contexts[1].Since))
	controller.Lock()
	removed := controller.removeShardLocked(1, ctx)
	controller.Unlock()
	require.Same(t, ctx, removed)
	fourth, available := controller.OwnershipSnapshot(1)
	require.True(t, available)
	require.Equal(t, first.Revision+1, fourth.Revision)
	require.Empty(t, fourth.Contexts)
}

func TestOwnershipSnapshotFollowsNativeRenewalAndStopHooks(t *testing.T) {
	t.Parallel()
	shard := NewTestContext(gomock.NewController(t), &persistencespb.ShardInfo{ShardId: 1, RangeId: 11}, tests.NewDynamicConfig())
	t.Cleanup(shard.lifecycleCancel)
	shard.Resource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	engine := historyi.NewMockEngine(gomock.NewController(t))
	shard.setOwnershipStateLocked(contextStateAcquiring)
	require.NoError(t, shard.transition(contextRequestAcquired{engine: engine}))
	controller := ownershipSnapshotController(map[int32]historyi.ControllableContext{1: shard})
	_, available := controller.OwnershipSnapshot(1)
	require.False(t, available)
	started, release := make(chan *persistence.UpdateShardRequest, 1), make(chan struct{})
	shard.Resource.ShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateShardRequest) error {
		started <- request
		<-release
		return nil
	})
	done := make(chan error, 1)
	go func() {
		shard.rwLock.Lock()
		err := shard.renewRangeLocked(false)
		shard.rwLock.Unlock()
		done <- err
	}()
	request := await.Rcv(t, started)
	_, available = controller.OwnershipSnapshot(1)
	close(release)
	require.False(t, available)
	require.NoError(t, await.Rcv(t, done))
	require.Equal(t, int64(11), request.PreviousRangeID)
	require.Equal(t, int64(12), request.ShardInfo.RangeId)
	owned, available := controller.OwnershipSnapshot(1)
	require.True(t, available)
	require.Equal(t, int64(12), owned.Contexts[1].RangeID)
	require.False(t, owned.Contexts[1].Since.IsZero())
	shard.UnloadForOwnershipLost()
	lost, available := controller.OwnershipSnapshot(1)
	require.True(t, available)
	require.Zero(t, lost.Contexts[1].RangeID)
	require.Equal(t, owned.Contexts[1].Revision+1, lost.Contexts[1].Revision)
	engine.EXPECT().Stop()
	shard.FinishStop()
	stopped, available := controller.OwnershipSnapshot(1)
	require.True(t, available)
	require.Zero(t, stopped.Contexts[1].RangeID)
	require.Equal(t, lost.Contexts[1].Revision+1, stopped.Contexts[1].Revision)
}
