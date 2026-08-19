package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

// A failed write leaves an event cached; the shard reloads and a later transaction persists a
// different event at that ID. The host-level cache must not serve the stale entry.
func TestStaleCachedEventNotServedAfterShardReload(t *testing.T) {
	const initiatedEventID = int64(5)
	const version = int64(1234)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	cfg := tests.NewDynamicConfig()
	logger := log.NewNoopLogger()

	newShard := func(rangeID int64) *shard.ContextTest {
		sc := shard.NewTestContext(ctrl, &persistencespb.ShardInfo{ShardId: 1, RangeId: rangeID}, cfg)
		reg := hsm.NewRegistry()
		require.NoError(t, RegisterStateMachine(reg))
		sc.SetStateMachineRegistry(reg)
		sc.Resource.NamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).
			Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
		sc.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).
			Return(cluster.TestCurrentClusterName).AnyTimes()
		sc.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().
			Return(cluster.TestCurrentClusterName).AnyTimes()
		sc.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
		sc.Resource.ExecutionMgr.EXPECT().GetHistoryBranchUtil().
			Return(persistence.NewHistoryBranchUtil(serialization.NewSerializer())).AnyTimes()
		t.Cleanup(sc.StopForTest)
		return sc
	}

	// Shard instance A, and the process-lifetime events cache introduced by #11450.
	shardA := newShard(1)
	hostCache := events.NewHostLevelEventsCache(
		shardA.GetExecutionManager(), cfg, metrics.NoopMetricsHandler, logger, false)
	shardA.SetEventsCacheForTesting(hostCache)

	// Shard A builds a timer event at eventID 5 -- cached at build time -- then fails to persist.
	msA := TestGlobalMutableState(shardA, hostCache, logger, version, tests.WorkflowID, tests.RunID)
	staleEvent := &historypb.HistoryEvent{
		EventId:   initiatedEventID,
		Version:   version,
		EventType: enumspb.EVENT_TYPE_TIMER_STARTED,
		Attributes: &historypb.HistoryEvent_TimerStartedEventAttributes{
			TimerStartedEventAttributes: &historypb.TimerStartedEventAttributes{TimerId: "t1"},
		},
	}
	msA.writeEventToCache(staleEvent)

	// Shard moves away and back: new shard context, same process, same host cache.
	shardB := newShard(2)
	shardB.SetEventsCacheForTesting(hostCache)

	// The retry persisted a child-initiated event at that same event ID.
	persistedEvent := &historypb.HistoryEvent{
		EventId:   initiatedEventID,
		Version:   version,
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
			StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
				WorkflowId: "child-wf",
			},
		},
	}
	// The host cache reads through the execution manager it was built with.
	shardA.Resource.ExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(
		&persistence.ReadHistoryBranchResponse{
			HistoryEvents: []*historypb.HistoryEvent{persistedEvent},
		}, nil).AnyTimes()

	msB := TestGlobalMutableState(shardB, hostCache, logger, version, tests.WorkflowID, tests.RunID)
	msB.GetExecutionInfo().VersionHistories = &historyspb.VersionHistories{
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branch-token"),
				Items:       []*historyspb.VersionHistoryItem{{EventId: initiatedEventID, Version: version}},
			},
		},
	}
	msB.pendingChildExecutionInfoIDs = map[int64]*persistencespb.ChildExecutionInfo{
		initiatedEventID: {InitiatedEventId: initiatedEventID, InitiatedEventBatchId: initiatedEventID},
	}

	got, err := msB.GetChildExecutionInitiatedEvent(ctx, initiatedEventID)
	require.NoError(t, err)

	// Nil attributes are what transfer_queue_active_task_executor.go:967 dereferences.
	require.NotNil(t, got.GetStartChildWorkflowExecutionInitiatedEventAttributes(),
		"stale event from the previous shard instance was served for the child's initiated event ID")
}
