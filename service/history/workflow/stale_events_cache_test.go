package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

// A failed write leaves an event cached; the shard reloads and a later transaction persists a
// different event at that ID. The host-level cache must not serve the stale entry to the new
// shard instance, while the instance that cached it still reads its own entry.
func TestStaleCachedEventNotServedAfterShardReload(t *testing.T) {
	const eventID = int64(5)
	const version = int64(1234)
	ctx := t.Context()
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

	versionHistories := func() *historyspb.VersionHistories {
		return &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte("branch-token"),
					Items:       []*historyspb.VersionHistoryItem{{EventId: eventID, Version: version}},
				},
			},
		}
	}

	// Shard instance A, and the process-lifetime events cache shared by every shard on the host.
	shardA := newShard(1)
	hostCache := events.NewHostLevelEventsCache(
		shardA.GetExecutionManager(), cfg, metrics.NoopMetricsHandler, logger, false)

	// Shard A schedules an activity at eventID 5. Applying the event caches it, and then the
	// transaction that built it fails to persist.
	msA := TestGlobalMutableState(shardA, hostCache, logger, version, tests.WorkflowID, tests.RunID)
	msA.GetExecutionInfo().VersionHistories = versionHistories()
	staleEvent := &historypb.HistoryEvent{
		EventId:   eventID,
		Version:   version,
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityId:   "activity-1",
				ActivityType: &commonpb.ActivityType{Name: "activity-type"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: "tq"},
			},
		},
	}
	_, err := msA.ApplyActivityTaskScheduledEvent(eventID, staleEvent)
	require.NoError(t, err)

	// The instance that cached it still reads its own entry, without going to the store.
	gotA, err := msA.GetActivityScheduledEvent(ctx, eventID)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, staleEvent, gotA)

	// Shard moves away and back: new shard context, same process, same host cache.
	shardB := newShard(2)

	// The retry persisted a child-initiated event at that same event ID.
	persistedEvent := &historypb.HistoryEvent{
		EventId:   eventID,
		Version:   version,
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
			StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
				WorkflowId: "child-wf",
			},
		},
	}
	// The host cache reads through the execution manager it was built with. Exactly one read:
	// the lookup below must miss the cache, and the one above must not have.
	shardA.Resource.ExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(
		&persistence.ReadHistoryBranchResponse{
			HistoryEvents: []*historypb.HistoryEvent{persistedEvent},
		}, nil).Times(1)

	msB := TestGlobalMutableState(shardB, hostCache, logger, version, tests.WorkflowID, tests.RunID)
	msB.GetExecutionInfo().VersionHistories = versionHistories()
	msB.pendingChildExecutionInfoIDs = map[int64]*persistencespb.ChildExecutionInfo{
		eventID: {InitiatedEventId: eventID, InitiatedEventBatchId: eventID},
	}

	// Serving shard A's entry here would hand the caller nil child attributes to dereference.
	got, err := msB.GetChildExecutionInitiatedEvent(ctx, eventID)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, persistedEvent, got)
}
