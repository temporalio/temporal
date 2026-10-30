package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRefreshCloseTaskParentVerification(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		invalidLocalHistory  bool
		invalidSourceHistory bool
		closeTaskID          int64
		queueKnown           bool
		highWatermark        int64
		pendingScope         bool
		differentHistory     bool
		localRunning         bool
		wantSkip             bool
	}{
		{name: "legacy close task", wantSkip: true},
		{name: "acked close task", closeTaskID: 50, queueKnown: true, highWatermark: 100, wantSkip: true},
		{name: "at high watermark", closeTaskID: 50, queueKnown: true, highWatermark: 50},
		{name: "pending in reader scope", closeTaskID: 50, queueKnown: true, highWatermark: 100, pendingScope: true},
		{name: "queue state unavailable", closeTaskID: 50},
		{name: "different history", differentHistory: true},
		{name: "snapshot closes workflow", localRunning: true},
		{name: "invalid local history", invalidLocalHistory: true},
		{name: "invalid source history", invalidSourceHistory: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			queueStates := make(map[int32]*persistencespb.QueueState)
			if tc.queueKnown {
				queueState := &persistencespb.QueueState{
					ExclusiveReaderHighWatermark: queues.ToPersistenceTaskKey(tasks.NewImmediateKey(tc.highWatermark)),
				}
				if tc.pendingScope {
					queueState.ReaderStates = map[int64]*persistencespb.QueueReaderState{
						0: {Scopes: []*persistencespb.QueueSliceScope{
							queues.ToPersistenceScope(queues.NewScope(
								queues.NewRange(tasks.NewImmediateKey(25), tasks.NewImmediateKey(75)),
								predicates.Universal[tasks.Task](),
							)),
						}},
					}
				}
				queueStates[int32(tasks.CategoryTransfer.ID())] = queueState
			}
			testShard := shard.NewTestContext(ctrl, &persistencespb.ShardInfo{ShardId: 1, QueueStates: queueStates}, tests.NewDynamicConfig())
			t.Cleanup(testShard.StopForTest)
			testShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
			testShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(tests.LocalNamespaceEntry.ID()).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
			testShard.Resource.ArchivalMetadata.EXPECT().GetHistoryConfig().Return(archiver.NewDisabledArchvialConfig()).AnyTimes()
			testShard.Resource.ArchivalMetadata.EXPECT().GetVisibilityConfig().Return(archiver.NewDisabledArchvialConfig()).AnyTimes()
			registry := hsm.NewRegistry()
			require.NoError(t, RegisterStateMachine(registry))
			testShard.SetStateMachineRegistry(registry)
			ms := NewMutableState(testShard, testShard.GetEventsCache(), testShard.GetLogger(), tests.LocalNamespaceEntry, tests.WorkflowID, tests.RunID, time.Now())
			ms.executionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
			ms.executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
			ms.executionInfo.CloseTransferTaskId = tc.closeTaskID
			ms.executionInfo.CloseTime = timestamppb.Now()
			ms.executionInfo.TaskGenerationShardClockTimestamp = -1
			ms.executionInfo.VersionHistories = versionhistory.NewVersionHistories(versionhistory.NewVersionHistory(
				[]byte("local"), []*historyspb.VersionHistoryItem{{EventId: 5, Version: 1}},
			))
			snapshot := ms.CloneToProto()
			snapshot.ExecutionInfo.CloseTransferTaskId = 999
			snapshot.ExecutionInfo.VersionHistories.Histories[0].BranchToken = []byte("warm")
			if tc.differentHistory {
				snapshot.ExecutionInfo.VersionHistories.Histories[0].Items[0].Version = 2
			}
			if tc.localRunning {
				ms.executionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING
				ms.executionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
			}

			if tc.invalidLocalHistory {
				ms.executionInfo.VersionHistories.CurrentVersionHistoryIndex = 1
			}
			if tc.invalidSourceHistory {
				snapshot.ExecutionInfo.VersionHistories.CurrentVersionHistoryIndex = 1
			}
			skipParentVerification, err := CanSkipParentVerification(testShard, ms, snapshot.ExecutionInfo.VersionHistories)
			if tc.invalidLocalHistory || tc.invalidSourceHistory {
				require.Error(t, err)
				require.False(t, skipParentVerification)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantSkip, skipParentVerification)

			require.NoError(t, ms.ApplySnapshot(snapshot))
			require.Equal(t, tc.closeTaskID, ms.executionInfo.CloseTransferTaskId)
			require.NoError(t, NewTaskRefresher(testShard).Refresh(t.Context(), ms, skipParentVerification))
			require.Equal(t, testShard.CurrentVectorClock().GetClock(), ms.executionInfo.TaskGenerationShardClockTimestamp)
			generatedTasks := ms.PopTasks()
			require.Len(t, generatedTasks[tasks.CategoryTransfer], 1)
			closeTask, ok := generatedTasks[tasks.CategoryTransfer][0].(*tasks.CloseExecutionTask)
			require.True(t, ok)
			require.Equal(t, tc.wantSkip, closeTask.SkipParentVerification)
			require.Len(t, generatedTasks[tasks.CategoryVisibility], 1)
			require.Len(t, generatedTasks[tasks.CategoryTimer], 1)
			require.IsType(t, &tasks.DeleteHistoryEventTask{}, generatedTasks[tasks.CategoryTimer][0])

		})
	}
}

func TestCanSkipParentVerification_NonWorkflow(t *testing.T) {
	controller := gomock.NewController(t)
	mutableState := historyi.NewMockMutableState(controller)
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
	})
	mutableState.EXPECT().IsWorkflow().Return(false)
	skip, err := CanSkipParentVerification(nil, mutableState, nil)
	require.NoError(t, err)
	require.False(t, skip)
}
