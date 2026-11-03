package replication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protorequire"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestSyncTaskIncludesPersistedEvents(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name             string
		cached           bool
		stateOnly        bool
		existingEvents   bool
		forceReplication bool
		readError        error
	}{
		{name: "cold progress with cached batches", cached: true},
		{name: "cold progress with paginated persistence read"},
		{name: "history read failure", readError: serviceerror.NewUnavailable("history unavailable")},
		{name: "state only transition", stateOnly: true},
		{name: "preserve events selected by target progress", existingEvents: true},
		{name: "force replication retains task range", cached: true, forceReplication: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			ctx := t.Context()
			key := definition.NewWorkflowKey("namespace", "workflow", "run")
			versionHistory := &historyspb.VersionHistory{
				BranchToken: []byte("branch"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 18, Version: 100},
				},
			}
			transitionHistory := []*persistencespb.VersionedTransition{
				{NamespaceFailoverVersion: 100, TransitionCount: 10},
			}
			executionInfo := &persistencespb.WorkflowExecutionInfo{
				TransitionHistory: transitionHistory,
				VersionHistories: &historyspb.VersionHistories{
					Histories: []*historyspb.VersionHistory{versionHistory},
				},
			}
			task := &tasks.SyncVersionedTransitionTask{
				WorkflowKey:         key,
				ArchetypeID:         chasm.WorkflowArchetypeID,
				TaskID:              42,
				VisibilityTimestamp: time.Unix(100, 0).UTC(),
				VersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 100, TransitionCount: 4,
				},
				FirstEventVersion:  100,
				FirstEventID:       7,
				NextEventID:        9,
				IsForceReplication: tc.forceReplication,
			}
			if tc.stateOnly {
				task.FirstEventID = 0
				task.NextEventID = 0
			}
			batches := []*commonpb.DataBlob{{Data: []byte("batch 7")}, {Data: []byte("batch 8")}}
			artifact := &replicationspb.VersionedTransitionArtifact{
				StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
					SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
						State: &persistencespb.WorkflowMutableState{ExecutionInfo: executionInfo},
					},
				},
			}
			if tc.existingEvents {
				artifact.EventBatches = batches
			}
			var released bool
			mutableState := historyi.NewMockMutableState(ctrl)
			mutableState.EXPECT().GetExecutionInfo().DoAndReturn(func() *persistencespb.WorkflowExecutionInfo {
				require.False(t, released)
				return executionInfo
			}).Times(2)
			mutableState.EXPECT().HasBufferedEvents().Return(false)
			mutableState.EXPECT().GetWorkflowKey().Return(key)
			progress := NewMockProgressCache(ctrl)
			progress.EXPECT().Get(key.RunID, int32(3)).Return(nil)
			retriever := NewMockSyncStateRetriever(ctrl)
			retriever.EXPECT().GetSyncWorkflowStateArtifactFromMutableState(
				ctx, key.NamespaceID, &commonpb.WorkflowExecution{WorkflowId: key.WorkflowID, RunId: key.RunID},
				mutableState, nil, nil, gomock.Any(),
			).DoAndReturn(func(_ context.Context, _ string, _ *commonpb.WorkflowExecution, _ historyi.MutableState,
				_ *persistencespb.VersionedTransition, _ [][]*historyspb.VersionHistoryItem, release historyi.ReleaseWorkflowContextFunc,
			) (*SyncStateResult, error) {
				release(nil)
				return &SyncStateResult{
					VersionedTransitionArtifact: artifact,
					VersionedTransitionHistory:  transitionHistory,
				}, nil
			})
			executionManager := persistence.NewMockExecutionManager(ctrl)
			logger := log.NewTestLogger()
			cache := persistence.NewEventsBlobCache(1024*1024, time.Minute, logger)
			if tc.cached {
				for i, batch := range batches {
					cache.Put(persistence.NewXDCCacheKey(key, 7+int64(i), 100), persistence.NewXDCCacheValue(
						nil, versionHistory.Items, []*commonpb.DataBlob{batch}, 8+int64(i),
					))
				}
			} else if !tc.stateOnly && !tc.existingEvents {
				request := &persistence.ReadHistoryBranchRequest{
					BranchToken: versionHistory.BranchToken, MinEventID: 7, MaxEventID: 9, PageSize: 1, ShardID: 2,
				}
				executionManager.EXPECT().ReadRawHistoryBranch(ctx, request).DoAndReturn(
					func(_ context.Context, _ *persistence.ReadHistoryBranchRequest) (*persistence.ReadRawHistoryBranchResponse, error) {
						require.True(t, released)
						if tc.readError != nil {
							return nil, tc.readError
						}
						return &persistence.ReadRawHistoryBranchResponse{
							HistoryEventBlobs: batches[:1], NextPageToken: []byte("page 2"),
						}, nil
					})
				if tc.readError == nil {
					executionManager.EXPECT().ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
						BranchToken: versionHistory.BranchToken, MinEventID: 7, MaxEventID: 9, PageSize: 1, ShardID: 2,
						NextPageToken: []byte("page 2"),
					}).Return(&persistence.ReadRawHistoryBranchResponse{HistoryEventBlobs: batches[1:]}, nil)
				}
			}
			if tc.readError == nil {
				progress.EXPECT().Update(key.RunID, int32(3), transitionHistory, gomock.Any()).DoAndReturn(
					func(_ string, _ int32, _ []*persistencespb.VersionedTransition, items []*historyspb.VersionHistoryItem) error {
						protorequire.ProtoSliceEqual(t, versionHistory.Items, items)
						return nil
					})
			}
			converter := &syncVersionedTransitionTaskConverter{
				shardID: 2, eventBlobCache: cache, executionManager: executionManager,
				replicationCache: progress, syncStateRetriever: retriever, logger: logger,
			}
			result, err := converter.convert(ctx, task, 3, nil, mutableState, func(error) { released = true })
			if tc.readError != nil {
				require.ErrorIs(t, err, tc.readError)
				require.Nil(t, result)
				return
			}
			require.NoError(t, err)
			if !tc.stateOnly {
				protorequire.ProtoSliceEqual(t, batches, result.GetSyncVersionedTransitionTaskAttributes().GetVersionedTransitionArtifact().GetEventBatches())
			} else {
				require.Empty(t, result.GetSyncVersionedTransitionTaskAttributes().GetVersionedTransitionArtifact().GetEventBatches())
			}
			protorequire.ProtoEqual(t, &replicationspb.ReplicationTask{
				TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_VERSIONED_TRANSITION_TASK,
				SourceTaskId: task.TaskID,
				Attributes: &replicationspb.ReplicationTask_SyncVersionedTransitionTaskAttributes{
					SyncVersionedTransitionTaskAttributes: &replicationspb.SyncVersionedTransitionTaskAttributes{
						VersionedTransitionArtifact: artifact,
						NamespaceId:                 key.NamespaceID, WorkflowId: key.WorkflowID, RunId: key.RunID, ArchetypeId: task.ArchetypeID,
					},
				},
				VersionedTransition: task.VersionedTransition,
				VisibilityTime:      timestamppb.New(task.VisibilityTimestamp),
			}, result)
			require.True(t, released)
		})
	}
}
