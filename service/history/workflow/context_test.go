package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/limiter"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	contextSuite struct {
		suite.Suite
		*require.Assertions

		mockShard *shard.ContextTest

		workflowContext *ContextImpl
	}
)

func TestContextSuite(t *testing.T) {
	suite.Run(t, new(contextSuite))
}

func (s *contextSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	configs := tests.NewDynamicConfig()

	controller := gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		controller,
		&persistencespb.ShardInfo{ShardId: 1},
		configs,
	)
	mockEngine := historyi.NewMockEngine(controller)
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()
	s.mockShard.SetEngineForTesting(mockEngine)
	s.NoError(RegisterStateMachine(s.mockShard.StateMachineRegistry()))
	mockClusterMetadata := s.mockShard.Resource.ClusterMetadata
	mockNamespaceCache := s.mockShard.Resource.NamespaceCache
	mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(false).AnyTimes()
	mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()
	mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(false, common.EmptyVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	s.workflowContext = NewContext(
		configs,
		tests.WorkflowKey,
		chasm.WorkflowArchetypeID,
		log.NewNoopLogger(),
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		limiter.NewKeyedBytesLimiter(),
	)
}

func (s *contextSuite) TestMergeReplicationTasks_NoNewRun() {
	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
	}

	err := s.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		nil, // no new run
	)
	s.NoError(err)
	s.Empty(currentWorkflowMutation.Tasks)
}

func (s *contextSuite) TestMergeReplicationTasks_LocalNamespace() {
	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		// no replication tasks
	}
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
	}

	err := s.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	s.NoError(err)
	s.Empty(currentWorkflowMutation.Tasks) // verify no change to tasks
	s.Empty(newWorkflowSnapshot.Tasks)     // verify no change to tasks
}

func (s *contextSuite) TestMergeReplicationTasks_SingleReplicationTask() {
	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey:         tests.WorkflowKey,
					VisibilityTimestamp: time.Now(),
					FirstEventID:        5,
					NextEventID:         10,
					Version:             tests.Version,
				},
				&tasks.HistoryReplicationTask{
					WorkflowKey:         tests.WorkflowKey,
					VisibilityTimestamp: time.Now(),
					FirstEventID:        10,
					NextEventID:         20,
					Version:             tests.Version,
				},
			},
		},
	}

	newRunID := uuid.NewString()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey: definition.NewWorkflowKey(
						string(tests.NamespaceID),
						tests.WorkflowID,
						newRunID,
					),
					VisibilityTimestamp: time.Now(),
					FirstEventID:        1,
					NextEventID:         3,
					Version:             tests.Version,
				},
			},
		},
	}

	err := s.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	s.NoError(err)
	s.Len(currentWorkflowMutation.Tasks[tasks.CategoryReplication], 2)
	s.Empty(newWorkflowSnapshot.Tasks[tasks.CategoryReplication]) // verify no change to tasks

	mergedReplicationTasks := currentWorkflowMutation.Tasks[tasks.CategoryReplication]
	s.Empty(mergedReplicationTasks[0].(*tasks.HistoryReplicationTask).NewRunID)
	s.Equal(newRunID, mergedReplicationTasks[1].(*tasks.HistoryReplicationTask).NewRunID)
}

func (s *contextSuite) TestMergeReplicationTasks_SyncVersionedTransitionTask_ShouldMergeTaskAndEquivalent() {
	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.SyncVersionedTransitionTask{
					WorkflowKey:  tests.WorkflowKey,
					FirstEventID: 5,
					NextEventID:  10,
					VersionedTransition: &persistencespb.VersionedTransition{
						NamespaceFailoverVersion: 1,
						TransitionCount:          1,
					},
					TaskEquivalents: []tasks.Task{
						&tasks.HistoryReplicationTask{
							WorkflowKey:         tests.WorkflowKey,
							VisibilityTimestamp: time.Now(),
							FirstEventID:        5,
							NextEventID:         10,
							Version:             tests.Version,
						},
					},
				},
			},
		},
	}

	newRunID := uuid.NewString()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey: definition.NewWorkflowKey(
						string(tests.NamespaceID),
						tests.WorkflowID,
						newRunID,
					),
					VisibilityTimestamp: time.Now(),
					FirstEventID:        1,
					NextEventID:         3,
					Version:             tests.Version,
				},
			},
		},
	}

	err := s.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	s.NoError(err)
	s.Len(currentWorkflowMutation.Tasks[tasks.CategoryReplication], 1)
	s.Empty(newWorkflowSnapshot.Tasks[tasks.CategoryReplication]) // verify no change to tasks

	mergedReplicationTasks := currentWorkflowMutation.Tasks[tasks.CategoryReplication]
	s.Equal(newRunID, mergedReplicationTasks[0].(*tasks.SyncVersionedTransitionTask).NewRunID)
	s.Equal(newRunID, mergedReplicationTasks[0].(*tasks.SyncVersionedTransitionTask).TaskEquivalents[0].(*tasks.HistoryReplicationTask).NewRunID)

}

func (s *contextSuite) TestMergeReplicationTasks_MultipleReplicationTasks() {
	// The case can happen when importing a workflow:
	// current workflow will be terminated and imported workflow can contain multiple replication tasks
	// This case is not supported right now
	// NOTE: ^ should be the case and both current and new runs should have replication tasks. However, the
	// actual implementation in WorkflowImporter will close the transaction of the new run with Passive
	// policy resulting in 0 replication tasks.
	// However the implementation of mergeUpdateWithNewReplicationTasks should still handle this case and not error out.

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey:         tests.WorkflowKey,
					VisibilityTimestamp: time.Now(),
					FirstEventID:        9,
					NextEventID:         10,
					Version:             tests.Version,
				},
			},
		},
	}

	newRunID := uuid.NewString()
	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey: definition.NewWorkflowKey(
						string(tests.NamespaceID),
						tests.WorkflowID,
						newRunID,
					),
					VisibilityTimestamp: time.Now(),
					FirstEventID:        1,
					NextEventID:         3,
					Version:             tests.Version,
				},
				&tasks.HistoryReplicationTask{
					WorkflowKey: definition.NewWorkflowKey(
						string(tests.NamespaceID),
						tests.WorkflowID,
						newRunID,
					),
					VisibilityTimestamp: time.Now(),
					FirstEventID:        3,
					NextEventID:         6,
					Version:             tests.Version,
				},
				&tasks.HistoryReplicationTask{
					WorkflowKey: definition.NewWorkflowKey(
						string(tests.NamespaceID),
						tests.WorkflowID,
						newRunID,
					),
					VisibilityTimestamp: time.Now(),
					FirstEventID:        6,
					NextEventID:         10,
					Version:             tests.Version,
				},
			},
		},
	}

	err := s.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	s.NoError(err)
	s.Len(currentWorkflowMutation.Tasks[tasks.CategoryReplication], 1) // verify no change to tasks
	s.Len(newWorkflowSnapshot.Tasks[tasks.CategoryReplication], 3)     // verify no change to tasks
}

func (s *contextSuite) TestMergeReplicationTasks_CurrentRunRunning() {
	// The case can happen when suppressing a current running workflow to be zombie
	// and creating a new workflow at the same time

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
		},
		Tasks: map[tasks.Category][]tasks.Task{},
	}

	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{},
	}

	err := s.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	s.NoError(err)
	s.Empty(currentWorkflowMutation.Tasks) // verify no change to tasks
	s.Empty(newWorkflowSnapshot.Tasks)     // verify no change to tasks
}

func (s *contextSuite) TestMergeReplicationTasks_OnlyCurrentRunHasReplicationTasks() {
	// The case can happen when importing a workflow (via replication task)
	// current workflow may be terminated and the imported workflow since it's received via replication task
	// will not generate replication tasks again.

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey:         tests.WorkflowKey,
					VisibilityTimestamp: time.Now(),
					FirstEventID:        5,
					NextEventID:         6,
					Version:             tests.Version,
				},
			},
		},
	}

	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{},
	}

	err := s.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	s.NoError(err)
	s.Len(currentWorkflowMutation.Tasks[tasks.CategoryReplication], 1) // verify no change to tasks
	s.Empty(newWorkflowSnapshot.Tasks)                                 // verify no change to tasks
}

func (s *contextSuite) TestMergeReplicationTasks_OnlyNewRunHasReplicationTasks() {
	// TODO: check if this case can happen or not.

	currentWorkflowMutation := &persistence.WorkflowMutation{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		},
		Tasks: map[tasks.Category][]tasks.Task{},
	}

	newWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		},
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryReplication: {
				&tasks.HistoryReplicationTask{
					WorkflowKey:         tests.WorkflowKey,
					VisibilityTimestamp: time.Now(),
					FirstEventID:        5,
					NextEventID:         6,
					Version:             tests.Version,
				},
			},
		},
	}

	err := s.workflowContext.mergeUpdateWithNewReplicationTasks(
		currentWorkflowMutation,
		newWorkflowSnapshot,
	)
	s.NoError(err)
	s.Empty(currentWorkflowMutation.Tasks)                         // verify no change to tasks
	s.Len(newWorkflowSnapshot.Tasks[tasks.CategoryReplication], 1) // verify no change to tasks
}

func (s *contextSuite) TestRefreshTask() {
	now := time.Now()

	baseMutableState := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:        tests.NamespaceID.String(),
			WorkflowId:         tests.WorkflowID,
			WorkflowRunTimeout: timestamp.DurationFromSeconds(200),
			ExecutionTime:      timestamppb.New(now),
			VersionHistories: &historyspb.VersionHistories{
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: []byte("token#1"),
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 1, Version: common.EmptyVersion},
						},
					},
				},
			},
			TransitionHistory: []*persistencespb.VersionedTransition{
				{
					NamespaceFailoverVersion: common.EmptyVersion,
					TransitionCount:          1,
				},
			},
			ExecutionStats: &persistencespb.ExecutionStats{
				HistorySize: 128,
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:     tests.RunID,
			State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: timestamppb.New(now),
		},
		NextEventId: 2,
	}

	testCases := []struct {
		name                  string
		persistedMutableState func() *persistencespb.WorkflowMutableState
		setupMock             func(mockShard *shard.ContextTest)
	}{
		{
			name: "open workflow",
			persistedMutableState: func() *persistencespb.WorkflowMutableState {
				return common.CloneProto(baseMutableState)
			},
			setupMock: func(mockShard *shard.ContextTest) {
				mockShard.MockEventsCache.EXPECT().GetEvent(
					gomock.Any(),
					mockShard.GetShardID(),
					gomock.Any(),
					common.FirstEventID,
					gomock.Any(),
				).Return(&historypb.HistoryEvent{
					EventId:    1,
					EventTime:  timestamppb.New(now),
					EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					Version:    common.EmptyVersion,
					Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{},
				}, nil).Times(2)
				mockShard.Resource.ExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
						s.Equal(persistence.UpdateWorkflowModeUpdateCurrent, request.Mode)
						s.NotEmpty(request.UpdateWorkflowMutation.Tasks)
						s.Empty(request.UpdateWorkflowEvents)
						return tests.UpdateWorkflowExecutionResponse, nil
					}).Times(1)
			},
		},
		{
			name: "completed workflow",
			persistedMutableState: func() *persistencespb.WorkflowMutableState {
				base := common.CloneProto(baseMutableState)
				base.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
				base.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
				base.NextEventId = 3
				base.ExecutionInfo.VersionHistories.Histories[0].Items[0].EventId = 2
				base.ExecutionInfo.TransitionHistory[0].TransitionCount = 2
				base.ExecutionInfo.CloseTime = timestamppb.New(now.Add(time.Second))
				return base
			},
			setupMock: func(mockShard *shard.ContextTest) {
				mockShard.Resource.ExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
						s.NotEmpty(request.UpdateWorkflowMutation.Tasks)
						s.Equal(persistence.UpdateWorkflowModeIgnoreCurrent, request.Mode)
						return tests.UpdateWorkflowExecutionResponse, nil
					}).Times(1)
			},
		},
		{
			name: "zombie workflow",
			persistedMutableState: func() *persistencespb.WorkflowMutableState {
				base := common.CloneProto(baseMutableState)
				base.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE
				return base
			},
			setupMock: func(mockShard *shard.ContextTest) {
				mockShard.MockEventsCache.EXPECT().GetEvent(
					gomock.Any(),
					mockShard.GetShardID(),
					gomock.Any(),
					common.FirstEventID,
					gomock.Any(),
				).Return(&historypb.HistoryEvent{
					EventId:    1,
					EventTime:  timestamppb.New(now),
					EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					Version:    common.EmptyVersion,
					Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{},
				}, nil).Times(2)
				mockShard.Resource.ExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
						s.NotEmpty(request.UpdateWorkflowMutation.Tasks)
						s.Equal(persistence.UpdateWorkflowModeBypassCurrent, request.Mode)
						return tests.UpdateWorkflowExecutionResponse, nil
					}).Times(1)
			},
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			var err error
			s.workflowContext.MutableState, err = NewMutableStateFromDB(
				s.mockShard,
				s.mockShard.MockEventsCache,
				s.mockShard.GetLogger(),
				tests.LocalNamespaceEntry,
				tc.persistedMutableState(),
				1,
			)
			s.NoError(err)

			tc.setupMock(s.mockShard)

			err = s.workflowContext.RefreshTasks(context.Background(), s.mockShard)
			s.NoError(err)
		})
	}
}

func (s *contextSuite) TestUpdateWorkflowExecutionWithNew_ChasmNoopSkipsPersistence() {
	now := time.Now().UTC()
	chasmArchetypeID := chasm.WorkflowArchetypeID + 101
	dbState := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: tests.NamespaceID.String(),
			WorkflowId:  tests.WorkflowID,
			VersionHistories: &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: []byte("token#1"),
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 1, Version: common.EmptyVersion},
						},
					},
				},
			},
			TransitionHistory: []*persistencespb.VersionedTransition{
				{
					NamespaceFailoverVersion: common.EmptyVersion,
					TransitionCount:          1,
				},
			},
			ExecutionStats: &persistencespb.ExecutionStats{
				HistorySize: 128,
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:     tests.RunID,
			State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			StartTime: timestamppb.New(now),
		},
		NextEventId: 2,
	}

	mutableState, err := NewMutableStateFromDB(
		s.mockShard,
		s.mockShard.MockEventsCache,
		s.mockShard.GetLogger(),
		tests.LocalNamespaceEntry,
		dbState,
		1,
	)
	s.NoError(err)
	_, err = mutableState.StartTransaction(tests.LocalNamespaceEntry)
	s.NoError(err)

	mockChasmTree := historyi.NewMockChasmTree(gomock.NewController(s.T()))
	mutableState.chasmTree = mockChasmTree
	mockChasmTree.EXPECT().ArchetypeID().Return(chasmArchetypeID).AnyTimes()
	mockChasmTree.EXPECT().IsStateDirty().Return(false).AnyTimes()
	mockChasmTree.EXPECT().IsDirty().Return(false).AnyTimes()
	mockChasmTree.EXPECT().CloseTransaction().Return(chasm.NodesMutation{}, nil).Times(1)

	s.workflowContext.archetypeID = chasmArchetypeID
	s.workflowContext.MutableState = mutableState

	err = s.workflowContext.UpdateWorkflowExecutionWithNew(
		context.Background(),
		s.mockShard,
		persistence.UpdateWorkflowModeIgnoreCurrent,
		nil,
		nil,
		historyi.TransactionPolicyActive,
		nil,
	)
	s.NoError(err)
}

func intermediatePage(pageNumber int32, markerName string) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		IntermediatePage: true,
		PageNumber:       pageNumber,
		Commands: []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_RECORD_MARKER,
			Attributes: &commandpb.Command_RecordMarkerCommandAttributes{
				RecordMarkerCommandAttributes: &commandpb.RecordMarkerCommandAttributes{MarkerName: markerName},
			},
		}},
	}
}

func finalPage(pageNumber int32, commands []*commandpb.Command) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		PageNumber: pageNumber,
		Commands:   commands,
	}
}

func (s *contextSuite) setTaskCompletionBufferSizeLimit(limit int) {
	s.workflowContext.config.WorkflowTaskCompletionBufferSizeLimit = func(string) int { return limit }
	mockMutableState := historyi.NewMockMutableState(gomock.NewController(s.T()))
	mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.LocalNamespaceEntry).AnyTimes()
	mockMutableState.EXPECT().GetStartedWorkflowTask().Return(&historyi.WorkflowTaskInfo{}).AnyTimes()
	mockMutableState.EXPECT().GetQueryRegistry().Return(NewQueryRegistry()).AnyTimes()
	mockMutableState.EXPECT().RemoveSpeculativeWorkflowTaskTimeoutTask().AnyTimes()
	s.workflowContext.MutableState = mockMutableState
}

// TestTaskCompletionBuffer_AttemptIsolation verifies that a buffer left behind by a
// different (schedID, attempt) is treated as stale: a page for a new attempt drops
// the old buffer, and a final page for the old attempt no longer sees its pages.
func (s *contextSuite) TestTaskCompletionBuffer_AttemptIsolation() {
	const schedID = int64(10)
	s.setTaskCompletionBufferSizeLimit(0)

	// Attempt 1 buffers page 0.
	s.NoError(s.workflowContext.AppendTaskCompletionPage(schedID, 1, intermediatePage(0, "attempt1")))

	// A page for attempt 2 arrives on the same context (timeout -> retry path, which
	// does not Clear()). It must drop attempt 1's buffer and start fresh.
	s.NoError(s.workflowContext.AppendTaskCompletionPage(schedID, 2, intermediatePage(0, "attempt2")))

	// Attempt 1's final page can no longer find its buffer.
	_, err := s.workflowContext.GetMergedTaskCompletionPages(schedID, 1, finalPage(1, nil))
	var bufferLost *serviceerror.WorkflowTaskCompletionBufferLost
	s.ErrorAs(err, &bufferLost)
}

// TestTaskCompletionBuffer_GapDetection verifies that a missing intermediate page in
// the expected range surfaces buffer-lost rather than silently dropping commands.
func (s *contextSuite) TestTaskCompletionBuffer_GapDetection() {
	const schedID = int64(11)
	s.setTaskCompletionBufferSizeLimit(0)

	s.NoError(s.workflowContext.AppendTaskCompletionPage(schedID, 1, intermediatePage(0, "p0")))
	// Page 1 never buffered; final page claims pages 0 and 1 preceded it.
	_, err := s.workflowContext.GetMergedTaskCompletionPages(schedID, 1, finalPage(2, nil))
	var bufferLost *serviceerror.WorkflowTaskCompletionBufferLost
	s.ErrorAs(err, &bufferLost)
}

// TestTaskCompletionBuffer_Idempotency verifies that a resent page does not overwrite
// the first write and the merge still yields the original commands in order.
func (s *contextSuite) TestTaskCompletionBuffer_Idempotency() {
	const schedID = int64(12)
	s.setTaskCompletionBufferSizeLimit(0)

	s.NoError(s.workflowContext.AppendTaskCompletionPage(schedID, 1, intermediatePage(0, "first")))
	// Resend page 0 with different content; first write must win.
	s.NoError(s.workflowContext.AppendTaskCompletionPage(schedID, 1, intermediatePage(0, "second")))

	merged, err := s.workflowContext.GetMergedTaskCompletionPages(schedID, 1, finalPage(1, intermediatePage(1, "final").Commands))
	s.NoError(err)
	s.Len(merged, 1)
	s.Equal("first", merged[0].GetRecordMarkerCommandAttributes().GetMarkerName())
}

func (s *contextSuite) TestTaskCompletionBuffer_PageCountLimit() {
	const schedID = int64(13)
	s.setTaskCompletionBufferSizeLimit(0)

	err := s.workflowContext.AppendTaskCompletionPage(schedID, 1, intermediatePage(maxWorkflowTaskCompletionPages, "too-many"))
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)

	s.NoError(s.workflowContext.AppendTaskCompletionPage(schedID, 1, intermediatePage(0, "p0")))
	_, err = s.workflowContext.GetMergedTaskCompletionPages(schedID, 1, finalPage(maxWorkflowTaskCompletionPages, nil))
	s.ErrorAs(err, &invalidArgument)
}

// TestTaskCompletionBuffer_PerWorkflowCapTerminates verifies that a page pushing the
// cumulative buffer past the per-workflow limit returns the terminate sentinel and
// releases the process budget.
func (s *contextSuite) TestTaskCompletionBuffer_PerWorkflowCapTerminates() {
	s.setTaskCompletionBufferSizeLimit(1)
	s.workflowContext.paginationLimiter = limiter.NewKeyedBytesLimiter()

	// 1-byte per-workflow limit: any non-empty page exceeds it.
	err := s.workflowContext.AppendTaskCompletionPage(10, 1, intermediatePage(0, "big"))
	s.ErrorIs(err, ErrTaskCompletionBufferSizeExceeded)
	s.Nil(s.workflowContext.taskCompletionBuffer)
	s.Equal(int64(0), s.workflowContext.paginationLimiter.Used())
}

// TestTaskCompletionBuffer_ProcessLimitRejects verifies that a page which would push the
// process total over the limit is rejected with buffer-lost without storing anything.
func (s *contextSuite) TestTaskCompletionBuffer_ProcessLimitRejects() {
	budget := limiter.NewKeyedBytesLimiter()
	s.workflowContext.paginationLimiter = budget

	s.setTaskCompletionBufferSizeLimit(0)

	processLimit := int64(s.workflowContext.config.WorkflowTaskCompletionBufferTotalSizeLimit())
	ok, _ := budget.TryReserve("filler", processLimit, 0, 0)
	s.True(ok)

	err := s.workflowContext.AppendTaskCompletionPage(10, 1, intermediatePage(0, "p0"))
	var bufferLost *serviceerror.WorkflowTaskCompletionBufferLost
	s.ErrorAs(err, &bufferLost)
	s.Nil(s.workflowContext.taskCompletionBuffer)
}

// TestTaskCompletionBuffer_NamespaceRatioRejects verifies that a page which would
// push a namespace over its share (ratio * process limit) is rejected with
// buffer-lost even though the process itself has plenty of room.
func (s *contextSuite) TestTaskCompletionBuffer_NamespaceRatioRejects() {
	budget := limiter.NewKeyedBytesLimiter()
	s.workflowContext.paginationLimiter = budget

	s.setTaskCompletionBufferSizeLimit(0)
	// Process budget is effectively unbounded; the namespace share is ~1 byte, so any
	// real page trips the per-namespace cap and not the process cap.
	s.workflowContext.config.WorkflowTaskCompletionBufferTotalSizeLimit = func() int { return 1 << 30 }
	s.workflowContext.config.WorkflowTaskCompletionBufferNamespaceRatio = func(string) float64 { return 1e-9 }

	err := s.workflowContext.AppendTaskCompletionPage(10, 1, intermediatePage(0, "p0"))
	var bufferLost *serviceerror.WorkflowTaskCompletionBufferLost
	s.ErrorAs(err, &bufferLost)
	s.Nil(s.workflowContext.taskCompletionBuffer)
	s.Equal(int64(0), budget.Used())
}

// TestTaskCompletionBuffer_ProcessLimitDropsPartialBuffer verifies that a process limit
// rejection of a later page drops the whole partial buffer and releases its
// already-reserved bytes
func (s *contextSuite) TestTaskCompletionBuffer_ProcessLimitDropsPartialBuffer() {
	budget := limiter.NewKeyedBytesLimiter()
	s.workflowContext.paginationLimiter = budget

	s.setTaskCompletionBufferSizeLimit(0)
	processLimit := int64(s.workflowContext.config.WorkflowTaskCompletionBufferTotalSizeLimit())

	s.NoError(s.workflowContext.AppendTaskCompletionPage(10, 1, intermediatePage(0, "p0")))
	page0Size := s.workflowContext.taskCompletionBuffer.totalSize
	s.Positive(page0Size)

	ok, _ := budget.TryReserve("filler", processLimit-page0Size, 0, 0)
	s.True(ok)

	err := s.workflowContext.AppendTaskCompletionPage(10, 1, intermediatePage(1, "p1"))
	var bufferLost *serviceerror.WorkflowTaskCompletionBufferLost
	s.ErrorAs(err, &bufferLost)
	s.Nil(s.workflowContext.taskCompletionBuffer)
	s.Equal(processLimit-page0Size, budget.Used())
}

// TestTaskCompletionBuffer_BudgetReleasedOnMergeAndClear verifies the process counter
// goes back to zero after a successful merge
func (s *contextSuite) TestTaskCompletionBuffer_BudgetReleasedOnMergeAndClear() {
	budget := limiter.NewKeyedBytesLimiter()
	s.workflowContext.paginationLimiter = budget

	// Disable the per-workflow cap so it never interferes with the process-counter assertions.
	s.setTaskCompletionBufferSizeLimit(0)

	s.NoError(s.workflowContext.AppendTaskCompletionPage(10, 1, intermediatePage(0, "p0")))
	s.NoError(s.workflowContext.AppendTaskCompletionPage(10, 1, intermediatePage(1, "p1")))
	s.Positive(budget.Used())

	_, err := s.workflowContext.GetMergedTaskCompletionPages(10, 1, finalPage(2, nil))
	s.NoError(err)
	s.Equal(int64(0), budget.Used())

	// And after Clear() of a fresh buffer.
	s.NoError(s.workflowContext.AppendTaskCompletionPage(10, 1, intermediatePage(0, "p0")))
	s.Positive(budget.Used())
	s.workflowContext.Clear()
	s.Equal(int64(0), budget.Used())
}
