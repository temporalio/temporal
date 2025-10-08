package replication

import (
	"context"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	clockspb "go.temporal.io/server/api/clock/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type (
	syncWorkflowStateSuite struct {
		suite.Suite
		*require.Assertions
		workflowCache              *wcache.MockCache
		eventBlobCache             persistence.XDCCache
		logger                     log.Logger
		mockShard                  *shard.ContextTest
		controller                 *gomock.Controller
		releaseFunc                func(err error)
		workflowContext            *historyi.MockWorkflowContext
		newRunWorkflowContext      *historyi.MockWorkflowContext
		namespaceID                string
		execution                  *commonpb.WorkflowExecution
		newRunId                   string
		workflowKey                definition.WorkflowKey
		syncStateRetriever         *SyncStateRetrieverImpl
		workflowConsistencyChecker *api.MockWorkflowConsistencyChecker
	}
)

func TestSyncWorkflowState(t *testing.T) {
	s := new(syncWorkflowStateSuite)
	suite.Run(t, s)
}

func (s *syncWorkflowStateSuite) SetupSuite() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.workflowContext = historyi.NewMockWorkflowContext(s.controller)
	s.newRunWorkflowContext = historyi.NewMockWorkflowContext(s.controller)
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	s.releaseFunc = func(err error) {}
	s.workflowCache = wcache.NewMockCache(s.controller)
	s.logger = s.mockShard.GetLogger()
	s.namespaceID = tests.NamespaceID.String()
	s.execution = &commonpb.WorkflowExecution{
		WorkflowId: uuid.New(),
		RunId:      uuid.New(),
	}
	s.newRunId = uuid.New()
	s.workflowKey = definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.execution.WorkflowId,
		RunID:       s.execution.RunId,
	}
	s.workflowConsistencyChecker = api.NewMockWorkflowConsistencyChecker(s.controller)
}

func (s *syncWorkflowStateSuite) TearDownSuite() {
}

func (s *syncWorkflowStateSuite) SetupTest() {
	s.eventBlobCache = persistence.NewEventsBlobCache(
		1024*1024,
		20*time.Second,
		s.logger,
	)
	s.syncStateRetriever = NewSyncStateRetriever(s.mockShard, s.workflowCache, s.workflowConsistencyChecker, s.eventBlobCache, s.logger)
}

func (s *syncWorkflowStateSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *syncWorkflowStateSuite) TestSyncWorkflowState_TransitionHistoryDisabled() {
	mu := historyi.NewMockMutableState(s.controller)
	s.workflowConsistencyChecker.EXPECT().GetChasmLeaseWithConsistencyCheck(gomock.Any(), nil, gomock.Any(), definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.execution.WorkflowId,
		RunID:       s.execution.RunId,
	}, chasm.ArchetypeAny, locks.PriorityLow).Return(
		api.NewWorkflowLease(nil, func(err error) {}, mu), nil)

	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: nil, // transition history is disabled
	}
	mu.EXPECT().HasBufferedEvents().Return(false)
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifact(
		context.Background(),
		s.namespaceID,
		s.execution,
		nil,
		nil,
	)
	s.Nil(result)
	s.Error(err)
	s.ErrorIs(err, consts.ErrTransitionHistoryDisabled)
}

func (s *syncWorkflowStateSuite) TestSyncWorkflowState_UnFlushedBufferedEvents() {
	mu := historyi.NewMockMutableState(s.controller)
	s.workflowConsistencyChecker.EXPECT().GetChasmLeaseWithConsistencyCheck(gomock.Any(), nil, gomock.Any(), definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.execution.WorkflowId,
		RunID:       s.execution.RunId,
	}, chasm.ArchetypeAny, locks.PriorityLow).Return(
		api.NewWorkflowLease(nil, func(err error) {}, mu), nil)

	mu.EXPECT().HasBufferedEvents().Return(true)
	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifact(
		context.Background(),
		s.namespaceID,
		s.execution,
		nil,
		nil,
	)
	s.Nil(result)
	s.Error(err)
	s.IsType(&serviceerror.WorkflowNotReady{}, err)
}

func (s *syncWorkflowStateSuite) TestSyncWorkflowState_ReturnMutation() {
	mu := historyi.NewMockMutableState(s.controller)
	s.workflowConsistencyChecker.EXPECT().GetChasmLeaseWithConsistencyCheck(gomock.Any(), nil, gomock.Any(), definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.execution.WorkflowId,
		RunID:       s.execution.RunId,
	}, chasm.ArchetypeAny, locks.PriorityLow).Return(
		api.NewWorkflowLease(nil, func(err error) {}, mu), nil)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 2, Version: 13},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
			{NamespaceFailoverVersion: 2, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories:    versionHistories,
		LastFirstEventTxnId: 1234, // some state that should be sanitized
	}
	mu.EXPECT().HasBufferedEvents().Return(false)
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mu.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{})
	mu.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})
	mu.EXPECT().GetPendingTimerInfos().Return(map[string]*persistencespb.TimerInfo{
		// should not be included in the mutation
		"timerID": {LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 8}},
	})
	mu.EXPECT().GetPendingChildExecutionInfos().Return(map[int64]*persistencespb.ChildExecutionInfo{
		// should be included in the mutation
		13: {
			LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 2, TransitionCount: 13},
			Clock:                         &clockspb.VectorClock{ShardId: s.mockShard.GetShardID(), Clock: 1234},
		},
	})
	mu.EXPECT().GetPendingRequestCancelExternalInfos().Return(map[int64]*persistencespb.RequestCancelInfo{
		// should not be included in the mutation
		5: {LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 5}},
	})
	mu.EXPECT().GetPendingSignalExternalInfos().Return(map[int64]*persistencespb.SignalInfo{
		// should be included in the mutation
		15: {LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 2, TransitionCount: 15}},
	})
	mu.EXPECT().HSM().Return(nil)
	mockChasmTree := historyi.NewMockChasmTree(s.controller)
	mockChasmTree.EXPECT().Snapshot(&persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12}).
		Return(chasm.NodesSnapshot{
			Nodes: map[string]*persistencespb.ChasmNode{
				"node-path": {
					Metadata: &persistencespb.ChasmNodeMetadata{
						LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 2, TransitionCount: 13},
					},
				},
			},
		})
	mu.EXPECT().ChasmTree().Return(mockChasmTree)

	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifact(
		context.Background(),
		s.namespaceID,
		s.execution,
		&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          12,
		},
		versionHistories)
	s.NoError(err)
	s.NotNil(result)
	syncAttributes := result.VersionedTransitionArtifact.GetSyncWorkflowStateMutationAttributes()
	s.NotNil(result.VersionedTransitionArtifact.GetSyncWorkflowStateMutationAttributes())

	mutation := syncAttributes.StateMutation
	// ensure it's a copy by checking the pointers are pointing to different memory addresses
	s.True(executionInfo != mutation.ExecutionInfo)
	s.Nil(mutation.ExecutionInfo.UpdateInfos)
	s.Nil(mutation.ExecutionInfo.SubStateMachinesByType)
	s.Nil(mutation.ExecutionInfo.SubStateMachineTombstoneBatches)
	s.Zero(mutation.ExecutionInfo.LastFirstEventTxnId) // field should be sanitized
	s.Empty(mutation.UpdatedActivityInfos)
	s.Len(mutation.UpdatedTimerInfos, 0)
	s.Len(mutation.UpdatedChildExecutionInfos, 1)
	s.Len(mutation.UpdatedRequestCancelInfos, 0)
	s.Len(mutation.UpdatedSignalInfos, 1)
	s.Len(mutation.UpdatedChasmNodes, 1)
	s.Nil(mutation.UpdatedChildExecutionInfos[13].Clock) // field should be sanitized

	s.Nil(result.VersionedTransitionArtifact.EventBatches)
	s.Nil(result.VersionedTransitionArtifact.NewRunInfo)
}

func (s *syncWorkflowStateSuite) TestGetSyncStateRetrieverForNewWorkflow_WithEvents() {
	mu := historyi.NewMockMutableState(s.controller)

	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 1, Version: 10},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
			{NamespaceFailoverVersion: 2, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1},
			},
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories:    versionHistories,
		LastFirstEventTxnId: 1234, // some state that should be sanitized
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mu.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{})
	mu.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})
	mu.EXPECT().GetPendingTimerInfos().Return(map[string]*persistencespb.TimerInfo{
		// should not be included in the mutation
		"timerID": {LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 8}},
	})
	mu.EXPECT().GetPendingChildExecutionInfos().Return(map[int64]*persistencespb.ChildExecutionInfo{
		// should be included in the mutation
		13: {
			LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 2, TransitionCount: 13},
			Clock:                         &clockspb.VectorClock{ShardId: s.mockShard.GetShardID(), Clock: 1234},
		},
	})
	mu.EXPECT().GetPendingRequestCancelExternalInfos().Return(map[int64]*persistencespb.RequestCancelInfo{
		// should not be included in the mutation
		5: {LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 5}},
	})
	mu.EXPECT().GetPendingSignalExternalInfos().Return(map[int64]*persistencespb.SignalInfo{
		// should be included in the mutation
		15: {LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 2, TransitionCount: 15}},
	})
	mu.EXPECT().HSM().Return(nil)
	mockChasmTree := historyi.NewMockChasmTree(s.controller)
	mockChasmTree.EXPECT().Snapshot(&persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 1,
		TransitionCount:          0,
	}).
		Return(chasm.NodesSnapshot{
			Nodes: map[string]*persistencespb.ChasmNode{
				"node-path": {
					Metadata: &persistencespb.ChasmNodeMetadata{
						LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 2, TransitionCount: 13},
					},
				},
			},
		})
	mu.EXPECT().ChasmTree().Return(mockChasmTree)

	s.mockShard.Resource.ExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken: versionHistories.Histories[0].GetBranchToken(),
		MinEventID:  1,
		MaxEventID:  versionHistories.Histories[0].Items[0].GetEventId() + 1,
		ShardID:     s.mockShard.GetShardID(),
		PageSize:    defaultPageSize,
	}).Return(&persistence.ReadRawHistoryBranchResponse{HistoryEventBlobs: s.getEventBlobs(1, 10)}, nil)

	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifactFromMutableStateForNewWorkflow(
		context.Background(),
		s.namespaceID,
		s.execution,
		mu,
		func(err error) {},
		&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          12,
		},
	)
	s.NoError(err)
	s.NotNil(result)
	syncAttributes := result.VersionedTransitionArtifact.GetSyncWorkflowStateMutationAttributes()
	s.NotNil(result.VersionedTransitionArtifact.GetSyncWorkflowStateMutationAttributes())

	mutation := syncAttributes.StateMutation
	// ensure it's a copy by checking the pointers are pointing to different memory addresses
	s.True(executionInfo != mutation.ExecutionInfo)
	s.Nil(mutation.ExecutionInfo.UpdateInfos)
	s.Nil(mutation.ExecutionInfo.SubStateMachinesByType)
	s.Nil(mutation.ExecutionInfo.SubStateMachineTombstoneBatches)
	s.Zero(mutation.ExecutionInfo.LastFirstEventTxnId) // field should be sanitized
	s.Empty(mutation.UpdatedActivityInfos)
	s.Len(mutation.UpdatedTimerInfos, 1)
	s.Len(mutation.UpdatedChildExecutionInfos, 1)
	s.Len(mutation.UpdatedRequestCancelInfos, 1)
	s.Len(mutation.UpdatedSignalInfos, 1)
	s.Len(mutation.UpdatedChasmNodes, 1)
	s.Nil(mutation.UpdatedChildExecutionInfos[13].Clock) // field should be sanitized

	s.Nil(result.VersionedTransitionArtifact.NewRunInfo)
}

func (s *syncWorkflowStateSuite) TestGetSyncStateRetrieverForNewWorkflow_NoEvents() {
	// Test that sync state retriever logic handles the case where mutable state has no event at all
	// and current version history is empty.

	mu := historyi.NewMockMutableState(s.controller)

	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories:                  []*historyspb.VersionHistory{{}},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1},
			},
		},
		VersionHistories:    versionHistories,
		LastFirstEventTxnId: 1234, // some state that should be sanitized
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mu.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{})
	mu.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})
	mu.EXPECT().GetPendingTimerInfos().Return(map[string]*persistencespb.TimerInfo{})
	mu.EXPECT().GetPendingChildExecutionInfos().Return(map[int64]*persistencespb.ChildExecutionInfo{})
	mu.EXPECT().GetPendingRequestCancelExternalInfos().Return(map[int64]*persistencespb.RequestCancelInfo{})
	mu.EXPECT().GetPendingSignalExternalInfos().Return(map[int64]*persistencespb.SignalInfo{})
	mu.EXPECT().HSM().Return(nil)
	mockChasmTree := historyi.NewMockChasmTree(s.controller)
	mockChasmTree.EXPECT().Snapshot(&persistencespb.VersionedTransition{
		NamespaceFailoverVersion: 1,
		TransitionCount:          0,
	}).Return(chasm.NodesSnapshot{
		Nodes: map[string]*persistencespb.ChasmNode{
			"node-path": {
				Metadata: &persistencespb.ChasmNodeMetadata{
					LastUpdateVersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
				},
			},
		},
	})
	mu.EXPECT().ChasmTree().Return(mockChasmTree)

	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifactFromMutableStateForNewWorkflow(
		context.Background(),
		s.namespaceID,
		s.execution,
		mu,
		func(err error) {},
		&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          5,
		},
	)
	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.VersionedTransitionArtifact.GetSyncWorkflowStateMutationAttributes())
}

func (s *syncWorkflowStateSuite) TestSyncWorkflowState_ReturnSnapshot() {
	testCases := []struct {
		name   string
		infoFn func() (*historyspb.VersionHistories, []*persistencespb.VersionedTransition, []*persistencespb.StateMachineTombstoneBatch, *persistencespb.VersionedTransition)
	}{
		{
			name: "tombstone batch is empty",
			infoFn: func() (*historyspb.VersionHistories, []*persistencespb.VersionedTransition, []*persistencespb.StateMachineTombstoneBatch, *persistencespb.VersionedTransition) {
				versionHistories := &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: []byte("branchToken1"),
							Items: []*historyspb.VersionHistoryItem{
								{EventId: 1, Version: 10},
								{EventId: 2, Version: 13},
							},
						},
					},
				}
				return versionHistories, []*persistencespb.VersionedTransition{
					{NamespaceFailoverVersion: 1, TransitionCount: 12},
					{NamespaceFailoverVersion: 2, TransitionCount: 15},
				}, nil, nil
			},
		},
		{
			name: "tombstone batch is not empty",
			infoFn: func() (*historyspb.VersionHistories, []*persistencespb.VersionedTransition, []*persistencespb.StateMachineTombstoneBatch, *persistencespb.VersionedTransition) {
				versionHistories := &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: []byte("branchToken1"),
							Items: []*historyspb.VersionHistoryItem{
								{EventId: 1, Version: 10},
								{EventId: 2, Version: 13},
							},
						},
					},
				}
				return versionHistories, []*persistencespb.VersionedTransition{
						{NamespaceFailoverVersion: 1, TransitionCount: 12},
						{NamespaceFailoverVersion: 2, TransitionCount: 15},
					}, []*persistencespb.StateMachineTombstoneBatch{
						{
							VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
						},
					}, nil
			},
		},
		{
			name: "tombstone batch is not empty, but target state transition is before break point",
			infoFn: func() (*historyspb.VersionHistories, []*persistencespb.VersionedTransition, []*persistencespb.StateMachineTombstoneBatch, *persistencespb.VersionedTransition) {
				versionHistories := &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: []byte("branchToken1"),
							Items: []*historyspb.VersionHistoryItem{
								{EventId: 1, Version: 10},
								{EventId: 2, Version: 13},
							},
						},
					},
				}
				return versionHistories, []*persistencespb.VersionedTransition{
						{NamespaceFailoverVersion: 1, TransitionCount: 13},
						{NamespaceFailoverVersion: 2, TransitionCount: 15},
					}, []*persistencespb.StateMachineTombstoneBatch{
						{
							VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
						},
					}, &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 13}
			},
		},
	}
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			mu := historyi.NewMockMutableState(s.controller)
			s.workflowConsistencyChecker.EXPECT().GetChasmLeaseWithConsistencyCheck(gomock.Any(), nil, gomock.Any(), definition.WorkflowKey{
				NamespaceID: s.namespaceID,
				WorkflowID:  s.execution.WorkflowId,
				RunID:       s.execution.RunId,
			}, chasm.ArchetypeAny, locks.PriorityLow).Return(
				api.NewWorkflowLease(nil, func(err error) {}, mu), nil)
			versionHistories, transitions, tombstoneBatches, breakPoint := tc.infoFn()
			executionInfo := &persistencespb.WorkflowExecutionInfo{
				TransitionHistory:               transitions,
				SubStateMachineTombstoneBatches: tombstoneBatches,
				VersionHistories:                versionHistories,
				LastTransitionHistoryBreakPoint: breakPoint,
			}
			mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
			mu.EXPECT().HasBufferedEvents().Return(false)
			mu.EXPECT().CloneToProto().Return(&persistencespb.WorkflowMutableState{
				ExecutionInfo: executionInfo,
			})
			result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifact(
				context.Background(),
				s.namespaceID,
				s.execution,
				&persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 1,
					TransitionCount:          13,
				},
				versionHistories)
			s.NoError(err)
			s.NotNil(result)
			s.NotNil(result.VersionedTransitionArtifact.GetSyncWorkflowStateSnapshotAttributes())
			s.Nil(result.VersionedTransitionArtifact.EventBatches)
			s.Nil(result.VersionedTransitionArtifact.NewRunInfo)
		})
	}
}

func (s *syncWorkflowStateSuite) TestSyncWorkflowState_NoVersionTransitionProvided_ReturnSnapshot() {
	mu := historyi.NewMockMutableState(s.controller)
	s.workflowConsistencyChecker.EXPECT().GetChasmLeaseWithConsistencyCheck(gomock.Any(), nil, gomock.Any(), definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.execution.WorkflowId,
		RunID:       s.execution.RunId,
	}, chasm.ArchetypeAny, locks.PriorityLow).Return(
		api.NewWorkflowLease(nil, func(err error) {}, mu), nil)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 2, Version: 13},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
			{NamespaceFailoverVersion: 2, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories: versionHistories,
	}
	mu.EXPECT().HasBufferedEvents().Return(false)
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mu.EXPECT().CloneToProto().Return(&persistencespb.WorkflowMutableState{
		ExecutionInfo: executionInfo,
	})
	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifact(
		context.Background(),
		s.namespaceID,
		s.execution,
		nil,
		versionHistories)
	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.VersionedTransitionArtifact.GetSyncWorkflowStateSnapshotAttributes())
	s.Nil(result.VersionedTransitionArtifact.EventBatches)
	s.Nil(result.VersionedTransitionArtifact.NewRunInfo)
}

func (s *syncWorkflowStateSuite) TestGetNewRunInfo() {
	mu := historyi.NewMockMutableState(s.controller)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 2, Version: 13},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
			{NamespaceFailoverVersion: 2, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories:  versionHistories,
		NewExecutionRunId: s.newRunId,
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	// New logic queries start version and checks cluster affinity
	mu.EXPECT().GetStartVersion().Return(int64(1), nil)
	cm := cluster.NewMockMetadata(s.controller)
	cm.EXPECT().GetClusterID().Return(int64(1))
	cm.EXPECT().IsVersionFromSameCluster(int64(1), int64(1)).Return(true)
	s.mockShard.SetClusterMetadata(cm)
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), s.mockShard, namespace.ID(s.namespaceID), &commonpb.WorkflowExecution{
		WorkflowId: s.execution.WorkflowId,
		RunId:      s.newRunId,
	}, locks.PriorityLow).Return(s.newRunWorkflowContext, s.releaseFunc, nil)
	s.newRunWorkflowContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).
		Return(mu, nil).Times(1)

	s.mockShard.Resource.ExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken: versionHistories.Histories[0].GetBranchToken(),
		MinEventID:  1,
		MaxEventID:  2,
		ShardID:     s.mockShard.GetShardID(),
		PageSize:    defaultPageSize,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{
			{Data: []byte("event1")}},
	}, nil)
	newRunInfo, err := s.syncStateRetriever.getNewRunInfo(context.Background(), namespace.ID(s.namespaceID), s.execution, s.newRunId)
	s.NoError(err)
	s.NotNil(newRunInfo)
}

func (s *syncWorkflowStateSuite) TestGetNewRunInfo_NewRunFromDifferentCluster_ReturnNil() {
	mu := historyi.NewMockMutableState(s.controller)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 1, Version: 7},
					{EventId: 2, Version: 13},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 7, TransitionCount: 12},
			{NamespaceFailoverVersion: 13, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories:  versionHistories,
		NewExecutionRunId: s.newRunId,
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	// New logic queries start version and checks cluster affinity
	mu.EXPECT().GetStartVersion().Return(int64(7), nil)
	cm := cluster.NewMockMetadata(s.controller)
	cm.EXPECT().GetClusterID().Return(int64(1))
	cm.EXPECT().IsVersionFromSameCluster(int64(7), int64(1)).Return(false)
	s.mockShard.SetClusterMetadata(cm)

	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), s.mockShard, namespace.ID(s.namespaceID), &commonpb.WorkflowExecution{
		WorkflowId: s.execution.WorkflowId,
		RunId:      s.newRunId,
	}, locks.PriorityLow).Return(s.newRunWorkflowContext, s.releaseFunc, nil)
	s.newRunWorkflowContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).
		Return(mu, nil).Times(1)

	newRunInfo, err := s.syncStateRetriever.getNewRunInfo(context.Background(), namespace.ID(s.namespaceID), s.execution, s.newRunId)
	s.NoError(err)
	s.Nil(newRunInfo)
}

func (s *syncWorkflowStateSuite) TestGetNewRunInfo_NotFound() {
	mu := historyi.NewMockMutableState(s.controller)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 2, Version: 13},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
			{NamespaceFailoverVersion: 2, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories:  versionHistories,
		NewExecutionRunId: s.newRunId,
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), s.mockShard, namespace.ID(s.namespaceID), &commonpb.WorkflowExecution{
		WorkflowId: s.execution.WorkflowId,
		RunId:      s.newRunId,
	}, locks.PriorityLow).Return(s.newRunWorkflowContext, s.releaseFunc, nil)
	s.newRunWorkflowContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).
		Return(nil, serviceerror.NewNotFound("not found")).Times(1)

	newRunInfo, err := s.syncStateRetriever.getNewRunInfo(context.Background(), namespace.ID(s.namespaceID), s.execution, s.newRunId)
	s.NoError(err)
	s.Nil(newRunInfo)
}

func (s *syncWorkflowStateSuite) addXDCCache(minEventID int64, version int64, nextEventID int64, eventBlobs []*commonpb.DataBlob, versionHistoryItems []*historyspb.VersionHistoryItem) {
	s.eventBlobCache.Put(persistence.NewXDCCacheKey(
		s.workflowKey,
		minEventID,
		version,
	), persistence.NewXDCCacheValue(
		nil,
		versionHistoryItems,
		eventBlobs,
		nextEventID,
	))
}

func (s *syncWorkflowStateSuite) getEventBlobs(firstEventID, nextEventID int64) []*commonpb.DataBlob {
	eventBlob := &commonpb.DataBlob{Data: []byte("event1")}
	eventBlobs := make([]*commonpb.DataBlob, nextEventID-firstEventID)
	for i := 0; i < int(nextEventID-firstEventID); i++ {
		eventBlobs[i] = eventBlob
	}
	return eventBlobs
}

func (s *syncWorkflowStateSuite) TestGetSyncStateEvents() {
	targetVersionHistoriesItems := [][]*historyspb.VersionHistoryItem{
		{
			{EventId: 1, Version: 10},
			{EventId: 18, Version: 13},
		},
		{
			{EventId: 10, Version: 10},
		},
	}
	versionHistoryItems := []*historyspb.VersionHistoryItem{
		{EventId: 1, Version: 10},
		{EventId: 30, Version: 13},
	}
	sourceVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("source branchToken1"),
				Items:       versionHistoryItems,
			},
		},
	}

	// get [19, 31) from DB
	s.mockShard.Resource.ExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken: sourceVersionHistories.Histories[0].GetBranchToken(),
		MinEventID:  19,
		MaxEventID:  31,
		ShardID:     s.mockShard.GetShardID(),
		PageSize:    defaultPageSize,
	}).Return(&persistence.ReadRawHistoryBranchResponse{HistoryEventBlobs: s.getEventBlobs(19, 31)}, nil)

	events, err := s.syncStateRetriever.getSyncStateEvents(context.Background(), s.workflowKey, targetVersionHistoriesItems, sourceVersionHistories, false)
	s.NoError(err)
	s.Len(events, 31-19)

	// get [19,21) from cache, [21, 31) from DB
	s.mockShard.Resource.ExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken: sourceVersionHistories.Histories[0].GetBranchToken(),
		MinEventID:  21,
		MaxEventID:  31,
		ShardID:     s.mockShard.GetShardID(),
		PageSize:    defaultPageSize,
	}).Return(&persistence.ReadRawHistoryBranchResponse{HistoryEventBlobs: s.getEventBlobs(21, 31)}, nil)

	s.addXDCCache(19, 13, 21, s.getEventBlobs(19, 21), versionHistoryItems)
	events, err = s.syncStateRetriever.getSyncStateEvents(context.Background(), s.workflowKey, targetVersionHistoriesItems, sourceVersionHistories, false)
	s.NoError(err)
	s.Len(events, 31-19)

	// get [19,31) from cache
	s.addXDCCache(21, 13, 41, s.getEventBlobs(21, 41), versionHistoryItems)
	events, err = s.syncStateRetriever.getSyncStateEvents(context.Background(), s.workflowKey, targetVersionHistoriesItems, sourceVersionHistories, false)
	s.NoError(err)
	s.Len(events, 31-19)
}

func (s *syncWorkflowStateSuite) TestGetEventsBlob_NewRun() {
	versionHistory := &historyspb.VersionHistory{
		BranchToken: []byte("branchToken1"),
		Items: []*historyspb.VersionHistoryItem{
			{EventId: 1, Version: 1},
		},
	}

	// get [1,4) from DB
	s.mockShard.Resource.ExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken: versionHistory.BranchToken,
		MinEventID:  common.FirstEventID,
		MaxEventID:  common.FirstEventID + 1,
		ShardID:     s.mockShard.GetShardID(),
		PageSize:    defaultPageSize,
	}).Return(&persistence.ReadRawHistoryBranchResponse{HistoryEventBlobs: s.getEventBlobs(1, 4)}, nil)
	events, err := s.syncStateRetriever.getEventsBlob(context.Background(), s.workflowKey, versionHistory, common.FirstEventID, common.FirstEventID+1, true)
	s.NoError(err)
	s.Len(events, 4-1)

	// get [1,4) from cache
	s.addXDCCache(1, 1, 4, s.getEventBlobs(1, 4), versionHistory.Items)
	events, err = s.syncStateRetriever.getEventsBlob(context.Background(), s.workflowKey, versionHistory, common.FirstEventID, common.FirstEventID+1, true)
	s.NoError(err)
	s.Len(events, 4-1)
}

func (s *syncWorkflowStateSuite) TestGetSyncStateEvents_EventsUpToDate_ReturnNothing() {
	targetVersionHistoriesItems := [][]*historyspb.VersionHistoryItem{
		{
			{EventId: 1, Version: 10},
			{EventId: 18, Version: 13},
		},
	}
	sourceVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("source branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 18, Version: 13},
				},
			},
		},
	}

	events, err := s.syncStateRetriever.getSyncStateEvents(context.Background(), s.workflowKey, targetVersionHistoriesItems, sourceVersionHistories, false)

	s.NoError(err)
	s.Nil(events)
}

func (s *syncWorkflowStateSuite) TestGetUpdatedSubStateMachine() {
	reg := hsm.NewRegistry()
	var def1 = hsmtest.NewDefinition("type1")
	err := reg.RegisterMachine(def1)
	s.NoError(err)

	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &hsmtest.NodeBackend{})
	s.NoError(err)
	root.InternalRepr().LastUpdateVersionedTransition = &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 10}
	child1, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "child1"}, hsmtest.NewData(hsmtest.State1))
	s.Nil(err)
	child1.InternalRepr().LastUpdateVersionedTransition = &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 8}
	child2, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "child2"}, hsmtest.NewData(hsmtest.State1))
	s.Nil(err)
	child2.InternalRepr().LastUpdateVersionedTransition = &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 10}

	result, err := s.syncStateRetriever.getUpdatedSubStateMachine(root, &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 9})
	s.NoError(err)
	s.Equal(1, len(result))
	s.Equal(len(child2.Path()), len(result[0].Path.Path))
	s.Equal(child2.Path()[0].ID, result[0].Path.Path[0].Id)
}
