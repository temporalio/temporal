package ndc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

type (
	workflowReplicatorSuite struct {
		suite.Suite
		*require.Assertions

		controller            *gomock.Controller
		mockShard             *shard.ContextTest
		mockEventCache        *events.MockCache
		mockWorkflowCache     *wcache.MockCache
		mockNamespaceCache    *namespace.MockRegistry
		mockRemoteAdminClient *adminservicemock.MockAdminServiceClient
		mockExecutionManager  *persistence.MockExecutionManager
		logger                log.Logger

		workflowID string
		runID      string
		now        time.Time

		workflowStateReplicator *WorkflowStateReplicatorImpl
	}
)

func TestWorkflowReplicatorSuite(t *testing.T) {
	s := new(workflowReplicatorSuite)
	suite.Run(t, s)
}

func (s *workflowReplicatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	// s.mockTaskRefresher = workflow.NewMockTaskRefresher(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	s.mockShard.SetStateMachineRegistry(reg)
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()

	s.mockExecutionManager = s.mockShard.Resource.ExecutionMgr
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockWorkflowCache = wcache.NewMockCache(s.controller)
	s.mockEventCache = s.mockShard.MockEventsCache
	s.mockRemoteAdminClient = s.mockShard.Resource.RemoteAdminClient
	eventReapplier := NewMockEventsReapplier(s.controller)
	s.logger = s.mockShard.GetLogger()

	s.workflowID = "some random workflow ID"
	s.runID = uuid.New()
	s.now = time.Now().UTC()
	s.workflowStateReplicator = NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		eventReapplier,
		serialization.NewSerializer(),
		s.logger,
	)
}

func (s *workflowReplicatorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *workflowReplicatorSuite) Test_ApplyWorkflowState_BrandNew() {
	namespaceID := uuid.New()
	namespaceName := "namespaceName"
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:    s.runID,
		BranchId:  uuid.New(),
		Ancestors: nil,
	}
	historyBranch, err := serialization.HistoryBranchToBlob(branchInfo)
	s.NoError(err)
	completionEventBatchId := int64(5)
	nextEventID := int64(7)
	request := &historyservice.ReplicateWorkflowStateRequest{
		WorkflowState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				WorkflowId:  s.workflowID,
				NamespaceId: namespaceID,
				VersionHistories: &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: historyBranch.GetData(),
							Items: []*historyspb.VersionHistoryItem{
								{
									EventId: int64(100),
									Version: int64(100),
								},
							},
						},
					},
				},
				CompletionEventBatchId: completionEventBatchId,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:  s.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			},
			NextEventId: nextEventID,
		},
		RemoteCluster: "test",
		NamespaceId:   namespaceID,
	}
	we := &commonpb.WorkflowExecution{
		WorkflowId: s.workflowID,
		RunId:      s.runID,
	}
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		we,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil).Times(1)

	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(nil, serviceerror.NewNotFound("ms not found"))
	mockWeCtx.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.CreateWorkflowModeBrandNew,
		"",
		int64(0),
		gomock.Any(),
		gomock.Any(),
		[]*persistence.WorkflowEvents{},
	).Return(nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: namespaceName},
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetWorkflowExecutionRawHistoryV2Response{},
		nil,
	)
	s.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound("test"))
	s.mockExecutionManager.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))
	fakeStartHistory := &historypb.HistoryEvent{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{},
		},
	}
	fakeCompletionEvent := &historypb.HistoryEvent{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{},
		},
	}
	s.mockEventCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), common.FirstEventID, gomock.Any()).Return(fakeStartHistory, nil).AnyTimes()
	s.mockEventCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), completionEventBatchId, gomock.Any()).Return(fakeCompletionEvent, nil).AnyTimes()
	err = s.workflowStateReplicator.SyncWorkflowState(context.Background(), request)
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_ApplyWorkflowState_Ancestors() {
	namespaceID := uuid.New()
	namespaceName := "namespaceName"
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:   uuid.New(),
		BranchId: uuid.New(),
		Ancestors: []*persistencespb.HistoryBranchRange{
			{
				BranchId:    uuid.New(),
				BeginNodeId: 1,
				EndNodeId:   3,
			},
			{
				BranchId:    uuid.New(),
				BeginNodeId: 3,
				EndNodeId:   4,
			},
		},
	}
	historyBranch, err := serialization.HistoryBranchToBlob(branchInfo)
	s.NoError(err)
	completionEventBatchId := int64(5)
	nextEventID := int64(7)
	request := &historyservice.ReplicateWorkflowStateRequest{
		WorkflowState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				WorkflowId:  s.workflowID,
				NamespaceId: namespaceID,
				VersionHistories: &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: historyBranch.GetData(),
							Items: []*historyspb.VersionHistoryItem{
								{
									EventId: int64(100),
									Version: int64(100),
								},
							},
						},
					},
				},
				CompletionEventBatchId: completionEventBatchId,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:  s.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			},
			NextEventId: nextEventID,
		},
		RemoteCluster: "test",
		NamespaceId:   namespaceID,
	}
	we := &commonpb.WorkflowExecution{
		WorkflowId: s.workflowID,
		RunId:      s.runID,
	}
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		we,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil).Times(1)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      branchInfo.GetTreeId(),
		},
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil).Times(1)

	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(nil, serviceerror.NewNotFound("ms not found"))
	mockWeCtx.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.CreateWorkflowModeBrandNew,
		"",
		int64(0),
		gomock.Any(),
		gomock.Any(),
		[]*persistence.WorkflowEvents{},
	).Return(nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: namespaceName},
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	expectedHistory := []*historypb.History{
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId: 1,
				},
				{
					EventId: 2,
				},
			},
		},
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId: 3,
				},
			},
		},
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId: 4,
				},
			},
		},
		{
			Events: []*historypb.HistoryEvent{
				{
					EventId: 5,
				},
				{
					EventId: 6,
				},
			},
		},
	}
	serializer := serialization.NewSerializer()
	var historyBlobs []*commonpb.DataBlob
	var nodeIds []int64
	for _, history := range expectedHistory {
		blob, err := serializer.SerializeEvents(history.GetEvents())
		s.NoError(err)
		historyBlobs = append(historyBlobs, blob)
		nodeIds = append(nodeIds, history.GetEvents()[0].GetEventId())
	}
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetWorkflowExecutionRawHistoryV2Response{
			HistoryBatches: historyBlobs,
			HistoryNodeIds: nodeIds,
		},
		nil,
	)
	s.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History: []*historypb.History{
			{
				Events: []*historypb.HistoryEvent{
					{
						EventId: 1,
					},
					{
						EventId: 2,
					},
				},
			},
		},
	}, nil)
	s.mockExecutionManager.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), gomock.Any()).Return(nil, nil).Times(3)
	fakeStartHistory := &historypb.HistoryEvent{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{},
		},
	}
	fakeCompletionEvent := &historypb.HistoryEvent{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{},
		},
	}
	s.mockEventCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), common.FirstEventID, gomock.Any()).Return(fakeStartHistory, nil).AnyTimes()
	s.mockEventCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), completionEventBatchId, gomock.Any()).Return(fakeCompletionEvent, nil).AnyTimes()
	err = s.workflowStateReplicator.SyncWorkflowState(context.Background(), request)
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_ApplyWorkflowState_NoClosedWorkflow_Error() {
	err := s.workflowStateReplicator.SyncWorkflowState(context.Background(), &historyservice.ReplicateWorkflowStateRequest{
		WorkflowState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				WorkflowId: s.workflowID,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:  s.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			},
		},
		RemoteCluster: "test",
	})
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
}

func (s *workflowReplicatorSuite) Test_ApplyWorkflowState_ExistWorkflow_Resend() {
	namespaceID := uuid.New()
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:    uuid.New(),
		BranchId:  uuid.New(),
		Ancestors: nil,
	}
	historyBranch, err := serialization.HistoryBranchToBlob(branchInfo)
	s.NoError(err)
	completionEventBatchId := int64(5)
	nextEventID := int64(7)
	request := &historyservice.ReplicateWorkflowStateRequest{
		WorkflowState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				WorkflowId:  s.workflowID,
				NamespaceId: namespaceID,
				VersionHistories: &historyspb.VersionHistories{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						{
							BranchToken: historyBranch.GetData(),
							Items: []*historyspb.VersionHistoryItem{
								{
									EventId: int64(100),
									Version: int64(100),
								},
							},
						},
					},
				},
				CompletionEventBatchId: completionEventBatchId,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:  s.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			},
			NextEventId: nextEventID,
		},
		RemoteCluster: "test",
	}
	we := &commonpb.WorkflowExecution{
		WorkflowId: s.workflowID,
		RunId:      s.runID,
	}
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		we,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: &historyspb.VersionHistories{
			CurrentVersionHistoryIndex: 0,
			Histories: []*historyspb.VersionHistory{
				{
					Items: []*historyspb.VersionHistoryItem{
						{
							EventId: int64(1),
							Version: int64(1),
						},
					},
				},
			},
		},
	})
	err = s.workflowStateReplicator.SyncWorkflowState(context.Background(), request)
	var expectedErr *serviceerrors.RetryReplication
	s.ErrorAs(err, &expectedErr)
	s.Equal(namespaceID, expectedErr.NamespaceId)
	s.Equal(s.workflowID, expectedErr.WorkflowId)
	s.Equal(s.runID, expectedErr.RunId)
	s.Equal(int64(1), expectedErr.StartEventId)
	s.Equal(int64(1), expectedErr.StartEventVersion)
}

func (s *workflowReplicatorSuite) Test_ApplyWorkflowState_ExistWorkflow_SyncHSM() {
	namespaceID := uuid.New()
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:    uuid.New(),
		BranchId:  uuid.New(),
		Ancestors: nil,
	}
	historyBranch, err := serialization.HistoryBranchToBlob(branchInfo)
	s.NoError(err)
	completionEventBatchId := int64(5)
	nextEventID := int64(7)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: historyBranch.GetData(),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(100),
						Version: int64(100),
					},
				},
			},
		},
	}
	request := &historyservice.ReplicateWorkflowStateRequest{
		WorkflowState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
				WorkflowId:             s.workflowID,
				NamespaceId:            namespaceID,
				VersionHistories:       versionHistories,
				CompletionEventBatchId: completionEventBatchId,
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:  s.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			},
			NextEventId: nextEventID,
		},
		RemoteCluster: "test",
	}
	we := &commonpb.WorkflowExecution{
		WorkflowId: s.workflowID,
		RunId:      s.runID,
	}
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		we,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
	})
	mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(namespaceID, s.workflowID, s.runID)).AnyTimes()

	engine := historyi.NewMockEngine(s.controller)
	s.mockShard.SetEngineForTesting(engine)
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	s.NoError(err)
	engine.EXPECT().SyncHSM(gomock.Any(), &historyi.SyncHSMRequest{
		WorkflowKey: definition.NewWorkflowKey(namespaceID, s.workflowID, s.runID),
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: request.WorkflowState.ExecutionInfo.SubStateMachinesByType,
		},
		EventVersionHistory: currentVersionHistory,
	}).Times(1)
	engine.EXPECT().Stop().AnyTimes()

	err = s.workflowStateReplicator.SyncWorkflowState(context.Background(), request)
	s.NoError(err)
}

type VersionedTransitionMatcher struct {
	expected *persistencespb.VersionedTransition
}

// Matches implements gomock.Matcher
func (m *VersionedTransitionMatcher) Matches(x interface{}) bool {
	// Type assertion to ensure the argument is of the correct type
	got, ok := x.(*persistencespb.VersionedTransition)
	if !ok {
		return false
	}

	// Use proto.Equal to compare the actual protobuf messages
	return m.expected.TransitionCount == got.TransitionCount && m.expected.NamespaceFailoverVersion == got.NamespaceFailoverVersion
}

// String is used for descriptive error messages when the matcher fails
func (m *VersionedTransitionMatcher) String() string {
	return fmt.Sprintf("is equal to %v", m.expected)
}

// Helper function to create the matcher
func EqVersionedTransition(expected *persistencespb.VersionedTransition) gomock.Matcher {
	return &VersionedTransitionMatcher{expected: expected}
}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_SameBranch_SyncSnapshot() {
	workflowStateReplicator := NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		nil,
		serialization.NewSerializer(),
		s.logger,
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.New()
	s.workflowStateReplicator.transactionMgr = NewMockTransactionManager(s.controller)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(30),
						Version: int64(2),
					},
				},
			},
		},
	}
	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
				State: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						WorkflowId:       s.workflowID,
						NamespaceId:      namespaceID,
						VersionHistories: versionHistories,
						TransitionHistory: []*persistencespb.VersionedTransition{
							{NamespaceFailoverVersion: 1, TransitionCount: 10},
							{NamespaceFailoverVersion: 2, TransitionCount: 20},
						},
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId: s.runID,
					},
				},
			},
		},
	}
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmEntity(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.ArchetypeAny,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 10},
			{NamespaceFailoverVersion: 2, TransitionCount: 18},
		},
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()
	mockMutableState.EXPECT().GetPendingChildIds().Return(nil).Times(1)
	mockMutableState.EXPECT().ApplySnapshot(versionedTransitionArtifact.GetSyncWorkflowStateSnapshotAttributes().State)
	mockTransactionManager.EXPECT().UpdateWorkflow(gomock.Any(), false, gomock.Any(), nil).Return(nil).Times(1)
	mockTaskRefresher.EXPECT().
		PartialRefresh(gomock.Any(), gomock.Any(), EqVersionedTransition(&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 2,
			TransitionCount:          19,
		}), nil,
		).Return(nil).Times(1)

	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), versionedTransitionArtifact, "test")
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_DifferentBranch_SyncState() {
	workflowStateReplicator := NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		nil,
		serialization.NewSerializer(),
		s.logger,
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.New()
	s.workflowStateReplicator.transactionMgr = NewMockTransactionManager(s.controller)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(30),
						Version: int64(2),
					},
				},
			},
		},
	}
	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
				State: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						WorkflowId:       s.workflowID,
						NamespaceId:      namespaceID,
						VersionHistories: versionHistories,
						TransitionHistory: []*persistencespb.VersionedTransition{
							{NamespaceFailoverVersion: 1, TransitionCount: 10},
							{NamespaceFailoverVersion: 2, TransitionCount: 20},
						},
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId: s.runID,
					},
				},
			},
		},
	}
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmEntity(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.ArchetypeAny,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 13}, // local transition is stale
		},
	}).AnyTimes()
	mockMutableState.EXPECT().GetPendingChildIds().Return(nil).Times(1)
	mockMutableState.EXPECT().ApplySnapshot(versionedTransitionArtifact.GetSyncWorkflowStateSnapshotAttributes().State)
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()
	mockTransactionManager.EXPECT().UpdateWorkflow(gomock.Any(), true, gomock.Any(), nil).Return(nil).Times(1)
	mockTaskRefresher.EXPECT().Refresh(gomock.Any(), mockMutableState).Return(nil).Times(1)

	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), versionedTransitionArtifact, "test")
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_SameBranch_SyncMutation() {
	workflowStateReplicator := NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		nil,
		serialization.NewSerializer(),
		s.logger,
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.New()
	s.workflowStateReplicator.transactionMgr = NewMockTransactionManager(s.controller)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(30),
						Version: int64(2),
					},
				},
			},
		},
	}
	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				StateMutation: &persistencespb.WorkflowMutableStateMutation{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						WorkflowId:       s.workflowID,
						NamespaceId:      namespaceID,
						VersionHistories: versionHistories,
						TransitionHistory: []*persistencespb.VersionedTransition{
							{NamespaceFailoverVersion: 1, TransitionCount: 10},
							{NamespaceFailoverVersion: 2, TransitionCount: 20},
						},
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId: s.runID,
					},
				},
				ExclusiveStartVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 1, TransitionCount: 10,
				},
			},
		},
	}
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmEntity(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.ArchetypeAny,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 10},
			{NamespaceFailoverVersion: 2, TransitionCount: 18},
		},
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()
	mockMutableState.EXPECT().GetPendingChildIds().Return(nil).Times(1)
	mockMutableState.EXPECT().ApplyMutation(versionedTransitionArtifact.GetSyncWorkflowStateMutationAttributes().StateMutation)
	mockTransactionManager.EXPECT().UpdateWorkflow(gomock.Any(), false, gomock.Any(), nil).Return(nil).Times(1)
	mockTaskRefresher.EXPECT().
		PartialRefresh(gomock.Any(), gomock.Any(), EqVersionedTransition(&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 2,
			TransitionCount:          19,
		}), nil,
		).Return(nil).Times(1)

	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), versionedTransitionArtifact, "test")
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_FirstTask_SyncMutation() {
	workflowStateReplicator := NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		nil,
		serialization.NewSerializer(),
		s.logger,
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.New()
	s.workflowStateReplicator.transactionMgr = NewMockTransactionManager(s.controller)
	versionHistories := versionhistory.NewVersionHistories(&historyspb.VersionHistory{})
	transitionHistory := []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 2, TransitionCount: 10},
	}
	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				StateMutation: &persistencespb.WorkflowMutableStateMutation{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						WorkflowId:        s.workflowID,
						NamespaceId:       namespaceID,
						TransitionHistory: transitionHistory,
						VersionHistories:  versionHistories,
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId:  s.runID,
						State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
						Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					},
				},
				ExclusiveStartVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 2, TransitionCount: 0,
				},
			},
		},
		IsFirstSync: true,
	}
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmEntity(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.ArchetypeAny,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{},
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	mockTaskRefresher.EXPECT().Refresh(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	mockTransactionManager.EXPECT().CreateWorkflow(
		gomock.Any(),
		gomock.AssignableToTypeOf(&WorkflowImpl{}),
	).DoAndReturn(func(ctx context.Context, wf Workflow) error {
		// Capture localMutableState from the workflow
		localMutableState := wf.GetMutableState()

		// Perform your comparisons here
		s.Equal(localMutableState.GetExecutionInfo().TransitionHistory, transitionHistory)

		return nil
	}).Times(1)
	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), versionedTransitionArtifact, "test")
	s.NoError(err)

}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_MutationProvidedWithGap_ReturnSyncStateError() {
	workflowStateReplicator := NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		nil,
		serialization.NewSerializer(),
		s.logger,
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.New()
	s.workflowStateReplicator.transactionMgr = NewMockTransactionManager(s.controller)
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(30),
						Version: int64(2),
					},
				},
			},
		},
	}
	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				StateMutation: &persistencespb.WorkflowMutableStateMutation{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						WorkflowId:       s.workflowID,
						NamespaceId:      namespaceID,
						VersionHistories: versionHistories,
						TransitionHistory: []*persistencespb.VersionedTransition{
							{NamespaceFailoverVersion: 1, TransitionCount: 10},
							{NamespaceFailoverVersion: 2, TransitionCount: 20},
						},
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId: s.runID,
					},
				},
				ExclusiveStartVersionedTransition: &persistencespb.VersionedTransition{
					NamespaceFailoverVersion: 2, TransitionCount: 15,
				},
			},
		},
	}
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmEntity(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.ArchetypeAny,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 10},
			{NamespaceFailoverVersion: 2, TransitionCount: 13},
		},
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), versionedTransitionArtifact, "test")
	s.IsType(&serviceerrors.SyncState{}, err)
}

type historyEventMatcher struct {
	expected *historypb.HistoryEvent
}

func (m *historyEventMatcher) Matches(x interface{}) bool {
	evt, ok := x.(*historypb.HistoryEvent)
	return ok && proto.Equal(evt, m.expected)
}

func (m *historyEventMatcher) String() string {
	return fmt.Sprintf("is equal to %v", m.expected)
}

func (s *workflowReplicatorSuite) Test_bringLocalEventsUpToSourceCurrentBranch_WithGapAndTailEvents() {
	namespaceID := uuid.New()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(30),
						Version: int64(2),
					},
				},
			},
		},
	}
	localVersionHistoryies := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("local-branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(20),
						Version: int64(2),
					},
				},
			},
			{
				BranchToken: []byte("branchToken2"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(20),
						Version: int64(1),
					},
				},
			},
		},
	}
	serializer := serialization.NewSerializer()
	gapEvents := []*historypb.HistoryEvent{
		{EventId: 21, Version: 2}, {EventId: 22, Version: 2},
		{EventId: 23, Version: 2}, {EventId: 24, Version: 2},
	}
	requestedEvents := []*historypb.HistoryEvent{
		{EventId: 25, Version: 2}, {EventId: 26, Version: 2},
	}
	tailEvents := []*historypb.HistoryEvent{
		{EventId: 27, Version: 2}, {EventId: 28, Version: 2},
		{EventId: 29, Version: 2}, {EventId: 30, Version: 2},
	}
	blobs, err := serializer.SerializeEvents(requestedEvents)
	s.NoError(err)
	gapBlobs, err := serializer.SerializeEvents(gapEvents)
	s.NoError(err)
	tailBlobs, err := serializer.SerializeEvents(tailEvents)
	s.NoError(err)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistoryies,
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 10},
			{NamespaceFailoverVersion: 2, TransitionCount: 13},
		},
		ExecutionStats: &persistencespb.ExecutionStats{
			HistorySize: 100,
		},
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()
	mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(namespaceID, s.workflowID, s.runID)).AnyTimes()
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)

	allEvents := append(gapEvents, requestedEvents...)
	allEvents = append(allEvents, tailEvents...)
	for _, event := range allEvents {
		mockMutableState.EXPECT().AddReapplyCandidateEvent(&historyEventMatcher{expected: event}).
			Times(1)
	}

	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	sourceClusterName := "test-cluster"
	mockShard := historyi.NewMockShardContext(s.controller)
	taskId1 := int64(46)
	taskId2 := int64(47)
	taskId3 := int64(48)
	mockShard.EXPECT().GenerateTaskID().Return(taskId1, nil).Times(1)
	mockShard.EXPECT().GenerateTaskID().Return(taskId2, nil).Times(1)
	mockShard.EXPECT().GenerateTaskID().Return(taskId3, nil).Times(1)
	mockShard.EXPECT().GetRemoteAdminClient(sourceClusterName).Return(s.mockRemoteAdminClient, nil).AnyTimes()
	mockShard.EXPECT().GetShardID().Return(int32(0)).AnyTimes()
	s.workflowStateReplicator.shardContext = mockShard
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId:       namespaceID,
		Execution:         &commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: s.runID},
		StartEventId:      20,
		StartEventVersion: 2,
		EndEventId:        25,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{gapBlobs},
		VersionHistory: versionHistories.Histories[0],
		HistoryNodeIds: []int64{21},
	}, nil)
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId:       namespaceID,
		Execution:         &commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: s.runID},
		StartEventId:      26,
		StartEventVersion: 2,
		EndEventId:        31,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{tailBlobs},
		VersionHistory: versionHistories.Histories[0],
		HistoryNodeIds: []int64{27},
	}, nil)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistoryies.Histories[0].BranchToken,
		History:           gapBlobs,
		PrevTransactionID: 0,
		TransactionID:     taskId1,
		NodeID:            21,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, s.workflowID, s.runID),
	}).Return(nil, nil).Times(1)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistoryies.Histories[0].BranchToken,
		History:           blobs,
		PrevTransactionID: taskId1,
		TransactionID:     taskId2,
		NodeID:            25,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, s.workflowID, s.runID),
	}).Return(nil, nil).Times(1)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistoryies.Histories[0].BranchToken,
		History:           tailBlobs,
		PrevTransactionID: taskId2,
		TransactionID:     taskId3,
		NodeID:            27,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, s.workflowID, s.runID),
	}).Return(nil, nil).Times(1)
	_, err = s.workflowStateReplicator.bringLocalEventsUpToSourceCurrentBranch(
		context.Background(),
		namespace.ID(namespaceID),
		s.workflowID,
		s.runID,
		sourceClusterName,
		mockWeCtx,
		mockMutableState,
		versionHistories,
		[]*commonpb.DataBlob{blobs},
		false)
	s.NoError(err)
	s.Equal(versionHistories.Histories[0].Items, mockMutableState.GetExecutionInfo().VersionHistories.Histories[0].Items)
}

func (s *workflowReplicatorSuite) Test_bringLocalEventsUpToSourceCurrentBranch_WithGapAndTailEvents_NewMutableState() {
	namespaceID := uuid.New()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(3),
						Version: int64(1),
					},
					{
						EventId: int64(6),
						Version: int64(2),
					},
				},
			},
		},
	}
	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("local-branchToken1"),
			},
		},
	}
	serializer := serialization.NewSerializer()
	gapEvents := []*historypb.HistoryEvent{
		{EventId: 1, Version: 1}, {EventId: 2, Version: 1}, {EventId: 3, Version: 1},
	}
	requestedEvents := []*historypb.HistoryEvent{
		{EventId: 4, Version: 2},
	}
	tailEvents := []*historypb.HistoryEvent{
		{EventId: 5, Version: 2}, {EventId: 6, Version: 2},
	}
	blobs, err := serializer.SerializeEvents(requestedEvents)
	s.NoError(err)
	gapBlobs, err := serializer.SerializeEvents(gapEvents)
	s.NoError(err)
	tailBlobs, err := serializer.SerializeEvents(tailEvents)
	s.NoError(err)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
		ExecutionStats:   &persistencespb.ExecutionStats{HistorySize: 0},
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()
	mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(namespaceID, s.workflowID, s.runID)).AnyTimes()
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)

	allEvents := append(gapEvents, requestedEvents...)
	allEvents = append(allEvents, tailEvents...)
	for _, event := range allEvents {
		mockMutableState.EXPECT().AddReapplyCandidateEvent(&historyEventMatcher{expected: event}).
			Times(1)
	}

	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	sourceClusterName := "test-cluster"
	mockShard := historyi.NewMockShardContext(s.controller)
	taskId1 := int64(46)
	taskId2 := int64(47)
	taskId3 := int64(48)
	mockShard.EXPECT().GenerateTaskID().Return(taskId1, nil).Times(1)
	mockShard.EXPECT().GenerateTaskID().Return(taskId2, nil).Times(1)
	mockShard.EXPECT().GenerateTaskID().Return(taskId3, nil).Times(1)
	mockShard.EXPECT().GetRemoteAdminClient(sourceClusterName).Return(s.mockRemoteAdminClient, nil).AnyTimes()
	mockShard.EXPECT().GetShardID().Return(int32(0)).AnyTimes()
	s.workflowStateReplicator.shardContext = mockShard
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId:       namespaceID,
		Execution:         &commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: s.runID},
		StartEventId:      0,
		StartEventVersion: 0,
		EndEventId:        4,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{gapBlobs},
		VersionHistory: versionHistories.Histories[0],
		HistoryNodeIds: []int64{1},
	}, nil)
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId:       namespaceID,
		Execution:         &commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: s.runID},
		StartEventId:      4,
		StartEventVersion: 2,
		EndEventId:        7,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{tailBlobs},
		VersionHistory: versionHistories.Histories[0],
		HistoryNodeIds: []int64{5},
	}, nil)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       true,
		BranchToken:       localVersionHistories.Histories[0].BranchToken,
		History:           gapBlobs,
		PrevTransactionID: 0,
		TransactionID:     taskId1,
		NodeID:            1,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, s.workflowID, s.runID),
	}).Return(nil, nil).Times(1)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistories.Histories[0].BranchToken,
		History:           blobs,
		PrevTransactionID: taskId1,
		TransactionID:     taskId2,
		NodeID:            4,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, s.workflowID, s.runID),
	}).Return(nil, nil).Times(1)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistories.Histories[0].BranchToken,
		History:           tailBlobs,
		PrevTransactionID: taskId2,
		TransactionID:     taskId3,
		NodeID:            5,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, s.workflowID, s.runID),
	}).Return(nil, nil).Times(1)
	_, err = s.workflowStateReplicator.bringLocalEventsUpToSourceCurrentBranch(
		context.Background(),
		namespace.ID(namespaceID),
		s.workflowID,
		s.runID,
		sourceClusterName,
		mockWeCtx,
		mockMutableState,
		versionHistories,
		[]*commonpb.DataBlob{blobs},
		true)
	s.NoError(err)
	s.Equal(versionHistories.Histories[0].Items, mockMutableState.GetExecutionInfo().VersionHistories.Histories[0].Items)
}

func (s *workflowReplicatorSuite) Test_bringLocalEventsUpToSourceCurrentBranch_WithGapAndTailEvents_NotConsecutive() {
	namespaceID := uuid.New()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(30),
						Version: int64(2),
					},
				},
			},
		},
	}
	localVersionHistoryies := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("local-branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(19),
						Version: int64(1),
					},
					{
						EventId: int64(20),
						Version: int64(2),
					},
				},
			},
			{
				BranchToken: []byte("branchToken2"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(20),
						Version: int64(1),
					},
				},
			},
		},
	}
	serializer := serialization.NewSerializer()
	gapEvents := []*historypb.HistoryEvent{
		{EventId: 21, Version: 2}, {EventId: 22, Version: 2},
		{EventId: 23, Version: 2}, {EventId: 24, Version: 2},
	}
	requestedEvents := []*historypb.HistoryEvent{
		{EventId: 25, Version: 2}, {EventId: 26, Version: 2},
	}
	tailEvents := []*historypb.HistoryEvent{
		{EventId: 28, Version: 2},
		{EventId: 29, Version: 2}, {EventId: 30, Version: 2},
	}
	blobs, err := serializer.SerializeEvents(requestedEvents)
	s.NoError(err)
	gapBlobs, err := serializer.SerializeEvents(gapEvents)
	s.NoError(err)
	tailBlobs, err := serializer.SerializeEvents(tailEvents)
	s.NoError(err)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistoryies,
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 10},
			{NamespaceFailoverVersion: 2, TransitionCount: 13},
		},
		ExecutionStats: &persistencespb.ExecutionStats{
			HistorySize: 100,
		},
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()
	mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(namespaceID, s.workflowID, s.runID)).AnyTimes()

	allEvents := append(gapEvents, requestedEvents...)
	for _, event := range allEvents {
		mockMutableState.EXPECT().AddReapplyCandidateEvent(&historyEventMatcher{expected: event}).
			Times(1)
	}

	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	sourceClusterName := "test-cluster"
	mockShard := historyi.NewMockShardContext(s.controller)
	taskId1 := int64(46)
	taskId2 := int64(47)
	taskId3 := int64(48)
	mockShard.EXPECT().GenerateTaskID().Return(taskId1, nil).Times(1)
	mockShard.EXPECT().GenerateTaskID().Return(taskId2, nil).Times(1)
	mockShard.EXPECT().GenerateTaskID().Return(taskId3, nil).Times(1)
	mockShard.EXPECT().GetRemoteAdminClient(sourceClusterName).Return(s.mockRemoteAdminClient, nil).AnyTimes()
	mockShard.EXPECT().GetShardID().Return(int32(0)).AnyTimes()
	s.workflowStateReplicator.shardContext = mockShard
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId:       namespaceID,
		Execution:         &commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: s.runID},
		StartEventId:      20,
		StartEventVersion: 2,
		EndEventId:        25,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{gapBlobs},
		VersionHistory: versionHistories.Histories[0],
		HistoryNodeIds: []int64{21},
	}, nil)
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId:       namespaceID,
		Execution:         &commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: s.runID},
		StartEventId:      26,
		StartEventVersion: 2,
		EndEventId:        31,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}).Return(&adminservice.GetWorkflowExecutionRawHistoryV2Response{
		HistoryBatches: []*commonpb.DataBlob{tailBlobs},
		VersionHistory: versionHistories.Histories[0],
		HistoryNodeIds: []int64{27},
	}, nil)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistoryies.Histories[0].BranchToken,
		History:           gapBlobs,
		PrevTransactionID: 0,
		TransactionID:     taskId1,
		NodeID:            21,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, s.workflowID, s.runID),
	}).Return(nil, nil).Times(1)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistoryies.Histories[0].BranchToken,
		History:           blobs,
		PrevTransactionID: taskId1,
		TransactionID:     taskId2,
		NodeID:            25,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, s.workflowID, s.runID),
	}).Return(nil, nil).Times(1)
	_, err = s.workflowStateReplicator.bringLocalEventsUpToSourceCurrentBranch(
		context.Background(),
		namespace.ID(namespaceID),
		s.workflowID,
		s.runID,
		sourceClusterName,
		mockWeCtx,
		mockMutableState,
		versionHistories,
		[]*commonpb.DataBlob{blobs},
		false)
	s.ErrorIs(err, ErrEventSlicesNotConsecutive)
}

func (s *workflowReplicatorSuite) Test_bringLocalEventsUpToSourceCurrentBranch_CreateNewBranch() {
	namespaceID := uuid.New()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(30),
						Version: int64(1),
					},
				},
			},
		},
	}
	localVersionHistoryies := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("local-branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(40),
						Version: int64(1),
					},
				},
			},
			{
				BranchToken: []byte("branchToken2"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(20),
						Version: int64(1),
					},
				},
			},
		},
	}

	mockMutableState := historyi.NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: localVersionHistoryies,
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 10},
			{NamespaceFailoverVersion: 2, TransitionCount: 13},
		},
		ExecutionStats: &persistencespb.ExecutionStats{
			HistorySize: 100,
		},
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()
	sourceClusterName := "test-cluster"

	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	forkedBranchToken := []byte("forked-branchToken")
	s.mockExecutionManager.EXPECT().ForkHistoryBranch(gomock.Any(), &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: localVersionHistoryies.Histories[0].BranchToken,
		ForkNodeID:      31,
		NamespaceID:     namespaceID,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(namespaceID, s.workflowID, s.runID),
		ShardID:         s.mockShard.GetShardID(),
		NewRunID:        s.runID,
	}).Return(&persistence.ForkHistoryBranchResponse{
		NewBranchToken: forkedBranchToken,
	}, nil)
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)
	newRunBranch, err := s.workflowStateReplicator.bringLocalEventsUpToSourceCurrentBranch(
		context.Background(),
		namespace.ID(namespaceID),
		s.workflowID,
		s.runID,
		sourceClusterName,
		mockWeCtx,
		mockMutableState,
		versionHistories,
		nil,
		false)
	s.NoError(err)

	s.Equal(forkedBranchToken, localVersionHistoryies.Histories[2].BranchToken)
	s.Equal(int32(2), localVersionHistoryies.CurrentVersionHistoryIndex)
	s.NotNil(newRunBranch)
}
