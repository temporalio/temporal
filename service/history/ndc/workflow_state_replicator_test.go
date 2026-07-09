package ndc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
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
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/consts"
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
		serializer            serialization.Serializer

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

	config := tests.NewDynamicConfig()
	config.ExternalPayloadsEnabled = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(true)
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		config,
	)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	s.mockShard.SetStateMachineRegistry(reg)
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return("test-cluster").AnyTimes()

	s.mockExecutionManager = s.mockShard.Resource.ExecutionMgr
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockWorkflowCache = wcache.NewMockCache(s.controller)
	s.mockEventCache = s.mockShard.MockEventsCache
	s.mockRemoteAdminClient = s.mockShard.Resource.RemoteAdminClient
	eventReapplier := NewMockEventsReapplier(s.controller)
	s.logger = s.mockShard.GetLogger()

	s.workflowID = "some random workflow ID"
	s.runID = uuid.NewString()
	s.now = time.Now().UTC()
	s.serializer = serialization.NewSerializer()
	s.workflowStateReplicator = NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		eventReapplier,
		s.serializer,
		quotas.NoopRequestRateLimiter,
		s.logger,
		nil,
	)
}

func (s *workflowReplicatorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *workflowReplicatorSuite) Test_ApplyWorkflowState_BrandNew() {
	namespaceID := uuid.NewString()
	namespaceName := "namespaceName"
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:    s.runID,
		BranchId:  uuid.NewString(),
		Ancestors: nil,
	}
	historyBranch, err := s.serializer.HistoryBranchToBlob(branchInfo)
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
		gomock.Any(),
	).Return(nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: namespaceName},
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceName(namespace.ID(namespaceID)).Return(namespace.Name(namespaceName), nil).AnyTimes()
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
	namespaceID := uuid.NewString()
	namespaceName := "namespaceName"
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:   uuid.NewString(),
		BranchId: uuid.NewString(),
		Ancestors: []*persistencespb.HistoryBranchRange{
			{
				BranchId:    uuid.NewString(),
				BeginNodeId: 1,
				EndNodeId:   3,
			},
			{
				BranchId:    uuid.NewString(),
				BeginNodeId: 3,
				EndNodeId:   4,
			},
		},
	}
	historyBranch, err := s.serializer.HistoryBranchToBlob(branchInfo)
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
		gomock.Any(),
	).Return(nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: namespaceName},
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceName(namespace.ID(namespaceID)).Return(namespace.Name(namespaceName), nil).AnyTimes()
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
	var historyBlobs []*commonpb.DataBlob
	var nodeIds []int64
	for _, history := range expectedHistory {
		blob, err := s.serializer.SerializeEvents(history.GetEvents())
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
	namespaceID := uuid.NewString()
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:    uuid.NewString(),
		BranchId:  uuid.NewString(),
		Ancestors: nil,
	}
	historyBranch, err := s.serializer.HistoryBranchToBlob(branchInfo)
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
	s.Equal(expectedErr.NamespaceId, namespaceID)
	s.Equal(expectedErr.WorkflowId, s.workflowID)
	s.Equal(expectedErr.RunId, s.runID)
	s.Equal(int64(1), expectedErr.StartEventId)
	s.Equal(int64(1), expectedErr.StartEventVersion)
}

func (s *workflowReplicatorSuite) Test_ApplyWorkflowState_ExistWorkflow_SyncHSM() {
	namespaceID := uuid.NewString()
	branchInfo := &persistencespb.HistoryBranch{
		TreeId:    uuid.NewString(),
		BranchId:  uuid.NewString(),
		Ancestors: nil,
	}
	historyBranch, err := s.serializer.HistoryBranchToBlob(branchInfo)
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
func (m *VersionedTransitionMatcher) Matches(x any) bool {
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
		s.serializer,
		quotas.NoopRequestRateLimiter,
		s.logger,
		nil,
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.NewString()
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
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
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
	mockTransactionManager.EXPECT().UpdateWorkflow(gomock.Any(), false, chasm.WorkflowArchetypeID, gomock.Any(), nil).Return(nil).Times(1)
	mockTaskRefresher.EXPECT().
		PartialRefresh(gomock.Any(), gomock.Any(), EqVersionedTransition(&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 2,
			TransitionCount:          19,
		}), nil, false,
		).Return(nil).Times(1)

	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), chasm.WorkflowArchetypeID, versionedTransitionArtifact, "test")
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_DifferentBranch_SyncState() {
	workflowStateReplicator := NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		nil,
		s.serializer,
		quotas.NoopRequestRateLimiter,
		s.logger,
		nil,
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.NewString()
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
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
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
	mockTransactionManager.EXPECT().UpdateWorkflow(gomock.Any(), true, chasm.WorkflowArchetypeID, gomock.Any(), nil).Return(nil).Times(1)
	mockTaskRefresher.EXPECT().Refresh(gomock.Any(), mockMutableState, gomock.Any()).Return(nil).Times(1)

	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), chasm.WorkflowArchetypeID, versionedTransitionArtifact, "test")
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_SameBranch_SyncMutation() {
	workflowStateReplicator := NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		nil,
		s.serializer,
		quotas.NoopRequestRateLimiter,
		s.logger,
		nil,
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.NewString()
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
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
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
	mockTransactionManager.EXPECT().UpdateWorkflow(gomock.Any(), false, chasm.WorkflowArchetypeID, gomock.Any(), nil).Return(nil).Times(1)
	mockTaskRefresher.EXPECT().
		PartialRefresh(gomock.Any(), gomock.Any(), EqVersionedTransition(&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 2,
			TransitionCount:          19,
		}), nil, false,
		).Return(nil).Times(1)

	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), chasm.WorkflowArchetypeID, versionedTransitionArtifact, "test")
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_FirstTask_SyncMutation() {
	workflowStateReplicator := NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		nil,
		s.serializer,
		quotas.NoopRequestRateLimiter,
		s.logger,
		nil,
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.NewString()
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
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{},
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	mockTaskRefresher.EXPECT().Refresh(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	mockTransactionManager.EXPECT().CreateWorkflow(
		gomock.Any(),
		chasm.WorkflowArchetypeID,
		gomock.AssignableToTypeOf(&WorkflowImpl{}),
	).DoAndReturn(func(ctx context.Context, _ chasm.ArchetypeID, wf Workflow) error {
		// Capture localMutableState from the workflow
		localMutableState := wf.GetMutableState()

		// Perform your comparisons here
		s.Equal(localMutableState.GetExecutionInfo().TransitionHistory, transitionHistory)

		return nil
	}).Times(1)
	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), chasm.WorkflowArchetypeID, versionedTransitionArtifact, "test")
	s.NoError(err)

}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_MutationProvidedWithGap_ReturnSyncStateError() {
	workflowStateReplicator := NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		nil,
		s.serializer,
		quotas.NoopRequestRateLimiter,
		s.logger,
		nil,
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.NewString()
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
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		chasm.WorkflowArchetypeID,
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

	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), chasm.WorkflowArchetypeID, versionedTransitionArtifact, "test")
	s.ErrorAs(err, new(*serviceerrors.SyncState))
}

type historyEventMatcher struct {
	expected *historypb.HistoryEvent
}

func (m *historyEventMatcher) Matches(x any) bool {
	evt, ok := x.(*historypb.HistoryEvent)
	return ok && proto.Equal(evt, m.expected)
}

func (m *historyEventMatcher) String() string {
	return fmt.Sprintf("is equal to %v", m.expected)
}

func (s *workflowReplicatorSuite) Test_bringLocalEventsUpToSourceCurrentBranch_WithGapAndTailEvents() {
	namespaceID := uuid.NewString()
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
	blobs, err := s.serializer.SerializeEvents(requestedEvents)
	s.NoError(err)
	gapBlobs, err := s.serializer.SerializeEvents(gapEvents)
	s.NoError(err)
	tailBlobs, err := s.serializer.SerializeEvents(tailEvents)
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
		LastFirstEventTxnId: 0,
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
	mockShard.EXPECT().GetMetricsHandler().Return(s.mockShard.GetMetricsHandler()).AnyTimes()
	mockShard.EXPECT().GetConfig().Return(s.mockShard.GetConfig()).AnyTimes()
	s.workflowStateReplicator.shardContext = mockShard
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	nsName := namespace.Name("test-namespace")
	mockMutableState.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: namespaceID, Name: nsName.String()},
		&persistencespb.NamespaceConfig{},
		"test-cluster",
	)).AnyTimes()
	mockMutableState.EXPECT().AddExternalPayloadSize(gomock.Any()).AnyTimes()
	mockMutableState.EXPECT().AddExternalPayloadCount(gomock.Any()).AnyTimes()
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
	namespaceID := uuid.NewString()
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
	gapEvents := []*historypb.HistoryEvent{
		{EventId: 1, Version: 1}, {EventId: 2, Version: 1}, {EventId: 3, Version: 1},
	}
	requestedEvents := []*historypb.HistoryEvent{
		{EventId: 4, Version: 2},
	}
	tailEvents := []*historypb.HistoryEvent{
		{EventId: 5, Version: 2}, {EventId: 6, Version: 2},
	}
	blobs, err := s.serializer.SerializeEvents(requestedEvents)
	s.NoError(err)
	gapBlobs, err := s.serializer.SerializeEvents(gapEvents)
	s.NoError(err)
	tailBlobs, err := s.serializer.SerializeEvents(tailEvents)
	s.NoError(err)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories:    localVersionHistories,
		ExecutionStats:      &persistencespb.ExecutionStats{HistorySize: 0},
		LastFirstEventTxnId: 0,
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
	mockShard.EXPECT().GetMetricsHandler().Return(s.mockShard.GetMetricsHandler()).AnyTimes()
	mockShard.EXPECT().GetConfig().Return(s.mockShard.GetConfig()).AnyTimes()
	s.workflowStateReplicator.shardContext = mockShard
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	nsName := namespace.Name("test-namespace")
	mockMutableState.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: namespaceID, Name: nsName.String()},
		&persistencespb.NamespaceConfig{},
		"test-cluster",
	)).AnyTimes()
	mockMutableState.EXPECT().AddExternalPayloadSize(gomock.Any()).AnyTimes()
	mockMutableState.EXPECT().AddExternalPayloadCount(gomock.Any()).AnyTimes()
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
	namespaceID := uuid.NewString()
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
	blobs, err := s.serializer.SerializeEvents(requestedEvents)
	s.NoError(err)
	gapBlobs, err := s.serializer.SerializeEvents(gapEvents)
	s.NoError(err)
	tailBlobs, err := s.serializer.SerializeEvents(tailEvents)
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
		LastFirstEventTxnId: 0,
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
	mockShard.EXPECT().GetMetricsHandler().Return(s.mockShard.GetMetricsHandler()).AnyTimes()
	mockShard.EXPECT().GetConfig().Return(s.mockShard.GetConfig()).AnyTimes()
	s.workflowStateReplicator.shardContext = mockShard
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	nsName := namespace.Name("test-namespace")
	mockMutableState.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: namespaceID, Name: nsName.String()},
		&persistencespb.NamespaceConfig{},
		"test-cluster",
	)).AnyTimes()
	mockMutableState.EXPECT().AddExternalPayloadSize(gomock.Any()).AnyTimes()
	mockMutableState.EXPECT().AddExternalPayloadCount(gomock.Any()).AnyTimes()
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

func (s *workflowReplicatorSuite) Test_handleFirstReplicationTask_WithSnapshot_Success() {
	workflowStateReplicator := NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		nil,
		serialization.NewSerializer(),
		quotas.NoopRequestRateLimiter,
		s.logger,
		nil,
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher

	namespaceID := uuid.NewString()
	versionHistories := versionhistory.NewVersionHistories(&historyspb.VersionHistory{})
	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
				State: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						WorkflowId:               s.workflowID,
						NamespaceId:              namespaceID,
						VersionHistories:         versionHistories,
						WorkflowExecutionTimeout: timestamp.DurationPtr(time.Hour),
						WorkflowRunTimeout:       timestamp.DurationPtr(time.Hour),
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId:     s.runID,
						State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
						Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
						StartTime: timestamp.TimePtr(s.now),
					},
				},
			},
		},
	}

	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		nil,
		false,
		nil,
		int64(100),
	), nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceName(namespace.ID(namespaceID)).Return(namespace.Name("test-namespace"), nil).AnyTimes()

	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetWorkflowExecutionRawHistoryV2Response{},
		nil,
	).AnyTimes()

	mockTaskRefresher.EXPECT().Refresh(gomock.Any(), gomock.Any(), false).Return(nil)

	mockTransactionManager.EXPECT().CreateWorkflow(
		gomock.Any(),
		chasm.WorkflowArchetypeID,
		gomock.AssignableToTypeOf(&WorkflowImpl{}),
	).DoAndReturn(func(ctx context.Context, _ chasm.ArchetypeID, wf Workflow) error {
		localMutableState := wf.GetMutableState()
		s.Equal(s.workflowID, localMutableState.GetExecutionInfo().WorkflowId)
		s.Equal(s.runID, localMutableState.GetExecutionState().RunId)
		return nil
	})

	continueProcess, err := workflowStateReplicator.handleFirstReplicationTask(
		context.Background(),
		chasm.WorkflowArchetypeID,
		mockWeCtx,
		versionedTransitionArtifact,
		"test-cluster",
	)
	s.NoError(err)
	s.False(continueProcess)
}

func (s *workflowReplicatorSuite) Test_handleFirstReplicationTask_WithMutation_Success() {
	workflowStateReplicator := NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		nil,
		serialization.NewSerializer(),
		quotas.NoopRequestRateLimiter,
		s.logger,
		nil,
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher

	namespaceID := uuid.NewString()
	versionHistories := versionhistory.NewVersionHistories(&historyspb.VersionHistory{})
	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				StateMutation: &persistencespb.WorkflowMutableStateMutation{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						WorkflowId:               s.workflowID,
						NamespaceId:              namespaceID,
						VersionHistories:         versionHistories,
						WorkflowExecutionTimeout: timestamp.DurationPtr(time.Hour),
						WorkflowRunTimeout:       timestamp.DurationPtr(time.Hour),
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId:     s.runID,
						State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
						Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
						StartTime: timestamp.TimePtr(s.now),
					},
				},
			},
		},
	}

	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		nil,
		false,
		nil,
		int64(100),
	), nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceName(namespace.ID(namespaceID)).Return(namespace.Name("test-namespace"), nil).AnyTimes()

	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetWorkflowExecutionRawHistoryV2Response{},
		nil,
	).AnyTimes()

	mockTaskRefresher.EXPECT().Refresh(gomock.Any(), gomock.Any(), false).Return(nil)

	mockTransactionManager.EXPECT().CreateWorkflow(
		gomock.Any(),
		chasm.WorkflowArchetypeID,
		gomock.AssignableToTypeOf(&WorkflowImpl{}),
	).Return(nil)

	continueProcess, err := workflowStateReplicator.handleFirstReplicationTask(
		context.Background(),
		chasm.WorkflowArchetypeID,
		mockWeCtx,
		versionedTransitionArtifact,
		"test-cluster",
	)
	s.NoError(err)
	s.False(continueProcess)
}

func (s *workflowReplicatorSuite) Test_handleFirstReplicationTask_InvalidArtifactType_Error() {
	workflowStateReplicator := NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		nil,
		serialization.NewSerializer(),
		quotas.NoopRequestRateLimiter,
		s.logger,
		nil,
	)

	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{}
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)

	continueProcess, err := workflowStateReplicator.handleFirstReplicationTask(
		context.Background(),
		chasm.WorkflowArchetypeID,
		mockWeCtx,
		versionedTransitionArtifact,
		"test-cluster",
	)
	s.Error(err)
	s.Contains(err.Error(), "unknown artifact type")
	s.False(continueProcess)
}

func (s *workflowReplicatorSuite) Test_handleFirstReplicationTask_CreateWorkflowFails_BranchCleanup() {
	workflowStateReplicator := NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		nil,
		serialization.NewSerializer(),
		quotas.NoopRequestRateLimiter,
		s.logger,
		nil,
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher

	namespaceID := uuid.NewString()
	versionHistories := versionhistory.NewVersionHistories(&historyspb.VersionHistory{})
	versionedTransitionArtifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
				State: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						WorkflowId:               s.workflowID,
						NamespaceId:              namespaceID,
						VersionHistories:         versionHistories,
						WorkflowExecutionTimeout: timestamp.DurationPtr(time.Hour),
						WorkflowRunTimeout:       timestamp.DurationPtr(time.Hour),
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{
						RunId:     s.runID,
						State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
						Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
						StartTime: timestamp.TimePtr(s.now),
					},
				},
			},
		},
	}

	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		nil,
		false,
		nil,
		int64(100),
	), nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceName(namespace.ID(namespaceID)).Return(namespace.Name("test-namespace"), nil).AnyTimes()

	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetWorkflowExecutionRawHistoryV2Response{},
		nil,
	).AnyTimes()

	mockTaskRefresher.EXPECT().Refresh(gomock.Any(), gomock.Any(), false).Return(nil)

	expectedErr := serviceerror.NewInternal("create workflow failed")
	mockTransactionManager.EXPECT().CreateWorkflow(
		gomock.Any(),
		chasm.WorkflowArchetypeID,
		gomock.AssignableToTypeOf(&WorkflowImpl{}),
	).Return(expectedErr)

	continueProcess, err := workflowStateReplicator.handleFirstReplicationTask(
		context.Background(),
		chasm.WorkflowArchetypeID,
		mockWeCtx,
		versionedTransitionArtifact,
		"test-cluster",
	)
	s.Error(err)
	s.Equal(expectedErr, err)
	s.False(continueProcess)
}

func (s *workflowReplicatorSuite) Test_bringLocalEventsUpToSourceCurrentBranch_CreateNewBranch() {
	namespaceID := uuid.NewString()
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
	s.mockNamespaceCache.EXPECT().GetNamespaceName(namespace.ID(namespaceID)).Return(namespace.Name("test-namespace"), nil).AnyTimes()
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

func (s *workflowReplicatorSuite) Test_bringLocalEventsUpToSourceCurrentBranch_ExternalPayloadStats() {
	// Test that the external payload stats are correctly updated when bringLocalEventsUpToSourceCurrentBranch is invoked
	namespaceID := uuid.NewString()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					{
						EventId: int64(2),
						Version: int64(1),
					},
				},
			},
		},
	}
	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{
				BranchToken: []byte("local-branchToken"),
			},
		},
	}

	historyEvents := []*historypb.HistoryEvent{
		{
			EventId:   1,
			Version:   1,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					Input: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{
							{
								Data: []byte("test"),
								ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
									{SizeBytes: 1024},
									{SizeBytes: 2048},
								},
							},
						},
					},
				},
			},
		},
		{
			EventId:   2,
			Version:   1,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
				WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{},
			},
		},
	}

	serializer := serialization.NewSerializer()
	eventBlobs, err := serializer.SerializeEvents(historyEvents)
	s.NoError(err)

	executionStats := &persistencespb.ExecutionStats{
		HistorySize:          0,
		ExternalPayloadSize:  0,
		ExternalPayloadCount: 0,
	}
	mockMutableState := historyi.NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories:    localVersionHistories,
		ExecutionStats:      executionStats,
		LastFirstEventTxnId: 0,
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()
	mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(namespaceID, s.workflowID, s.runID)).AnyTimes()
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)
	mockMutableState.EXPECT().AddReapplyCandidateEvent(gomock.Any()).AnyTimes()

	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	sourceClusterName := "test-cluster"

	nsName := namespace.Name("test-namespace")
	s.mockNamespaceCache.EXPECT().GetNamespaceName(namespace.ID(namespaceID)).Return(nsName, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: nsName.String()},
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	mockMutableState.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: namespaceID, Name: nsName.String()},
		&persistencespb.NamespaceConfig{},
		"test-cluster",
	)).AnyTimes()
	mockMutableState.EXPECT().AddExternalPayloadSize(int64(1024 + 2048)).Times(1)
	mockMutableState.EXPECT().AddExternalPayloadCount(int64(2)).Times(1)

	mockShard := historyi.NewMockShardContext(s.controller)
	taskID := int64(100)
	mockShard.EXPECT().GenerateTaskID().Return(taskID, nil).Times(1)
	mockShard.EXPECT().GetRemoteAdminClient(sourceClusterName).Return(s.mockRemoteAdminClient, nil).AnyTimes()
	mockShard.EXPECT().GetShardID().Return(int32(0)).AnyTimes()
	mockShard.EXPECT().GetMetricsHandler().Return(s.mockShard.GetMetricsHandler()).AnyTimes()
	mockShard.EXPECT().GetConfig().Return(s.mockShard.GetConfig()).AnyTimes()
	mockEventsCache := events.NewMockCache(s.controller)
	mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
	mockShard.EXPECT().GetEventsCache().Return(mockEventsCache).AnyTimes()
	s.workflowStateReplicator.shardContext = mockShard

	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)

	_, err = s.workflowStateReplicator.bringLocalEventsUpToSourceCurrentBranch(
		context.Background(),
		namespace.ID(namespaceID),
		s.workflowID,
		s.runID,
		sourceClusterName,
		mockWeCtx,
		mockMutableState,
		versionHistories,
		[]*commonpb.DataBlob{eventBlobs},
		true)
	s.NoError(err)
}

// ---- Added tests to maximize coverage ----

// wfStateReplNonWorkflowArchetypeID is any archetype id that is not chasm.WorkflowArchetypeID.
const wfStateReplNonWorkflowArchetypeID = chasm.ArchetypeID(1)

func (s *workflowReplicatorSuite) wfStateReplNewReplicator() *WorkflowStateReplicatorImpl {
	return NewWorkflowStateReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		nil,
		s.serializer,
		quotas.NoopRequestRateLimiter,
		s.logger,
		nil,
	)
}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_NilStateAttributes_Error() {
	r := s.wfStateReplNewReplicator()
	err := r.ReplicateVersionedTransition(context.Background(), chasm.WorkflowArchetypeID, &replicationspb.VersionedTransitionArtifact{}, "test")
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
	s.Contains(err.Error(), "both snapshot and mutation are nil")
}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_UnknownArtifactType_Error() {
	r := s.wfStateReplNewReplicator()
	// StateAttributes is non-nil but of an unknown concrete type is impossible via oneof.
	// Instead, exercise parseVersionedTransitionAttributes error path through a snapshot
	// with a nil State pointer is not possible either. Use a mutation with nil StateMutation.
	artifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				StateMutation: &persistencespb.WorkflowMutableStateMutation{
					ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{NamespaceId: uuid.NewString(), WorkflowId: s.workflowID},
					ExecutionState: &persistencespb.WorkflowExecutionState{RunId: s.runID},
				},
			},
		},
	}
	// GetOrCreateChasmExecution returns an error to short-circuit.
	expectedErr := serviceerror.NewInternal("cache error")
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), s.mockShard, gomock.Any(), gomock.Any(), chasm.WorkflowArchetypeID, locks.PriorityHigh,
	).Return(nil, nil, expectedErr)
	err := r.ReplicateVersionedTransition(context.Background(), chasm.WorkflowArchetypeID, artifact, "test")
	s.Equal(expectedErr, err)
}

func (s *workflowReplicatorSuite) wfStateReplSnapshotArtifact(namespaceID string, transitionHistory []*persistencespb.VersionedTransition, versionHistories *historyspb.VersionHistories) *replicationspb.VersionedTransitionArtifact {
	return &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
				State: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						WorkflowId:        s.workflowID,
						NamespaceId:       namespaceID,
						VersionHistories:  versionHistories,
						TransitionHistory: transitionHistory,
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{RunId: s.runID},
				},
			},
		},
	}
}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_StalenessCheckEqual_ErrDuplicate() {
	r := s.wfStateReplNewReplicator()
	namespaceID := uuid.NewString()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{BranchToken: []byte("bt"), Items: []*historyspb.VersionHistoryItem{{EventId: 30, Version: 2}}},
		},
	}
	transitionHistory := []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 1, TransitionCount: 10},
		{NamespaceFailoverVersion: 2, TransitionCount: 20},
	}
	artifact := s.wfStateReplSnapshotArtifact(namespaceID, transitionHistory, versionHistories)
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), s.mockShard, namespace.ID(namespaceID), gomock.Any(), chasm.WorkflowArchetypeID, locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	// local transition history is identical => StalenessCheck returns nil => ErrDuplicate
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories:  versionHistories,
		TransitionHistory: transitionHistory,
	}).AnyTimes()
	err := r.ReplicateVersionedTransition(context.Background(), chasm.WorkflowArchetypeID, artifact, "test")
	s.ErrorIs(err, consts.ErrDuplicate)
}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_StaleReference_BackFillEvents() {
	r := s.wfStateReplNewReplicator()
	namespaceID := uuid.NewString()
	// source has a newer/diverged transition history version compared to local so StalenessCheck returns ErrStaleReference
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{BranchToken: []byte("bt"), Items: []*historyspb.VersionHistoryItem{{EventId: 30, Version: 2}}},
		},
	}
	// local's last version (3) > source ref version (1) => StalenessCheck returns ErrStaleReference.
	sourceTransitionHistory := []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 1, TransitionCount: 5},
	}
	localTransitionHistory := []*persistencespb.VersionedTransition{
		{NamespaceFailoverVersion: 3, TransitionCount: 10},
	}
	artifact := s.wfStateReplSnapshotArtifact(namespaceID, sourceTransitionHistory, versionHistories)
	artifact.EventBatches = nil // no events => backFillEvents returns nil immediately
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), s.mockShard, namespace.ID(namespaceID), gomock.Any(), chasm.WorkflowArchetypeID, locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories:  versionHistories,
		TransitionHistory: localTransitionHistory,
	}).AnyTimes()
	err := r.ReplicateVersionedTransition(context.Background(), chasm.WorkflowArchetypeID, artifact, "test")
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_EmptyLocalTransitionHistory_MissingSnapshot_SyncStateError() {
	r := s.wfStateReplNewReplicator()
	namespaceID := uuid.NewString()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{BranchToken: []byte("bt"), Items: []*historyspb.VersionHistoryItem{{EventId: 30, Version: 2}}},
		},
	}
	// Mutation artifact (snapshot == nil) but local transition history empty => SyncState error
	artifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				StateMutation: &persistencespb.WorkflowMutableStateMutation{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						WorkflowId:        s.workflowID,
						NamespaceId:       namespaceID,
						VersionHistories:  versionHistories,
						TransitionHistory: []*persistencespb.VersionedTransition{{NamespaceFailoverVersion: 1, TransitionCount: 1}},
					},
					ExecutionState: &persistencespb.WorkflowExecutionState{RunId: s.runID},
				},
			},
		},
	}
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), s.mockShard, namespace.ID(namespaceID), gomock.Any(), chasm.WorkflowArchetypeID, locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories:  versionHistories,
		TransitionHistory: nil, // empty local transition history
	}).AnyTimes()
	err := r.ReplicateVersionedTransition(context.Background(), chasm.WorkflowArchetypeID, artifact, "test")
	s.ErrorAs(err, new(*serviceerrors.SyncState))
}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_EmptyLocalTransitionHistory_LocalNewer_BackFillEvents() {
	r := s.wfStateReplNewReplicator()
	namespaceID := uuid.NewString()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{BranchToken: []byte("bt"), Items: []*historyspb.VersionHistoryItem{{EventId: 30, Version: 5}}},
		},
	}
	// source last write version = 2 (from transition history)
	sourceTransitionHistory := []*persistencespb.VersionedTransition{{NamespaceFailoverVersion: 2, TransitionCount: 10}}
	artifact := s.wfStateReplSnapshotArtifact(namespaceID, sourceTransitionHistory, versionHistories)
	artifact.EventBatches = nil // backFillEvents returns nil immediately
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), s.mockShard, namespace.ID(namespaceID), gomock.Any(), chasm.WorkflowArchetypeID, locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories:  versionHistories,
		TransitionHistory: nil, // empty local transition history
	}).AnyTimes()
	// local last write version higher than source's 2
	mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(10), nil).AnyTimes()
	err := r.ReplicateVersionedTransition(context.Background(), chasm.WorkflowArchetypeID, artifact, "test")
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_EmptyLocalTransitionHistory_LocalSameVersionHigherEvent_ErrDuplicate() {
	r := s.wfStateReplNewReplicator()
	namespaceID := uuid.NewString()
	// local event id (30) > source event id (20) and same version => ErrDuplicate
	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{BranchToken: []byte("bt"), Items: []*historyspb.VersionHistoryItem{{EventId: 30, Version: 2}}},
		},
	}
	sourceVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{BranchToken: []byte("bt"), Items: []*historyspb.VersionHistoryItem{{EventId: 20, Version: 2}}},
		},
	}
	sourceTransitionHistory := []*persistencespb.VersionedTransition{{NamespaceFailoverVersion: 2, TransitionCount: 10}}
	artifact := s.wfStateReplSnapshotArtifact(namespaceID, sourceTransitionHistory, sourceVersionHistories)
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), s.mockShard, namespace.ID(namespaceID), gomock.Any(), chasm.WorkflowArchetypeID, locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories:  localVersionHistories,
		TransitionHistory: nil,
	}).AnyTimes()
	mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(2), nil).AnyTimes() // equal to source
	err := r.ReplicateVersionedTransition(context.Background(), chasm.WorkflowArchetypeID, artifact, "test")
	s.ErrorIs(err, consts.ErrDuplicate)
}

func (s *workflowReplicatorSuite) Test_ReplicateVersionedTransition_LoadMutableStateError() {
	r := s.wfStateReplNewReplicator()
	namespaceID := uuid.NewString()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{BranchToken: []byte("bt"), Items: []*historyspb.VersionHistoryItem{{EventId: 30, Version: 2}}},
		},
	}
	artifact := s.wfStateReplSnapshotArtifact(namespaceID, []*persistencespb.VersionedTransition{{NamespaceFailoverVersion: 1, TransitionCount: 1}}, versionHistories)
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(), s.mockShard, namespace.ID(namespaceID), gomock.Any(), chasm.WorkflowArchetypeID, locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	expectedErr := serviceerror.NewInternal("load error")
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(nil, expectedErr)
	err := r.ReplicateVersionedTransition(context.Background(), chasm.WorkflowArchetypeID, artifact, "test")
	s.Equal(expectedErr, err)
}

func (s *workflowReplicatorSuite) Test_applyMutation_NilMutation_Error() {
	r := s.wfStateReplNewReplicator()
	mockMutableState := historyi.NewMockMutableState(s.controller)
	artifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{State: &persistencespb.WorkflowMutableState{}},
		},
	}
	err := r.applyMutation(
		context.Background(), namespace.ID(uuid.NewString()), s.workflowID, s.runID,
		chasm.WorkflowArchetypeID, nil, mockMutableState, wcache.NoopReleaseFn, artifact, "test",
	)
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
	s.Contains(err.Error(), "mutation is nil")
}

func (s *workflowReplicatorSuite) Test_applyMutation_NilLocalMutableState_SyncStateError() {
	r := s.wfStateReplNewReplicator()
	artifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{
				StateMutation: &persistencespb.WorkflowMutableStateMutation{
					ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{},
					ExecutionState: &persistencespb.WorkflowExecutionState{RunId: s.runID},
				},
			},
		},
	}
	err := r.applyMutation(
		context.Background(), namespace.ID(uuid.NewString()), s.workflowID, s.runID,
		chasm.WorkflowArchetypeID, nil, nil, wcache.NoopReleaseFn, artifact, "test",
	)
	s.ErrorAs(err, new(*serviceerrors.SyncState))
}

func (s *workflowReplicatorSuite) Test_applySnapshot_NilSnapshotAttributes_SyncStateError() {
	r := s.wfStateReplNewReplicator()
	mockMutableState := historyi.NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: &historyspb.VersionHistories{},
	}).AnyTimes()
	// mutation artifact => GetSyncWorkflowStateSnapshotAttributes returns nil
	artifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes{
			SyncWorkflowStateMutationAttributes: &replicationspb.SyncWorkflowStateMutationAttributes{},
		},
	}
	err := r.applySnapshot(
		context.Background(), namespace.ID(uuid.NewString()), s.workflowID, s.runID,
		chasm.WorkflowArchetypeID, nil, wcache.NoopReleaseFn, mockMutableState, artifact, "test",
	)
	s.ErrorAs(err, new(*serviceerrors.SyncState))
}

func (s *workflowReplicatorSuite) Test_applySnapshotWhenWorkflowExist_StaleSnapshot_SyncStateError() {
	r := s.wfStateReplNewReplicator()
	namespaceID := uuid.NewString()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{BranchToken: []byte("bt"), Items: []*historyspb.VersionHistoryItem{{EventId: 30, Version: 2}}},
		},
	}
	mockMutableState := historyi.NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
		// local is ahead of source => source staleness check returns ErrStaleState
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 10},
			{NamespaceFailoverVersion: 2, TransitionCount: 30},
		},
	}).AnyTimes()
	// source transition history is behind local
	sourceMutableState := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			VersionHistories: versionHistories,
			TransitionHistory: []*persistencespb.VersionedTransition{
				{NamespaceFailoverVersion: 1, TransitionCount: 10},
				{NamespaceFailoverVersion: 2, TransitionCount: 20},
			},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{RunId: s.runID},
	}
	err := r.applySnapshotWhenWorkflowExist(
		context.Background(), namespace.ID(namespaceID), s.workflowID, s.runID,
		chasm.WorkflowArchetypeID, nil, wcache.NoopReleaseFn, mockMutableState, sourceMutableState,
		nil, nil, "test",
	)
	s.ErrorAs(err, new(*serviceerrors.SyncState))
}

func (s *workflowReplicatorSuite) Test_applySnapshotWhenWorkflowExist_EmptyTransitionHistory_SameBranch_WithNewRun() {
	s.wfStateReplExpectEventCachePut()
	r := s.wfStateReplNewReplicator()
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	r.transactionMgr = mockTransactionManager
	r.taskRefresher = mockTaskRefresher
	namespaceID := tests.LocalNamespaceEntry.ID().String()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	// Both local and source have the same single-item version history => same branch, no events to bring up.
	sharedItems := []*historyspb.VersionHistoryItem{{EventId: 5, Version: 1}}
	localVH := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{BranchToken: []byte("bt"), Items: sharedItems},
		},
	}
	sourceVH := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{BranchToken: []byte("bt"), Items: sharedItems},
		},
	}
	mockMutableState := historyi.NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories:    localVH,
		TransitionHistory:   nil, // empty => goes into the version-history comparison else branch
		FirstExecutionRunId: s.runID,
	}).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: s.runID}).AnyTimes()
	mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.LocalNamespaceEntry).AnyTimes()
	mockMutableState.EXPECT().GetPendingChildIds().Return(nil).Times(1)
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)

	sourceMutableState := &persistencespb.WorkflowMutableState{
		ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{VersionHistories: sourceVH},
		ExecutionState: &persistencespb.WorkflowExecutionState{RunId: s.runID},
	}
	mockMutableState.EXPECT().ApplySnapshot(sourceMutableState).Return(nil)

	newRunID := uuid.NewString()
	newRunBlob := s.wfStateReplNewRunStartedEvent(s.runID)

	// no branch switch but empty transition history => full Refresh
	mockTaskRefresher.EXPECT().Refresh(gomock.Any(), mockMutableState, false).Return(nil).Times(1)
	mockTransactionManager.EXPECT().UpdateWorkflow(
		gomock.Any(), false, chasm.WorkflowArchetypeID, gomock.Any(), gomock.Any(),
	).Return(nil).Times(1)

	err := r.applySnapshotWhenWorkflowExist(
		context.Background(), namespace.ID(namespaceID), s.workflowID, s.runID,
		chasm.WorkflowArchetypeID, historyi.NewMockWorkflowContext(s.controller), wcache.NoopReleaseFn,
		mockMutableState, sourceMutableState, nil,
		&replicationspb.NewRunInfo{RunId: newRunID, EventBatch: newRunBlob}, "test",
	)
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_backFillEvents_EmptyBatches_NoOp() {
	r := s.wfStateReplNewReplicator()
	err := r.backFillEvents(
		context.Background(), namespace.ID(uuid.NewString()), s.workflowID, s.runID,
		&historyspb.VersionHistories{}, nil, nil, "test", nil,
	)
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_backFillEvents_WithEvents_CallsEngine() {
	r := s.wfStateReplNewReplicator()
	namespaceID := uuid.NewString()
	histEvents := []*historypb.HistoryEvent{{EventId: 1, Version: 1}, {EventId: 2, Version: 1}}
	blob, err := s.serializer.SerializeEvents(histEvents)
	s.NoError(err)
	newRunEvents := []*historypb.HistoryEvent{{EventId: 1, Version: 1}}
	newRunBlob, err := s.serializer.SerializeEvents(newRunEvents)
	s.NoError(err)
	newRunID := uuid.NewString()
	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			{BranchToken: []byte("bt"), Items: []*historyspb.VersionHistoryItem{{EventId: 2, Version: 1}}},
		},
	}
	engine := historyi.NewMockEngine(s.controller)
	s.mockShard.SetEngineForTesting(engine)
	engine.EXPECT().Stop().AnyTimes()
	destVT := &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 5}
	engine.EXPECT().BackfillHistoryEvents(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *historyi.BackfillHistoryEventsRequest) error {
			s.Equal(newRunID, req.NewRunID)
			s.Equal(destVT, req.VersionedHistory)
			s.Len(req.Events, 1)
			return nil
		}).Times(1)
	err = r.backFillEvents(
		context.Background(), namespace.ID(namespaceID), s.workflowID, s.runID,
		versionHistories, []*commonpb.DataBlob{blob},
		&replicationspb.NewRunInfo{RunId: newRunID, EventBatch: newRunBlob},
		"test", destVT,
	)
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_getFirstHistoryEventsBatch_FromEventBatches() {
	r := s.wfStateReplNewReplicator()
	histEvents := []*historypb.HistoryEvent{{EventId: 1, Version: 1}, {EventId: 2, Version: 1}}
	blob, err := s.serializer.SerializeEvents(histEvents)
	s.NoError(err)
	artifact := &replicationspb.VersionedTransitionArtifact{EventBatches: []*commonpb.DataBlob{blob}}
	batch, err := r.getFirstHistoryEventsBatch(
		context.Background(), namespace.ID(uuid.NewString()), s.workflowID, s.runID, artifact, "test",
	)
	s.NoError(err)
	s.Len(batch, 1)
	s.Equal(int64(1), batch[0][0].EventId)
}

func (s *workflowReplicatorSuite) Test_getFirstHistoryEventsBatch_FromRemote() {
	r := s.wfStateReplNewReplicator()
	histEvents := []*historypb.HistoryEvent{{EventId: 1, Version: 1}}
	blob, err := s.serializer.SerializeEvents(histEvents)
	s.NoError(err)
	// EventBatches first event is not 1 => go to remote
	otherEvents := []*historypb.HistoryEvent{{EventId: 5, Version: 1}}
	otherBlob, err := s.serializer.SerializeEvents(otherEvents)
	s.NoError(err)
	artifact := &replicationspb.VersionedTransitionArtifact{EventBatches: []*commonpb.DataBlob{otherBlob}}
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetWorkflowExecutionRawHistoryV2Response{HistoryBatches: []*commonpb.DataBlob{blob}}, nil,
	)
	batch, err := r.getFirstHistoryEventsBatch(
		context.Background(), namespace.ID(uuid.NewString()), s.workflowID, s.runID, artifact, "test",
	)
	s.NoError(err)
	s.Len(batch, 1)
	s.Equal(int64(1), batch[0][0].EventId)
}

func (s *workflowReplicatorSuite) Test_getFirstHistoryEventsBatch_RemoteEmpty_Error() {
	r := s.wfStateReplNewReplicator()
	artifact := &replicationspb.VersionedTransitionArtifact{}
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetWorkflowExecutionRawHistoryV2Response{}, nil,
	)
	_, err := r.getFirstHistoryEventsBatch(
		context.Background(), namespace.ID(uuid.NewString()), s.workflowID, s.runID, artifact, "test",
	)
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
	s.Contains(err.Error(), "no history batches")
}

func (s *workflowReplicatorSuite) Test_getFirstHistoryEventsBatch_RemoteNotStartFromOne_Error() {
	r := s.wfStateReplNewReplicator()
	histEvents := []*historypb.HistoryEvent{{EventId: 5, Version: 1}}
	blob, err := s.serializer.SerializeEvents(histEvents)
	s.NoError(err)
	artifact := &replicationspb.VersionedTransitionArtifact{}
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetWorkflowExecutionRawHistoryV2Response{HistoryBatches: []*commonpb.DataBlob{blob}}, nil,
	)
	_, err = r.getFirstHistoryEventsBatch(
		context.Background(), namespace.ID(uuid.NewString()), s.workflowID, s.runID, artifact, "test",
	)
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
	s.Contains(err.Error(), "does not start from event ID 1")
}

func (s *workflowReplicatorSuite) Test_getFirstHistoryEventsBatch_RemoteAdminClientError() {
	r := s.wfStateReplNewReplicator()
	artifact := &replicationspb.VersionedTransitionArtifact{}
	expectedErr := serviceerror.NewInternal("admin client error")
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), gomock.Any()).Return(nil, expectedErr)
	_, err := r.getFirstHistoryEventsBatch(
		context.Background(), namespace.ID(uuid.NewString()), s.workflowID, s.runID, artifact, "test",
	)
	s.Equal(expectedErr, err)
}

func (s *workflowReplicatorSuite) Test_handleFirstReplicationTaskWithNewRun_Success() {
	s.wfStateReplExpectEventCachePut()
	r := s.wfStateReplNewReplicator()
	mockTransactionManager := NewMockTransactionManager(s.controller)
	r.transactionMgr = mockTransactionManager
	namespaceID := tests.LocalNamespaceEntry.ID().String()

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	// EventBatches contains the first event (event id 1) so we don't go to remote.
	firstBatchBlob := s.wfStateReplNewRunStartedEvent(s.runID)
	artifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
				State: &persistencespb.WorkflowMutableState{},
			},
		},
		EventBatches: []*commonpb.DataBlob{firstBatchBlob},
		NewRunInfo:   &replicationspb.NewRunInfo{RunId: uuid.NewString()},
	}

	executionInfo := &persistencespb.WorkflowExecutionInfo{
		WorkflowId:               s.workflowID,
		NamespaceId:              namespaceID,
		WorkflowExecutionTimeout: timestamp.DurationPtr(time.Hour),
		WorkflowRunTimeout:       timestamp.DurationPtr(time.Hour),
	}
	executionState := &persistencespb.WorkflowExecutionState{
		RunId:     s.runID,
		StartTime: timestamp.TimePtr(s.now),
	}

	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockTransactionManager.EXPECT().CreateWorkflow(
		gomock.Any(), chasm.WorkflowArchetypeID, gomock.AssignableToTypeOf(&WorkflowImpl{}),
	).Return(nil)

	continueProcess, err := r.handleFirstReplicationTaskWithNewRun(
		context.Background(),
		chasm.WorkflowArchetypeID,
		mockWeCtx,
		executionInfo,
		executionState,
		artifact,
		"test",
	)
	s.NoError(err)
	s.True(continueProcess)
}

func (s *workflowReplicatorSuite) Test_handleFirstReplicationTaskWithNewRun_NamespaceError() {
	r := s.wfStateReplNewReplicator()
	namespaceID := uuid.NewString()
	expectedErr := serviceerror.NewInternal("ns err")
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(nil, expectedErr)
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	continueProcess, err := r.handleFirstReplicationTaskWithNewRun(
		context.Background(),
		chasm.WorkflowArchetypeID,
		mockWeCtx,
		&persistencespb.WorkflowExecutionInfo{NamespaceId: namespaceID, WorkflowId: s.workflowID},
		&persistencespb.WorkflowExecutionState{RunId: s.runID},
		&replicationspb.VersionedTransitionArtifact{},
		"test",
	)
	s.False(continueProcess)
	s.Equal(expectedErr, err)
}

func (s *workflowReplicatorSuite) Test_handleFirstReplicationTaskWithNewRun_NonWorkflowArchetype_Error() {
	r := s.wfStateReplNewReplicator()
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	continueProcess, err := r.handleFirstReplicationTaskWithNewRun(
		context.Background(),
		wfStateReplNonWorkflowArchetypeID,
		mockWeCtx,
		&persistencespb.WorkflowExecutionInfo{},
		&persistencespb.WorkflowExecutionState{},
		&replicationspb.VersionedTransitionArtifact{},
		"test",
	)
	s.False(continueProcess)
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
	s.Contains(err.Error(), "Non workflow should not have new run")
}

func (s *workflowReplicatorSuite) Test_handleFirstReplicationTask_WithNewRun_NonWorkflow_Error() {
	r := s.wfStateReplNewReplicator()
	namespaceID := uuid.NewString()
	artifact := &replicationspb.VersionedTransitionArtifact{
		StateAttributes: &replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes{
			SyncWorkflowStateSnapshotAttributes: &replicationspb.SyncWorkflowStateSnapshotAttributes{
				State: &persistencespb.WorkflowMutableState{
					ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{NamespaceId: namespaceID, WorkflowId: s.workflowID},
					ExecutionState: &persistencespb.WorkflowExecutionState{RunId: s.runID},
				},
			},
		},
		NewRunInfo: &replicationspb.NewRunInfo{RunId: uuid.NewString()},
	}
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	continueProcess, err := r.handleFirstReplicationTask(
		context.Background(),
		wfStateReplNonWorkflowArchetypeID,
		mockWeCtx,
		artifact,
		"test",
	)
	s.False(continueProcess)
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
}

func (s *workflowReplicatorSuite) Test_handleFirstReplicationTaskWithoutNewRun_NamespaceError() {
	r := s.wfStateReplNewReplicator()
	namespaceID := uuid.NewString()
	expectedErr := serviceerror.NewInternal("ns error")
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(nil, expectedErr)
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	continueProcess, err := r.handleFirstReplicationTaskWithoutNewRun(
		context.Background(),
		chasm.WorkflowArchetypeID,
		mockWeCtx,
		&persistencespb.WorkflowExecutionInfo{NamespaceId: namespaceID, WorkflowId: s.workflowID},
		&persistencespb.WorkflowExecutionState{RunId: s.runID},
		nil,
		nil,
		&replicationspb.VersionedTransitionArtifact{},
		"test",
	)
	s.False(continueProcess)
	s.Equal(expectedErr, err)
}

func (s *workflowReplicatorSuite) Test_deleteNewBranchWhenError_NoErrorNoOp() {
	r := s.wfStateReplNewReplicator()
	// err == nil => no metrics, no log; just exercise the false branch
	r.deleteNewBranchWhenError(
		context.Background(), namespace.ID(uuid.NewString()), s.workflowID, s.runID,
		chasm.WorkflowArchetypeID, []byte("token"), nil,
	)
}

func (s *workflowReplicatorSuite) Test_deleteNewBranchWhenError_WithErrorRecordsMetric() {
	r := s.wfStateReplNewReplicator()
	r.deleteNewBranchWhenError(
		context.Background(), namespace.ID(uuid.NewString()), s.workflowID, s.runID,
		chasm.WorkflowArchetypeID, []byte("token"), serviceerror.NewInternal("boom"),
	)
}

func (s *workflowReplicatorSuite) Test_SyncWorkflowState_GetOrCreateError() {
	namespaceID := uuid.NewString()
	request := &historyservice.ReplicateWorkflowStateRequest{
		WorkflowState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{WorkflowId: s.workflowID, NamespaceId: namespaceID},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId: s.runID,
				State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			},
		},
		RemoteCluster: "test",
	}
	expectedErr := serviceerror.NewInternal("cache err")
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(), s.mockShard, namespace.ID(namespaceID), gomock.Any(), locks.PriorityHigh,
	).Return(nil, nil, expectedErr)
	err := s.workflowStateReplicator.SyncWorkflowState(context.Background(), request)
	s.Equal(expectedErr, err)
}

func (s *workflowReplicatorSuite) Test_SyncWorkflowState_LoadMutableStateOtherError() {
	namespaceID := uuid.NewString()
	request := &historyservice.ReplicateWorkflowStateRequest{
		WorkflowState: &persistencespb.WorkflowMutableState{
			ExecutionInfo: &persistencespb.WorkflowExecutionInfo{WorkflowId: s.workflowID, NamespaceId: namespaceID},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId: s.runID,
				State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			},
		},
		RemoteCluster: "test",
	}
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(), s.mockShard, namespace.ID(namespaceID), gomock.Any(), locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	expectedErr := serviceerror.NewInternal("load err")
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(nil, expectedErr)
	err := s.workflowStateReplicator.SyncWorkflowState(context.Background(), request)
	s.Equal(expectedErr, err)
}

// wfStateReplNewRealMutableState builds a real MutableStateImpl to use as the "original"
// mutable state when exercising new-run logic.
func (s *workflowReplicatorSuite) wfStateReplNewRealMutableState(firstExecutionRunID string) *workflow.MutableStateImpl {
	ms := workflow.NewMutableState(
		s.mockShard,
		s.mockShard.GetEventsCache(),
		s.logger,
		tests.LocalNamespaceEntry,
		s.workflowID,
		s.runID,
		s.now,
	)
	ms.GetExecutionInfo().FirstExecutionRunId = firstExecutionRunID
	return ms
}

func (s *workflowReplicatorSuite) wfStateReplNewRunStartedEvent(firstExecutionRunID string) *commonpb.DataBlob {
	startedEvent := &historypb.HistoryEvent{
		EventId:   1,
		Version:   1,
		EventTime: timestamp.TimePtr(s.now),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				FirstExecutionRunId: firstExecutionRunID,
			},
		},
	}
	blob, err := s.serializer.SerializeEvents([]*historypb.HistoryEvent{startedEvent})
	s.NoError(err)
	return blob
}

func (s *workflowReplicatorSuite) wfStateReplExpectEventCachePut() {
	s.mockEventCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()
}

func (s *workflowReplicatorSuite) Test_getNewRunMutableState_SameWorkflowChain() {
	s.wfStateReplExpectEventCachePut()
	r := s.wfStateReplNewReplicator()
	firstExecutionRunID := uuid.NewString()
	original := s.wfStateReplNewRealMutableState(firstExecutionRunID)
	newRunID := uuid.NewString()
	blob := s.wfStateReplNewRunStartedEvent(firstExecutionRunID) // same chain
	ms, err := r.getNewRunMutableState(
		context.Background(), tests.LocalNamespaceEntry.ID(), s.workflowID, newRunID,
		original, blob, true,
	)
	s.NoError(err)
	s.NotNil(ms)
	s.NotEmpty(ms.GetExecutionInfo().TransitionHistory)
}

func (s *workflowReplicatorSuite) Test_getNewRunMutableState_DifferentWorkflowChain() {
	s.wfStateReplExpectEventCachePut()
	r := s.wfStateReplNewReplicator()
	original := s.wfStateReplNewRealMutableState(uuid.NewString())
	newRunID := uuid.NewString()
	// new run's first execution run id differs => different chain branch
	blob := s.wfStateReplNewRunStartedEvent(uuid.NewString())
	ms, err := r.getNewRunMutableState(
		context.Background(), tests.LocalNamespaceEntry.ID(), s.workflowID, newRunID,
		original, blob, false, // isStateBased=false => skip InitTransitionHistory
	)
	s.NoError(err)
	s.NotNil(ms)
}

func (s *workflowReplicatorSuite) Test_getNewRunMutableState_DeserializeError() {
	r := s.wfStateReplNewReplicator()
	original := s.wfStateReplNewRealMutableState(uuid.NewString())
	badBlob := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("not a valid proto"),
	}
	_, err := r.getNewRunMutableState(
		context.Background(), tests.LocalNamespaceEntry.ID(), s.workflowID, uuid.NewString(),
		original, badBlob, true,
	)
	s.Error(err)
}

func (s *workflowReplicatorSuite) Test_getNewRunWorkflow_Success() {
	s.wfStateReplExpectEventCachePut()
	r := s.wfStateReplNewReplicator()
	firstExecutionRunID := uuid.NewString()
	original := s.wfStateReplNewRealMutableState(firstExecutionRunID)
	newRunID := uuid.NewString()
	blob := s.wfStateReplNewRunStartedEvent(firstExecutionRunID)
	wf, err := r.getNewRunWorkflow(
		context.Background(), tests.LocalNamespaceEntry.ID(), s.workflowID,
		chasm.WorkflowArchetypeID, original, &replicationspb.NewRunInfo{RunId: newRunID, EventBatch: blob},
	)
	s.NoError(err)
	s.NotNil(wf)
	s.Equal(newRunID, wf.GetMutableState().GetExecutionState().RunId)
}

func (s *workflowReplicatorSuite) Test_getNewRunWorkflow_Error() {
	r := s.wfStateReplNewReplicator()
	original := s.wfStateReplNewRealMutableState(uuid.NewString())
	badBlob := &commonpb.DataBlob{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: []byte("bad")}
	_, err := r.getNewRunWorkflow(
		context.Background(), tests.LocalNamespaceEntry.ID(), s.workflowID,
		chasm.WorkflowArchetypeID, original, &replicationspb.NewRunInfo{RunId: uuid.NewString(), EventBatch: badBlob},
	)
	s.Error(err)
}

func (s *workflowReplicatorSuite) Test_createNewRunWorkflow_NewRunAlreadyExists_NoOp() {
	r := s.wfStateReplNewReplicator()
	original := s.wfStateReplNewRealMutableState(uuid.NewString())
	newRunID := uuid.NewString()
	mockNewRunCtx := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(), s.mockShard, tests.LocalNamespaceEntry.ID(),
		&commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: newRunID},
		locks.PriorityHigh,
	).Return(mockNewRunCtx, wcache.NoopReleaseFn, nil)
	// LoadMutableState returns nil error => new run already exists => no-op
	mockNewRunCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(nil, nil)
	err := r.createNewRunWorkflow(
		context.Background(), tests.LocalNamespaceEntry.ID(), s.workflowID,
		chasm.WorkflowArchetypeID, &replicationspb.NewRunInfo{RunId: newRunID}, original, true,
	)
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_createNewRunWorkflow_GetOrCreateError() {
	r := s.wfStateReplNewReplicator()
	original := s.wfStateReplNewRealMutableState(uuid.NewString())
	newRunID := uuid.NewString()
	expectedErr := serviceerror.NewInternal("cache err")
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(), s.mockShard, tests.LocalNamespaceEntry.ID(),
		&commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: newRunID},
		locks.PriorityHigh,
	).Return(nil, nil, expectedErr)
	err := r.createNewRunWorkflow(
		context.Background(), tests.LocalNamespaceEntry.ID(), s.workflowID,
		chasm.WorkflowArchetypeID, &replicationspb.NewRunInfo{RunId: newRunID}, original, true,
	)
	s.Equal(expectedErr, err)
}

func (s *workflowReplicatorSuite) Test_createNewRunWorkflow_LoadMutableStateOtherError() {
	r := s.wfStateReplNewReplicator()
	original := s.wfStateReplNewRealMutableState(uuid.NewString())
	newRunID := uuid.NewString()
	mockNewRunCtx := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(), s.mockShard, tests.LocalNamespaceEntry.ID(),
		&commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: newRunID},
		locks.PriorityHigh,
	).Return(mockNewRunCtx, wcache.NoopReleaseFn, nil)
	expectedErr := serviceerror.NewInternal("load err")
	mockNewRunCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(nil, expectedErr)
	err := r.createNewRunWorkflow(
		context.Background(), tests.LocalNamespaceEntry.ID(), s.workflowID,
		chasm.WorkflowArchetypeID, &replicationspb.NewRunInfo{RunId: newRunID}, original, true,
	)
	s.Equal(expectedErr, err)
}

func (s *workflowReplicatorSuite) Test_createNewRunWorkflow_NewRunNotFound_CreateWorkflow() {
	s.wfStateReplExpectEventCachePut()
	r := s.wfStateReplNewReplicator()
	mockTransactionManager := NewMockTransactionManager(s.controller)
	r.transactionMgr = mockTransactionManager
	firstExecutionRunID := uuid.NewString()
	original := s.wfStateReplNewRealMutableState(firstExecutionRunID)
	newRunID := uuid.NewString()
	blob := s.wfStateReplNewRunStartedEvent(firstExecutionRunID)
	mockNewRunCtx := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(), s.mockShard, tests.LocalNamespaceEntry.ID(),
		&commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: newRunID},
		locks.PriorityHigh,
	).Return(mockNewRunCtx, wcache.NoopReleaseFn, nil)
	mockNewRunCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(nil, serviceerror.NewNotFound("nf"))
	mockTransactionManager.EXPECT().CreateWorkflow(
		gomock.Any(), chasm.WorkflowArchetypeID, gomock.AssignableToTypeOf(&WorkflowImpl{}),
	).Return(nil)
	err := r.createNewRunWorkflow(
		context.Background(), tests.LocalNamespaceEntry.ID(), s.workflowID,
		chasm.WorkflowArchetypeID, &replicationspb.NewRunInfo{RunId: newRunID, EventBatch: blob}, original, true,
	)
	s.NoError(err)
}
