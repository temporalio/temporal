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
		persistencespb.ShardInfo_builder{
			ShardId: 10,
			RangeId: 1,
		}.Build(),
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
	)
}

func (s *workflowReplicatorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *workflowReplicatorSuite) Test_ApplyWorkflowState_BrandNew() {
	namespaceID := uuid.NewString()
	namespaceName := "namespaceName"
	branchInfo := persistencespb.HistoryBranch_builder{
		TreeId:    s.runID,
		BranchId:  uuid.NewString(),
		Ancestors: nil,
	}.Build()
	historyBranch, err := s.serializer.HistoryBranchToBlob(branchInfo)
	s.NoError(err)
	completionEventBatchId := int64(5)
	nextEventID := int64(7)
	request := historyservice.ReplicateWorkflowStateRequest_builder{
		WorkflowState: persistencespb.WorkflowMutableState_builder{
			ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
				WorkflowId:  s.workflowID,
				NamespaceId: namespaceID,
				VersionHistories: historyspb.VersionHistories_builder{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						historyspb.VersionHistory_builder{
							BranchToken: historyBranch.GetData(),
							Items: []*historyspb.VersionHistoryItem{
								historyspb.VersionHistoryItem_builder{
									EventId: int64(100),
									Version: int64(100),
								}.Build(),
							},
						}.Build(),
					},
				}.Build(),
				CompletionEventBatchId: completionEventBatchId,
			}.Build(),
			ExecutionState: persistencespb.WorkflowExecutionState_builder{
				RunId:  s.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			}.Build(),
			NextEventId: nextEventID,
		}.Build(),
		RemoteCluster: "test",
		NamespaceId:   namespaceID,
	}.Build()
	we := commonpb.WorkflowExecution_builder{
		WorkflowId: s.workflowID,
		RunId:      s.runID,
	}.Build()
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
		persistencespb.NamespaceInfo_builder{Name: namespaceName}.Build(),
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
	fakeStartHistory := historypb.HistoryEvent_builder{
		WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{},
	}.Build()
	fakeCompletionEvent := historypb.HistoryEvent_builder{
		WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{},
	}.Build()
	s.mockEventCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), common.FirstEventID, gomock.Any()).Return(fakeStartHistory, nil).AnyTimes()
	s.mockEventCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), completionEventBatchId, gomock.Any()).Return(fakeCompletionEvent, nil).AnyTimes()
	err = s.workflowStateReplicator.SyncWorkflowState(context.Background(), request)
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_ApplyWorkflowState_Ancestors() {
	namespaceID := uuid.NewString()
	namespaceName := "namespaceName"
	branchInfo := persistencespb.HistoryBranch_builder{
		TreeId:   uuid.NewString(),
		BranchId: uuid.NewString(),
		Ancestors: []*persistencespb.HistoryBranchRange{
			persistencespb.HistoryBranchRange_builder{
				BranchId:    uuid.NewString(),
				BeginNodeId: 1,
				EndNodeId:   3,
			}.Build(),
			persistencespb.HistoryBranchRange_builder{
				BranchId:    uuid.NewString(),
				BeginNodeId: 3,
				EndNodeId:   4,
			}.Build(),
		},
	}.Build()
	historyBranch, err := s.serializer.HistoryBranchToBlob(branchInfo)
	s.NoError(err)
	completionEventBatchId := int64(5)
	nextEventID := int64(7)
	request := historyservice.ReplicateWorkflowStateRequest_builder{
		WorkflowState: persistencespb.WorkflowMutableState_builder{
			ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
				WorkflowId:  s.workflowID,
				NamespaceId: namespaceID,
				VersionHistories: historyspb.VersionHistories_builder{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						historyspb.VersionHistory_builder{
							BranchToken: historyBranch.GetData(),
							Items: []*historyspb.VersionHistoryItem{
								historyspb.VersionHistoryItem_builder{
									EventId: int64(100),
									Version: int64(100),
								}.Build(),
							},
						}.Build(),
					},
				}.Build(),
				CompletionEventBatchId: completionEventBatchId,
			}.Build(),
			ExecutionState: persistencespb.WorkflowExecutionState_builder{
				RunId:  s.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			}.Build(),
			NextEventId: nextEventID,
		}.Build(),
		RemoteCluster: "test",
		NamespaceId:   namespaceID,
	}.Build()
	we := commonpb.WorkflowExecution_builder{
		WorkflowId: s.workflowID,
		RunId:      s.runID,
	}.Build()
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
		commonpb.WorkflowExecution_builder{
			WorkflowId: s.workflowID,
			RunId:      branchInfo.GetTreeId(),
		}.Build(),
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
		persistencespb.NamespaceInfo_builder{Name: namespaceName}.Build(),
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceName(namespace.ID(namespaceID)).Return(namespace.Name(namespaceName), nil).AnyTimes()
	expectedHistory := []*historypb.History{
		historypb.History_builder{
			Events: []*historypb.HistoryEvent{
				historypb.HistoryEvent_builder{
					EventId: 1,
				}.Build(),
				historypb.HistoryEvent_builder{
					EventId: 2,
				}.Build(),
			},
		}.Build(),
		historypb.History_builder{
			Events: []*historypb.HistoryEvent{
				historypb.HistoryEvent_builder{
					EventId: 3,
				}.Build(),
			},
		}.Build(),
		historypb.History_builder{
			Events: []*historypb.HistoryEvent{
				historypb.HistoryEvent_builder{
					EventId: 4,
				}.Build(),
			},
		}.Build(),
		historypb.History_builder{
			Events: []*historypb.HistoryEvent{
				historypb.HistoryEvent_builder{
					EventId: 5,
				}.Build(),
				historypb.HistoryEvent_builder{
					EventId: 6,
				}.Build(),
			},
		}.Build(),
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
		adminservice.GetWorkflowExecutionRawHistoryV2Response_builder{
			HistoryBatches: historyBlobs,
			HistoryNodeIds: nodeIds,
		}.Build(),
		nil,
	)
	s.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History: []*historypb.History{
			historypb.History_builder{
				Events: []*historypb.HistoryEvent{
					historypb.HistoryEvent_builder{
						EventId: 1,
					}.Build(),
					historypb.HistoryEvent_builder{
						EventId: 2,
					}.Build(),
				},
			}.Build(),
		},
	}, nil)
	s.mockExecutionManager.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNotFound(""))
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), gomock.Any()).Return(nil, nil).Times(3)
	fakeStartHistory := historypb.HistoryEvent_builder{
		WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{},
	}.Build()
	fakeCompletionEvent := historypb.HistoryEvent_builder{
		WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{},
	}.Build()
	s.mockEventCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), common.FirstEventID, gomock.Any()).Return(fakeStartHistory, nil).AnyTimes()
	s.mockEventCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), gomock.Any(), completionEventBatchId, gomock.Any()).Return(fakeCompletionEvent, nil).AnyTimes()
	err = s.workflowStateReplicator.SyncWorkflowState(context.Background(), request)
	s.NoError(err)
}

func (s *workflowReplicatorSuite) Test_ApplyWorkflowState_NoClosedWorkflow_Error() {
	err := s.workflowStateReplicator.SyncWorkflowState(context.Background(), historyservice.ReplicateWorkflowStateRequest_builder{
		WorkflowState: persistencespb.WorkflowMutableState_builder{
			ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
				WorkflowId: s.workflowID,
			}.Build(),
			ExecutionState: persistencespb.WorkflowExecutionState_builder{
				RunId:  s.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			}.Build(),
		}.Build(),
		RemoteCluster: "test",
	}.Build())
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
}

func (s *workflowReplicatorSuite) Test_ApplyWorkflowState_ExistWorkflow_Resend() {
	namespaceID := uuid.NewString()
	branchInfo := persistencespb.HistoryBranch_builder{
		TreeId:    uuid.NewString(),
		BranchId:  uuid.NewString(),
		Ancestors: nil,
	}.Build()
	historyBranch, err := s.serializer.HistoryBranchToBlob(branchInfo)
	s.NoError(err)
	completionEventBatchId := int64(5)
	nextEventID := int64(7)
	request := historyservice.ReplicateWorkflowStateRequest_builder{
		WorkflowState: persistencespb.WorkflowMutableState_builder{
			ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
				WorkflowId:  s.workflowID,
				NamespaceId: namespaceID,
				VersionHistories: historyspb.VersionHistories_builder{
					CurrentVersionHistoryIndex: 0,
					Histories: []*historyspb.VersionHistory{
						historyspb.VersionHistory_builder{
							BranchToken: historyBranch.GetData(),
							Items: []*historyspb.VersionHistoryItem{
								historyspb.VersionHistoryItem_builder{
									EventId: int64(100),
									Version: int64(100),
								}.Build(),
							},
						}.Build(),
					},
				}.Build(),
				CompletionEventBatchId: completionEventBatchId,
			}.Build(),
			ExecutionState: persistencespb.WorkflowExecutionState_builder{
				RunId:  s.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			}.Build(),
			NextEventId: nextEventID,
		}.Build(),
		RemoteCluster: "test",
	}.Build()
	we := commonpb.WorkflowExecution_builder{
		WorkflowId: s.workflowID,
		RunId:      s.runID,
	}.Build()
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
	mockMutableState.EXPECT().GetExecutionInfo().Return(persistencespb.WorkflowExecutionInfo_builder{
		VersionHistories: historyspb.VersionHistories_builder{
			CurrentVersionHistoryIndex: 0,
			Histories: []*historyspb.VersionHistory{
				historyspb.VersionHistory_builder{
					Items: []*historyspb.VersionHistoryItem{
						historyspb.VersionHistoryItem_builder{
							EventId: int64(1),
							Version: int64(1),
						}.Build(),
					},
				}.Build(),
			},
		}.Build(),
	}.Build())
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
	namespaceID := uuid.NewString()
	branchInfo := persistencespb.HistoryBranch_builder{
		TreeId:    uuid.NewString(),
		BranchId:  uuid.NewString(),
		Ancestors: nil,
	}.Build()
	historyBranch, err := s.serializer.HistoryBranchToBlob(branchInfo)
	s.NoError(err)
	completionEventBatchId := int64(5)
	nextEventID := int64(7)
	versionHistories := historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: historyBranch.GetData(),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(100),
						Version: int64(100),
					}.Build(),
				},
			}.Build(),
		},
	}.Build()
	request := historyservice.ReplicateWorkflowStateRequest_builder{
		WorkflowState: persistencespb.WorkflowMutableState_builder{
			ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
				WorkflowId:             s.workflowID,
				NamespaceId:            namespaceID,
				VersionHistories:       versionHistories,
				CompletionEventBatchId: completionEventBatchId,
			}.Build(),
			ExecutionState: persistencespb.WorkflowExecutionState_builder{
				RunId:  s.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			}.Build(),
			NextEventId: nextEventID,
		}.Build(),
		RemoteCluster: "test",
	}.Build()
	we := commonpb.WorkflowExecution_builder{
		WorkflowId: s.workflowID,
		RunId:      s.runID,
	}.Build()
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
	mockMutableState.EXPECT().GetExecutionInfo().Return(persistencespb.WorkflowExecutionInfo_builder{
		VersionHistories: versionHistories,
	}.Build())
	mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(namespaceID, s.workflowID, s.runID)).AnyTimes()

	engine := historyi.NewMockEngine(s.controller)
	s.mockShard.SetEngineForTesting(engine)
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	s.NoError(err)
	engine.EXPECT().SyncHSM(gomock.Any(), &historyi.SyncHSMRequest{
		WorkflowKey: definition.NewWorkflowKey(namespaceID, s.workflowID, s.runID),
		StateMachineNode: persistencespb.StateMachineNode_builder{
			Children: request.GetWorkflowState().GetExecutionInfo().GetSubStateMachinesByType(),
		}.Build(),
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
	return m.expected.GetTransitionCount() == got.GetTransitionCount() && m.expected.GetNamespaceFailoverVersion() == got.GetNamespaceFailoverVersion()
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
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.NewString()
	s.workflowStateReplicator.transactionMgr = NewMockTransactionManager(s.controller)
	versionHistories := historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(19),
						Version: int64(1),
					}.Build(),
					historyspb.VersionHistoryItem_builder{
						EventId: int64(30),
						Version: int64(2),
					}.Build(),
				},
			}.Build(),
		},
	}.Build()
	versionedTransitionArtifact := replicationspb.VersionedTransitionArtifact_builder{
		SyncWorkflowStateSnapshotAttributes: replicationspb.SyncWorkflowStateSnapshotAttributes_builder{
			State: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					WorkflowId:       s.workflowID,
					NamespaceId:      namespaceID,
					VersionHistories: versionHistories,
					TransitionHistory: []*persistencespb.VersionedTransition{
						persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 10}.Build(),
						persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 2, TransitionCount: 20}.Build(),
					},
				}.Build(),
				ExecutionState: persistencespb.WorkflowExecutionState_builder{
					RunId: s.runID,
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build()
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		commonpb.WorkflowExecution_builder{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		}.Build(),
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(persistencespb.WorkflowExecutionInfo_builder{
		VersionHistories: versionHistories,
		TransitionHistory: []*persistencespb.VersionedTransition{
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 10}.Build(),
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 2, TransitionCount: 18}.Build(),
		},
	}.Build()).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		RunId: s.runID,
	}.Build()).AnyTimes()
	mockMutableState.EXPECT().GetPendingChildIds().Return(nil).Times(1)
	mockMutableState.EXPECT().ApplySnapshot(versionedTransitionArtifact.GetSyncWorkflowStateSnapshotAttributes().GetState())
	mockTransactionManager.EXPECT().UpdateWorkflow(gomock.Any(), false, chasm.WorkflowArchetypeID, gomock.Any(), nil).Return(nil).Times(1)
	mockTaskRefresher.EXPECT().
		PartialRefresh(gomock.Any(), gomock.Any(), EqVersionedTransition(persistencespb.VersionedTransition_builder{
			NamespaceFailoverVersion: 2,
			TransitionCount:          19,
		}.Build()), nil, false,
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
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.NewString()
	s.workflowStateReplicator.transactionMgr = NewMockTransactionManager(s.controller)
	versionHistories := historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(19),
						Version: int64(1),
					}.Build(),
					historyspb.VersionHistoryItem_builder{
						EventId: int64(30),
						Version: int64(2),
					}.Build(),
				},
			}.Build(),
		},
	}.Build()
	versionedTransitionArtifact := replicationspb.VersionedTransitionArtifact_builder{
		SyncWorkflowStateSnapshotAttributes: replicationspb.SyncWorkflowStateSnapshotAttributes_builder{
			State: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					WorkflowId:       s.workflowID,
					NamespaceId:      namespaceID,
					VersionHistories: versionHistories,
					TransitionHistory: []*persistencespb.VersionedTransition{
						persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 10}.Build(),
						persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 2, TransitionCount: 20}.Build(),
					},
				}.Build(),
				ExecutionState: persistencespb.WorkflowExecutionState_builder{
					RunId: s.runID,
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build()
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		commonpb.WorkflowExecution_builder{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		}.Build(),
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(persistencespb.WorkflowExecutionInfo_builder{
		VersionHistories: versionHistories,
		TransitionHistory: []*persistencespb.VersionedTransition{
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 13}.Build(), // local transition is stale
		},
	}.Build()).AnyTimes()
	mockMutableState.EXPECT().GetPendingChildIds().Return(nil).Times(1)
	mockMutableState.EXPECT().ApplySnapshot(versionedTransitionArtifact.GetSyncWorkflowStateSnapshotAttributes().GetState())
	mockMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		RunId: s.runID,
	}.Build()).AnyTimes()
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
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.NewString()
	s.workflowStateReplicator.transactionMgr = NewMockTransactionManager(s.controller)
	versionHistories := historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(19),
						Version: int64(1),
					}.Build(),
					historyspb.VersionHistoryItem_builder{
						EventId: int64(30),
						Version: int64(2),
					}.Build(),
				},
			}.Build(),
		},
	}.Build()
	versionedTransitionArtifact := replicationspb.VersionedTransitionArtifact_builder{
		SyncWorkflowStateMutationAttributes: replicationspb.SyncWorkflowStateMutationAttributes_builder{
			StateMutation: persistencespb.WorkflowMutableStateMutation_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					WorkflowId:       s.workflowID,
					NamespaceId:      namespaceID,
					VersionHistories: versionHistories,
					TransitionHistory: []*persistencespb.VersionedTransition{
						persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 10}.Build(),
						persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 2, TransitionCount: 20}.Build(),
					},
				}.Build(),
				ExecutionState: persistencespb.WorkflowExecutionState_builder{
					RunId: s.runID,
				}.Build(),
			}.Build(),
			ExclusiveStartVersionedTransition: persistencespb.VersionedTransition_builder{
				NamespaceFailoverVersion: 1, TransitionCount: 10,
			}.Build(),
		}.Build(),
	}.Build()
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		commonpb.WorkflowExecution_builder{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		}.Build(),
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(persistencespb.WorkflowExecutionInfo_builder{
		VersionHistories: versionHistories,
		TransitionHistory: []*persistencespb.VersionedTransition{
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 10}.Build(),
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 2, TransitionCount: 18}.Build(),
		},
	}.Build()).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		RunId: s.runID,
	}.Build()).AnyTimes()
	mockMutableState.EXPECT().GetPendingChildIds().Return(nil).Times(1)
	mockMutableState.EXPECT().ApplyMutation(versionedTransitionArtifact.GetSyncWorkflowStateMutationAttributes().GetStateMutation())
	mockTransactionManager.EXPECT().UpdateWorkflow(gomock.Any(), false, chasm.WorkflowArchetypeID, gomock.Any(), nil).Return(nil).Times(1)
	mockTaskRefresher.EXPECT().
		PartialRefresh(gomock.Any(), gomock.Any(), EqVersionedTransition(persistencespb.VersionedTransition_builder{
			NamespaceFailoverVersion: 2,
			TransitionCount:          19,
		}.Build()), nil, false,
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
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.NewString()
	s.workflowStateReplicator.transactionMgr = NewMockTransactionManager(s.controller)
	versionHistories := versionhistory.NewVersionHistories(&historyspb.VersionHistory{})
	transitionHistory := []*persistencespb.VersionedTransition{
		persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 2, TransitionCount: 10}.Build(),
	}
	versionedTransitionArtifact := replicationspb.VersionedTransitionArtifact_builder{
		SyncWorkflowStateMutationAttributes: replicationspb.SyncWorkflowStateMutationAttributes_builder{
			StateMutation: persistencespb.WorkflowMutableStateMutation_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					WorkflowId:        s.workflowID,
					NamespaceId:       namespaceID,
					TransitionHistory: transitionHistory,
					VersionHistories:  versionHistories,
				}.Build(),
				ExecutionState: persistencespb.WorkflowExecutionState_builder{
					RunId:  s.runID,
					State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
				}.Build(),
			}.Build(),
			ExclusiveStartVersionedTransition: persistencespb.VersionedTransition_builder{
				NamespaceFailoverVersion: 2, TransitionCount: 0,
			}.Build(),
		}.Build(),
		IsFirstSync: true,
	}.Build()
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		commonpb.WorkflowExecution_builder{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		}.Build(),
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
		s.Equal(localMutableState.GetExecutionInfo().GetTransitionHistory(), transitionHistory)

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
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher
	namespaceID := uuid.NewString()
	s.workflowStateReplicator.transactionMgr = NewMockTransactionManager(s.controller)
	versionHistories := historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(19),
						Version: int64(1),
					}.Build(),
					historyspb.VersionHistoryItem_builder{
						EventId: int64(30),
						Version: int64(2),
					}.Build(),
				},
			}.Build(),
		},
	}.Build()
	versionedTransitionArtifact := replicationspb.VersionedTransitionArtifact_builder{
		SyncWorkflowStateMutationAttributes: replicationspb.SyncWorkflowStateMutationAttributes_builder{
			StateMutation: persistencespb.WorkflowMutableStateMutation_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					WorkflowId:       s.workflowID,
					NamespaceId:      namespaceID,
					VersionHistories: versionHistories,
					TransitionHistory: []*persistencespb.VersionedTransition{
						persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 10}.Build(),
						persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 2, TransitionCount: 20}.Build(),
					},
				}.Build(),
				ExecutionState: persistencespb.WorkflowExecutionState_builder{
					RunId: s.runID,
				}.Build(),
			}.Build(),
			ExclusiveStartVersionedTransition: persistencespb.VersionedTransition_builder{
				NamespaceFailoverVersion: 2, TransitionCount: 15,
			}.Build(),
		}.Build(),
	}.Build()
	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateChasmExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		commonpb.WorkflowExecution_builder{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		}.Build(),
		chasm.WorkflowArchetypeID,
		locks.PriorityHigh,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(persistencespb.WorkflowExecutionInfo_builder{
		VersionHistories: versionHistories,
		TransitionHistory: []*persistencespb.VersionedTransition{
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 10}.Build(),
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 2, TransitionCount: 13}.Build(),
		},
	}.Build()).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		RunId: s.runID,
	}.Build()).AnyTimes()

	err := workflowStateReplicator.ReplicateVersionedTransition(context.Background(), chasm.WorkflowArchetypeID, versionedTransitionArtifact, "test")
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
	namespaceID := uuid.NewString()
	versionHistories := historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(19),
						Version: int64(1),
					}.Build(),
					historyspb.VersionHistoryItem_builder{
						EventId: int64(30),
						Version: int64(2),
					}.Build(),
				},
			}.Build(),
		},
	}.Build()
	localVersionHistoryies := historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: []byte("local-branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(19),
						Version: int64(1),
					}.Build(),
					historyspb.VersionHistoryItem_builder{
						EventId: int64(20),
						Version: int64(2),
					}.Build(),
				},
			}.Build(),
			historyspb.VersionHistory_builder{
				BranchToken: []byte("branchToken2"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(20),
						Version: int64(1),
					}.Build(),
				},
			}.Build(),
		},
	}.Build()
	gapEvents := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventId: 21, Version: 2}.Build(), historypb.HistoryEvent_builder{EventId: 22, Version: 2}.Build(),
		historypb.HistoryEvent_builder{EventId: 23, Version: 2}.Build(), historypb.HistoryEvent_builder{EventId: 24, Version: 2}.Build(),
	}
	requestedEvents := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventId: 25, Version: 2}.Build(), historypb.HistoryEvent_builder{EventId: 26, Version: 2}.Build(),
	}
	tailEvents := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventId: 27, Version: 2}.Build(), historypb.HistoryEvent_builder{EventId: 28, Version: 2}.Build(),
		historypb.HistoryEvent_builder{EventId: 29, Version: 2}.Build(), historypb.HistoryEvent_builder{EventId: 30, Version: 2}.Build(),
	}
	blobs, err := s.serializer.SerializeEvents(requestedEvents)
	s.NoError(err)
	gapBlobs, err := s.serializer.SerializeEvents(gapEvents)
	s.NoError(err)
	tailBlobs, err := s.serializer.SerializeEvents(tailEvents)
	s.NoError(err)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(persistencespb.WorkflowExecutionInfo_builder{
		VersionHistories: localVersionHistoryies,
		TransitionHistory: []*persistencespb.VersionedTransition{
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 10}.Build(),
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 2, TransitionCount: 13}.Build(),
		},
		ExecutionStats: persistencespb.ExecutionStats_builder{
			HistorySize: 100,
		}.Build(),
		LastFirstEventTxnId: 0,
	}.Build()).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		RunId: s.runID,
	}.Build()).AnyTimes()
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
	mockShard.EXPECT().GetConfig().Return(s.mockShard.GetConfig()).AnyTimes()
	s.workflowStateReplicator.shardContext = mockShard
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Name: "test-namespace"}.Build(),
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	nsName := namespace.Name("test-namespace")
	mockMutableState.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Id: namespaceID, Name: nsName.String()}.Build(),
		&persistencespb.NamespaceConfig{},
		"test-cluster",
	)).AnyTimes()
	mockMutableState.EXPECT().AddExternalPayloadSize(gomock.Any()).AnyTimes()
	mockMutableState.EXPECT().AddExternalPayloadCount(gomock.Any()).AnyTimes()
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), adminservice.GetWorkflowExecutionRawHistoryV2Request_builder{
		NamespaceId:       namespaceID,
		Execution:         commonpb.WorkflowExecution_builder{WorkflowId: s.workflowID, RunId: s.runID}.Build(),
		StartEventId:      20,
		StartEventVersion: 2,
		EndEventId:        25,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}.Build()).Return(adminservice.GetWorkflowExecutionRawHistoryV2Response_builder{
		HistoryBatches: []*commonpb.DataBlob{gapBlobs},
		VersionHistory: versionHistories.GetHistories()[0],
		HistoryNodeIds: []int64{21},
	}.Build(), nil)
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), adminservice.GetWorkflowExecutionRawHistoryV2Request_builder{
		NamespaceId:       namespaceID,
		Execution:         commonpb.WorkflowExecution_builder{WorkflowId: s.workflowID, RunId: s.runID}.Build(),
		StartEventId:      26,
		StartEventVersion: 2,
		EndEventId:        31,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}.Build()).Return(adminservice.GetWorkflowExecutionRawHistoryV2Response_builder{
		HistoryBatches: []*commonpb.DataBlob{tailBlobs},
		VersionHistory: versionHistories.GetHistories()[0],
		HistoryNodeIds: []int64{27},
	}.Build(), nil)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistoryies.GetHistories()[0].GetBranchToken(),
		History:           gapBlobs,
		PrevTransactionID: 0,
		TransactionID:     taskId1,
		NodeID:            21,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, s.workflowID, s.runID),
	}).Return(nil, nil).Times(1)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistoryies.GetHistories()[0].GetBranchToken(),
		History:           blobs,
		PrevTransactionID: taskId1,
		TransactionID:     taskId2,
		NodeID:            25,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, s.workflowID, s.runID),
	}).Return(nil, nil).Times(1)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistoryies.GetHistories()[0].GetBranchToken(),
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
	s.Equal(versionHistories.GetHistories()[0].GetItems(), mockMutableState.GetExecutionInfo().GetVersionHistories().GetHistories()[0].GetItems())
}

func (s *workflowReplicatorSuite) Test_bringLocalEventsUpToSourceCurrentBranch_WithGapAndTailEvents_NewMutableState() {
	namespaceID := uuid.NewString()
	versionHistories := historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(3),
						Version: int64(1),
					}.Build(),
					historyspb.VersionHistoryItem_builder{
						EventId: int64(6),
						Version: int64(2),
					}.Build(),
				},
			}.Build(),
		},
	}.Build()
	localVersionHistories := historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: []byte("local-branchToken1"),
			}.Build(),
		},
	}.Build()
	gapEvents := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventId: 1, Version: 1}.Build(), historypb.HistoryEvent_builder{EventId: 2, Version: 1}.Build(), historypb.HistoryEvent_builder{EventId: 3, Version: 1}.Build(),
	}
	requestedEvents := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventId: 4, Version: 2}.Build(),
	}
	tailEvents := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventId: 5, Version: 2}.Build(), historypb.HistoryEvent_builder{EventId: 6, Version: 2}.Build(),
	}
	blobs, err := s.serializer.SerializeEvents(requestedEvents)
	s.NoError(err)
	gapBlobs, err := s.serializer.SerializeEvents(gapEvents)
	s.NoError(err)
	tailBlobs, err := s.serializer.SerializeEvents(tailEvents)
	s.NoError(err)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(persistencespb.WorkflowExecutionInfo_builder{
		VersionHistories:    localVersionHistories,
		ExecutionStats:      persistencespb.ExecutionStats_builder{HistorySize: 0}.Build(),
		LastFirstEventTxnId: 0,
	}.Build()).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		RunId: s.runID,
	}.Build()).AnyTimes()
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
	mockShard.EXPECT().GetConfig().Return(s.mockShard.GetConfig()).AnyTimes()
	s.workflowStateReplicator.shardContext = mockShard
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Name: "test-namespace"}.Build(),
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	nsName := namespace.Name("test-namespace")
	mockMutableState.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Id: namespaceID, Name: nsName.String()}.Build(),
		&persistencespb.NamespaceConfig{},
		"test-cluster",
	)).AnyTimes()
	mockMutableState.EXPECT().AddExternalPayloadSize(gomock.Any()).AnyTimes()
	mockMutableState.EXPECT().AddExternalPayloadCount(gomock.Any()).AnyTimes()
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), adminservice.GetWorkflowExecutionRawHistoryV2Request_builder{
		NamespaceId:       namespaceID,
		Execution:         commonpb.WorkflowExecution_builder{WorkflowId: s.workflowID, RunId: s.runID}.Build(),
		StartEventId:      0,
		StartEventVersion: 0,
		EndEventId:        4,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}.Build()).Return(adminservice.GetWorkflowExecutionRawHistoryV2Response_builder{
		HistoryBatches: []*commonpb.DataBlob{gapBlobs},
		VersionHistory: versionHistories.GetHistories()[0],
		HistoryNodeIds: []int64{1},
	}.Build(), nil)
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), adminservice.GetWorkflowExecutionRawHistoryV2Request_builder{
		NamespaceId:       namespaceID,
		Execution:         commonpb.WorkflowExecution_builder{WorkflowId: s.workflowID, RunId: s.runID}.Build(),
		StartEventId:      4,
		StartEventVersion: 2,
		EndEventId:        7,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}.Build()).Return(adminservice.GetWorkflowExecutionRawHistoryV2Response_builder{
		HistoryBatches: []*commonpb.DataBlob{tailBlobs},
		VersionHistory: versionHistories.GetHistories()[0],
		HistoryNodeIds: []int64{5},
	}.Build(), nil)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       true,
		BranchToken:       localVersionHistories.GetHistories()[0].GetBranchToken(),
		History:           gapBlobs,
		PrevTransactionID: 0,
		TransactionID:     taskId1,
		NodeID:            1,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, s.workflowID, s.runID),
	}).Return(nil, nil).Times(1)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistories.GetHistories()[0].GetBranchToken(),
		History:           blobs,
		PrevTransactionID: taskId1,
		TransactionID:     taskId2,
		NodeID:            4,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, s.workflowID, s.runID),
	}).Return(nil, nil).Times(1)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistories.GetHistories()[0].GetBranchToken(),
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
	s.Equal(versionHistories.GetHistories()[0].GetItems(), mockMutableState.GetExecutionInfo().GetVersionHistories().GetHistories()[0].GetItems())
}

func (s *workflowReplicatorSuite) Test_bringLocalEventsUpToSourceCurrentBranch_WithGapAndTailEvents_NotConsecutive() {
	namespaceID := uuid.NewString()
	versionHistories := historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(19),
						Version: int64(1),
					}.Build(),
					historyspb.VersionHistoryItem_builder{
						EventId: int64(30),
						Version: int64(2),
					}.Build(),
				},
			}.Build(),
		},
	}.Build()
	localVersionHistoryies := historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: []byte("local-branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(19),
						Version: int64(1),
					}.Build(),
					historyspb.VersionHistoryItem_builder{
						EventId: int64(20),
						Version: int64(2),
					}.Build(),
				},
			}.Build(),
			historyspb.VersionHistory_builder{
				BranchToken: []byte("branchToken2"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(20),
						Version: int64(1),
					}.Build(),
				},
			}.Build(),
		},
	}.Build()
	gapEvents := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventId: 21, Version: 2}.Build(), historypb.HistoryEvent_builder{EventId: 22, Version: 2}.Build(),
		historypb.HistoryEvent_builder{EventId: 23, Version: 2}.Build(), historypb.HistoryEvent_builder{EventId: 24, Version: 2}.Build(),
	}
	requestedEvents := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventId: 25, Version: 2}.Build(), historypb.HistoryEvent_builder{EventId: 26, Version: 2}.Build(),
	}
	tailEvents := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{EventId: 28, Version: 2}.Build(),
		historypb.HistoryEvent_builder{EventId: 29, Version: 2}.Build(), historypb.HistoryEvent_builder{EventId: 30, Version: 2}.Build(),
	}
	blobs, err := s.serializer.SerializeEvents(requestedEvents)
	s.NoError(err)
	gapBlobs, err := s.serializer.SerializeEvents(gapEvents)
	s.NoError(err)
	tailBlobs, err := s.serializer.SerializeEvents(tailEvents)
	s.NoError(err)
	mockMutableState := historyi.NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(persistencespb.WorkflowExecutionInfo_builder{
		VersionHistories: localVersionHistoryies,
		TransitionHistory: []*persistencespb.VersionedTransition{
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 10}.Build(),
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 2, TransitionCount: 13}.Build(),
		},
		ExecutionStats: persistencespb.ExecutionStats_builder{
			HistorySize: 100,
		}.Build(),
		LastFirstEventTxnId: 0,
	}.Build()).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		RunId: s.runID,
	}.Build()).AnyTimes()
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
	mockShard.EXPECT().GetConfig().Return(s.mockShard.GetConfig()).AnyTimes()
	s.workflowStateReplicator.shardContext = mockShard
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Name: "test-namespace"}.Build(),
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	nsName := namespace.Name("test-namespace")
	mockMutableState.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Id: namespaceID, Name: nsName.String()}.Build(),
		&persistencespb.NamespaceConfig{},
		"test-cluster",
	)).AnyTimes()
	mockMutableState.EXPECT().AddExternalPayloadSize(gomock.Any()).AnyTimes()
	mockMutableState.EXPECT().AddExternalPayloadCount(gomock.Any()).AnyTimes()
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), adminservice.GetWorkflowExecutionRawHistoryV2Request_builder{
		NamespaceId:       namespaceID,
		Execution:         commonpb.WorkflowExecution_builder{WorkflowId: s.workflowID, RunId: s.runID}.Build(),
		StartEventId:      20,
		StartEventVersion: 2,
		EndEventId:        25,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}.Build()).Return(adminservice.GetWorkflowExecutionRawHistoryV2Response_builder{
		HistoryBatches: []*commonpb.DataBlob{gapBlobs},
		VersionHistory: versionHistories.GetHistories()[0],
		HistoryNodeIds: []int64{21},
	}.Build(), nil)
	s.mockRemoteAdminClient.EXPECT().GetWorkflowExecutionRawHistoryV2(gomock.Any(), adminservice.GetWorkflowExecutionRawHistoryV2Request_builder{
		NamespaceId:       namespaceID,
		Execution:         commonpb.WorkflowExecution_builder{WorkflowId: s.workflowID, RunId: s.runID}.Build(),
		StartEventId:      26,
		StartEventVersion: 2,
		EndEventId:        31,
		EndEventVersion:   2,
		MaximumPageSize:   1000,
		NextPageToken:     nil,
	}.Build()).Return(adminservice.GetWorkflowExecutionRawHistoryV2Response_builder{
		HistoryBatches: []*commonpb.DataBlob{tailBlobs},
		VersionHistory: versionHistories.GetHistories()[0],
		HistoryNodeIds: []int64{27},
	}.Build(), nil)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistoryies.GetHistories()[0].GetBranchToken(),
		History:           gapBlobs,
		PrevTransactionID: 0,
		TransactionID:     taskId1,
		NodeID:            21,
		Info:              persistence.BuildHistoryGarbageCleanupInfo(namespaceID, s.workflowID, s.runID),
	}).Return(nil, nil).Times(1)
	s.mockExecutionManager.EXPECT().AppendRawHistoryNodes(gomock.Any(), &persistence.AppendRawHistoryNodesRequest{
		ShardID:           mockShard.GetShardID(),
		IsNewBranch:       false,
		BranchToken:       localVersionHistoryies.GetHistories()[0].GetBranchToken(),
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
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher

	namespaceID := uuid.NewString()
	versionHistories := versionhistory.NewVersionHistories(&historyspb.VersionHistory{})
	versionedTransitionArtifact := replicationspb.VersionedTransitionArtifact_builder{
		SyncWorkflowStateSnapshotAttributes: replicationspb.SyncWorkflowStateSnapshotAttributes_builder{
			State: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					WorkflowId:               s.workflowID,
					NamespaceId:              namespaceID,
					VersionHistories:         versionHistories,
					WorkflowExecutionTimeout: timestamp.DurationPtr(time.Hour),
					WorkflowRunTimeout:       timestamp.DurationPtr(time.Hour),
				}.Build(),
				ExecutionState: persistencespb.WorkflowExecutionState_builder{
					RunId:     s.runID,
					State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					StartTime: timestamp.TimePtr(s.now),
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build()

	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Name: "test-namespace"}.Build(),
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
		s.Equal(s.workflowID, localMutableState.GetExecutionInfo().GetWorkflowId())
		s.Equal(s.runID, localMutableState.GetExecutionState().GetRunId())
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
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher

	namespaceID := uuid.NewString()
	versionHistories := versionhistory.NewVersionHistories(&historyspb.VersionHistory{})
	versionedTransitionArtifact := replicationspb.VersionedTransitionArtifact_builder{
		SyncWorkflowStateMutationAttributes: replicationspb.SyncWorkflowStateMutationAttributes_builder{
			StateMutation: persistencespb.WorkflowMutableStateMutation_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					WorkflowId:               s.workflowID,
					NamespaceId:              namespaceID,
					VersionHistories:         versionHistories,
					WorkflowExecutionTimeout: timestamp.DurationPtr(time.Hour),
					WorkflowRunTimeout:       timestamp.DurationPtr(time.Hour),
				}.Build(),
				ExecutionState: persistencespb.WorkflowExecutionState_builder{
					RunId:     s.runID,
					State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					StartTime: timestamp.TimePtr(s.now),
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build()

	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Name: "test-namespace"}.Build(),
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
	)
	mockTransactionManager := NewMockTransactionManager(s.controller)
	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	workflowStateReplicator.transactionMgr = mockTransactionManager
	workflowStateReplicator.taskRefresher = mockTaskRefresher

	namespaceID := uuid.NewString()
	versionHistories := versionhistory.NewVersionHistories(&historyspb.VersionHistory{})
	versionedTransitionArtifact := replicationspb.VersionedTransitionArtifact_builder{
		SyncWorkflowStateSnapshotAttributes: replicationspb.SyncWorkflowStateSnapshotAttributes_builder{
			State: persistencespb.WorkflowMutableState_builder{
				ExecutionInfo: persistencespb.WorkflowExecutionInfo_builder{
					WorkflowId:               s.workflowID,
					NamespaceId:              namespaceID,
					VersionHistories:         versionHistories,
					WorkflowExecutionTimeout: timestamp.DurationPtr(time.Hour),
					WorkflowRunTimeout:       timestamp.DurationPtr(time.Hour),
				}.Build(),
				ExecutionState: persistencespb.WorkflowExecutionState_builder{
					RunId:     s.runID,
					State:     enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
					Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
					StartTime: timestamp.TimePtr(s.now),
				}.Build(),
			}.Build(),
		}.Build(),
	}.Build()

	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Name: "test-namespace"}.Build(),
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
	versionHistories := historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(30),
						Version: int64(1),
					}.Build(),
				},
			}.Build(),
		},
	}.Build()
	localVersionHistoryies := historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: []byte("local-branchToken1"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(40),
						Version: int64(1),
					}.Build(),
				},
			}.Build(),
			historyspb.VersionHistory_builder{
				BranchToken: []byte("branchToken2"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(20),
						Version: int64(1),
					}.Build(),
				},
			}.Build(),
		},
	}.Build()

	mockMutableState := historyi.NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(persistencespb.WorkflowExecutionInfo_builder{
		NamespaceId:      namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: localVersionHistoryies,
		TransitionHistory: []*persistencespb.VersionedTransition{
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 1, TransitionCount: 10}.Build(),
			persistencespb.VersionedTransition_builder{NamespaceFailoverVersion: 2, TransitionCount: 13}.Build(),
		},
		ExecutionStats: persistencespb.ExecutionStats_builder{
			HistorySize: 100,
		}.Build(),
	}.Build()).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		RunId: s.runID,
	}.Build()).AnyTimes()
	sourceClusterName := "test-cluster"

	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	s.mockNamespaceCache.EXPECT().GetNamespaceName(namespace.ID(namespaceID)).Return(namespace.Name("test-namespace"), nil).AnyTimes()
	forkedBranchToken := []byte("forked-branchToken")
	s.mockExecutionManager.EXPECT().ForkHistoryBranch(gomock.Any(), &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: localVersionHistoryies.GetHistories()[0].GetBranchToken(),
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

	s.Equal(forkedBranchToken, localVersionHistoryies.GetHistories()[2].GetBranchToken())
	s.Equal(int32(2), localVersionHistoryies.GetCurrentVersionHistoryIndex())
	s.NotNil(newRunBranch)
}

func (s *workflowReplicatorSuite) Test_bringLocalEventsUpToSourceCurrentBranch_ExternalPayloadStats() {
	// Test that the external payload stats are correctly updated when bringLocalEventsUpToSourceCurrentBranch is invoked
	namespaceID := uuid.NewString()
	versionHistories := historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: []byte("branchToken"),
				Items: []*historyspb.VersionHistoryItem{
					historyspb.VersionHistoryItem_builder{
						EventId: int64(2),
						Version: int64(1),
					}.Build(),
				},
			}.Build(),
		},
	}.Build()
	localVersionHistories := historyspb.VersionHistories_builder{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			historyspb.VersionHistory_builder{
				BranchToken: []byte("local-branchToken"),
			}.Build(),
		},
	}.Build()

	historyEvents := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventId:   1,
			Version:   1,
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			WorkflowExecutionStartedEventAttributes: historypb.WorkflowExecutionStartedEventAttributes_builder{
				Input: commonpb.Payloads_builder{
					Payloads: []*commonpb.Payload{
						commonpb.Payload_builder{
							Data: []byte("test"),
							ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
								commonpb.Payload_ExternalPayloadDetails_builder{SizeBytes: 1024}.Build(),
								commonpb.Payload_ExternalPayloadDetails_builder{SizeBytes: 2048}.Build(),
							},
						}.Build(),
					},
				}.Build(),
			}.Build(),
		}.Build(),
		historypb.HistoryEvent_builder{
			EventId:                              2,
			Version:                              1,
			EventType:                            enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{},
		}.Build(),
	}

	serializer := serialization.NewSerializer()
	eventBlobs, err := serializer.SerializeEvents(historyEvents)
	s.NoError(err)

	executionStats := persistencespb.ExecutionStats_builder{
		HistorySize:          0,
		ExternalPayloadSize:  0,
		ExternalPayloadCount: 0,
	}.Build()
	mockMutableState := historyi.NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetExecutionInfo().Return(persistencespb.WorkflowExecutionInfo_builder{
		VersionHistories:    localVersionHistories,
		ExecutionStats:      executionStats,
		LastFirstEventTxnId: 0,
	}.Build()).AnyTimes()
	mockMutableState.EXPECT().GetExecutionState().Return(persistencespb.WorkflowExecutionState_builder{
		RunId: s.runID,
	}.Build()).AnyTimes()
	mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(namespaceID, s.workflowID, s.runID)).AnyTimes()
	mockMutableState.EXPECT().SetHistoryBuilder(gomock.Any()).Times(1)
	mockMutableState.EXPECT().AddReapplyCandidateEvent(gomock.Any()).AnyTimes()

	mockWeCtx := historyi.NewMockWorkflowContext(s.controller)
	sourceClusterName := "test-cluster"

	nsName := namespace.Name("test-namespace")
	s.mockNamespaceCache.EXPECT().GetNamespaceName(namespace.ID(namespaceID)).Return(nsName, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespace.ID(namespaceID)).Return(namespace.NewNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Name: nsName.String()}.Build(),
		nil,
		false,
		nil,
		int64(100),
	), nil).AnyTimes()
	mockMutableState.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		persistencespb.NamespaceInfo_builder{Id: namespaceID, Name: nsName.String()}.Build(),
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
