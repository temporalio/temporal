// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ndc

import (
	"context"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
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
		NamespaceId:   namespaceID,
	}
	we := &commonpb.WorkflowExecution{
		WorkflowId: s.workflowID,
		RunId:      s.runID,
	}
	mockWeCtx := workflow.NewMockContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		we,
		workflow.LockPriorityLow,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	s.mockWorkflowCache.EXPECT().GetOrCreateCurrentWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		s.workflowID,
		workflow.LockPriorityLow,
	).Return(wcache.NoopReleaseFn, nil)
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
	mockWeCtx := workflow.NewMockContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		we,
		workflow.LockPriorityLow,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	s.mockWorkflowCache.EXPECT().GetOrCreateCurrentWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		s.workflowID,
		workflow.LockPriorityLow,
	).Return(wcache.NoopReleaseFn, nil)
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
		blob, err := serializer.SerializeEvents(history.GetEvents(), enumspb.ENCODING_TYPE_PROTO3)
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
	mockWeCtx := workflow.NewMockContext(s.controller)
	mockMutableState := workflow.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		we,
		workflow.LockPriorityLow,
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
	mockWeCtx := workflow.NewMockContext(s.controller)
	mockMutableState := workflow.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		namespace.ID(namespaceID),
		we,
		workflow.LockPriorityLow,
	).Return(mockWeCtx, wcache.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(mockMutableState, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
	})
	mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(namespaceID, s.workflowID, s.runID)).AnyTimes()

	engine := shard.NewMockEngine(s.controller)
	s.mockShard.SetEngineForTesting(engine)
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	s.NoError(err)
	engine.EXPECT().SyncHSM(gomock.Any(), &shard.SyncHSMRequest{
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
