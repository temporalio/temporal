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

package history

import (
	"context"
	"testing"
	"time"

	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/service/history/events"

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

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

type (
	nDCHistoryReplicatorSuite struct {
		suite.Suite
		*require.Assertions

		controller            *gomock.Controller
		mockShard             *shard.ContextTest
		mockEventCache        *events.MockCache
		mockHistoryCache      *workflow.MockCache
		mockNamespaceCache    *namespace.MockRegistry
		mockRemoteAdminClient *adminservicemock.MockAdminServiceClient
		mockExecutionManager  *persistence.MockExecutionManager
		logger                log.Logger

		workflowID string
		runID      string
		now        time.Time

		historyReplicator *nDCHistoryReplicatorImpl
	}
)

func TestNDCHistoryReplicatorSuite(t *testing.T) {
	s := new(nDCHistoryReplicatorSuite)
	suite.Run(t, s)
}

func (s *nDCHistoryReplicatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	// s.mockTaskRefresher = workflow.NewMockTaskRefresher(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 10,
				RangeId: 1,
			}},
		tests.NewDynamicConfig(),
	)

	s.mockExecutionManager = s.mockShard.Resource.ExecutionMgr
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockHistoryCache = workflow.NewMockCache(s.controller)
	s.mockEventCache = s.mockShard.MockEventsCache
	s.mockRemoteAdminClient = s.mockShard.Resource.RemoteAdminClient
	eventReapplier := NewMocknDCEventsReapplier(s.controller)
	s.logger = s.mockShard.GetLogger()

	s.workflowID = "some random workflow ID"
	s.runID = uuid.New()
	s.now = time.Now().UTC()
	s.historyReplicator = newNDCHistoryReplicator(
		s.mockShard,
		s.mockHistoryCache,
		eventReapplier,
		s.logger,
		serialization.NewSerializer(),
	)
}

func (s *nDCHistoryReplicatorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *nDCHistoryReplicatorSuite) Test_ApplyWorkflowState_BrandNew() {
	namespaceID := uuid.New()
	namespaceName := "namespaceName"
	historyBranch, err := serialization.HistoryBranchToBlob(&persistencespb.HistoryBranch{
		TreeId:    uuid.New(),
		BranchId:  uuid.New(),
		Ancestors: nil,
	})
	s.NoError(err)
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
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:  s.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			},
		},
		RemoteCluster: "test",
	}
	we := commonpb.WorkflowExecution{
		WorkflowId: s.workflowID,
		RunId:      s.runID,
	}
	mockWeCtx := workflow.NewMockContext(s.controller)
	s.mockHistoryCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		namespace.ID(namespaceID),
		we,
		workflow.CallerTypeTask,
	).Return(mockWeCtx, workflow.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		gomock.Any(),
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
	s.mockEventCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), common.FirstEventID, gomock.Any()).Return(fakeStartHistory, nil).AnyTimes()
	err = s.historyReplicator.ApplyWorkflowState(context.Background(), request)
	s.NoError(err)
}

func (s *nDCHistoryReplicatorSuite) Test_ApplyWorkflowState_Ancestors() {
	namespaceID := uuid.New()
	namespaceName := "namespaceName"
	historyBranch, err := serialization.HistoryBranchToBlob(&persistencespb.HistoryBranch{
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
	})
	s.NoError(err)
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
			},
			ExecutionState: &persistencespb.WorkflowExecutionState{
				RunId:  s.runID,
				State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
				Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			},
		},
		RemoteCluster: "test",
	}
	we := commonpb.WorkflowExecution{
		WorkflowId: s.workflowID,
		RunId:      s.runID,
	}
	mockWeCtx := workflow.NewMockContext(s.controller)
	s.mockHistoryCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		namespace.ID(namespaceID),
		we,
		workflow.CallerTypeTask,
	).Return(mockWeCtx, workflow.NoopReleaseFn, nil)
	mockWeCtx.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		gomock.Any(),
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
	s.mockEventCache.EXPECT().GetEvent(gomock.Any(), gomock.Any(), common.FirstEventID, gomock.Any()).Return(fakeStartHistory, nil).AnyTimes()
	err = s.historyReplicator.ApplyWorkflowState(context.Background(), request)
	s.NoError(err)
}

func (s *nDCHistoryReplicatorSuite) Test_ApplyWorkflowState_NoClosedWorkflow_Error() {
	err := s.historyReplicator.ApplyWorkflowState(context.Background(), &historyservice.ReplicateWorkflowStateRequest{
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
