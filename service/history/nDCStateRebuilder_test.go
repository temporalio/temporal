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

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

type (
	nDCStateRebuilderSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shard.ContextTest
		mockEventsCache     *events.MockCache
		mockTaskRefresher   *workflow.MockTaskRefresher
		mockNamespaceCache  *namespace.MockRegistry
		mockClusterMetadata *cluster.MockMetadata

		mockExecutionManager *persistence.MockExecutionManager
		logger               log.Logger

		namespaceID namespace.ID
		workflowID  string
		runID       string
		now         time.Time

		nDCStateRebuilder *nDCStateRebuilderImpl
	}
)

func TestNDCStateRebuilderSuite(t *testing.T) {
	s := new(nDCStateRebuilderSuite)
	suite.Run(t, s)
}

func (s *nDCStateRebuilderSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTaskRefresher = workflow.NewMockTaskRefresher(s.controller)

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
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.MockEventsCache
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	s.workflowID = "some random workflow ID"
	s.runID = uuid.New()
	s.now = time.Now().UTC()
	s.nDCStateRebuilder = newNDCStateRebuilder(
		s.mockShard, s.logger,
	)
	s.nDCStateRebuilder.taskRefresher = s.mockTaskRefresher
}

func (s *nDCStateRebuilderSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *nDCStateRebuilderSuite) TestInitializeBuilders() {
	mutableState, stateBuilder := s.nDCStateRebuilder.initializeBuilders(tests.GlobalNamespaceEntry, s.now)
	s.NotNil(mutableState)
	s.NotNil(stateBuilder)
	s.NotNil(mutableState.GetExecutionInfo().GetVersionHistories())
}

func (s *nDCStateRebuilderSuite) TestApplyEvents() {

	requestID := uuid.New()
	events := []*historypb.HistoryEvent{
		{
			EventId:    1,
			EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{}},
		},
		{
			EventId:    2,
			EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{}},
		},
	}

	workflowKey := definition.NewWorkflowKey(s.namespaceID.String(), s.workflowID, s.runID)

	mockStateBuilder := workflow.NewMockMutableStateRebuilder(s.controller)
	mockStateBuilder.EXPECT().ApplyEvents(
		s.namespaceID,
		requestID,
		commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		events,
		[]*historypb.HistoryEvent(nil),
	).Return(nil, nil)

	err := s.nDCStateRebuilder.applyEvents(workflowKey, mockStateBuilder, events, requestID)
	s.NoError(err)
}

func (s *nDCStateRebuilderSuite) TestPagination() {
	firstEventID := common.FirstEventID
	nextEventID := int64(101)
	branchToken := []byte("some random branch token")

	event1 := &historypb.HistoryEvent{
		EventId:    1,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{}},
	}
	event2 := &historypb.HistoryEvent{
		EventId:    2,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{}},
	}
	event3 := &historypb.HistoryEvent{
		EventId:    3,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{}},
	}
	event4 := &historypb.HistoryEvent{
		EventId:    4,
		EventType:  enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{}},
	}
	event5 := &historypb.HistoryEvent{
		EventId:    5,
		EventType:  enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{}},
	}
	history1 := []*historypb.History{{Events: []*historypb.HistoryEvent{event1, event2, event3}}}
	transactionID1 := int64(10)
	history2 := []*historypb.History{{Events: []*historypb.HistoryEvent{event4, event5}}}
	transactionID2 := int64(20)
	expectedHistory := append(history1, history2...)
	expectedTransactionIDs := []int64{transactionID1, transactionID2}
	pageToken := []byte("some random token")

	shardID := s.mockShard.GetShardID()
	s.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history1,
		TransactionIDs: []int64{transactionID1},
		NextPageToken:  pageToken,
		Size:           12345,
	}, nil)
	s.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: pageToken,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history2,
		TransactionIDs: []int64{transactionID2},
		NextPageToken:  nil,
		Size:           67890,
	}, nil)

	paginationFn := s.nDCStateRebuilder.getPaginationFn(context.Background(), firstEventID, nextEventID, branchToken)
	iter := collection.NewPagingIterator(paginationFn)

	var result []HistoryBlobsPaginationItem
	for iter.HasNext() {
		item, err := iter.Next()
		s.NoError(err)
		result = append(result, item)
	}
	var historyResult []*historypb.History
	var transactionIDsResult []int64
	for _, item := range result {
		historyResult = append(historyResult, item.History)
		transactionIDsResult = append(transactionIDsResult, item.TransactionID)
	}

	s.Equal(expectedHistory, historyResult)
	s.Equal(expectedTransactionIDs, transactionIDsResult)
}

func (s *nDCStateRebuilderSuite) TestRebuild() {
	requestID := uuid.New()
	version := int64(12)
	lastEventID := int64(2)
	branchToken := []byte("other random branch token")
	targetBranchToken := []byte("some other random branch token")

	targetNamespaceID := namespace.ID(uuid.New())
	targetNamespace := namespace.Name("other random namespace name")
	targetWorkflowID := "other random workflow ID"
	targetRunID := uuid.New()

	firstEventID := common.FirstEventID
	nextEventID := lastEventID + 1
	events1 := []*historypb.HistoryEvent{{
		EventId:   1,
		Version:   version,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
			WorkflowType:             &commonpb.WorkflowType{Name: "some random workflow type"},
			TaskQueue:                &taskqueuepb.TaskQueue{Name: "some random workflow type"},
			Input:                    payloads.EncodeString("some random input"),
			WorkflowExecutionTimeout: timestamp.DurationPtr(123 * time.Second),
			WorkflowRunTimeout:       timestamp.DurationPtr(233 * time.Second),
			WorkflowTaskTimeout:      timestamp.DurationPtr(45 * time.Second),
			Identity:                 "some random identity",
		}},
	}}
	events2 := []*historypb.HistoryEvent{{
		EventId:   2,
		Version:   version,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &historypb.WorkflowExecutionSignaledEventAttributes{
			SignalName: "some random signal name",
			Input:      payloads.EncodeString("some random signal input"),
			Identity:   "some random identity",
		}},
	}}
	history1 := []*historypb.History{{Events: events1}}
	history2 := []*historypb.History{{Events: events2}}
	pageToken := []byte("some random pagination token")

	historySize1 := 12345
	historySize2 := 67890
	shardID := s.mockShard.GetShardID()
	s.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history1,
		TransactionIDs: []int64{10},
		NextPageToken:  pageToken,
		Size:           historySize1,
	}, nil)
	expectedLastFirstTransactionID := int64(20)
	s.mockExecutionManager.EXPECT().ReadHistoryBranchByBatch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: pageToken,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:        history2,
		TransactionIDs: []int64{expectedLastFirstTransactionID},
		NextPageToken:  nil,
		Size:           historySize2,
	}, nil)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(targetNamespaceID).Return(namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: targetNamespaceID.String(), Name: targetNamespace.String()},
		&persistencespb.NamespaceConfig{},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1234,
	), nil).AnyTimes()
	s.mockTaskRefresher.EXPECT().RefreshTasks(gomock.Any(), s.now, gomock.Any()).Return(nil)

	rebuildMutableState, rebuiltHistorySize, err := s.nDCStateRebuilder.rebuild(
		context.Background(),
		s.now,
		definition.NewWorkflowKey(s.namespaceID.String(), s.workflowID, s.runID),
		branchToken,
		lastEventID,
		convert.Int64Ptr(version),
		definition.NewWorkflowKey(targetNamespaceID.String(), targetWorkflowID, targetRunID),
		targetBranchToken,
		requestID,
	)
	s.NoError(err)
	s.NotNil(rebuildMutableState)
	rebuildExecutionInfo := rebuildMutableState.GetExecutionInfo()
	s.Equal(targetNamespaceID, namespace.ID(rebuildExecutionInfo.NamespaceId))
	s.Equal(targetWorkflowID, rebuildExecutionInfo.WorkflowId)
	s.Equal(targetRunID, rebuildMutableState.GetExecutionState().RunId)
	s.Equal(int64(historySize1+historySize2), rebuiltHistorySize)
	s.Equal(versionhistory.NewVersionHistories(
		versionhistory.NewVersionHistory(
			targetBranchToken,
			[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID, version)},
		),
	), rebuildMutableState.GetExecutionInfo().GetVersionHistories())
	s.Equal(timestamp.TimeValue(rebuildMutableState.GetExecutionInfo().StartTime), s.now)
	s.Equal(expectedLastFirstTransactionID, rebuildExecutionInfo.LastFirstEventTxnId)
}
