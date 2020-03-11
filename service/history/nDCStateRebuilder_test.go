// Copyright (c) 2019 Uber Technologies, Inc.
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
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/collection"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	nDCStateRebuilderSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shardContextTest
		mockEventsCache     *MockeventsCache
		mockTaskRefresher   *MockmutableStateTaskRefresher
		mockDomainCache     *cache.MockDomainCache
		mockClusterMetadata *cluster.MockMetadata

		mockHistoryV2Mgr *mocks.HistoryV2Manager
		logger           log.Logger

		domainID   string
		workflowID string
		runID      string

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
	s.mockTaskRefresher = NewMockmutableStateTaskRefresher(s.controller)

	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardID:          10,
				RangeID:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)

	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr
	s.mockDomainCache = s.mockShard.resource.DomainCache
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.mockEventsCache
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockEventsCache.EXPECT().putEvent(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	s.workflowID = "some random workflow ID"
	s.runID = uuid.New()
	s.nDCStateRebuilder = newNDCStateRebuilder(
		s.mockShard, s.logger,
	)
	s.nDCStateRebuilder.taskRefresher = s.mockTaskRefresher
}

func (s *nDCStateRebuilderSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *nDCStateRebuilderSuite) TestInitializeBuilders() {
	mutableState, stateBuilder := s.nDCStateRebuilder.initializeBuilders(testGlobalDomainEntry)
	s.NotNil(mutableState)
	s.NotNil(stateBuilder)
	s.NotNil(mutableState.GetVersionHistories())
}

func (s *nDCStateRebuilderSuite) TestApplyEvents() {

	requestID := uuid.New()
	events := []*commonproto.HistoryEvent{
		{
			EventId:    1,
			EventType:  enums.EventTypeWorkflowExecutionStarted,
			Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &commonproto.WorkflowExecutionStartedEventAttributes{}},
		},
		{
			EventId:    2,
			EventType:  enums.EventTypeWorkflowExecutionSignaled,
			Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{}},
		},
	}

	workflowIdentifier := definition.NewWorkflowIdentifier(s.domainID, s.workflowID, s.runID)

	mockStateBuilder := NewMockstateBuilder(s.controller)
	mockStateBuilder.EXPECT().applyEvents(
		s.domainID,
		requestID,
		commonproto.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.runID,
		},
		events,
		[]*commonproto.HistoryEvent(nil),
		true,
	).Return(nil, nil).Times(1)

	err := s.nDCStateRebuilder.applyEvents(workflowIdentifier, mockStateBuilder, events, requestID)
	s.NoError(err)
}

func (s *nDCStateRebuilderSuite) TestPagination() {
	firstEventID := common.FirstEventID
	nextEventID := int64(101)
	branchToken := []byte("some random branch token")
	workflowIdentifier := definition.NewWorkflowIdentifier(s.domainID, s.workflowID, s.runID)

	event1 := &commonproto.HistoryEvent{
		EventId:    1,
		EventType:  enums.EventTypeWorkflowExecutionStarted,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &commonproto.WorkflowExecutionStartedEventAttributes{}},
	}
	event2 := &commonproto.HistoryEvent{
		EventId:    2,
		EventType:  enums.EventTypeDecisionTaskScheduled,
		Attributes: &commonproto.HistoryEvent_DecisionTaskScheduledEventAttributes{DecisionTaskScheduledEventAttributes: &commonproto.DecisionTaskScheduledEventAttributes{}},
	}
	event3 := &commonproto.HistoryEvent{
		EventId:    3,
		EventType:  enums.EventTypeDecisionTaskStarted,
		Attributes: &commonproto.HistoryEvent_DecisionTaskStartedEventAttributes{DecisionTaskStartedEventAttributes: &commonproto.DecisionTaskStartedEventAttributes{}},
	}
	event4 := &commonproto.HistoryEvent{
		EventId:    4,
		EventType:  enums.EventTypeDecisionTaskCompleted,
		Attributes: &commonproto.HistoryEvent_DecisionTaskCompletedEventAttributes{DecisionTaskCompletedEventAttributes: &commonproto.DecisionTaskCompletedEventAttributes{}},
	}
	event5 := &commonproto.HistoryEvent{
		EventId:    5,
		EventType:  enums.EventTypeActivityTaskScheduled,
		Attributes: &commonproto.HistoryEvent_ActivityTaskScheduledEventAttributes{ActivityTaskScheduledEventAttributes: &commonproto.ActivityTaskScheduledEventAttributes{}},
	}
	history1 := []*commonproto.History{{[]*commonproto.HistoryEvent{event1, event2, event3}}}
	history2 := []*commonproto.History{{[]*commonproto.HistoryEvent{event4, event5}}}
	history := append(history1, history2...)
	pageToken := []byte("some random token")

	shardId := s.mockShard.GetShardID()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       &shardId,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       history1,
		NextPageToken: pageToken,
		Size:          12345,
	}, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: pageToken,
		ShardID:       &shardId,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       history2,
		NextPageToken: nil,
		Size:          67890,
	}, nil).Once()

	paginationFn := s.nDCStateRebuilder.getPaginationFn(workflowIdentifier, firstEventID, nextEventID, branchToken)
	iter := collection.NewPagingIterator(paginationFn)

	var result []*commonproto.History
	for iter.HasNext() {
		item, err := iter.Next()
		s.NoError(err)
		result = append(result, item.(*commonproto.History))
	}

	s.Equal(history, result)
}

func (s *nDCStateRebuilderSuite) TestRebuild() {
	requestID := uuid.New()
	version := int64(12)
	lastEventID := int64(2)
	branchToken := []byte("other random branch token")
	targetBranchToken := []byte("some other random branch token")
	now := time.Now()

	targetDomainID := uuid.New()
	targetDomainName := "other random domain name"
	targetWorkflowID := "other random workflow ID"
	targetRunID := uuid.New()

	firstEventID := common.FirstEventID
	nextEventID := lastEventID + 1
	events1 := []*commonproto.HistoryEvent{{
		EventId:   1,
		Version:   version,
		EventType: enums.EventTypeWorkflowExecutionStarted,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &commonproto.WorkflowExecutionStartedEventAttributes{
			WorkflowType:                        &commonproto.WorkflowType{Name: "some random workflow type"},
			TaskList:                            &commonproto.TaskList{Name: "some random workflow type"},
			Input:                               []byte("some random input"),
			ExecutionStartToCloseTimeoutSeconds: 123,
			TaskStartToCloseTimeoutSeconds:      233,
			Identity:                            "some random identity",
		}},
	}}
	events2 := []*commonproto.HistoryEvent{{
		EventId:   2,
		Version:   version,
		EventType: enums.EventTypeWorkflowExecutionSignaled,
		Attributes: &commonproto.HistoryEvent_WorkflowExecutionSignaledEventAttributes{WorkflowExecutionSignaledEventAttributes: &commonproto.WorkflowExecutionSignaledEventAttributes{
			SignalName: "some random signal name",
			Input:      []byte("some random signal input"),
			Identity:   "some random identity",
		}},
	}}
	history1 := []*commonproto.History{{events1}}
	history2 := []*commonproto.History{{events2}}
	pageToken := []byte("some random pagination token")

	historySize1 := 12345
	historySize2 := 67890
	shardId := s.mockShard.GetShardID()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       &shardId,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       history1,
		NextPageToken: pageToken,
		Size:          historySize1,
	}, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: pageToken,
		ShardID:       &shardId,
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       history2,
		NextPageToken: nil,
		Size:          historySize2,
	}, nil).Once()

	s.mockDomainCache.EXPECT().GetDomainByID(targetDomainID).Return(cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: targetDomainID, Name: targetDomainName},
		&persistence.DomainConfig{},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		1234,
		s.mockClusterMetadata,
	), nil).AnyTimes()
	s.mockTaskRefresher.EXPECT().refreshTasks(now, gomock.Any()).Return(nil).Times(1)

	rebuildMutableState, rebuiltHistorySize, err := s.nDCStateRebuilder.rebuild(
		context.Background(),
		now,
		definition.NewWorkflowIdentifier(s.domainID, s.workflowID, s.runID),
		branchToken,
		lastEventID,
		version,
		definition.NewWorkflowIdentifier(targetDomainID, targetWorkflowID, targetRunID),
		targetBranchToken,
		requestID,
	)
	s.NoError(err)
	s.NotNil(rebuildMutableState)
	rebuildExecutionInfo := rebuildMutableState.GetExecutionInfo()
	s.Equal(targetDomainID, rebuildExecutionInfo.DomainID)
	s.Equal(targetWorkflowID, rebuildExecutionInfo.WorkflowID)
	s.Equal(targetRunID, rebuildExecutionInfo.RunID)
	s.Equal(int64(historySize1+historySize2), rebuiltHistorySize)
	s.Equal(persistence.NewVersionHistories(
		persistence.NewVersionHistory(
			targetBranchToken,
			[]*persistence.VersionHistoryItem{persistence.NewVersionHistoryItem(lastEventID, version)},
		),
	), rebuildMutableState.GetVersionHistories())
	s.Equal(rebuildMutableState.GetExecutionInfo().StartTimestamp, now)
}
