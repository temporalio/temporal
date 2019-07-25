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
	ctx "context"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

type (
	nDCStateRebuilderSuite struct {
		suite.Suite

		logger              log.Logger
		mockExecutionMgr    *mocks.ExecutionManager
		mockHistoryV2Mgr    *mocks.HistoryV2Manager
		mockShardManager    *mocks.ShardManager
		mockClusterMetadata *mocks.ClusterMetadata
		mockProducer        *mocks.KafkaProducer
		mockMetadataMgr     *mocks.MetadataManager
		mockMessagingClient messaging.Client
		mockService         service.Service
		mockShard           *shardContextImpl
		mockDomainCache     *cache.DomainCacheMock
		mockClientBean      *client.MockClientBean
		mockEventsCache     *MockEventsCache

		mockMutableState *mockMutableState
		domainID         string
		workflowID       string
		runID            string

		nDCStateRebuilder *nDCStateRebuilderImpl
	}
)

func TestNDCStateRebuilderSuite(t *testing.T) {
	s := new(nDCStateRebuilderSuite)
	suite.Run(t, s)
}

func (s *nDCStateRebuilderSuite) SetupTest() {
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockShardManager = &mocks.ShardManager{}
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockMetadataMgr = &mocks.MetadataManager{}
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, metricsClient, s.mockClientBean)
	s.mockDomainCache = &cache.DomainCacheMock{}
	s.mockEventsCache = &MockEventsCache{}

	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: 10, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		historyV2Mgr:              s.mockHistoryV2Mgr,
		shardManager:              s.mockShardManager,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		domainCache:               s.mockDomainCache,
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
		eventsCache:               s.mockEventsCache,
		timeSource:                clock.NewRealTimeSource(),
	}
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

	s.domainID = uuid.New()
	s.workflowID = "some random workflow ID"
	s.runID = uuid.New()
	s.mockMutableState = &mockMutableState{}
	s.nDCStateRebuilder = newNDCStateRebuilder(
		s.mockShard, s.mockMutableState, s.logger,
	)
}

func (s *nDCStateRebuilderSuite) TearDownTest() {
	s.mockHistoryV2Mgr.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockShardManager.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockMetadataMgr.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
	s.mockDomainCache.AssertExpectations(s.T())
	s.mockEventsCache.AssertExpectations(s.T())

	s.mockMutableState.AssertExpectations(s.T())
}

func (s *nDCStateRebuilderSuite) TestInitializeBuilders() {
	version := int64(123)
	mutableState, stateBuilder := s.nDCStateRebuilder.initializeBuilders(version)
	s.NotNil(mutableState)
	s.NotNil(stateBuilder)
	s.NotNil(mutableState.GetVersionHistories())
}

func (s *nDCStateRebuilderSuite) TestApplyEvents() {

	requestID := uuid.New()
	events := []*shared.HistoryEvent{
		{
			EventId:                                 common.Int64Ptr(1),
			EventType:                               shared.EventTypeWorkflowExecutionStarted.Ptr(),
			WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{},
		},
		{
			EventId:                                  common.Int64Ptr(2),
			EventType:                                shared.EventTypeWorkflowExecutionSignaled.Ptr(),
			WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{},
		},
	}

	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   s.domainID,
		WorkflowID: s.workflowID,
		RunID:      s.runID,
	})

	mockStateBuilder := &mockStateBuilder{}
	defer mockStateBuilder.AssertExpectations(s.T())
	mockStateBuilder.On("applyEvents",
		s.domainID,
		requestID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(s.workflowID),
			RunId:      common.StringPtr(s.runID),
		},
		events,
		[]*shared.HistoryEvent(nil),
		int32(nDCMutableStateEventStoreVersion),
		int32(nDCMutableStateEventStoreVersion),
	).Return(nil, nil, nil, nil).Once()

	err := s.nDCStateRebuilder.applyEvents(mockStateBuilder, events, requestID)
	s.NoError(err)
}

func (s *nDCStateRebuilderSuite) TestPagination() {
	firstEventID := common.FirstEventID
	nextEventID := int64(101)
	branchToken := []byte("some random branch token")
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   s.domainID,
		WorkflowID: s.workflowID,
		RunID:      s.runID,
	})

	event1 := &shared.HistoryEvent{
		EventId:                                 common.Int64Ptr(1),
		WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{},
	}
	event2 := &shared.HistoryEvent{
		EventId:                              common.Int64Ptr(2),
		DecisionTaskScheduledEventAttributes: &shared.DecisionTaskScheduledEventAttributes{},
	}
	event3 := &shared.HistoryEvent{
		EventId:                            common.Int64Ptr(3),
		DecisionTaskStartedEventAttributes: &shared.DecisionTaskStartedEventAttributes{},
	}
	event4 := &shared.HistoryEvent{
		EventId:                              common.Int64Ptr(4),
		DecisionTaskCompletedEventAttributes: &shared.DecisionTaskCompletedEventAttributes{},
	}
	event5 := &shared.HistoryEvent{
		EventId:                              common.Int64Ptr(5),
		ActivityTaskScheduledEventAttributes: &shared.ActivityTaskScheduledEventAttributes{},
	}
	history1 := []*shared.History{{[]*shared.HistoryEvent{event1, event2, event3}}}
	history2 := []*shared.History{{[]*shared.HistoryEvent{event4, event5}}}
	history := append(history1, history2...)
	pageToken := []byte("some random token")

	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       common.IntPtr(s.mockShard.GetShardID()),
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
		ShardID:       common.IntPtr(s.mockShard.GetShardID()),
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       history2,
		NextPageToken: nil,
		Size:          67890,
	}, nil).Once()

	paginationFn := s.nDCStateRebuilder.getPaginationFn(common.FirstEventID, nextEventID, branchToken)
	iter := collection.NewPagingIterator(paginationFn)

	result := []*shared.History{}
	for iter.HasNext() {
		item, err := iter.Next()
		s.NoError(err)
		result = append(result, item.(*shared.History))
	}

	s.Equal(history, result)
}

func (s *nDCStateRebuilderSuite) TestRebuild() {
	requestID := uuid.New()
	version := int64(12)

	branchToken0 := []byte("some random branch token")
	lastEventID0 := int64(5)
	versionHistory0 := persistence.NewVersionHistory(
		branchToken0,
		[]*persistence.VersionHistoryItem{persistence.NewVersionHistoryItem(lastEventID0, version)},
	)
	branchToken1 := []byte("other random branch token")
	lastEventID1 := int64(2)
	versionHistory1 := persistence.NewVersionHistory(
		branchToken1,
		[]*persistence.VersionHistoryItem{persistence.NewVersionHistoryItem(lastEventID1, version)},
	)
	versionHistories := persistence.NewVersionHistories(versionHistory0)
	_, _, err := versionHistories.AddVersionHistory(versionHistory1)
	s.NoError(err)
	s.mockMutableState.On("GetVersionHistories").Return(versionHistories)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   s.domainID,
		WorkflowID: s.workflowID,
		RunID:      s.runID,
	})

	firstEventID := common.FirstEventID
	nextEventID := lastEventID1 + 1
	events1 := []*shared.HistoryEvent{{
		EventId:   common.Int64Ptr(1),
		Version:   common.Int64Ptr(version),
		EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
		WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
			WorkflowType:                        &shared.WorkflowType{Name: common.StringPtr("some random workflow type")},
			TaskList:                            &shared.TaskList{Name: common.StringPtr("some random workflow type")},
			Input:                               []byte("some random input"),
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(123),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(233),
			Identity:                            common.StringPtr("some random identity"),
		},
	}}
	events2 := []*shared.HistoryEvent{{
		EventId:   common.Int64Ptr(2),
		Version:   common.Int64Ptr(version),
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr("some random signal name"),
			Input:      []byte("some random signal input"),
			Identity:   common.StringPtr("some random identity"),
		},
	}}
	history1 := []*shared.History{{events1}}
	history2 := []*shared.History{{events2}}
	pageToken := []byte("some random pagination token")

	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken1,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       common.IntPtr(s.mockShard.GetShardID()),
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       history1,
		NextPageToken: pageToken,
		Size:          12345,
	}, nil).Once()
	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken1,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: pageToken,
		ShardID:       common.IntPtr(s.mockShard.GetShardID()),
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       history2,
		NextPageToken: nil,
		Size:          12345,
	}, nil).Once()

	s.mockDomainCache.On("GetDomainByID", s.domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: s.domainID},
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
	), nil)

	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
	rebuildMutableState, err := s.nDCStateRebuilder.rebuild(ctx.Background(), 1, requestID)
	s.NoError(err)
	s.NotNil(rebuildMutableState)
	s.Equal(versionHistories, rebuildMutableState.GetVersionHistories())
	s.Equal(1, versionHistories.GetCurrentBranchIndex())
}

func (s *nDCStateRebuilderSuite) TestPrepareMutableState_NoRebuild() {
	branchToken := []byte("some random branch token")
	lastEventID := int64(2)
	version := int64(12)
	versionHistoryItem := persistence.NewVersionHistoryItem(lastEventID, version)
	versionHistory := persistence.NewVersionHistory(
		branchToken,
		[]*persistence.VersionHistoryItem{versionHistoryItem},
	)
	versionHistories := persistence.NewVersionHistories(versionHistory)
	s.mockMutableState.On("GetVersionHistories").Return(versionHistories)

	rebuildMutableState, isRebuilt, err := s.nDCStateRebuilder.prepareMutableState(ctx.Background(), 0, version)
	s.NoError(err)
	s.False(isRebuilt)
	s.Equal(s.mockMutableState, rebuildMutableState)
}

func (s *nDCStateRebuilderSuite) TestPrepareMutableState_Rebuild() {
	version := int64(12)
	incomingVersion := version + 1

	// current branch
	branchToken0 := []byte("some random branch token")
	lastEventID0 := int64(2)

	versionHistoryItem0 := persistence.NewVersionHistoryItem(lastEventID0, version)
	versionHistory0 := persistence.NewVersionHistory(
		branchToken0,
		[]*persistence.VersionHistoryItem{versionHistoryItem0},
	)

	// stale branch, used for rebuild
	branchToken1 := []byte("other random branch token")
	lastEventID1 := lastEventID0 - 1
	versionHistoryItem1 := persistence.NewVersionHistoryItem(lastEventID1, version)
	versionHistory1 := persistence.NewVersionHistory(
		branchToken1,
		[]*persistence.VersionHistoryItem{versionHistoryItem1},
	)

	versionHistories := persistence.NewVersionHistories(versionHistory0)
	_, _, err := versionHistories.AddVersionHistory(versionHistory1)
	s.Nil(err)

	s.mockMutableState.On("GetVersionHistories").Return(versionHistories)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   s.domainID,
		WorkflowID: s.workflowID,
		RunID:      s.runID,
	})

	firstEventID := common.FirstEventID
	nextEventID := lastEventID1 + 1
	events := []*shared.HistoryEvent{{
		EventId:   common.Int64Ptr(1),
		Version:   common.Int64Ptr(version),
		EventType: shared.EventTypeWorkflowExecutionStarted.Ptr(),
		WorkflowExecutionStartedEventAttributes: &shared.WorkflowExecutionStartedEventAttributes{
			WorkflowType:                        &shared.WorkflowType{Name: common.StringPtr("some random workflow type")},
			TaskList:                            &shared.TaskList{Name: common.StringPtr("some random workflow type")},
			Input:                               []byte("some random input"),
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(123),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(233),
			Identity:                            common.StringPtr("some random identity"),
		},
	}}
	history := []*shared.History{{events}}

	s.mockHistoryV2Mgr.On("ReadHistoryBranchByBatch", &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken1,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      nDCDefaultPageSize,
		NextPageToken: nil,
		ShardID:       common.IntPtr(s.mockShard.GetShardID()),
	}).Return(&persistence.ReadHistoryBranchByBatchResponse{
		History:       history,
		NextPageToken: nil,
		Size:          12345,
	}, nil).Once()

	s.mockDomainCache.On("GetDomainByID", s.domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: s.domainID},
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
	), nil)

	s.mockEventsCache.On("putEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
	rebuildMutableState, isRebuilt, err := s.nDCStateRebuilder.prepareMutableState(ctx.Background(), 1, incomingVersion)
	s.NoError(err)
	s.NotNil(rebuildMutableState)
	s.True(isRebuilt)
	s.Equal(versionHistories, rebuildMutableState.GetVersionHistories())
}
