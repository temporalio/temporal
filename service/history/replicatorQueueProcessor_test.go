// Copyright (c) 2017 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

type (
	replicatorQueueProcessorSuite struct {
		currentClusterNamer string
		logger              log.Logger
		mockShard           ShardContext
		mockExecutionMgr    *mocks.ExecutionManager
		mockHistoryV2Mgr    *mocks.HistoryV2Manager
		mockProducer        *mocks.KafkaProducer
		mockDomainCache     *cache.DomainCacheMock
		mockClusterMetadata *mocks.ClusterMetadata
		mockClientBean      *client.MockClientBean
		mockMessagingClient messaging.Client
		mockService         service.Service

		suite.Suite
		replicatorQueueProcessor *replicatorQueueProcessorImpl
	}
)

func TestReplicatorQueueProcessorSuite(t *testing.T) {
	s := new(replicatorQueueProcessorSuite)
	suite.Run(t, s)
}

func (s *replicatorQueueProcessorSuite) SetupSuite() {

}

func (s *replicatorQueueProcessorSuite) TearDownSuite() {

}

func (s *replicatorQueueProcessorSuite) SetupTest() {
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.currentClusterNamer = cluster.TestCurrentClusterName
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockDomainCache = &cache.DomainCacheMock{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, metricsClient, s.mockClientBean, nil, nil, nil)
	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: 0, RangeID: 1, TransferAckLevel: 0},
		clusterMetadata:           s.mockClusterMetadata,
		transferSequenceNumber:    1,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		metricsClient:             metricsClient,
		domainCache:               s.mockDomainCache,
		executionManager:          s.mockExecutionMgr,
		standbyClusterCurrentTime: make(map[string]time.Time),
		timeSource:                clock.NewRealTimeSource(),
	}
	historyCache := newHistoryCache(s.mockShard)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)

	s.replicatorQueueProcessor = newReplicatorQueueProcessor(
		s.mockShard, historyCache, s.mockProducer, s.mockExecutionMgr, s.mockHistoryV2Mgr, s.logger,
	).(*replicatorQueueProcessorImpl)
}

func (s *replicatorQueueProcessorSuite) TearDownTest() {
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockHistoryV2Mgr.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_WorkflowMissing() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	task := &persistence.ReplicationTaskInfo{
		TaskType:    persistence.ReplicationTaskTypeSyncActivity,
		TaskID:      taskID,
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduledID: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	}).Return(nil, &shared.EntityNotExistsError{})
	s.mockDomainCache.On("GetDomainByID", domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: domainID, Name: domainName},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		1234,
		nil,
	), nil)

	_, err := s.replicatorQueueProcessor.process(task, true)
	s.Nil(err)
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_WorkflowCompleted() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	task := &persistence.ReplicationTaskInfo{
		TaskType:    persistence.ReplicationTaskTypeSyncActivity,
		TaskID:      taskID,
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduledID: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	msBuilder := &mockMutableState{}
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	msBuilder.On("StartTransaction", mock.Anything).Return(false, nil).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(false)
	s.mockDomainCache.On("GetDomainByID", domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: domainID, Name: domainName},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		version,
		nil,
	), nil)

	_, err := s.replicatorQueueProcessor.process(task, true)
	s.Nil(err)
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_ActivityCompleted() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	task := &persistence.ReplicationTaskInfo{
		TaskType:    persistence.ReplicationTaskTypeSyncActivity,
		TaskID:      taskID,
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduledID: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	msBuilder := &mockMutableState{}
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	msBuilder.On("StartTransaction", mock.Anything).Return(false, nil).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetActivityInfo", scheduleID).Return(nil, false)
	s.mockDomainCache.On("GetDomainByID", domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: domainID, Name: domainName},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		version,
		nil,
	), nil)

	_, err := s.replicatorQueueProcessor.process(task, true)
	s.Nil(err)
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_ActivityRetry() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	task := &persistence.ReplicationTaskInfo{
		TaskType:    persistence.ReplicationTaskTypeSyncActivity,
		TaskID:      taskID,
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduledID: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	msBuilder := &mockMutableState{}
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)

	activityVersion := int64(333)
	activityScheduleID := scheduleID
	activityScheduledTime := time.Now()
	activityStartedID := common.EmptyEventID
	activityStartedTime := time.Time{}
	activityHeartbeatTime := time.Time{}
	activityAttempt := int32(16384)
	activityDetails := []byte("some random activity progress")
	activityLastFailureReason := "some random reason"
	activityLastWorkerIdentity := "some random worker identity"
	activityLastFailureDetails := []byte("some random failure details")
	msBuilder.On("StartTransaction", mock.Anything).Return(false, nil).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetActivityInfo", scheduleID).Return(&persistence.ActivityInfo{
		Version:                  activityVersion,
		ScheduleID:               activityScheduleID,
		ScheduledTime:            activityScheduledTime,
		StartedID:                activityStartedID,
		StartedTime:              activityStartedTime,
		LastHeartBeatUpdatedTime: activityHeartbeatTime,
		Details:                  activityDetails,
		Attempt:                  activityAttempt,
		LastFailureReason:        activityLastFailureReason,
		LastWorkerIdentity:       activityLastWorkerIdentity,
		LastFailureDetails:       activityLastFailureDetails,
	}, true)
	s.mockDomainCache.On("GetDomainByID", domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: domainID, Name: domainName},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		version,
		nil,
	), nil)

	s.mockProducer.On("Publish", &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskType.Ptr(replicator.ReplicationTaskTypeSyncActivity),
		SyncActicvityTaskAttributes: &replicator.SyncActicvityTaskAttributes{
			DomainId:           common.StringPtr(domainID),
			WorkflowId:         common.StringPtr(workflowID),
			RunId:              common.StringPtr(runID),
			Version:            common.Int64Ptr(activityVersion),
			ScheduledId:        common.Int64Ptr(activityScheduleID),
			ScheduledTime:      common.Int64Ptr(activityScheduledTime.UnixNano()),
			StartedId:          common.Int64Ptr(activityStartedID),
			StartedTime:        nil,
			LastHeartbeatTime:  common.Int64Ptr(activityHeartbeatTime.UnixNano()),
			Details:            activityDetails,
			Attempt:            common.Int32Ptr(activityAttempt),
			LastFailureReason:  common.StringPtr(activityLastFailureReason),
			LastWorkerIdentity: common.StringPtr(activityLastWorkerIdentity),
			LastFailureDetails: activityLastFailureDetails,
		},
	}).Return(nil).Once()

	_, err := s.replicatorQueueProcessor.process(task, true)
	s.Nil(err)
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_ActivityRunning() {
	domainName := "some random domain name"
	domainID := testDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	task := &persistence.ReplicationTaskInfo{
		TaskType:    persistence.ReplicationTaskTypeSyncActivity,
		TaskID:      taskID,
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduledID: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecutionForBackground(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	msBuilder := &mockMutableState{}
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)

	activityVersion := int64(333)
	activityScheduleID := scheduleID
	activityScheduledTime := time.Now()
	activityStartedID := activityScheduleID + 1
	activityStartedTime := activityScheduledTime.Add(time.Minute)
	activityHeartbeatTime := activityStartedTime.Add(time.Minute)
	activityAttempt := int32(16384)
	activityDetails := []byte("some random activity progress")
	activityLastFailureReason := "some random reason"
	activityLastWorkerIdentity := "some random worker identity"
	activityLastFailureDetails := []byte("some random failure details")
	msBuilder.On("StartTransaction", mock.Anything).Return(false, nil).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetActivityInfo", scheduleID).Return(&persistence.ActivityInfo{
		Version:                  activityVersion,
		ScheduleID:               activityScheduleID,
		ScheduledTime:            activityScheduledTime,
		StartedID:                activityStartedID,
		StartedTime:              activityStartedTime,
		LastHeartBeatUpdatedTime: activityHeartbeatTime,
		Details:                  activityDetails,
		Attempt:                  activityAttempt,
		LastFailureReason:        activityLastFailureReason,
		LastWorkerIdentity:       activityLastWorkerIdentity,
		LastFailureDetails:       activityLastFailureDetails,
	}, true)
	s.mockDomainCache.On("GetDomainByID", domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
		&persistence.DomainInfo{ID: domainID, Name: domainName},
		&persistence.DomainConfig{Retention: 1},
		&persistence.DomainReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []*persistence.ClusterReplicationConfig{
				{ClusterName: cluster.TestCurrentClusterName},
				{ClusterName: cluster.TestAlternativeClusterName},
			},
		},
		version,
		nil,
	), nil)
	s.mockProducer.On("Publish", &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskType.Ptr(replicator.ReplicationTaskTypeSyncActivity),
		SyncActicvityTaskAttributes: &replicator.SyncActicvityTaskAttributes{
			DomainId:           common.StringPtr(domainID),
			WorkflowId:         common.StringPtr(workflowID),
			RunId:              common.StringPtr(runID),
			Version:            common.Int64Ptr(activityVersion),
			ScheduledId:        common.Int64Ptr(activityScheduleID),
			ScheduledTime:      common.Int64Ptr(activityScheduledTime.UnixNano()),
			StartedId:          common.Int64Ptr(activityStartedID),
			StartedTime:        common.Int64Ptr(activityStartedTime.UnixNano()),
			LastHeartbeatTime:  common.Int64Ptr(activityHeartbeatTime.UnixNano()),
			Details:            activityDetails,
			Attempt:            common.Int32Ptr(activityAttempt),
			LastFailureReason:  common.StringPtr(activityLastFailureReason),
			LastWorkerIdentity: common.StringPtr(activityLastWorkerIdentity),
			LastFailureDetails: activityLastFailureDetails,
		},
	}).Return(nil).Once()

	_, err := s.replicatorQueueProcessor.process(task, true)
	s.Nil(err)
}

func (s *replicatorQueueProcessorSuite) TestPaginateHistoryWithShardID() {
	firstEventID := int64(133)
	nextEventID := int64(134)
	pageSize := 1
	shardID := common.IntPtr(1)

	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   []byte("asd"),
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      pageSize,
		NextPageToken: []byte{},
		ShardID:       shardID,
	}
	s.mockHistoryV2Mgr.On("ReadHistoryBranch", req).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*workflow.HistoryEvent{
			{
				EventId: common.Int64Ptr(int64(1)),
			},
		},
		NextPageToken:    []byte{},
		Size:             1,
		LastFirstEventID: nextEventID,
	}, nil).Once()
	hEvents, bEvents, token, size, err := PaginateHistory(s.mockHistoryV2Mgr, false, []byte("asd"),
		firstEventID, nextEventID, []byte{}, pageSize, shardID)
	s.NotNil(hEvents)
	s.NotNil(bEvents)
	s.NotNil(token)
	s.Equal(1, size)
	s.NoError(err)
}
