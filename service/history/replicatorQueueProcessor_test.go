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

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/temporalio/temporal/.gen/go/replicator"
	"github.com/temporalio/temporal/.gen/go/shared"
	workflow "github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	"github.com/temporalio/temporal/common/messaging"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service"
)

type (
	replicatorQueueProcessorSuite struct {
		suite.Suite
		*require.Assertions

		controller       *gomock.Controller
		mockDomainCache  *cache.MockDomainCache
		mockMutableState *MockmutableState

		logger              log.Logger
		mockShard           ShardContext
		mockExecutionMgr    *mocks.ExecutionManager
		mockHistoryV2Mgr    *mocks.HistoryV2Manager
		mockProducer        *mocks.KafkaProducer
		mockClusterMetadata *mocks.ClusterMetadata
		mockMessagingClient messaging.Client
		mockService         service.Service

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
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockDomainCache = cache.NewMockDomainCache(s.controller)
	s.mockMutableState = NewMockmutableState(s.controller)

	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockProducer = &mocks.KafkaProducer{}

	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, metricsClient, nil, nil, nil, nil)
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
	s.controller.Finish()
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
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
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
	), nil).AnyTimes()

	_, err := s.replicatorQueueProcessor.process(newTaskInfo(nil, task, s.logger))
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
	context.(*workflowExecutionContextImpl).mutableState = s.mockMutableState
	release(nil)
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil).Times(1)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
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
	), nil).AnyTimes()

	_, err := s.replicatorQueueProcessor.process(newTaskInfo(nil, task, s.logger))
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

	context.(*workflowExecutionContextImpl).mutableState = s.mockMutableState
	release(nil)
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil).Times(1)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(nil, false).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
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
	), nil).AnyTimes()

	_, err := s.replicatorQueueProcessor.process(newTaskInfo(nil, task, s.logger))
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

	context.(*workflowExecutionContextImpl).mutableState = s.mockMutableState
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
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil).Times(1)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(&persistence.ActivityInfo{
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
	}, true).AnyTimes()
	versionHistory := &persistence.VersionHistory{
		BranchToken: []byte{},
		Items: []*persistence.VersionHistoryItem{
			{
				EventID: scheduleID,
				Version: 333,
			},
		},
	}
	versionHistories := &persistence.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*persistence.VersionHistory{
			versionHistory,
		},
	}
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
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
	), nil).AnyTimes()

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
			VersionHistory:     versionHistory.ToThrift(),
		},
	}).Return(nil).Once()

	_, err := s.replicatorQueueProcessor.process(newTaskInfo(nil, task, s.logger))
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

	context.(*workflowExecutionContextImpl).mutableState = s.mockMutableState
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
	s.mockMutableState.EXPECT().StartTransaction(gomock.Any()).Return(false, nil).Times(1)
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduleID).Return(&persistence.ActivityInfo{
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
	}, true).AnyTimes()
	versionHistory := &persistence.VersionHistory{
		BranchToken: []byte{},
		Items: []*persistence.VersionHistoryItem{
			{
				EventID: scheduleID,
				Version: 333,
			},
		},
	}
	versionHistories := &persistence.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*persistence.VersionHistory{
			versionHistory,
		},
	}
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockDomainCache.EXPECT().GetDomainByID(domainID).Return(cache.NewGlobalDomainCacheEntryForTest(
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
	), nil).AnyTimes()
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
			VersionHistory:     versionHistory.ToThrift(),
		},
	}).Return(nil).Once()

	_, err := s.replicatorQueueProcessor.process(newTaskInfo(nil, task, s.logger))
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
