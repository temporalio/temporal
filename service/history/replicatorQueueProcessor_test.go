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
	"os"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

type (
	replicatorQueueProcessorSuite struct {
		currentClusterNamer string
		logger              bark.Logger
		mockShard           ShardContext
		mockExecutionMgr    *mocks.ExecutionManager
		mockHistoryMgr      *mocks.HistoryManager
		mockHistoryV2Mgr    *mocks.HistoryV2Manager
		mockProducer        *mocks.KafkaProducer
		mockMetadataMgr     *mocks.MetadataManager
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
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

}

func (s *replicatorQueueProcessorSuite) TearDownSuite() {

}

func (s *replicatorQueueProcessorSuite) SetupTest() {
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.currentClusterNamer = cluster.TestCurrentClusterName
	log2 := log.New()
	log2.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(log2)
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockHistoryMgr = &mocks.HistoryManager{}
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockMetadataMgr = &mocks.MetadataManager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, metricsClient, s.mockClientBean, s.logger)
	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: 0, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		metricsClient:             metricsClient,
		domainCache:               cache.NewDomainCache(s.mockMetadataMgr, s.mockClusterMetadata, metricsClient, s.logger),
		executionManager:          s.mockExecutionMgr,
		standbyClusterCurrentTime: make(map[string]time.Time),
	}
	historyCache := newHistoryCache(s.mockShard)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)

	s.replicatorQueueProcessor = newReplicatorQueueProcessor(
		s.mockShard, historyCache, s.mockProducer, s.mockExecutionMgr, s.mockHistoryMgr, s.mockHistoryV2Mgr, s.logger,
	).(*replicatorQueueProcessorImpl)
}

func (s *replicatorQueueProcessorSuite) TearDownTest() {
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockHistoryV2Mgr.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_WorkflowMissing() {
	domainID := validDomainID
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

	_, err := s.replicatorQueueProcessor.process(task, true)
	s.Nil(err)
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_WorkflowCompleted() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	nextEventID := int64(133)
	task := &persistence.ReplicationTaskInfo{
		TaskType:    persistence.ReplicationTaskTypeSyncActivity,
		TaskID:      taskID,
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduledID: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecution(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	msBuilder := &mockMutableState{}
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{
		CurrentVersion:   version,
		StartVersion:     version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	})
	msBuilder.On("IsWorkflowExecutionRunning").Return(false)

	_, err := s.replicatorQueueProcessor.process(task, true)
	s.Nil(err)
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_ActivityCompleted() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	nextEventID := int64(133)
	task := &persistence.ReplicationTaskInfo{
		TaskType:    persistence.ReplicationTaskTypeSyncActivity,
		TaskID:      taskID,
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduledID: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecution(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	msBuilder := &mockMutableState{}
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)

	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{
		CurrentVersion:   version,
		StartVersion:     version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	})
	msBuilder.On("GetLastWriteVersion").Return(version)
	msBuilder.On("UpdateReplicationStateVersion", version, false).Once()
	msBuilder.On("GetActivityInfo", scheduleID).Return(nil, false)
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			FailoverVersion: version,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()

	_, err := s.replicatorQueueProcessor.process(task, true)
	s.Nil(err)
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_ActivityRetry() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	nextEventID := int64(133)
	task := &persistence.ReplicationTaskInfo{
		TaskType:    persistence.ReplicationTaskTypeSyncActivity,
		TaskID:      taskID,
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduledID: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecution(
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

	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{
		CurrentVersion:   version,
		StartVersion:     version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	})
	msBuilder.On("GetLastWriteVersion").Return(version)
	msBuilder.On("UpdateReplicationStateVersion", version, false).Once()
	msBuilder.On("GetActivityInfo", scheduleID).Return(&persistence.ActivityInfo{
		Version:                  activityVersion,
		ScheduleID:               activityScheduleID,
		ScheduledTime:            activityScheduledTime,
		StartedID:                activityStartedID,
		StartedTime:              activityStartedTime,
		LastHeartBeatUpdatedTime: activityHeartbeatTime,
		Details:                  activityDetails,
		Attempt:                  activityAttempt,
	}, true)
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			FailoverVersion: version,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockProducer.On("Publish", &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskType.Ptr(replicator.ReplicationTaskTypeSyncActivity),
		SyncActicvityTaskAttributes: &replicator.SyncActicvityTaskAttributes{
			DomainId:          common.StringPtr(domainID),
			WorkflowId:        common.StringPtr(workflowID),
			RunId:             common.StringPtr(runID),
			Version:           common.Int64Ptr(activityVersion),
			ScheduledId:       common.Int64Ptr(activityScheduleID),
			ScheduledTime:     common.Int64Ptr(activityScheduledTime.UnixNano()),
			StartedId:         common.Int64Ptr(activityStartedID),
			StartedTime:       nil,
			LastHeartbeatTime: nil,
			Details:           activityDetails,
			Attempt:           common.Int32Ptr(activityAttempt),
		},
	}).Return(nil).Once()

	_, err := s.replicatorQueueProcessor.process(task, true)
	s.Nil(err)
}

func (s *replicatorQueueProcessorSuite) TestSyncActivity_ActivityRunning() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	taskID := int64(1444)
	version := int64(2333)
	nextEventID := int64(133)
	task := &persistence.ReplicationTaskInfo{
		TaskType:    persistence.ReplicationTaskTypeSyncActivity,
		TaskID:      taskID,
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       runID,
		ScheduledID: scheduleID,
	}
	s.mockExecutionMgr.On("CompleteReplicationTask", &persistence.CompleteReplicationTaskRequest{TaskID: taskID}).Return(nil).Once()

	context, release, _ := s.replicatorQueueProcessor.historyCache.getOrCreateWorkflowExecution(
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

	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{
		CurrentVersion:   version,
		StartVersion:     version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	})
	msBuilder.On("GetLastWriteVersion").Return(version)
	msBuilder.On("UpdateReplicationStateVersion", version, false).Once()
	msBuilder.On("GetActivityInfo", scheduleID).Return(&persistence.ActivityInfo{
		Version:                  activityVersion,
		ScheduleID:               activityScheduleID,
		ScheduledTime:            activityScheduledTime,
		StartedID:                activityStartedID,
		StartedTime:              activityStartedTime,
		LastHeartBeatUpdatedTime: activityHeartbeatTime,
		Details:                  activityDetails,
		Attempt:                  activityAttempt,
	}, true)
	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster.TestCurrentClusterName},
				},
			},
			FailoverVersion: version,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockProducer.On("Publish", &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskType.Ptr(replicator.ReplicationTaskTypeSyncActivity),
		SyncActicvityTaskAttributes: &replicator.SyncActicvityTaskAttributes{
			DomainId:          common.StringPtr(domainID),
			WorkflowId:        common.StringPtr(workflowID),
			RunId:             common.StringPtr(runID),
			Version:           common.Int64Ptr(activityVersion),
			ScheduledId:       common.Int64Ptr(activityScheduleID),
			ScheduledTime:     common.Int64Ptr(activityScheduledTime.UnixNano()),
			StartedId:         common.Int64Ptr(activityStartedID),
			StartedTime:       common.Int64Ptr(activityStartedTime.UnixNano()),
			LastHeartbeatTime: common.Int64Ptr(activityHeartbeatTime.UnixNano()),
			Details:           activityDetails,
			Attempt:           common.Int32Ptr(activityAttempt),
		},
	}).Return(nil).Once()

	_, err := s.replicatorQueueProcessor.process(task, true)
	s.Nil(err)
}
