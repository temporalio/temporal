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
	ctx "context"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/shared"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

type (
	historyReplicatorSuite struct {
		suite.Suite
		logger              bark.Logger
		mockExecutionMgr    *mocks.ExecutionManager
		mockHistoryMgr      *mocks.HistoryManager
		mockHistoryV2Mgr    *mocks.HistoryV2Manager
		mockShardManager    *mocks.ShardManager
		mockClusterMetadata *mocks.ClusterMetadata
		mockProducer        *mocks.KafkaProducer
		mockMetadataMgr     *mocks.MetadataManager
		mockMessagingClient messaging.Client
		mockService         service.Service
		mockShard           *shardContextImpl
		mockMutableState    *mockMutableState
		mockTxProcessor     *MockTransferQueueProcessor
		mockTimerProcessor  *MockTimerQueueProcessor
		mockClientBean      *client.MockClientBean
		mockWorkflowResetor *mockWorkflowResetor

		historyReplicator *historyReplicator
	}
)

func TestHistoryReplicatorSuite(t *testing.T) {
	s := new(historyReplicatorSuite)
	suite.Run(t, s)
}

func (s *historyReplicatorSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

}

func (s *historyReplicatorSuite) TearDownSuite() {

}

func (s *historyReplicatorSuite) SetupTest() {
	log2 := log.New()
	log2.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(log2)
	s.mockHistoryMgr = &mocks.HistoryManager{}
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockShardManager = &mocks.ShardManager{}
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockMetadataMgr = &mocks.MetadataManager{}
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, metricsClient, s.mockClientBean, s.logger)

	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: 0, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		shardManager:              s.mockShardManager,
		historyMgr:                s.mockHistoryMgr,
		historyV2Mgr:              s.mockHistoryV2Mgr,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		domainCache:               cache.NewDomainCache(s.mockMetadataMgr, s.mockClusterMetadata, metricsClient, s.logger),
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
		standbyClusterCurrentTime: make(map[string]time.Time),
	}
	s.mockTxProcessor = &MockTransferQueueProcessor{}
	s.mockTimerProcessor = &MockTimerQueueProcessor{}

	s.mockMutableState = &mockMutableState{}
	historyCache := newHistoryCache(s.mockShard)
	s.mockClusterMetadata.On("IsGlobalDomainEnabled").Return(true)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	h := &historyEngineImpl{
		currentClusterName: s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:              s.mockShard,
		historyMgr:         s.mockHistoryMgr,
		executionManager:   s.mockExecutionMgr,
		historyCache:       historyCache,
		logger:             s.logger,
		tokenSerializer:    common.NewJSONTaskTokenSerializer(),
		metricsClient:      s.mockShard.GetMetricsClient(),
		txProcessor:        s.mockTxProcessor,
		timerProcessor:     s.mockTimerProcessor,
	}
	s.historyReplicator = newHistoryReplicator(s.mockShard, h, historyCache, s.mockShard.domainCache, s.mockHistoryMgr, s.mockHistoryV2Mgr, s.logger)
	s.mockWorkflowResetor = &mockWorkflowResetor{}
	s.historyReplicator.resetor = s.mockWorkflowResetor
}

func (s *historyReplicatorSuite) TearDownTest() {
	s.historyReplicator = nil
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockShardManager.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockMetadataMgr.AssertExpectations(s.T())
	s.mockTxProcessor.AssertExpectations(s.T())
	s.mockTimerProcessor.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
	s.mockWorkflowResetor.AssertExpectations(s.T())
}

func (s *historyReplicatorSuite) TestSyncActivity_WorkflowNotFound() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	request := &h.SyncActivityRequest{
		DomainId:   common.StringPtr(domainID),
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	}).Return(nil, &shared.EntityNotExistsError{})

	err := s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestSyncActivity_WorkflowClosed() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecution(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:   common.StringPtr(domainID),
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}
	msBuilder.On("IsWorkflowExecutionRunning").Return(false)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{})

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestSyncActivity_IncomingScheduleIDLarger_IncomingVersionSmaller() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(100)

	lastWriteVersion := version + 100
	nextEventID := scheduleID - 10

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecution(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:    common.StringPtr(domainID),
		WorkflowId:  common.StringPtr(workflowID),
		RunId:       common.StringPtr(runID),
		Version:     common.Int64Ptr(version),
		ScheduledId: common.Int64Ptr(scheduleID),
	}
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{
		CurrentVersion:   lastWriteVersion,
		StartVersion:     lastWriteVersion,
		LastWriteVersion: lastWriteVersion,
		LastWriteEventID: nextEventID - 1,
	})
	msBuilder.On("GetLastWriteVersion").Return(lastWriteVersion)
	msBuilder.On("UpdateReplicationStateVersion", lastWriteVersion, false).Once()
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
			FailoverVersion: lastWriteVersion,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestSyncActivity_IncomingScheduleIDLarger_IncomingVersionLarger() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(100)

	lastWriteVersion := version - 100
	nextEventID := scheduleID - 10

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecution(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:    common.StringPtr(domainID),
		WorkflowId:  common.StringPtr(workflowID),
		RunId:       common.StringPtr(runID),
		Version:     common.Int64Ptr(version),
		ScheduledId: common.Int64Ptr(scheduleID),
	}
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{
		CurrentVersion:   lastWriteVersion,
		StartVersion:     lastWriteVersion,
		LastWriteVersion: lastWriteVersion,
		LastWriteEventID: nextEventID - 1,
	})
	msBuilder.On("GetLastWriteVersion").Return(lastWriteVersion)
	msBuilder.On("UpdateReplicationStateVersion", lastWriteVersion, false).Once()
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
			FailoverVersion: lastWriteVersion,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Equal(newRetryTaskErrorWithHint(ErrRetrySyncActivityMsg, domainID, workflowID, runID, nextEventID), err)
}

func (s *historyReplicatorSuite) TestSyncActivity_ActivityCompleted() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(100)

	lastWriteVersion := version
	nextEventID := scheduleID + 10

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecution(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:    common.StringPtr(domainID),
		WorkflowId:  common.StringPtr(workflowID),
		RunId:       common.StringPtr(runID),
		Version:     common.Int64Ptr(version),
		ScheduledId: common.Int64Ptr(scheduleID),
	}
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{
		CurrentVersion:   lastWriteVersion,
		StartVersion:     lastWriteVersion,
		LastWriteVersion: lastWriteVersion,
		LastWriteEventID: nextEventID - 1,
	})
	msBuilder.On("UpdateReplicationStateVersion", lastWriteVersion, false).Once()
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
			FailoverVersion: lastWriteVersion,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	msBuilder.On("GetActivityInfo", scheduleID).Return(nil, false)

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestSyncActivity_ActivityRunning_LocalActivityVersionLarger() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	scheduleID := int64(144)
	version := int64(100)

	lastWriteVersion := version + 10
	nextEventID := scheduleID + 10

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecution(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:    common.StringPtr(domainID),
		WorkflowId:  common.StringPtr(workflowID),
		RunId:       common.StringPtr(runID),
		Version:     common.Int64Ptr(version),
		ScheduledId: common.Int64Ptr(scheduleID),
	}
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{
		CurrentVersion:   lastWriteVersion,
		StartVersion:     lastWriteVersion,
		LastWriteVersion: lastWriteVersion,
		LastWriteEventID: nextEventID - 1,
	})
	msBuilder.On("UpdateReplicationStateVersion", lastWriteVersion, false).Once()
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
			FailoverVersion: lastWriteVersion,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	msBuilder.On("GetActivityInfo", scheduleID).Return(&persistence.ActivityInfo{
		Version: lastWriteVersion - 1,
	}, true)

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestSyncActivity_ActivityRunning_Update_SameVersionSameAttempt() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)
	scheduleID := int64(144)
	scheduledTime := time.Now()
	startedID := scheduleID + 1
	startedTime := scheduledTime.Add(time.Minute)
	heartBeatUpdatedTime := startedTime.Add(time.Minute)
	attempt := int32(0)
	details := []byte("some random actitity heartbeat progress")

	nextEventID := scheduleID + 10

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecution(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:          common.StringPtr(domainID),
		WorkflowId:        common.StringPtr(workflowID),
		RunId:             common.StringPtr(runID),
		Version:           common.Int64Ptr(version),
		ScheduledId:       common.Int64Ptr(scheduleID),
		ScheduledTime:     common.Int64Ptr(scheduledTime.UnixNano()),
		StartedId:         common.Int64Ptr(startedID),
		StartedTime:       common.Int64Ptr(startedTime.UnixNano()),
		Attempt:           common.Int32Ptr(attempt),
		LastHeartbeatTime: common.Int64Ptr(heartBeatUpdatedTime.UnixNano()),
		Details:           details,
	}
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{
		CurrentVersion:   version,
		StartVersion:     version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	})
	msBuilder.On("UpdateReplicationStateVersion", version, false).Once()
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
	activityInfo := &persistence.ActivityInfo{
		Version:    version,
		ScheduleID: scheduleID,
		Attempt:    attempt,
	}
	msBuilder.On("GetActivityInfo", scheduleID).Return(activityInfo, true)
	s.mockClusterMetadata.On("IsVersionFromSameCluster", version, activityInfo.Version).Return(true)

	expectedErr := errors.New("this is error is used to by pass lots of mocking")
	msBuilder.On("ReplicateActivityInfo", request, false).Return(expectedErr)

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Equal(expectedErr, err)
}

func (s *historyReplicatorSuite) TestSyncActivity_ActivityRunning_Update_SameVersionLargerAttempt() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)
	scheduleID := int64(144)
	scheduledTime := time.Now()
	startedID := scheduleID + 1
	startedTime := scheduledTime.Add(time.Minute)
	heartBeatUpdatedTime := startedTime.Add(time.Minute)
	attempt := int32(100)
	details := []byte("some random actitity heartbeat progress")

	nextEventID := scheduleID + 10

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecution(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:          common.StringPtr(domainID),
		WorkflowId:        common.StringPtr(workflowID),
		RunId:             common.StringPtr(runID),
		Version:           common.Int64Ptr(version),
		ScheduledId:       common.Int64Ptr(scheduleID),
		ScheduledTime:     common.Int64Ptr(scheduledTime.UnixNano()),
		StartedId:         common.Int64Ptr(startedID),
		StartedTime:       common.Int64Ptr(startedTime.UnixNano()),
		Attempt:           common.Int32Ptr(attempt),
		LastHeartbeatTime: common.Int64Ptr(heartBeatUpdatedTime.UnixNano()),
		Details:           details,
	}
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{
		CurrentVersion:   version,
		StartVersion:     version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	})
	msBuilder.On("UpdateReplicationStateVersion", version, false).Once()
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
	activityInfo := &persistence.ActivityInfo{
		Version:    version,
		ScheduleID: scheduleID,
		Attempt:    attempt - 1,
	}
	msBuilder.On("GetActivityInfo", scheduleID).Return(activityInfo, true)
	s.mockClusterMetadata.On("IsVersionFromSameCluster", version, activityInfo.Version).Return(true)

	expectedErr := errors.New("this is error is used to by pass lots of mocking")
	msBuilder.On("ReplicateActivityInfo", request, true).Return(expectedErr)

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Equal(expectedErr, err)
}

func (s *historyReplicatorSuite) TestSyncActivity_ActivityRunning_Update_LargerVersion() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)
	scheduleID := int64(144)
	scheduledTime := time.Now()
	startedID := scheduleID + 1
	startedTime := scheduledTime.Add(time.Minute)
	heartBeatUpdatedTime := startedTime.Add(time.Minute)
	attempt := int32(100)
	details := []byte("some random actitity heartbeat progress")

	nextEventID := scheduleID + 10

	context, release, err := s.historyReplicator.historyCache.getOrCreateWorkflowExecution(
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())
	context.(*workflowExecutionContextImpl).msBuilder = msBuilder
	release(nil)
	request := &h.SyncActivityRequest{
		DomainId:          common.StringPtr(domainID),
		WorkflowId:        common.StringPtr(workflowID),
		RunId:             common.StringPtr(runID),
		Version:           common.Int64Ptr(version),
		ScheduledId:       common.Int64Ptr(scheduleID),
		ScheduledTime:     common.Int64Ptr(scheduledTime.UnixNano()),
		StartedId:         common.Int64Ptr(startedID),
		StartedTime:       common.Int64Ptr(startedTime.UnixNano()),
		Attempt:           common.Int32Ptr(attempt),
		LastHeartbeatTime: common.Int64Ptr(heartBeatUpdatedTime.UnixNano()),
		Details:           details,
	}
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{
		CurrentVersion:   version,
		StartVersion:     version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	})
	msBuilder.On("UpdateReplicationStateVersion", version, false).Once()
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
	activityInfo := &persistence.ActivityInfo{
		Version:    version - 1,
		ScheduleID: scheduleID,
		Attempt:    attempt + 1,
	}
	msBuilder.On("GetActivityInfo", scheduleID).Return(activityInfo, true)
	s.mockClusterMetadata.On("IsVersionFromSameCluster", version, activityInfo.Version).Return(false)

	expectedErr := errors.New("this is error is used to by pass lots of mocking")
	msBuilder.On("ReplicateActivityInfo", request, true).Return(expectedErr)

	err = s.historyReplicator.SyncActivity(ctx.Background(), request)
	s.Equal(expectedErr, err)
}

func (s *historyReplicatorSuite) TestApplyStartEvent() {

}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_MissingCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UnixNano()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(nil, &shared.EntityNotExistsError{})

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, runID, version,
		now, s.logger, &h.ReplicateEventsRequest{})
	s.Equal(newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, domainID, workflowID, runID, common.FirstEventID), err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingEqualToCurrent_CurrentRunning() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UnixNano()
	currentRunID := uuid.New()
	currentVersion := version
	currentNextEventID := int64(2333)

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
			FailoverVersion: currentVersion,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(currentRunID),
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo:    &persistence.WorkflowExecutionInfo{RunID: currentRunID, NextEventID: currentNextEventID, State: persistence.WorkflowStateRunning},
			ReplicationState: &persistence.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, runID, version,
		now, s.logger, &h.ReplicateEventsRequest{})
	s.Equal(newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, domainID, workflowID, currentRunID, currentNextEventID), err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingEqualToCurrent_CurrentRunning_OutOfOrder() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	createTaskID := int64(5667)
	now := time.Now().UnixNano()
	currentRunID := uuid.New()
	currentVersion := version
	currentNextEventID := int64(2333)
	currentCreateTaskID := createTaskID + 10

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
			FailoverVersion: currentVersion,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(currentRunID),
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo: &persistence.WorkflowExecutionInfo{
				RunID:        currentRunID,
				NextEventID:  currentNextEventID,
				CreateTaskID: currentCreateTaskID,
				State:        persistence.WorkflowStateRunning,
			},
			ReplicationState: &persistence.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, runID, version,
		now, s.logger, &h.ReplicateEventsRequest{CreateTaskId: common.Int64Ptr(createTaskID)})
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingLargerThanCurrent_CurrentRunning() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UnixNano()
	currentRunID := uuid.New()
	currentVersion := version - 100
	currentNextEventID := int64(2333)

	cluster := cluster.TestAlternativeClusterName
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster)

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(
		&persistence.GetDomainResponse{
			Info:   &persistence.DomainInfo{ID: domainID, Name: domainName},
			Config: &persistence.DomainConfig{Retention: 1},
			ReplicationConfig: &persistence.DomainReplicationConfig{
				ActiveClusterName: cluster,
				Clusters: []*persistence.ClusterReplicationConfig{
					&persistence.ClusterReplicationConfig{ClusterName: cluster},
				},
			},
			FailoverVersion: currentVersion,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")
	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil)
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	msBuilderCurrent.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		RunID: currentRunID,
	})
	msBuilderCurrent.On("GetLastWriteVersion").Return(currentVersion)
	msBuilderCurrent.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentVersion,
		LastWriteEventID: currentNextEventID - 1,
	})
	msBuilderCurrent.On("GetNextEventID").Return(currentNextEventID)
	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true) // this is used to update the version on mutable state
	msBuilderCurrent.On("UpdateReplicationStateVersion", version, true).Once()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:       currentRunID,
		CloseStatus: persistence.WorkflowCloseStatusNone,
	}, nil)

	terminationEvent := &shared.HistoryEvent{
		EventId:   common.Int64Ptr(currentNextEventID),
		Timestamp: common.Int64Ptr(now),
		Version:   common.Int64Ptr(version),
		EventType: shared.EventTypeWorkflowExecutionTerminated.Ptr(),
		WorkflowExecutionTerminatedEventAttributes: &shared.WorkflowExecutionTerminatedEventAttributes{
			Reason:   common.StringPtr(workflowTerminationReason),
			Identity: common.StringPtr(workflowTerminationIdentity),
			Details:  nil,
		},
	}
	terminateRequest := &h.ReplicateEventsRequest{
		SourceCluster: common.StringPtr(cluster),
		DomainUUID:    common.StringPtr(domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(currentRunID),
		},
		FirstEventId:  common.Int64Ptr(currentNextEventID),
		NextEventId:   common.Int64Ptr(currentNextEventID + 1),
		Version:       common.Int64Ptr(version),
		History:       &shared.History{Events: []*shared.HistoryEvent{terminationEvent}},
		NewRunHistory: nil,
	}

	msBuilderCurrent.On("ReplicateWorkflowExecutionTerminatedEvent", currentNextEventID, mock.MatchedBy(func(input *shared.HistoryEvent) bool {
		return reflect.DeepEqual(terminationEvent, input)
	})).Return(nil)
	contextCurrent.On("replicateWorkflowExecution", terminateRequest, mock.Anything, mock.Anything, currentNextEventID, mock.Anything, mock.Anything).Return(nil).Once()
	s.mockTxProcessor.On("NotifyNewTask", cluster, mock.Anything)
	s.mockTimerProcessor.On("NotifyNewTimers", cluster, mock.Anything, mock.Anything)
	msBuilderCurrent.On("ClearStickyness").Once()

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, runID, version,
		now, s.logger, &h.ReplicateEventsRequest{})
	s.Equal(newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, domainID, workflowID, runID, common.FirstEventID), err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingNotLessThanCurrent_CurrentFinished() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UnixNano()
	currentRunID := uuid.New()
	currentVersion := version - 100
	currentNextEventID := int64(2333)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(currentRunID),
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo:    &persistence.WorkflowExecutionInfo{RunID: currentRunID, NextEventID: currentNextEventID, State: persistence.WorkflowStateCompleted},
			ReplicationState: &persistence.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, runID, version,
		now, s.logger, &h.ReplicateEventsRequest{})
	s.Equal(newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, domainID, workflowID, runID, common.FirstEventID), err)
}

func (s *historyReplicatorSuite) TestWorkflowReset() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UnixNano()
	currentRunID := uuid.New()
	currentVersion := version - 100
	currentNextEventID := int64(2333)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(currentRunID),
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo:    &persistence.WorkflowExecutionInfo{RunID: currentRunID, NextEventID: currentNextEventID, State: persistence.WorkflowStateCompleted},
			ReplicationState: &persistence.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	reqCtx := ctx.Background()
	req := &h.ReplicateEventsRequest{
		ResetWorkflow: common.BoolPtr(true),
	}

	s.mockWorkflowResetor.On("ApplyResetEvent", reqCtx, req, domainID, workflowID, currentRunID).Return(nil).Once()

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(reqCtx, domainID, workflowID, runID, version,
		now, s.logger, req)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsMissingMutableState_IncomingLessThanCurrent() {
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(123)
	now := time.Now().UnixNano()
	currentRunID := uuid.New()
	currentVersion := version + 100

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
			FailoverVersion: currentVersion,
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(currentRunID),
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State: &persistence.WorkflowMutableState{
			ExecutionInfo:    &persistence.WorkflowExecutionInfo{RunID: currentRunID},
			ReplicationState: &persistence.ReplicationState{LastWriteVersion: currentVersion},
		},
	}, nil)

	err := s.historyReplicator.ApplyOtherEventsMissingMutableState(ctx.Background(), domainID, workflowID, runID,
		version, now, s.logger, &h.ReplicateEventsRequest{})
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_NoGarbageCollection() {
	incomingVersion := int64(110)
	currentLastWriteVersion := int64(123)

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{
				EventType: shared.EventTypeWorkflowExecutionCanceled.Ptr(),
				Timestamp: common.Int64Ptr(time.Now().UnixNano()),
			},
		}},
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion})
	msBuilderIn.On("GetLastWriteVersion").Return(currentLastWriteVersion)
	msBuilderIn.On("UpdateReplicationStateVersion", currentLastWriteVersion, true).Once()
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingLessThanCurrent_CanGarbageCollection() {
	incomingVersion := int64(110)
	currentLastWriteVersion := int64(123)

	signalName := "some random signal name"
	signalInput := []byte("some random signal input")
	signalIdentity := "some random signal identity"

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{
				EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
				Timestamp: common.Int64Ptr(time.Now().UnixNano()),
				WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
					SignalName: common.StringPtr(signalName),
					Input:      signalInput,
					Identity:   common.StringPtr(signalIdentity),
				},
			},
		}},
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion})
	msBuilderIn.On("GetLastWriteVersion").Return(currentLastWriteVersion)
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	msBuilderIn.On("UpdateReplicationStateVersion", currentLastWriteVersion, true).Once()
	msBuilderIn.On("AddWorkflowExecutionSignaled", signalName, signalInput, signalIdentity).Return(&shared.HistoryEvent{
		EventType: shared.EventTypeWorkflowExecutionSignaled.Ptr(),
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
		WorkflowExecutionSignaledEventAttributes: &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr(signalName),
			Input:      signalInput,
			Identity:   common.StringPtr(signalIdentity),
		},
	}).Once()

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

	context.On("updateWorkflowExecution", ([]persistence.Task)(nil), ([]persistence.Task)(nil), mock.Anything).Return(nil).Once()
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingEqualToCurrent() {
	incomingVersion := int64(110)
	currentLastWriteVersion := incomingVersion

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())
	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{LastWriteVersion: currentLastWriteVersion})

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasNotActive_SameCluster() {
	currentLastWriteVersion := int64(10)
	incomingVersion := currentLastWriteVersion + 10

	prevActiveCluster := cluster.TestAlternativeClusterName
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
	})
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)
	s.mockClusterMetadata.On("IsVersionFromSameCluster", incomingVersion, currentLastWriteVersion).Return(true)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasNotActive_DiffCluster() {
	currentLastWriteVersion := int64(10)
	incomingVersion := currentLastWriteVersion + 10

	prevActiveCluster := cluster.TestAlternativeClusterName
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		History: &shared.History{Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
	})
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)
	s.mockClusterMetadata.On("IsVersionFromSameCluster", incomingVersion, currentLastWriteVersion).Return(false)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Nil(msBuilderOut)
	s.Equal(ErrMoreThan2DC, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_MissingReplicationInfo() {
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	currentReplicationInfoLastWriteVersion := currentLastWriteVersion - 10
	currentReplicationInfoLastEventID := currentLastEventID - 11
	incomingVersion := currentLastWriteVersion + 10

	incomingActiveCluster := cluster.TestAlternativeClusterName
	prevActiveCluster := cluster.TestCurrentClusterName
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		Version:         common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*shared.ReplicationInfo{},
		History: &shared.History{Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
		LastReplicationInfo: map[string]*persistence.ReplicationInfo{
			incomingActiveCluster: &persistence.ReplicationInfo{
				Version:     currentReplicationInfoLastWriteVersion,
				LastEventID: currentReplicationInfoLastEventID,
			},
		},
	})
	exeInfo := &persistence.WorkflowExecutionInfo{
		StartTimestamp: startTimeStamp,
		RunID:          runID,
	}
	msBuilderIn.On("GetExecutionInfo").Return(exeInfo)
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	mockConflictResolver := &mockConflictResolver{}
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger bark.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := &mockMutableState{}
	msBuilderMid.On("GetNextEventID").Return(int64(12345)) // this is used by log
	mockConflictResolver.On("reset", runID, mock.Anything, currentReplicationInfoLastEventID, exeInfo).Return(msBuilderMid, nil)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionLocalLarger() {
	runID := uuid.New()

	currentLastWriteVersion := int64(120)
	currentLastEventID := int64(980)
	currentReplicationInfoLastWriteVersion := currentLastWriteVersion - 20
	currentReplicationInfoLastEventID := currentLastEventID - 15
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentReplicationInfoLastWriteVersion - 10
	incomingReplicationInfoLastEventID := currentReplicationInfoLastEventID - 20

	incomingActiveCluster := cluster.TestAlternativeClusterName
	prevActiveCluster := cluster.TestCurrentClusterName
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*shared.ReplicationInfo{
			prevActiveCluster: &shared.ReplicationInfo{
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
		LastReplicationInfo: map[string]*persistence.ReplicationInfo{
			incomingActiveCluster: &persistence.ReplicationInfo{
				Version:     currentReplicationInfoLastWriteVersion,
				LastEventID: currentReplicationInfoLastEventID,
			},
		},
	})
	exeInfo := &persistence.WorkflowExecutionInfo{
		StartTimestamp: startTimeStamp,
		RunID:          runID,
	}
	msBuilderIn.On("GetExecutionInfo").Return(exeInfo)
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	mockConflictResolver := &mockConflictResolver{}
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger bark.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := &mockMutableState{}
	msBuilderMid.On("GetNextEventID").Return(int64(12345)) // this is used by log
	mockConflictResolver.On("reset", runID, mock.Anything, currentReplicationInfoLastEventID, exeInfo).Return(msBuilderMid, nil)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionLocalSmaller() {
	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastEventID := currentLastEventID

	prevActiveCluster := cluster.TestCurrentClusterName
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*shared.ReplicationInfo{
			prevActiveCluster: &shared.ReplicationInfo{
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Nil(msBuilderOut)
	s.Equal(ErrImpossibleRemoteClaimSeenHigherVersion, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_ResolveConflict() {
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID - 10

	prevActiveCluster := cluster.TestCurrentClusterName
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*shared.ReplicationInfo{
			prevActiveCluster: &shared.ReplicationInfo{
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	msBuilderIn.On("HasBufferedEvents").Return(false)
	exeInfo := &persistence.WorkflowExecutionInfo{
		StartTimestamp: startTimeStamp,
		RunID:          runID,
	}
	msBuilderIn.On("GetExecutionInfo").Return(exeInfo)
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	mockConflictResolver := &mockConflictResolver{}
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger bark.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := &mockMutableState{}
	msBuilderMid.On("GetNextEventID").Return(int64(12345)) // this is used by log
	mockConflictResolver.On("reset", runID, mock.Anything, incomingReplicationInfoLastEventID, exeInfo).Return(msBuilderMid, nil)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_Corrputed() {
	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID + 10

	prevActiveCluster := cluster.TestCurrentClusterName
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*shared.ReplicationInfo{
			prevActiveCluster: &shared.ReplicationInfo{
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Nil(msBuilderOut)
	s.Equal(ErrCorruptedReplicationInfo, err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_ResolveConflict_OtherCase() {
	// other cases will be tested in TestConflictResolutionTerminateContinueAsNew
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_NoBufferedEvent_NoOp() {
	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID

	prevActiveCluster := cluster.TestCurrentClusterName
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*shared.ReplicationInfo{
			prevActiveCluster: &shared.ReplicationInfo{
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}

	msBuilderIn.On("HasBufferedEvents").Return(false)
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	})
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn,
		request, s.logger)
	s.Equal(msBuilderIn, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEventsVersionChecking_IncomingGreaterThanCurrent_CurrentWasActive_ReplicationInfoVersionEqual_HasBufferedEvent_ResolveConflict() {
	runID := uuid.New()

	currentLastWriteVersion := int64(10)
	currentLastEventID := int64(98)
	incomingVersion := currentLastWriteVersion + 10
	incomingReplicationInfoLastWriteVersion := currentLastWriteVersion
	incomingReplicationInfoLastEventID := currentLastEventID

	prevActiveCluster := cluster.TestCurrentClusterName
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilderIn := &mockMutableState{}
	defer msBuilderIn.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		Version: common.Int64Ptr(incomingVersion),
		ReplicationInfo: map[string]*shared.ReplicationInfo{
			prevActiveCluster: &shared.ReplicationInfo{
				Version:     common.Int64Ptr(incomingReplicationInfoLastWriteVersion),
				LastEventId: common.Int64Ptr(incomingReplicationInfoLastEventID),
			},
		},
		History: &shared.History{Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Timestamp: common.Int64Ptr(time.Now().UnixNano())},
		}},
	}
	startTimeStamp := time.Now()
	pendingDecisionInfo := &decisionInfo{
		Version:    currentLastWriteVersion,
		ScheduleID: 56,
		StartedID:  57,
	}
	msBuilderIn.On("GetLastWriteVersion").Return(currentLastWriteVersion)
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID,
	}).Once()
	msBuilderIn.On("HasBufferedEvents").Return(true).Once()
	msBuilderIn.On("GetInFlightDecisionTask").Return(pendingDecisionInfo, true)
	msBuilderIn.On("UpdateReplicationStateVersion", currentLastWriteVersion, true).Once()
	msBuilderIn.On("AddDecisionTaskFailedEvent", pendingDecisionInfo.ScheduleID, pendingDecisionInfo.StartedID,
		workflow.DecisionTaskFailedCauseFailoverCloseDecision, ([]byte)(nil), identityHistoryService, "", "", "", int64(0)).Return(&shared.HistoryEvent{}).Once()
	context.On("updateWorkflowExecution", ([]persistence.Task)(nil), ([]persistence.Task)(nil), mock.Anything).Return(nil).Once()

	// after the flush, the pending buffered events are gone, however, the last event ID should increase
	msBuilderIn.On("GetReplicationState").Return(&persistence.ReplicationState{
		LastWriteVersion: currentLastWriteVersion,
		LastWriteEventID: currentLastEventID + 2,
	}).Once()

	exeInfo := &persistence.WorkflowExecutionInfo{
		StartTimestamp: startTimeStamp,
		RunID:          runID,
	}
	msBuilderIn.On("GetExecutionInfo").Return(exeInfo)
	msBuilderIn.On("IsWorkflowExecutionRunning").Return(true)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentLastWriteVersion).Return(prevActiveCluster)

	mockConflictResolver := &mockConflictResolver{}
	s.historyReplicator.getNewConflictResolver = func(context workflowExecutionContext, logger bark.Logger) conflictResolver {
		return mockConflictResolver
	}
	msBuilderMid := &mockMutableState{}
	msBuilderMid.On("GetNextEventID").Return(int64(12345)) // this is used by log
	mockConflictResolver.On("reset", runID, mock.Anything, incomingReplicationInfoLastEventID, exeInfo).Return(msBuilderMid, nil)
	msBuilderOut, err := s.historyReplicator.ApplyOtherEventsVersionChecking(ctx.Background(), context, msBuilderIn, request, s.logger)
	s.Equal(msBuilderMid, msBuilderOut)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingLessThanCurrent() {
	currentNextEventID := int64(10)
	incomingFirstEventID := currentNextEventID - 4

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		FirstEventId: common.Int64Ptr(incomingFirstEventID),
		History:      &shared.History{},
	}
	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("GetReplicationState").Return(&persistence.ReplicationState{}) // logger will use this

	err := s.historyReplicator.ApplyOtherEvents(ctx.Background(), context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingEqualToCurrent() {
	// TODO
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingGreaterThanCurrent_NoForceBuffer() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	context.On("getDomainID").Return(domainID)
	context.On("getExecution").Return(&workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	})

	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		SourceCluster: common.StringPtr(incomingSourceCluster),
		Version:       common.Int64Ptr(incomingVersion),
		FirstEventId:  common.Int64Ptr(incomingFirstEventID),
		NextEventId:   common.Int64Ptr(incomingNextEventID),
		History:       &shared.History{},
	}

	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)

	err := s.historyReplicator.ApplyOtherEvents(ctx.Background(), context, msBuilder, request, s.logger)
	s.Equal(newRetryTaskErrorWithHint(ErrRetryBufferEventsMsg, domainID, workflowID, runID, currentNextEventID), err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingGreaterThanCurrent_ForceBuffer_NoExistingBuffer_WorkflowRunning() {
	currentSourceCluster := "some random current source cluster"
	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		SourceCluster:           common.StringPtr(incomingSourceCluster),
		Version:                 common.Int64Ptr(incomingVersion),
		FirstEventId:            common.Int64Ptr(incomingFirstEventID),
		NextEventId:             common.Int64Ptr(incomingNextEventID),
		ForceBufferEvents:       common.BoolPtr(true),
		History:                 &shared.History{Events: []*shared.HistoryEvent{&shared.HistoryEvent{}}},
		NewRunHistory:           &shared.History{Events: []*shared.HistoryEvent{&shared.HistoryEvent{}}},
		EventStoreVersion:       common.Int32Ptr(233),
		NewRunEventStoreVersion: common.Int32Ptr(2333),
	}

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentVersion).Return(currentSourceCluster)
	msBuilder.On("GetAllBufferedReplicationTasks").Return(map[int64]*persistence.BufferedReplicationTask{}).Once()
	msBuilder.On("GetLastWriteVersion").Return(currentVersion)
	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("BufferReplicationTask", request).Return(nil).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)

	context.On("updateHelper", ([]persistence.Task)(nil), ([]persistence.Task)(nil),
		mock.Anything, mock.Anything, false, (*historyBuilder)(nil), currentSourceCluster).Return(nil).Once()

	err := s.historyReplicator.ApplyOtherEvents(ctx.Background(), context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingGreaterThanCurrent_ForceBuffer_NoExistingBuffer_WorkflowClosed() {
	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		SourceCluster:     common.StringPtr(incomingSourceCluster),
		Version:           common.Int64Ptr(incomingVersion),
		FirstEventId:      common.Int64Ptr(incomingFirstEventID),
		NextEventId:       common.Int64Ptr(incomingNextEventID),
		ForceBufferEvents: common.BoolPtr(true),
		History:           &shared.History{Events: []*shared.HistoryEvent{&shared.HistoryEvent{}}},
	}

	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("IsWorkflowExecutionRunning").Return(false)

	err := s.historyReplicator.ApplyOtherEvents(ctx.Background(), context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingGreaterThanCurrent_ForceBuffer_StaleExistingBuffer() {
	currentSourceCluster := "some random current source cluster"
	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		SourceCluster:     common.StringPtr(incomingSourceCluster),
		Version:           common.Int64Ptr(incomingVersion),
		FirstEventId:      common.Int64Ptr(incomingFirstEventID),
		NextEventId:       common.Int64Ptr(incomingNextEventID),
		ForceBufferEvents: common.BoolPtr(true),
		History:           &shared.History{Events: []*shared.HistoryEvent{&shared.HistoryEvent{}}},
	}

	staleBufferReplicationTask := &persistence.BufferedReplicationTask{
		FirstEventID: request.GetFirstEventId(),
		NextEventID:  request.GetNextEventId(),
		Version:      request.GetVersion() - 1,
	}

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", currentVersion).Return(currentSourceCluster)
	msBuilder.On("GetAllBufferedReplicationTasks").Return(map[int64]*persistence.BufferedReplicationTask{
		incomingFirstEventID: staleBufferReplicationTask,
	}).Once()
	msBuilder.On("GetLastWriteVersion").Return(currentVersion)
	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("BufferReplicationTask", request).Return(nil).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)

	context.On("updateHelper", ([]persistence.Task)(nil), ([]persistence.Task)(nil),
		mock.Anything, mock.Anything, false, (*historyBuilder)(nil), currentSourceCluster).Return(nil).Once()

	err := s.historyReplicator.ApplyOtherEvents(ctx.Background(), context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyOtherEvents_IncomingGreaterThanCurrent_ForceBuffer_StaleIncoming() {
	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		SourceCluster:     common.StringPtr(incomingSourceCluster),
		Version:           common.Int64Ptr(incomingVersion),
		FirstEventId:      common.Int64Ptr(incomingFirstEventID),
		NextEventId:       common.Int64Ptr(incomingNextEventID),
		ForceBufferEvents: common.BoolPtr(true),
		History:           &shared.History{Events: []*shared.HistoryEvent{&shared.HistoryEvent{}}},
	}

	bufferReplicationTask := &persistence.BufferedReplicationTask{
		FirstEventID: request.GetFirstEventId(),
		NextEventID:  request.GetNextEventId(),
		Version:      request.GetVersion() + 1,
	}
	msBuilder.On("GetNextEventID").Return(currentNextEventID)
	msBuilder.On("GetAllBufferedReplicationTasks").Return(map[int64]*persistence.BufferedReplicationTask{
		incomingFirstEventID: bufferReplicationTask,
	}).Once()
	msBuilder.On("IsWorkflowExecutionRunning").Return(true)

	err := s.historyReplicator.ApplyOtherEvents(ctx.Background(), context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestApplyReplicationTask() {
	// TODO
}

func (s *historyReplicatorSuite) TestApplyReplicationTask_WorkflowClosed() {
	currentVersion := int64(4096)
	currentNextEventID := int64(10)

	incomingSourceCluster := "some random incoming source cluster"
	incomingVersion := currentVersion * 2
	incomingFirstEventID := currentNextEventID + 4
	incomingNextEventID := incomingFirstEventID + 4

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	request := &h.ReplicateEventsRequest{
		SourceCluster:     common.StringPtr(incomingSourceCluster),
		Version:           common.Int64Ptr(incomingVersion),
		FirstEventId:      common.Int64Ptr(incomingFirstEventID),
		NextEventId:       common.Int64Ptr(incomingNextEventID),
		ForceBufferEvents: common.BoolPtr(true),
		History:           &shared.History{Events: []*shared.HistoryEvent{&shared.HistoryEvent{}}},
	}

	msBuilder.On("IsWorkflowExecutionRunning").Return(false)

	err := s.historyReplicator.ApplyReplicationTask(ctx.Background(), context, msBuilder, request, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestFlushReplicationBuffer() {
	// TODO
}

func (s *historyReplicatorSuite) TestFlushReplicationBuffer_AlreadyFinished() {
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	msBuilder.On("IsWorkflowExecutionRunning").Return(false).Once()

	err := s.historyReplicator.flushReplicationBuffer(ctx.Background(), context, msBuilder, s.logger)
	s.Nil(err)
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_BrandNew() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetCurrentBranch").Return(nil)
	historySize := 111
	msBuilder.On("GetEventStoreVersion").Return(int32(0))
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(&p.AppendHistoryEventsResponse{Size: historySize}, nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		s.Equal(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			HistorySize:                 int64(historySize),
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			CreateWorkflowMode:          persistence.CreateWorkflowModeBrandNew,
			PreviousRunID:               "",
			ReplicationState:            replicationState,
		}, input)
		return true
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()

	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{ID: domainID, Name: "domain name"},
		TableVersion:      p.DomainTableVersionV1,
		Config:            &p.DomainConfig{},
		ReplicationConfig: &p.DomainReplicationConfig{},
	}, nil)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestCurrentClusterName)
	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history, sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.True(now.Equal(transferTasks[0].GetVisibilityTimestamp()))
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_ISE() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetCurrentBranch").Return(nil)
	historySize := 111
	msBuilder.On("GetEventStoreVersion").Return(int32(0))
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(&p.AppendHistoryEventsResponse{Size: historySize}, nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil).Once() // this is called when err is returned, and shard will try to update

	errRet := &shared.InternalServiceError{}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{ID: domainID, Name: "domain name"},
		TableVersion:      p.DomainTableVersionV1,
		Config:            &p.DomainConfig{},
		ReplicationConfig: &p.DomainReplicationConfig{},
	}, nil)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestCurrentClusterName)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Equal(errRet, err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_SameRunID() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetCurrentBranch").Return(nil)
	historySize := 111
	msBuilder.On("GetEventStoreVersion").Return(int32(0))
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(&p.AppendHistoryEventsResponse{Size: historySize}, nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version
	currentRunID := runID
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{ID: domainID, Name: "domain name"},
		TableVersion:      p.DomainTableVersionV1,
		Config:            &p.DomainConfig{},
		ReplicationConfig: &p.DomainReplicationConfig{},
	}, nil)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestCurrentClusterName)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentComplete_IncomingLessThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetCurrentBranch").Return(nil)
	historySize := 111
	msBuilder.On("GetEventStoreVersion").Return(int32(0))
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(&p.AppendHistoryEventsResponse{Size: historySize}, nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version + 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateCompleted
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			HistorySize:                 int64(historySize),
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			CreateWorkflowMode:          persistence.CreateWorkflowModeBrandNew,
			PreviousRunID:               "",
			ReplicationState:            replicationState,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			HistorySize:                 int64(historySize),
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			CreateWorkflowMode:          persistence.CreateWorkflowModeWorkflowIDReuse,
			PreviousRunID:               currentRunID,
			PreviousLastWriteVersion:    currentVersion,
			ReplicationState:            replicationState,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{ID: domainID, Name: "domain name"},
		TableVersion:      p.DomainTableVersionV1,
		Config:            &p.DomainConfig{},
		ReplicationConfig: &p.DomainReplicationConfig{},
	}, nil)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestCurrentClusterName)
	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.True(now.Equal(transferTasks[0].GetVisibilityTimestamp()))
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentComplete_IncomingEqualToThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetCurrentBranch").Return(nil)
	historySize := 111
	msBuilder.On("GetEventStoreVersion").Return(int32(0))
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(&p.AppendHistoryEventsResponse{Size: historySize}, nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateCompleted
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			HistorySize:                 int64(historySize),
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			CreateWorkflowMode:          persistence.CreateWorkflowModeBrandNew,
			PreviousRunID:               "",
			ReplicationState:            replicationState,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			HistorySize:                 int64(historySize),
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			CreateWorkflowMode:          persistence.CreateWorkflowModeWorkflowIDReuse,
			PreviousRunID:               currentRunID,
			PreviousLastWriteVersion:    currentVersion,
			ReplicationState:            replicationState,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{ID: domainID, Name: "domain name"},
		TableVersion:      p.DomainTableVersionV1,
		Config:            &p.DomainConfig{},
		ReplicationConfig: &p.DomainReplicationConfig{},
	}, nil)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestCurrentClusterName)
	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.True(now.Equal(transferTasks[0].GetVisibilityTimestamp()))
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentComplete_IncomingNotLessThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetCurrentBranch").Return(nil)
	historySize := 111
	msBuilder.On("GetEventStoreVersion").Return(int32(0))
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(&p.AppendHistoryEventsResponse{Size: historySize}, nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version - 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateCompleted
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			HistorySize:                 int64(historySize),
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			CreateWorkflowMode:          persistence.CreateWorkflowModeBrandNew,
			PreviousRunID:               "",
			ReplicationState:            replicationState,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			HistorySize:                 int64(historySize),
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			CreateWorkflowMode:          persistence.CreateWorkflowModeWorkflowIDReuse,
			PreviousRunID:               currentRunID,
			PreviousLastWriteVersion:    currentVersion,
			ReplicationState:            replicationState,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{ID: domainID, Name: "domain name"},
		TableVersion:      p.DomainTableVersionV1,
		Config:            &p.DomainConfig{},
		ReplicationConfig: &p.DomainReplicationConfig{},
	}, nil)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestCurrentClusterName)
	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.True(now.Equal(transferTasks[0].GetVisibilityTimestamp()))
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingLessThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetCurrentBranch").Return(nil)
	historySize := 111
	msBuilder.On("GetEventStoreVersion").Return(int32(0))
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(&p.AppendHistoryEventsResponse{Size: historySize}, nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version + 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()
	s.mockHistoryMgr.On("DeleteWorkflowExecutionHistory", mock.Anything).Return(nil).Once()
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{ID: domainID, Name: "domain name"},
		TableVersion:      p.DomainTableVersionV1,
		Config:            &p.DomainConfig{},
		ReplicationConfig: &p.DomainReplicationConfig{},
	}, nil)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestCurrentClusterName)

	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingEqualToCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetCurrentBranch").Return(nil)
	historySize := 111
	msBuilder.On("GetEventStoreVersion").Return(int32(0))
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(&p.AppendHistoryEventsResponse{Size: historySize}, nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version
	currentRunID := uuid.New()
	currentNextEventID := int64(3456)
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")
	contextCurrent.On("getExecution").Return(&workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	})

	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	msBuilderCurrent.On("GetAllBufferedReplicationTasks").Return(map[int64]*persistence.BufferedReplicationTask{})
	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true)
	msBuilderCurrent.On("HasBufferedReplicationTasks").Return(false)
	msBuilderCurrent.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      currentRunID,
	})
	msBuilderCurrent.On("GetNextEventID").Return(currentNextEventID)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{ID: domainID, Name: "domain name"},
		TableVersion:      p.DomainTableVersionV1,
		Config:            &p.DomainConfig{},
		ReplicationConfig: &p.DomainReplicationConfig{},
	}, nil)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestCurrentClusterName)
	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Equal(newRetryTaskErrorWithHint(ErrRetryExistingWorkflowMsg, domainID, workflowID, currentRunID, currentNextEventID), err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingEqualToCurrent_OutOfOrder() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"
	createTaskID := int64(2333)

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1)},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2)},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		CreateTaskID:         createTaskID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetCurrentBranch").Return(nil)
	historySize := 111
	msBuilder.On("GetEventStoreVersion").Return(int32(0))
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(&p.AppendHistoryEventsResponse{Size: historySize}, nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version
	currentRunID := uuid.New()
	currentNextEventID := int64(3456)
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.Anything).Return(nil, errRet).Once()

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")
	contextCurrent.On("getExecution").Return(&workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	})

	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	msBuilderCurrent.On("GetAllBufferedReplicationTasks").Return(map[int64]*persistence.BufferedReplicationTask{})
	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true)
	msBuilderCurrent.On("HasBufferedReplicationTasks").Return(false)
	msBuilderCurrent.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:     domainID,
		WorkflowID:   workflowID,
		RunID:        currentRunID,
		CreateTaskID: createTaskID + 10,
	})
	msBuilderCurrent.On("GetNextEventID").Return(currentNextEventID)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID: currentRunID,
		// other attributes are not used
	}, nil)
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{ID: domainID, Name: "domain name"},
		TableVersion:      p.DomainTableVersionV1,
		Config:            &p.DomainConfig{},
		ReplicationConfig: &p.DomainReplicationConfig{},
	}, nil)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestCurrentClusterName)
	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestReplicateWorkflowStarted_CurrentRunning_IncomingLargerThanCurrent() {
	domainID := validDomainID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(144)
	tasklist := "some random tasklist"
	workflowType := "some random workflow type"
	workflowTimeout := int32(3721)
	decisionTimeout := int32(4411)

	initiatedID := int64(4810)
	parentDomainID := validDomainID
	parentWorkflowID := "some random workflow ID"
	parentRunID := uuid.New()
	sourceCluster := "some random source cluster"

	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	msBuilder := &mockMutableState{}
	defer msBuilder.AssertExpectations(s.T())

	di := &decisionInfo{
		Version:         version,
		ScheduleID:      common.FirstEventID + 1,
		StartedID:       common.EmptyEventID,
		DecisionTimeout: decisionTimeout,
		TaskList:        tasklist,
	}
	sBuilder := &mockStateBuilder{}
	requestID := uuid.New()
	now := time.Now()
	history := &shared.History{
		Events: []*shared.HistoryEvent{
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(1), Timestamp: common.Int64Ptr(now.UnixNano())},
			&shared.HistoryEvent{Version: common.Int64Ptr(version), EventId: common.Int64Ptr(2), Timestamp: common.Int64Ptr(now.UnixNano())},
		},
	}
	nextEventID := di.ScheduleID + 1
	replicationState := &persistence.ReplicationState{
		StartVersion:     version,
		CurrentVersion:   version,
		LastWriteVersion: version,
		LastWriteEventID: nextEventID - 1,
	}
	transferTasks := []persistence.Task{&persistence.CloseExecutionTask{}}
	timerTasks := []persistence.Task{&persistence.DeleteHistoryEventTask{}}

	msBuilder.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		CreateRequestID:      requestID,
		DomainID:             domainID,
		WorkflowID:           workflowID,
		RunID:                runID,
		InitiatedID:          initiatedID,
		ParentDomainID:       parentDomainID,
		ParentWorkflowID:     parentWorkflowID,
		ParentRunID:          parentRunID,
		TaskList:             tasklist,
		WorkflowTypeName:     workflowType,
		WorkflowTimeout:      workflowTimeout,
		DecisionTimeoutValue: decisionTimeout,
	})
	msBuilder.On("UpdateReplicationStateLastEventID", sourceCluster, version, nextEventID-1).Once()
	msBuilder.On("GetReplicationState").Return(replicationState)
	msBuilder.On("GetCurrentVersion").Return(version)
	msBuilder.On("GetNextEventID").Return(nextEventID)
	msBuilder.On("GetCurrentBranch").Return(nil)
	historySize := 111
	msBuilder.On("GetEventStoreVersion").Return(int32(0))
	s.mockHistoryMgr.On("AppendHistoryEvents", mock.Anything).Return(&p.AppendHistoryEventsResponse{Size: historySize}, nil).Once()
	sBuilder.On("getTransferTasks").Return(transferTasks)
	sBuilder.On("getTimerTasks").Return(timerTasks)

	currentVersion := version - 1
	currentRunID := uuid.New()
	currentState := persistence.WorkflowStateRunning
	errRet := &persistence.WorkflowExecutionAlreadyStartedError{
		RunID:            currentRunID,
		State:            currentState,
		LastWriteVersion: currentVersion,
	}
	// the test above already assert the create workflow request, so here just use anyting
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			HistorySize:                 int64(historySize),
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			CreateWorkflowMode:          persistence.CreateWorkflowModeBrandNew,
			PreviousRunID:               "",
			ReplicationState:            replicationState,
		}, input)
	})).Return(nil, errRet).Once()
	s.mockExecutionMgr.On("CreateWorkflowExecution", mock.MatchedBy(func(input *persistence.CreateWorkflowExecutionRequest) bool {
		input.RangeID = 0
		return reflect.DeepEqual(&persistence.CreateWorkflowExecutionRequest{
			RequestID: requestID,
			DomainID:  domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			ParentDomainID: parentDomainID,
			ParentExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedID:                 initiatedID,
			TaskList:                    tasklist,
			WorkflowTypeName:            workflowType,
			WorkflowTimeout:             workflowTimeout,
			DecisionTimeoutValue:        decisionTimeout,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			HistorySize:                 int64(historySize),
			TransferTasks:               transferTasks,
			DecisionVersion:             di.Version,
			DecisionScheduleID:          di.ScheduleID,
			DecisionStartedID:           di.StartedID,
			DecisionStartToCloseTimeout: di.DecisionTimeout,
			TimerTasks:                  timerTasks,
			CreateWorkflowMode:          persistence.CreateWorkflowModeWorkflowIDReuse,
			PreviousRunID:               currentRunID,
			PreviousLastWriteVersion:    version,
			ReplicationState:            replicationState,
		}, input)
	})).Return(&persistence.CreateWorkflowExecutionResponse{}, nil).Once()

	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")
	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)
	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(false)
	s.mockMetadataMgr.On("GetDomain", mock.Anything).Return(&persistence.GetDomainResponse{
		Info:              &persistence.DomainInfo{ID: domainID, Name: "domain name"},
		TableVersion:      p.DomainTableVersionV1,
		Config:            &p.DomainConfig{},
		ReplicationConfig: &p.DomainReplicationConfig{},
	}, nil)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", version).Return(cluster.TestCurrentClusterName)
	err := s.historyReplicator.replicateWorkflowStarted(ctx.Background(), context, msBuilder, di, sourceCluster, history,
		sBuilder, s.logger)
	s.Nil(err)
	s.Equal(1, len(transferTasks))
	s.Equal(version, transferTasks[0].GetVersion())
	s.True(now.Equal(transferTasks[0].GetVisibilityTimestamp()))
	s.Equal(1, len(timerTasks))
	s.Equal(version, timerTasks[0].GetVersion())
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateCurrentRunningIfNotSelf_TargetRunning() {
	runID := uuid.New()
	incomingVersion := int64(4096)
	incomingTimestamp := int64(11238)

	msBuilderTarget := &mockMutableState{}
	defer msBuilderTarget.AssertExpectations(s.T())

	msBuilderTarget.On("IsWorkflowExecutionRunning").Return(true)
	msBuilderTarget.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{RunID: runID})
	prevRunID, err := s.historyReplicator.conflictResolutionTerminateCurrentRunningIfNotSelf(ctx.Background(), msBuilderTarget, incomingVersion, incomingTimestamp, s.logger)
	s.Nil(err)
	s.Equal(runID, prevRunID)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateCurrentRunningIfNotSelf_TargetClosed_CurrentClosed() {
	incomingVersion := int64(4096)
	incomingTimestamp := int64(11238)

	domainID := validDomainID
	workflowID := "some random target workflow ID"
	targetRunID := uuid.New()

	msBuilderTarget := &mockMutableState{}
	defer msBuilderTarget.AssertExpectations(s.T())

	msBuilderTarget.On("IsWorkflowExecutionRunning").Return(false)
	msBuilderTarget.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       targetRunID,
		CloseStatus: persistence.WorkflowCloseStatusContinuedAsNew,
	})

	currentRunID := uuid.New()
	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:       currentRunID,
		CloseStatus: persistence.WorkflowCloseStatusCompleted,
	}, nil)

	prevRunID, err := s.historyReplicator.conflictResolutionTerminateCurrentRunningIfNotSelf(ctx.Background(), msBuilderTarget, incomingVersion, incomingTimestamp, s.logger)
	s.Nil(err)
	s.Equal(currentRunID, prevRunID)
}

func (s *historyReplicatorSuite) TestConflictResolutionTerminateCurrentRunningIfNotSelf_TargetClosed_CurrentRunning() {
	incomingVersion := int64(4096)
	incomingTimestamp := int64(11238)
	incomingCluster := cluster.TestAlternativeClusterName
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", incomingVersion).Return(incomingCluster)

	domainVersion := int64(4081)
	domainName := "some random domain name"
	domainID := validDomainID
	workflowID := "some random target workflow ID"
	targetRunID := uuid.New()

	msBuilderTarget := &mockMutableState{}
	defer msBuilderTarget.AssertExpectations(s.T())
	msBuilderTarget.On("IsWorkflowExecutionRunning").Return(false)
	msBuilderTarget.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:    domainID,
		WorkflowID:  workflowID,
		RunID:       targetRunID,
		CloseStatus: persistence.WorkflowCloseStatusContinuedAsNew,
	})

	// this mocks are for the terminate current workflow operation
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
			FailoverVersion: domainVersion, // this does not matter
			IsGlobalDomain:  true,
			TableVersion:    persistence.DomainTableVersionV1,
		}, nil,
	).Once()

	currentRunID := uuid.New()
	contextCurrent := &mockWorkflowExecutionContext{}
	defer contextCurrent.AssertExpectations(s.T())
	contextCurrent.On("lock", mock.Anything).Return(nil)
	contextCurrent.On("unlock")
	msBuilderCurrent := &mockMutableState{}
	defer msBuilderCurrent.AssertExpectations(s.T())

	contextCurrent.On("loadWorkflowExecution").Return(msBuilderCurrent, nil).Once()
	currentExecution := &shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(currentRunID),
	}
	contextCurrentCacheKey := definition.NewWorkflowIdentifier(domainID, currentExecution.GetWorkflowId(), currentExecution.GetRunId())
	s.historyReplicator.historyCache.PutIfNotExist(contextCurrentCacheKey, contextCurrent)

	currentNextEventID := int64(999)
	msBuilderCurrent.On("GetReplicationState").Return(&persistence.ReplicationState{})
	msBuilderCurrent.On("GetNextEventID").Return(currentNextEventID)
	msBuilderCurrent.On("IsWorkflowExecutionRunning").Return(true) // this is used to update the version on mutable state
	msBuilderCurrent.On("UpdateReplicationStateVersion", incomingVersion, true).Once()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{
		RunID:       currentRunID,
		CloseStatus: persistence.WorkflowCloseStatusNone,
	}, nil)

	terminationEvent := &shared.HistoryEvent{
		EventId:   common.Int64Ptr(currentNextEventID),
		Timestamp: common.Int64Ptr(incomingTimestamp),
		Version:   common.Int64Ptr(incomingVersion),
		EventType: shared.EventTypeWorkflowExecutionTerminated.Ptr(),
		WorkflowExecutionTerminatedEventAttributes: &shared.WorkflowExecutionTerminatedEventAttributes{
			Reason:   common.StringPtr(workflowTerminationReason),
			Identity: common.StringPtr(workflowTerminationIdentity),
			Details:  nil,
		},
	}
	terminateRequest := &h.ReplicateEventsRequest{
		SourceCluster:     common.StringPtr(incomingCluster),
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: currentExecution,
		FirstEventId:      common.Int64Ptr(currentNextEventID),
		NextEventId:       common.Int64Ptr(currentNextEventID + 1),
		Version:           common.Int64Ptr(incomingVersion),
		History:           &shared.History{Events: []*shared.HistoryEvent{terminationEvent}},
		NewRunHistory:     nil,
	}

	msBuilderCurrent.On("ReplicateWorkflowExecutionTerminatedEvent", int64(999), mock.MatchedBy(func(input *shared.HistoryEvent) bool {
		return reflect.DeepEqual(terminationEvent, input)
	})).Return(nil)
	contextCurrent.On("replicateWorkflowExecution", terminateRequest, mock.Anything, mock.Anything, currentNextEventID, mock.Anything, mock.Anything).Return(nil).Once()
	s.mockTxProcessor.On("NotifyNewTask", incomingCluster, mock.Anything)
	s.mockTimerProcessor.On("NotifyNewTimers", incomingCluster, mock.Anything, mock.Anything)
	msBuilderCurrent.On("ClearStickyness").Once()

	prevRunID, err := s.historyReplicator.conflictResolutionTerminateCurrentRunningIfNotSelf(ctx.Background(), msBuilderTarget, incomingVersion, incomingTimestamp, s.logger)
	s.Nil(err)
	s.Equal(currentRunID, prevRunID)
}
