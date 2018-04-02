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

	"github.com/stretchr/testify/mock"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	timerQueueAckMgrSuite struct {
		suite.Suite

		mockExecutionMgr    *mocks.ExecutionManager
		mockShardMgr        *mocks.ShardManager
		mockMetadataMgr     *mocks.MetadataManager
		mockHistoryMgr      *mocks.HistoryManager
		mockShard           ShardContext
		mockService         service.Service
		mockMessagingClient messaging.Client
		mockProducer        *mocks.KafkaProducer
		mockClusterMetadata *mocks.ClusterMetadata
		metricsClient       metrics.Client
		logger              bark.Logger
		clusterName         string
		timerQueueAckMgr    *timerQueueAckMgrImpl
	}
)

func TestTimerQueueAckMgrSuite(t *testing.T) {
	s := new(timerQueueAckMgrSuite)
	suite.Run(t, s)
}

func (s *timerQueueAckMgrSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

}

func (s *timerQueueAckMgrSuite) TearDownSuite() {

}

func (s *timerQueueAckMgrSuite) SetupTest() {
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockShardMgr = &mocks.ShardManager{}
	s.mockMetadataMgr = &mocks.MetadataManager{}
	s.mockHistoryMgr = &mocks.HistoryManager{}
	s.logger = bark.NewLoggerFromLogrus(log.New())
	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockProducer = &mocks.KafkaProducer{}
	s.mockMessagingClient = mocks.NewMockMessagingClient(s.mockProducer, nil)
	s.mockService = service.NewTestService(s.mockClusterMetadata, s.mockMessagingClient, s.metricsClient, s.logger)
	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 copyShardInfo(&persistence.ShardInfo{ShardID: 0, RangeID: 1}),
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		shardManager:              s.mockShardMgr,
		historyMgr:                s.mockHistoryMgr,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewConfig(dynamicconfig.NewNopCollection(), 1),
		logger:                    s.logger,
		domainCache:               cache.NewDomainCache(s.mockMetadataMgr, s.mockClusterMetadata, s.logger),
		metricsClient:             s.metricsClient,
	}

	// this is used by shard context, not relevent to this test, so we do not care how many times "GetCurrentClusterName" os called
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.clusterName = cluster.TestCurrentClusterName
	s.timerQueueAckMgr = newTimerQueueAckMgr(s.mockShard, s.metricsClient, s.mockExecutionMgr, s.clusterName, s.logger)
}

func (s *timerQueueAckMgrSuite) TearDownTest() {

}

func (s *timerQueueAckMgrSuite) TestIsProcessNow() {
	timeBefore := time.Now().Add(-10 * time.Second)
	s.True(s.timerQueueAckMgr.isProcessNow(timeBefore))

	timeAfter := time.Now().Add(10 * time.Second)
	s.False(s.timerQueueAckMgr.isProcessNow(timeAfter))
}

func (s *timerQueueAckMgrSuite) TestGetTimerTasks() {
	minTimestamp := time.Now().Add(-10 * time.Second)
	maxTimestamp := time.Now().Add(10 * time.Second)
	batchSize := 10

	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: minTimestamp,
		MaxTimestamp: maxTimestamp,
		BatchSize:    batchSize,
	}

	response := &persistence.GetTimerIndexTasksResponse{
		Timers: []*persistence.TimerTaskInfo{
			&persistence.TimerTaskInfo{
				DomainID:            "some random domain ID",
				WorkflowID:          "some random workflow ID",
				RunID:               "some random run ID",
				VisibilityTimestamp: time.Now().Add(-5 * time.Second),
				TaskID:              int64(59),
				TaskType:            1,
				TimeoutType:         2,
				EventID:             int64(28),
				ScheduleAttempt:     0,
			},
		},
		NextPageToken: []byte("some random next page token"),
	}

	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()

	timers, token, err := s.timerQueueAckMgr.getTimerTasks(minTimestamp, maxTimestamp, batchSize)
	s.Nil(err)
	s.Equal(response.Timers, timers)
	s.Equal(response.NextPageToken, token)
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_NoLookAhead_NoNextPage() {
	domainID := "some random domain ID"
	ackLevel := s.timerQueueAckMgr.ackLevel
	readLevel := s.timerQueueAckMgr.readLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerAckLevel(s.clusterName), ackLevel)
	s.Equal(s.mockShard.GetTimerAckLevel(s.clusterName), readLevel.VisibilityTimestamp)

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(&persistence.GetDomainResponse{
		// only thing used is the replication config and is global domain
		IsGlobalDomain: false,
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: s.clusterName,
			// Clusters attr is not used.
		},
	}, nil).Once()

	timer := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               "some random run ID",
		VisibilityTimestamp: time.Now().Add(-5 * time.Second),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
	}
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: readLevel.VisibilityTimestamp,
		MaxTimestamp: timerQueueAckMgrMaxTimestamp,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{timer},
		NextPageToken: nil,
	}
	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{timer}, filteredTasks)
	s.Nil(lookAheadTask)
	s.False(moreTasks)
	timerSequenceID := TimerSequenceID{VisibilityTimestamp: timer.VisibilityTimestamp, TaskID: timer.TaskID}
	s.Equal(map[TimerSequenceID]bool{timerSequenceID: false}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(timerSequenceID, s.timerQueueAckMgr.readLevel)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_NoLookAhead_HasNextPage() {
	domainID := "some random domain ID"
	ackLevel := s.timerQueueAckMgr.ackLevel
	readLevel := s.timerQueueAckMgr.readLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerAckLevel(s.clusterName), ackLevel)
	s.Equal(s.mockShard.GetTimerAckLevel(s.clusterName), readLevel.VisibilityTimestamp)

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(&persistence.GetDomainResponse{
		IsGlobalDomain: true,
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: s.clusterName,
			// Clusters attr is not used.
		},
	}, nil).Once()

	failoverVersion := int64(79)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", failoverVersion).Return(s.clusterName)

	timer := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               "some random run ID",
		VisibilityTimestamp: time.Now().Add(-5 * time.Second),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
		Version:             failoverVersion,
	}
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: readLevel.VisibilityTimestamp,
		MaxTimestamp: timerQueueAckMgrMaxTimestamp,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{timer},
		NextPageToken: []byte("some random next page token"),
	}
	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{timer}, filteredTasks)
	s.Nil(lookAheadTask)
	s.True(moreTasks)
	timerSequenceID := TimerSequenceID{VisibilityTimestamp: timer.VisibilityTimestamp, TaskID: timer.TaskID}
	s.Equal(map[TimerSequenceID]bool{timerSequenceID: false}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(timerSequenceID, s.timerQueueAckMgr.readLevel)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_HasLookAhead_NoNextPage() {
	domainID := "some random domain ID"
	ackLevel := s.timerQueueAckMgr.ackLevel
	readLevel := s.timerQueueAckMgr.readLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerAckLevel(s.clusterName), ackLevel)
	s.Equal(s.mockShard.GetTimerAckLevel(s.clusterName), readLevel.VisibilityTimestamp)

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(&persistence.GetDomainResponse{
		// only thing used is the replication config and is global domain
		IsGlobalDomain: false,
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: s.clusterName,
			// Clusters attr is not used.
		},
	}, nil).Once()

	timer := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               "some random run ID",
		VisibilityTimestamp: time.Now().Add(5 * time.Second),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
	}

	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: readLevel.VisibilityTimestamp,
		MaxTimestamp: timerQueueAckMgrMaxTimestamp,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{timer},
		NextPageToken: nil,
	}
	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{}, filteredTasks)
	s.Equal(timer, lookAheadTask)
	s.False(moreTasks)

	s.Equal(map[TimerSequenceID]bool{}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(readLevel, s.timerQueueAckMgr.readLevel)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_HasLookAhead_HasNextPage() {
	domainID := "some random domain ID"
	ackLevel := s.timerQueueAckMgr.ackLevel
	readLevel := s.timerQueueAckMgr.readLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerAckLevel(s.clusterName), ackLevel)
	s.Equal(s.mockShard.GetTimerAckLevel(s.clusterName), readLevel.VisibilityTimestamp)

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(&persistence.GetDomainResponse{
		IsGlobalDomain: true,
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: s.clusterName,
			// Clusters attr is not used.
		},
	}, nil).Once()

	failoverVersion := int64(79)
	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", failoverVersion).Return(s.clusterName)

	timer := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               "some random run ID",
		VisibilityTimestamp: time.Now().Add(5 * time.Second),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
		Version:             failoverVersion,
	}
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: readLevel.VisibilityTimestamp,
		MaxTimestamp: timerQueueAckMgrMaxTimestamp,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{timer},
		NextPageToken: []byte("some random next page token"),
	}
	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{}, filteredTasks)
	s.Equal(timer, lookAheadTask)
	s.False(moreTasks)

	s.Equal(map[TimerSequenceID]bool{}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(readLevel, s.timerQueueAckMgr.readLevel)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
}

func (s *timerQueueAckMgrSuite) TestReadCompleteUpdateTimerTasks() {
	domainID := "some random domain ID"
	readLevel := s.timerQueueAckMgr.readLevel

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(&persistence.GetDomainResponse{
		// only thing used is the replication config and is global domain
		IsGlobalDomain: true,
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: s.clusterName,
			// Clusters attr is not used.
		},
	}, nil).Once()

	// create 3 timers, timer1 < timer2 < timer3 < now
	timer1 := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               "some random run ID",
		VisibilityTimestamp: time.Now().Add(-5 * time.Second),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
	}
	timer2 := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               "some random run ID",
		VisibilityTimestamp: timer1.VisibilityTimestamp,
		TaskID:              timer1.TaskID + 1,
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(29),
		ScheduleAttempt:     0,
	}
	timer3 := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               "some random run ID",
		VisibilityTimestamp: timer1.VisibilityTimestamp.Add(1 * time.Second),
		TaskID:              timer2.TaskID + 1,
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(30),
		ScheduleAttempt:     0,
	}
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: readLevel.VisibilityTimestamp,
		MaxTimestamp: timerQueueAckMgrMaxTimestamp,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{timer1, timer2, timer3},
		NextPageToken: nil,
	}
	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{timer1, timer2, timer3}, filteredTasks)
	s.Nil(lookAheadTask)
	s.False(moreTasks)

	// we are not testing shard context
	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("CompleteTimerTask", &persistence.CompleteTimerTaskRequest{
		VisibilityTimestamp: timer1.VisibilityTimestamp,
		TaskID:              timer1.TaskID}).Return(nil).Once()
	timerSequenceID1 := TimerSequenceID{VisibilityTimestamp: timer1.VisibilityTimestamp, TaskID: timer1.TaskID}
	s.timerQueueAckMgr.completeTimerTask(timer1)
	s.True(s.timerQueueAckMgr.outstandingTasks[timerSequenceID1])
	s.timerQueueAckMgr.updateAckLevel()
	s.Equal(timer1.VisibilityTimestamp, s.mockShard.GetTimerAckLevel(s.clusterName))

	// there will be no call to update shard
	s.mockExecutionMgr.On("CompleteTimerTask", &persistence.CompleteTimerTaskRequest{
		VisibilityTimestamp: timer3.VisibilityTimestamp,
		TaskID:              timer3.TaskID}).Return(nil).Once()
	timerSequenceID3 := TimerSequenceID{VisibilityTimestamp: timer3.VisibilityTimestamp, TaskID: timer3.TaskID}
	s.timerQueueAckMgr.completeTimerTask(timer3)
	s.True(s.timerQueueAckMgr.outstandingTasks[timerSequenceID3])
	s.timerQueueAckMgr.updateAckLevel()
	// ack level remains unchanged
	s.Equal(timer1.VisibilityTimestamp, s.mockShard.GetTimerAckLevel(s.clusterName))

	// we are not testing shard context
	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	s.mockExecutionMgr.On("CompleteTimerTask", &persistence.CompleteTimerTaskRequest{
		VisibilityTimestamp: timer2.VisibilityTimestamp,
		TaskID:              timer2.TaskID}).Return(nil).Once()
	timerSequenceID2 := TimerSequenceID{VisibilityTimestamp: timer2.VisibilityTimestamp, TaskID: timer2.TaskID}
	s.timerQueueAckMgr.completeTimerTask(timer2)
	s.True(s.timerQueueAckMgr.outstandingTasks[timerSequenceID2])
	s.timerQueueAckMgr.updateAckLevel()
	s.Equal(timer3.VisibilityTimestamp, s.mockShard.GetTimerAckLevel(s.clusterName))
}

func (s *timerQueueAckMgrSuite) TestReadRetryCompleteUpdateTimerTasks() {
	domainID := "some random domain ID"
	ackLevel := s.timerQueueAckMgr.ackLevel
	readLevel := s.timerQueueAckMgr.readLevel

	s.mockMetadataMgr.On("GetDomain", &persistence.GetDomainRequest{ID: domainID}).Return(&persistence.GetDomainResponse{
		// only thing used is the replication config and is global domain
		IsGlobalDomain: true,
		ReplicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: s.clusterName,
			// Clusters attr is not used.
		},
	}, nil).Once()

	// create 3 timers, timer1 < timer2 < timer3 < now
	timer := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               "some random run ID",
		VisibilityTimestamp: time.Now().Add(-5 * time.Second),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
	}
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: readLevel.VisibilityTimestamp,
		MaxTimestamp: timerQueueAckMgrMaxTimestamp,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{timer},
		NextPageToken: nil,
	}
	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{timer}, filteredTasks)
	s.Nil(lookAheadTask)
	s.False(moreTasks)

	timerSequenceID := TimerSequenceID{VisibilityTimestamp: timer.VisibilityTimestamp, TaskID: timer.TaskID}
	s.timerQueueAckMgr.retryTimerTask(timer)
	// nothing changed to ack level, as well as outstanding task and task to cluster map
	s.False(s.timerQueueAckMgr.outstandingTasks[timerSequenceID])
	s.Equal(timerSequenceID, s.timerQueueAckMgr.readLevel)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
	s.Equal([]*persistence.TimerTaskInfo{timer}, s.timerQueueAckMgr.retryTasks)

	// do another round of processing, db layer return nothing
	request = &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: s.timerQueueAckMgr.readLevel.VisibilityTimestamp,
		MaxTimestamp: timerQueueAckMgrMaxTimestamp,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}
	response = &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{},
		NextPageToken: nil,
	}
	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err = s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{timer}, filteredTasks)
	s.Nil(lookAheadTask)
	s.False(moreTasks)
	// nothing changed to ack level, as well as outstanding task and task to cluster map
	s.False(s.timerQueueAckMgr.outstandingTasks[timerSequenceID])
	s.Equal(timerSequenceID, s.timerQueueAckMgr.readLevel)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
	s.Equal([]*persistence.TimerTaskInfo{}, s.timerQueueAckMgr.retryTasks)
}
