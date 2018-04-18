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
		mockShard           *shardContextImpl
		mockService         service.Service
		mockMessagingClient messaging.Client
		mockProducer        *mocks.KafkaProducer
		mockClusterMetadata *mocks.ClusterMetadata
		metricsClient       metrics.Client
		logger              bark.Logger
		clusterName         string
		timerQueueAckMgr    *timerQueueAckMgrImpl
	}

	timerQueueFailoverAckMgrSuite struct {
		suite.Suite

		mockExecutionMgr         *mocks.ExecutionManager
		mockShardMgr             *mocks.ShardManager
		mockMetadataMgr          *mocks.MetadataManager
		mockHistoryMgr           *mocks.HistoryManager
		mockShard                *shardContextImpl
		mockService              service.Service
		mockMessagingClient      messaging.Client
		mockProducer             *mocks.KafkaProducer
		mockClusterMetadata      *mocks.ClusterMetadata
		metricsClient            metrics.Client
		logger                   bark.Logger
		domainID                 string
		standbyClusterName       string
		timerQueueFailoverAckMgr *timerQueueAckMgrImpl
	}
)

func TestTimerQueueAckMgrSuite(t *testing.T) {
	s := new(timerQueueAckMgrSuite)
	suite.Run(t, s)
}

func TestTimerQueueFailoverAckMgrSuite(t *testing.T) {
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
		service: s.mockService,
		shardInfo: copyShardInfo(&persistence.ShardInfo{
			ShardID: 0,
			RangeID: 1,
			ClusterTimerAckLevel: map[string]time.Time{
				cluster.TestCurrentClusterName:     time.Now().Add(-8 * time.Second),
				cluster.TestAlternativeClusterName: time.Now().Add(-10 * time.Second),
			},
		}),
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
	s.clusterName = cluster.TestCurrentClusterName
	s.timerQueueAckMgr = newTimerQueueAckMgr(s.mockShard, s.metricsClient, s.clusterName, s.logger)
}

func (s *timerQueueAckMgrSuite) TearDownTest() {
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockShardMgr.AssertExpectations(s.T())
	s.mockMetadataMgr.AssertExpectations(s.T())
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockClusterMetadata.AssertExpectations(s.T())
}

// Test for normal ack manager

func (s *timerQueueAckMgrSuite) TestIsProcessNow() {
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	now := s.mockShard.GetCurrentTime(s.clusterName)
	timeBefore := now.Add(-10 * time.Second)
	s.True(s.timerQueueAckMgr.isProcessNow(timeBefore))

	timeAfter := now.Add(10 * time.Second)
	s.False(s.timerQueueAckMgr.isProcessNow(timeAfter))
}

func (s *timerQueueAckMgrSuite) TestGetTimerTasks_More() {
	minTimestamp := time.Now().Add(-10 * time.Second)
	maxTimestamp := time.Now().Add(10 * time.Second)
	batchSize := 1

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
				RunID:               uuid.New(),
				VisibilityTimestamp: time.Now().Add(-5 * time.Second),
				TaskID:              int64(59),
				TaskType:            1,
				TimeoutType:         2,
				EventID:             int64(28),
				ScheduleAttempt:     0,
			},
		},
	}

	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()

	timers, more, err := s.timerQueueAckMgr.getTimerTasks(minTimestamp, maxTimestamp, batchSize)
	s.Nil(err)
	s.Equal(response.Timers, timers)
	s.True(more)
}

func (s *timerQueueAckMgrSuite) TestGetTimerTasks_NoMore() {
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
				RunID:               uuid.New(),
				VisibilityTimestamp: time.Now().Add(-5 * time.Second),
				TaskID:              int64(59),
				TaskType:            1,
				TimeoutType:         2,
				EventID:             int64(28),
				ScheduleAttempt:     0,
			},
		},
	}

	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()

	timers, more, err := s.timerQueueAckMgr.getTimerTasks(minTimestamp, maxTimestamp, batchSize)
	s.Nil(err)
	s.Equal(response.Timers, timers)
	s.False(more)
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_NoLookAhead_NoNextPage() {
	domainID := "some random domain ID"
	ackLevel := s.timerQueueAckMgr.ackLevel
	readLevel := s.timerQueueAckMgr.readLevel
	macAckLevel := s.timerQueueAckMgr.maxAckLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), readLevel.VisibilityTimestamp)
	s.Equal(timerQueueAckMgrMaxTimestamp, macAckLevel)

	timer := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               uuid.New(),
		VisibilityTimestamp: time.Now().Add(-5 * time.Second),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
	}
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: readLevel.VisibilityTimestamp,
		MaxTimestamp: macAckLevel,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers: []*persistence.TimerTaskInfo{timer},
	}
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
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
	macAckLevel := s.timerQueueAckMgr.maxAckLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), readLevel.VisibilityTimestamp)
	s.Equal(timerQueueAckMgrMaxTimestamp, macAckLevel)

	timer := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               uuid.New(),
		VisibilityTimestamp: time.Now().Add(-5 * time.Second),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
		Version:             int64(79),
	}
	// make the batch size == return size to there will be a next page
	s.mockShard.config.TimerTaskBatchSize = 1
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: readLevel.VisibilityTimestamp,
		MaxTimestamp: macAckLevel,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers: []*persistence.TimerTaskInfo{timer},
	}
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
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
	macAckLevel := s.timerQueueAckMgr.maxAckLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), readLevel.VisibilityTimestamp)
	s.Equal(timerQueueAckMgrMaxTimestamp, macAckLevel)

	timer := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               uuid.New(),
		VisibilityTimestamp: time.Now().Add(5 * time.Second),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
	}

	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: readLevel.VisibilityTimestamp,
		MaxTimestamp: macAckLevel,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers: []*persistence.TimerTaskInfo{timer},
	}
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
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
	macAckLevel := s.timerQueueAckMgr.maxAckLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), readLevel.VisibilityTimestamp)
	s.Equal(timerQueueAckMgrMaxTimestamp, macAckLevel)

	timer := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               uuid.New(),
		VisibilityTimestamp: time.Now().Add(5 * time.Second),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
		Version:             int64(79),
	}
	// make the batch size == return size to there will be a next page
	s.mockShard.config.TimerTaskBatchSize = 1
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: readLevel.VisibilityTimestamp,
		MaxTimestamp: macAckLevel,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers: []*persistence.TimerTaskInfo{timer},
	}
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
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

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_AnyTask_ReadLevel() {
	domainID := "some random domain ID"
	ackLevel := s.timerQueueAckMgr.ackLevel
	readLevel := s.timerQueueAckMgr.readLevel
	macAckLevel := s.timerQueueAckMgr.maxAckLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), readLevel.VisibilityTimestamp)
	s.Equal(timerQueueAckMgrMaxTimestamp, macAckLevel)

	timer := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               uuid.New(),
		VisibilityTimestamp: time.Now().Add(-5 * time.Second),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
		Version:             int64(79),
	}
	// make the batch size == return size to there will be a next page
	s.mockShard.config.TimerTaskBatchSize = 1
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: readLevel.VisibilityTimestamp,
		MaxTimestamp: macAckLevel,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers: []*persistence.TimerTaskInfo{timer},
	}
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal(1, len(filteredTasks))
	s.Nil(lookAheadTask)
	s.True(moreTasks)
	timerSequenceID := TimerSequenceID{VisibilityTimestamp: timer.VisibilityTimestamp, TaskID: timer.TaskID}
	s.Equal(1, len(s.timerQueueAckMgr.outstandingTasks))
	s.Equal(timerSequenceID, s.timerQueueAckMgr.readLevel)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
}

func (s *timerQueueAckMgrSuite) TestReadCompleteUpdateTimerTasks() {
	domainID := "some random domain ID"
	readLevel := s.timerQueueAckMgr.readLevel
	macAckLevel := s.timerQueueAckMgr.maxAckLevel

	// create 3 timers, timer1 < timer2 < timer3 < now
	timer1 := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               uuid.New(),
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
		RunID:               uuid.New(),
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
		RunID:               uuid.New(),
		VisibilityTimestamp: timer1.VisibilityTimestamp.Add(1 * time.Second),
		TaskID:              timer2.TaskID + 1,
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(30),
		ScheduleAttempt:     0,
	}
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: readLevel.VisibilityTimestamp,
		MaxTimestamp: macAckLevel,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers: []*persistence.TimerTaskInfo{timer1, timer2, timer3},
	}
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{timer1, timer2, timer3}, filteredTasks)
	s.Nil(lookAheadTask)
	s.False(moreTasks)

	// we are not testing shard context
	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	timerSequenceID1 := TimerSequenceID{VisibilityTimestamp: timer1.VisibilityTimestamp, TaskID: timer1.TaskID}
	s.timerQueueAckMgr.completeTimerTask(timer1)
	s.True(s.timerQueueAckMgr.outstandingTasks[timerSequenceID1])
	s.timerQueueAckMgr.updateAckLevel()
	s.Equal(timer1.VisibilityTimestamp, s.mockShard.GetTimerClusterAckLevel(s.clusterName))

	// there will be no call to update shard
	timerSequenceID3 := TimerSequenceID{VisibilityTimestamp: timer3.VisibilityTimestamp, TaskID: timer3.TaskID}
	s.timerQueueAckMgr.completeTimerTask(timer3)
	s.True(s.timerQueueAckMgr.outstandingTasks[timerSequenceID3])
	s.timerQueueAckMgr.updateAckLevel()
	// ack level remains unchanged
	s.Equal(timer1.VisibilityTimestamp, s.mockShard.GetTimerClusterAckLevel(s.clusterName))

	// we are not testing shard context
	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	timerSequenceID2 := TimerSequenceID{VisibilityTimestamp: timer2.VisibilityTimestamp, TaskID: timer2.TaskID}
	s.timerQueueAckMgr.completeTimerTask(timer2)
	s.True(s.timerQueueAckMgr.outstandingTasks[timerSequenceID2])
	s.timerQueueAckMgr.updateAckLevel()
	s.Equal(timer3.VisibilityTimestamp, s.mockShard.GetTimerClusterAckLevel(s.clusterName))
}

// Tests for failover ack manager
func (s *timerQueueFailoverAckMgrSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

}

func (s *timerQueueFailoverAckMgrSuite) TearDownSuite() {

}

func (s *timerQueueFailoverAckMgrSuite) SetupTest() {
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
		service: s.mockService,
		shardInfo: copyShardInfo(&persistence.ShardInfo{
			ShardID: 0,
			RangeID: 1,
			ClusterTimerAckLevel: map[string]time.Time{
				cluster.TestCurrentClusterName:     time.Now(),
				cluster.TestAlternativeClusterName: time.Now().Add(-10 * time.Second),
			},
		}),
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

	s.domainID = "some random domain ID"
	s.standbyClusterName = cluster.TestAlternativeClusterName
	s.timerQueueFailoverAckMgr = newTimerQueueFailoverAckMgr(s.mockShard, s.metricsClient, s.standbyClusterName, s.logger)
}

func (s *timerQueueFailoverAckMgrSuite) TearDownTest() {
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockShardMgr.AssertExpectations(s.T())
	s.mockMetadataMgr.AssertExpectations(s.T())
	s.mockHistoryMgr.AssertExpectations(s.T())
	s.mockProducer.AssertExpectations(s.T())
	s.mockClusterMetadata.AssertExpectations(s.T())
}

func (s *timerQueueFailoverAckMgrSuite) TestIsProcessNow() {
	// failover test to process whether to process a timer is use the current cluster's time
	now := s.mockShard.GetCurrentTime(s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName())
	timeBefore := now.Add(-5 * time.Second)
	s.True(s.timerQueueFailoverAckMgr.isProcessNow(timeBefore))

	timeAfter := now.Add(5 * time.Second)
	s.False(s.timerQueueFailoverAckMgr.isProcessNow(timeAfter))
}

func (s *timerQueueFailoverAckMgrSuite) TestReadTimerTasks_HasNextPage() {
	ackLevel := s.timerQueueFailoverAckMgr.ackLevel
	readLevel := s.timerQueueFailoverAckMgr.readLevel
	macAckLevel := s.timerQueueFailoverAckMgr.maxAckLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.standbyClusterName), ackLevel)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.standbyClusterName), readLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName()), macAckLevel)

	// make the batch size == return size to there will be a next page
	s.mockShard.config.TimerTaskBatchSize = 1
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: readLevel.VisibilityTimestamp,
		MaxTimestamp: macAckLevel,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}

	timer1 := &persistence.TimerTaskInfo{
		DomainID:            s.domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               uuid.New(),
		VisibilityTimestamp: time.Now().Add(-5 * time.Second),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
	}

	timer2 := &persistence.TimerTaskInfo{
		DomainID:            "some other random domain ID",
		WorkflowID:          "some random workflow ID",
		RunID:               uuid.New(),
		VisibilityTimestamp: time.Now().Add(-5 * time.Second),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
	}

	response := &persistence.GetTimerIndexTasksResponse{
		Timers: []*persistence.TimerTaskInfo{timer1, timer2},
	}

	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()
	timers, lookAheadTimer, more, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{timer1}, timers)
	s.Nil(lookAheadTimer)
	s.True(more)
	timerSequenceID := TimerSequenceID{VisibilityTimestamp: timer1.VisibilityTimestamp, TaskID: timer1.TaskID}
	s.Equal(timerSequenceID, s.timerQueueFailoverAckMgr.readLevel)
}

func (s *timerQueueFailoverAckMgrSuite) TestReadTimerTasks_NoNextPage() {
	ackLevel := s.timerQueueFailoverAckMgr.ackLevel
	readLevel := s.timerQueueFailoverAckMgr.readLevel
	macAckLevel := s.timerQueueFailoverAckMgr.maxAckLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.standbyClusterName), ackLevel)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.standbyClusterName), readLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName()), macAckLevel)

	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: readLevel.VisibilityTimestamp,
		MaxTimestamp: macAckLevel,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}

	response := &persistence.GetTimerIndexTasksResponse{
		Timers: []*persistence.TimerTaskInfo{},
	}
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()

	timers, lookAheadTimer, more, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{}, timers)
	s.Nil(lookAheadTimer)
	s.False(more)
	s.Equal(ackLevel, s.timerQueueFailoverAckMgr.readLevel)
}

func (s *timerQueueFailoverAckMgrSuite) TestReadTimerTasks_InTheFuture() {
	// when domain failover happen, it is possible that remote cluster's time is after
	// current cluster's time
	now := time.Now()
	s.timerQueueFailoverAckMgr.readLevel.VisibilityTimestamp = now.Add(1 * time.Second)
	s.timerQueueFailoverAckMgr.maxAckLevel = now

	timers, lookAheadTimer, more, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal(0, len(timers))
	s.Nil(lookAheadTimer)
	s.False(more)
}

func (s *timerQueueFailoverAckMgrSuite) TestReadCompleteUpdateTimerTasks() {
	now := time.Now()
	s.timerQueueFailoverAckMgr.readLevel.VisibilityTimestamp = now.Add(-10 * time.Second)
	s.timerQueueFailoverAckMgr.maxAckLevel = now

	readLevel := s.timerQueueFailoverAckMgr.readLevel
	macAckLevel := s.timerQueueFailoverAckMgr.maxAckLevel

	// create 3 timers, timer1 < timer2 < timer3 < now
	timer1 := &persistence.TimerTaskInfo{
		DomainID:            s.domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               uuid.New(),
		VisibilityTimestamp: time.Now().Add(-5 * time.Second),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
	}
	// this timer task does not belonw to failover processing
	timer2 := &persistence.TimerTaskInfo{
		DomainID:            "some other random domain ID",
		WorkflowID:          "some random workflow ID",
		RunID:               uuid.New(),
		VisibilityTimestamp: timer1.VisibilityTimestamp,
		TaskID:              timer1.TaskID + 1,
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(29),
		ScheduleAttempt:     0,
	}
	timer3 := &persistence.TimerTaskInfo{
		DomainID:            s.domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               uuid.New(),
		VisibilityTimestamp: timer1.VisibilityTimestamp.Add(1 * time.Second),
		TaskID:              timer2.TaskID + 1,
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(30),
		ScheduleAttempt:     0,
	}
	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp: readLevel.VisibilityTimestamp,
		MaxTimestamp: macAckLevel,
		BatchSize:    s.mockShard.GetConfig().TimerTaskBatchSize,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers: []*persistence.TimerTaskInfo{timer1, timer3},
	}
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)
	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{timer1, timer3}, filteredTasks)
	s.Nil(lookAheadTask)
	s.False(moreTasks)

	// there will be no call to update shard
	timerSequenceID3 := TimerSequenceID{VisibilityTimestamp: timer3.VisibilityTimestamp, TaskID: timer3.TaskID}
	s.timerQueueFailoverAckMgr.completeTimerTask(timer3)
	s.True(s.timerQueueFailoverAckMgr.outstandingTasks[timerSequenceID3])
	s.timerQueueFailoverAckMgr.updateAckLevel()
	select {
	case <-s.timerQueueFailoverAckMgr.getFinishedChan():
		s.Fail("timer queue ack mgr finished chan should not be fired")
	default:
	}

	// there will be no call to update shard, see issue #646
	timerSequenceID1 := TimerSequenceID{VisibilityTimestamp: timer1.VisibilityTimestamp, TaskID: timer1.TaskID}
	s.timerQueueFailoverAckMgr.completeTimerTask(timer1)
	s.True(s.timerQueueFailoverAckMgr.outstandingTasks[timerSequenceID1])
	s.timerQueueFailoverAckMgr.updateAckLevel()
	select {
	case <-s.timerQueueFailoverAckMgr.getFinishedChan():
	default:
		s.Fail("timer queue ack mgr finished chan should be fired")
	}
}
