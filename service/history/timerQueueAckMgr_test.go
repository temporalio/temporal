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

	gomock "github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	timerQueueAckMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shardContextTest
		mockClusterMetadata *cluster.MockMetadata

		mockExecutionMgr *mocks.ExecutionManager
		mockShardMgr     *mocks.ShardManager

		logger           log.Logger
		clusterName      string
		timerQueueAckMgr *timerQueueAckMgrImpl
	}

	timerQueueFailoverAckMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shardContextTest
		mockClusterMetadata *cluster.MockMetadata

		mockExecutionMgr *mocks.ExecutionManager
		mockShardMgr     *mocks.ShardManager

		logger                   log.Logger
		domainID                 string
		timerQueueFailoverAckMgr *timerQueueAckMgrImpl
		minLevel                 time.Time
		maxLevel                 time.Time
	}
)

func TestTimerQueueAckMgrSuite(t *testing.T) {
	s := new(timerQueueAckMgrSuite)
	suite.Run(t, s)
}

func TestTimerQueueFailoverAckMgrSuite(t *testing.T) {
	s := new(timerQueueFailoverAckMgrSuite)
	suite.Run(t, s)
}

func (s *timerQueueAckMgrSuite) SetupSuite() {

}

func (s *timerQueueAckMgrSuite) TearDownSuite() {

}

func (s *timerQueueAckMgrSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	config := NewDynamicConfigForTest()
	config.ShardUpdateMinInterval = dynamicconfig.GetDurationPropertyFn(0 * time.Second)

	s.controller = gomock.NewController(s.T())
	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfo{
			ShardID: 0,
			RangeID: 1,
			ClusterTimerAckLevel: map[string]time.Time{
				cluster.TestCurrentClusterName:     time.Now().Add(-8 * time.Second),
				cluster.TestAlternativeClusterName: time.Now().Add(-10 * time.Second),
			},
		},
		config,
	)

	s.mockShardMgr = s.mockShard.resource.ShardMgr
	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata

	s.logger = s.mockShard.GetLogger()

	// this is used by shard context, not relevant to this test, so we do not care how many times "GetCurrentClusterName" is called
	s.clusterName = cluster.TestCurrentClusterName
	s.timerQueueAckMgr = newTimerQueueAckMgr(
		0,
		s.mockShard,
		s.mockShard.GetMetricsClient(),
		s.mockShard.GetTimerClusterAckLevel(s.clusterName),
		func() time.Time {
			return s.mockShard.GetCurrentTime(s.clusterName)
		},
		func(ackLevel timerKey) error {
			return s.mockShard.UpdateTimerClusterAckLevel(s.clusterName, ackLevel.VisibilityTimestamp)
		},
		s.logger,
		s.clusterName,
	)
}

func (s *timerQueueAckMgrSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

// Test for normal ack manager

func (s *timerQueueAckMgrSuite) TestIsProcessNow() {
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.True(s.timerQueueAckMgr.isProcessNow(time.Time{}))

	now := s.mockShard.GetCurrentTime(s.clusterName)
	s.True(s.timerQueueAckMgr.isProcessNow(now))

	timeBefore := now.Add(-10 * time.Second)
	s.True(s.timerQueueAckMgr.isProcessNow(timeBefore))

	timeAfter := now.Add(10 * time.Second)
	s.False(s.timerQueueAckMgr.isProcessNow(timeAfter))
}

func (s *timerQueueAckMgrSuite) TestGetTimerTasks_More() {
	minTimestamp := time.Now().Add(-10 * time.Second)
	maxTimestamp := time.Now().Add(10 * time.Second)
	batchSize := 10

	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp:  minTimestamp,
		MaxTimestamp:  maxTimestamp,
		BatchSize:     batchSize,
		NextPageToken: []byte("some random input next page token"),
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
		NextPageToken: []byte("some random output next page token"),
	}

	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()

	timers, token, err := s.timerQueueAckMgr.getTimerTasks(minTimestamp, maxTimestamp, batchSize, request.NextPageToken)
	s.Nil(err)
	s.Equal(response.Timers, timers)
	s.Equal(response.NextPageToken, token)
}

func (s *timerQueueAckMgrSuite) TestGetTimerTasks_NoMore() {
	minTimestamp := time.Now().Add(-10 * time.Second)
	maxTimestamp := time.Now().Add(10 * time.Second)
	batchSize := 10

	request := &persistence.GetTimerIndexTasksRequest{
		MinTimestamp:  minTimestamp,
		MaxTimestamp:  maxTimestamp,
		BatchSize:     batchSize,
		NextPageToken: nil,
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
		NextPageToken: nil,
	}

	s.mockExecutionMgr.On("GetTimerIndexTasks", request).Return(response, nil).Once()

	timers, token, err := s.timerQueueAckMgr.getTimerTasks(minTimestamp, maxTimestamp, batchSize, request.NextPageToken)
	s.Nil(err)
	s.Equal(response.Timers, timers)
	s.Empty(token)
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_NoLookAhead_NoNextPage() {
	domainID := "some random domain ID"
	ackLevel := s.timerQueueAckMgr.ackLevel
	minQueryLevel := s.timerQueueAckMgr.minQueryLevel
	token := s.timerQueueAckMgr.pageToken
	maxQueryLevel := s.timerQueueAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), minQueryLevel)
	s.Empty(token)
	s.Equal(minQueryLevel, maxQueryLevel)

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
	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{timer},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(&persistence.GetTimerIndexTasksResponse{}, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{timer}, filteredTasks)
	s.Nil(lookAheadTask)
	s.False(moreTasks)

	timerSequenceID := timerKey{VisibilityTimestamp: timer.VisibilityTimestamp, TaskID: timer.TaskID}
	s.Equal(map[timerKey]bool{timerSequenceID: false}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
	s.Empty(s.timerQueueAckMgr.pageToken)
	s.Equal(s.timerQueueAckMgr.minQueryLevel, s.timerQueueAckMgr.maxQueryLevel)
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_NoLookAhead_HasNextPage() {
	domainID := "some random domain ID"
	ackLevel := s.timerQueueAckMgr.ackLevel
	minQueryLevel := s.timerQueueAckMgr.minQueryLevel
	token := s.timerQueueAckMgr.pageToken
	maxQueryLevel := s.timerQueueAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), minQueryLevel)
	s.Empty(token)
	s.Equal(minQueryLevel, maxQueryLevel)

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

	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{timer},
		NextPageToken: []byte("some random next page token"),
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	readTimestamp := time.Now() // the approximate time of calling readTimerTasks
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{timer}, filteredTasks)
	s.Nil(lookAheadTask)
	s.True(moreTasks)
	timerSequenceID := timerKey{VisibilityTimestamp: timer.VisibilityTimestamp, TaskID: timer.TaskID}
	s.Equal(map[timerKey]bool{timerSequenceID: false}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
	s.Equal(minQueryLevel, s.timerQueueAckMgr.minQueryLevel)
	s.Equal(response.NextPageToken, s.timerQueueAckMgr.pageToken)
	s.True(s.timerQueueAckMgr.maxQueryLevel.After(readTimestamp))
	s.True(s.timerQueueAckMgr.maxQueryLevel.Before(readTimestamp.Add(s.mockShard.GetConfig().TimerProcessorMaxTimeShift()).Add(time.Second)))
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_HasLookAhead_NoNextPage() {
	domainID := "some random domain ID"
	ackLevel := s.timerQueueAckMgr.ackLevel
	minQueryLevel := s.timerQueueAckMgr.minQueryLevel
	token := s.timerQueueAckMgr.pageToken
	maxQueryLevel := s.timerQueueAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), minQueryLevel)
	s.Empty(token)
	s.Equal(minQueryLevel, maxQueryLevel)

	timer := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               uuid.New(),
		VisibilityTimestamp: time.Now().Add(s.mockShard.GetConfig().TimerProcessorMaxTimeShift()),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
	}

	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{timer},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{}, filteredTasks)
	s.Equal(timer, lookAheadTask)
	s.False(moreTasks)

	s.Equal(map[timerKey]bool{}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
	s.Equal(s.timerQueueAckMgr.maxQueryLevel, s.timerQueueAckMgr.minQueryLevel)
	s.Empty(s.timerQueueAckMgr.pageToken)
	s.Equal(timer.VisibilityTimestamp, s.timerQueueAckMgr.maxQueryLevel)
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_HasLookAhead_HasNextPage() {
	domainID := "some random domain ID"
	ackLevel := s.timerQueueAckMgr.ackLevel
	minQueryLevel := s.timerQueueAckMgr.minQueryLevel
	token := s.timerQueueAckMgr.pageToken
	maxQueryLevel := s.timerQueueAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), minQueryLevel)
	s.Empty(token)
	s.Equal(minQueryLevel, maxQueryLevel)

	timer := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               uuid.New(),
		VisibilityTimestamp: time.Now().Add(s.mockShard.GetConfig().TimerProcessorMaxTimeShift()),
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
		Version:             int64(79),
	}

	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{timer},
		NextPageToken: []byte("some random next page token"),
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{}, filteredTasks)
	s.Equal(timer, lookAheadTask)
	s.False(moreTasks)

	s.Equal(map[timerKey]bool{}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
	s.Equal(s.timerQueueAckMgr.maxQueryLevel, s.timerQueueAckMgr.minQueryLevel)
	s.Empty(s.timerQueueAckMgr.pageToken)
	s.Equal(timer.VisibilityTimestamp, s.timerQueueAckMgr.maxQueryLevel)
}

func (s *timerQueueAckMgrSuite) TestReadCompleteUpdateTimerTasks() {
	domainID := "some random domain ID"

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
	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{timer1, timer2, timer3},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(&persistence.GetTimerIndexTasksResponse{}, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{timer1, timer2, timer3}, filteredTasks)
	s.Nil(lookAheadTask)
	s.False(moreTasks)

	// we are not testing shard context
	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	timerSequenceID1 := timerKey{VisibilityTimestamp: timer1.VisibilityTimestamp, TaskID: timer1.TaskID}
	s.timerQueueAckMgr.completeTimerTask(timer1)
	s.True(s.timerQueueAckMgr.outstandingTasks[timerSequenceID1])
	s.timerQueueAckMgr.updateAckLevel()
	s.Equal(timer1.VisibilityTimestamp, s.mockShard.GetTimerClusterAckLevel(s.clusterName))

	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	timerSequenceID3 := timerKey{VisibilityTimestamp: timer3.VisibilityTimestamp, TaskID: timer3.TaskID}
	s.timerQueueAckMgr.completeTimerTask(timer3)
	s.True(s.timerQueueAckMgr.outstandingTasks[timerSequenceID3])
	s.timerQueueAckMgr.updateAckLevel()
	// ack level remains unchanged
	s.Equal(timer1.VisibilityTimestamp, s.mockShard.GetTimerClusterAckLevel(s.clusterName))

	// we are not testing shard context
	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	timerSequenceID2 := timerKey{VisibilityTimestamp: timer2.VisibilityTimestamp, TaskID: timer2.TaskID}
	s.timerQueueAckMgr.completeTimerTask(timer2)
	s.True(s.timerQueueAckMgr.outstandingTasks[timerSequenceID2])
	s.timerQueueAckMgr.updateAckLevel()
	s.Equal(timer3.VisibilityTimestamp, s.mockShard.GetTimerClusterAckLevel(s.clusterName))
}

func (s *timerQueueAckMgrSuite) TestReadLookAheadTask() {
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(s.clusterName).AnyTimes()
	level := s.mockShard.UpdateTimerMaxReadLevel(s.clusterName)
	s.timerQueueAckMgr.minQueryLevel = level
	s.timerQueueAckMgr.maxQueryLevel = s.timerQueueAckMgr.minQueryLevel

	domainID := "some random domain ID"
	timer := &persistence.TimerTaskInfo{
		DomainID:            domainID,
		WorkflowID:          "some random workflow ID",
		RunID:               uuid.New(),
		VisibilityTimestamp: level,
		TaskID:              int64(59),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
		Version:             int64(79),
	}

	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{timer},
		NextPageToken: []byte("some random next page token"),
	}
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	lookAheadTask, err := s.timerQueueAckMgr.readLookAheadTask()
	s.Nil(err)
	s.Equal(timer, lookAheadTask)
}

// Tests for failover ack manager
func (s *timerQueueFailoverAckMgrSuite) SetupSuite() {

}

func (s *timerQueueFailoverAckMgrSuite) TearDownSuite() {

}

func (s *timerQueueFailoverAckMgrSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	config := NewDynamicConfigForTest()
	config.ShardUpdateMinInterval = dynamicconfig.GetDurationPropertyFn(0 * time.Second)

	s.controller = gomock.NewController(s.T())
	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfo{
			ShardID: 0,
			RangeID: 1,
			ClusterTimerAckLevel: map[string]time.Time{
				cluster.TestCurrentClusterName:     time.Now(),
				cluster.TestAlternativeClusterName: time.Now().Add(-10 * time.Second),
			},
			TimerFailoverLevels: make(map[string]persistence.TimerFailoverLevel),
		},
		config,
	)

	s.mockShardMgr = s.mockShard.resource.ShardMgr
	s.mockExecutionMgr = s.mockShard.resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	s.domainID = "some random failover domain ID"
	s.minLevel = time.Now().Add(-10 * time.Minute)
	s.maxLevel = time.Now().Add(10 * time.Minute)
	s.timerQueueFailoverAckMgr = newTimerQueueFailoverAckMgr(
		s.mockShard,
		s.mockShard.GetMetricsClient(),
		s.minLevel,
		s.maxLevel,
		func() time.Time {
			return s.mockShard.GetCurrentTime(s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName())
		},
		func(ackLevel timerKey) error {
			return s.mockShard.UpdateTimerFailoverLevel(
				s.domainID,
				persistence.TimerFailoverLevel{
					MinLevel:  ackLevel.VisibilityTimestamp,
					MaxLevel:  ackLevel.VisibilityTimestamp,
					DomainIDs: map[string]struct{}{s.domainID: struct{}{}},
				},
			)
		},
		func() error {
			return s.mockShard.DeleteTimerFailoverLevel(s.domainID)
		},
		s.logger,
	)
}

func (s *timerQueueFailoverAckMgrSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *timerQueueFailoverAckMgrSuite) TestIsProcessNow() {
	// failover test to process whether to process a timer is use the current cluster's time
	now := s.mockShard.GetCurrentTime(s.mockShard.GetService().GetClusterMetadata().GetCurrentClusterName())
	s.True(s.timerQueueFailoverAckMgr.isProcessNow(time.Time{}))
	s.True(s.timerQueueFailoverAckMgr.isProcessNow(now))

	timeBefore := now.Add(-5 * time.Second)
	s.True(s.timerQueueFailoverAckMgr.isProcessNow(timeBefore))

	timeAfter := now.Add(5 * time.Second)
	s.False(s.timerQueueFailoverAckMgr.isProcessNow(timeAfter))
}

func (s *timerQueueFailoverAckMgrSuite) TestReadTimerTasks_HasNextPage() {
	ackLevel := s.timerQueueFailoverAckMgr.ackLevel
	minQueryLevel := s.timerQueueFailoverAckMgr.minQueryLevel
	token := s.timerQueueFailoverAckMgr.pageToken
	maxQueryLevel := s.timerQueueFailoverAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.minLevel, ackLevel.VisibilityTimestamp)
	s.Equal(s.minLevel, minQueryLevel)
	s.Empty(token)
	s.Equal(s.maxLevel, maxQueryLevel)

	timer1 := &persistence.TimerTaskInfo{
		DomainID:            "some random domain ID",
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
		TaskID:              int64(60),
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(28),
		ScheduleAttempt:     0,
	}

	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{timer1, timer2},
		NextPageToken: []byte("some random next page token"),
	}

	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	readTimestamp := time.Now() // the approximate time of calling readTimerTasks
	timers, lookAheadTimer, more, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{timer1, timer2}, timers)
	s.Nil(lookAheadTimer)
	s.True(more)
	s.Equal(ackLevel, s.timerQueueFailoverAckMgr.ackLevel)
	s.Equal(minQueryLevel, s.timerQueueFailoverAckMgr.minQueryLevel)
	s.Equal(response.NextPageToken, s.timerQueueFailoverAckMgr.pageToken)
	s.True(s.timerQueueFailoverAckMgr.maxQueryLevel.After(readTimestamp))
	s.Equal(maxQueryLevel, s.timerQueueFailoverAckMgr.maxQueryLevel)
}

func (s *timerQueueFailoverAckMgrSuite) TestReadTimerTasks_NoNextPage() {
	ackLevel := s.timerQueueFailoverAckMgr.ackLevel
	minQueryLevel := s.timerQueueFailoverAckMgr.minQueryLevel
	token := s.timerQueueFailoverAckMgr.pageToken
	maxQueryLevel := s.timerQueueFailoverAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.minLevel, ackLevel.VisibilityTimestamp)
	s.Equal(s.minLevel, minQueryLevel)
	s.Empty(token)
	s.Equal(s.maxLevel, maxQueryLevel)

	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()

	readTimestamp := time.Now() // the approximate time of calling readTimerTasks
	timers, lookAheadTimer, more, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{}, timers)
	s.Nil(lookAheadTimer)
	s.False(more)

	s.Equal(ackLevel, s.timerQueueFailoverAckMgr.ackLevel)
	s.Equal(maximumTime, s.timerQueueFailoverAckMgr.minQueryLevel)
	s.Empty(s.timerQueueFailoverAckMgr.pageToken)
	s.True(s.timerQueueFailoverAckMgr.maxQueryLevel.After(readTimestamp))
	s.Equal(maxQueryLevel, s.timerQueueFailoverAckMgr.maxQueryLevel)
}

func (s *timerQueueFailoverAckMgrSuite) TestReadTimerTasks_InTheFuture() {
	ackLevel := s.timerQueueFailoverAckMgr.ackLevel

	// when domain failover happen, it is possible that remote cluster's time is after
	// current cluster's time
	maxQueryLevel := time.Now()
	s.timerQueueFailoverAckMgr.minQueryLevel = maxQueryLevel.Add(1 * time.Second)
	s.timerQueueFailoverAckMgr.maxQueryLevel = maxQueryLevel

	timers, lookAheadTimer, more, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal(0, len(timers))
	s.Nil(lookAheadTimer)
	s.False(more)

	s.Equal(ackLevel, s.timerQueueFailoverAckMgr.ackLevel)
	s.Equal(maximumTime, s.timerQueueFailoverAckMgr.minQueryLevel)
	s.Empty(s.timerQueueFailoverAckMgr.pageToken)
	s.Equal(maxQueryLevel, s.timerQueueFailoverAckMgr.maxQueryLevel)
}

func (s *timerQueueFailoverAckMgrSuite) TestReadCompleteUpdateTimerTasks() {
	from := time.Now().Add(-10 * time.Second)
	s.timerQueueFailoverAckMgr.minQueryLevel = from
	s.timerQueueFailoverAckMgr.maxQueryLevel = from
	s.timerQueueFailoverAckMgr.ackLevel = timerKey{VisibilityTimestamp: from}

	// create 3 timers, timer1 < timer2 < timer3 < now
	timer1 := &persistence.TimerTaskInfo{
		DomainID:            "some random domain ID",
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
		VisibilityTimestamp: timer1.VisibilityTimestamp,
		TaskID:              timer1.TaskID + 1,
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(29),
		ScheduleAttempt:     0,
	}
	timer3 := &persistence.TimerTaskInfo{
		DomainID:            "some random domain ID",
		WorkflowID:          "some random workflow ID",
		RunID:               uuid.New(),
		VisibilityTimestamp: timer1.VisibilityTimestamp.Add(1 * time.Second),
		TaskID:              timer2.TaskID + 1,
		TaskType:            1,
		TimeoutType:         2,
		EventID:             int64(30),
		ScheduleAttempt:     0,
	}
	response := &persistence.GetTimerIndexTasksResponse{
		Timers:        []*persistence.TimerTaskInfo{timer1, timer2, timer3},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.On("GetTimerIndexTasks", mock.Anything).Return(response, nil).Once()
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]*persistence.TimerTaskInfo{timer1, timer2, timer3}, filteredTasks)
	s.Nil(lookAheadTask)
	s.False(moreTasks)

	timerSequenceID2 := timerKey{VisibilityTimestamp: timer2.VisibilityTimestamp, TaskID: timer2.TaskID}
	s.timerQueueFailoverAckMgr.completeTimerTask(timer2)
	s.True(s.timerQueueFailoverAckMgr.outstandingTasks[timerSequenceID2])
	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	s.timerQueueFailoverAckMgr.updateAckLevel()
	select {
	case <-s.timerQueueFailoverAckMgr.getFinishedChan():
		s.Fail("timer queue ack mgr finished chan should not be fired")
	default:
	}

	timerSequenceID3 := timerKey{VisibilityTimestamp: timer3.VisibilityTimestamp, TaskID: timer3.TaskID}
	s.timerQueueFailoverAckMgr.completeTimerTask(timer3)
	s.True(s.timerQueueFailoverAckMgr.outstandingTasks[timerSequenceID3])
	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	s.timerQueueFailoverAckMgr.updateAckLevel()
	select {
	case <-s.timerQueueFailoverAckMgr.getFinishedChan():
		s.Fail("timer queue ack mgr finished chan should not be fired")
	default:
	}

	timerSequenceID1 := timerKey{VisibilityTimestamp: timer1.VisibilityTimestamp, TaskID: timer1.TaskID}
	s.timerQueueFailoverAckMgr.completeTimerTask(timer1)
	s.True(s.timerQueueFailoverAckMgr.outstandingTasks[timerSequenceID1])
	s.mockShardMgr.On("UpdateShard", mock.Anything).Return(nil).Once()
	s.timerQueueFailoverAckMgr.updateAckLevel()
	select {
	case <-s.timerQueueFailoverAckMgr.getFinishedChan():
	default:
		s.Fail("timer queue ack mgr finished chan should be fired")
	}
}
