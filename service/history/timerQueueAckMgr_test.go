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
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
)

type (
	timerQueueAckMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shard.ContextTest
		mockClusterMetadata *cluster.MockMetadata

		mockExecutionMgr *persistence.MockExecutionManager
		mockShardMgr     *persistence.MockShardManager

		logger           log.Logger
		clusterName      string
		timerQueueAckMgr *timerQueueAckMgrImpl
	}

	timerQueueFailoverAckMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shard.ContextTest
		mockClusterMetadata *cluster.MockMetadata

		mockExecutionMgr *persistence.MockExecutionManager
		mockShardMgr     *persistence.MockShardManager

		logger                   log.Logger
		namespaceID              string
		timerQueueFailoverAckMgr *timerQueueAckMgrImpl
		minLevel                 time.Time
		maxLevel                 time.Time
	}
)

var (
	TestNamespaceId = primitives.MustValidateUUID("deadbeef-c001-face-0000-000000000000")
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

	config := tests.NewDynamicConfig()
	config.ShardUpdateMinInterval = dynamicconfig.GetDurationPropertyFn(0 * time.Second)

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 1,
				RangeId: 1,
				ClusterTimerAckLevel: map[string]*time.Time{
					cluster.TestCurrentClusterName:     timestamp.TimeNowPtrUtcAddSeconds(-8),
					cluster.TestAlternativeClusterName: timestamp.TimeNowPtrUtcAddSeconds(-10),
				}},
		},
		config,
	)

	s.mockShardMgr = s.mockShard.Resource.ShardMgr
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata

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
	s.mockShard.StopForTest()
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
	minTimestamp := time.Now().UTC().Add(-10 * time.Second)
	maxTimestamp := time.Now().UTC().Add(10 * time.Second)
	batchSize := 10

	request := &persistence.GetTimerTasksRequest{
		ShardID:       s.mockShard.GetShardID(),
		MinTimestamp:  minTimestamp,
		MaxTimestamp:  maxTimestamp,
		BatchSize:     batchSize,
		NextPageToken: []byte("some random input next page token"),
	}

	response := &persistence.GetTimerTasksResponse{
		Tasks: []tasks.Task{
			&tasks.ActivityTimeoutTask{
				WorkflowKey: definition.NewWorkflowKey(
					TestNamespaceId,
					"some random workflow ID",
					uuid.New(),
				),
				VisibilityTimestamp: time.Now().UTC().Add(-5 * time.Second),
				TaskID:              int64(59),
				TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
				EventID:             int64(28),
				Attempt:             1,
			},
		},
		NextPageToken: []byte("some random output next page token"),
	}

	s.mockExecutionMgr.EXPECT().GetTimerTasks(request).Return(response, nil)

	timers, token, err := s.timerQueueAckMgr.getTimerTasks(minTimestamp, maxTimestamp, batchSize, request.NextPageToken)
	s.Nil(err)
	s.Equal(response.Tasks, timers)
	s.Equal(response.NextPageToken, token)
}

func (s *timerQueueAckMgrSuite) TestGetTimerTasks_NoMore() {
	minTimestamp := time.Now().UTC().Add(-10 * time.Second)
	maxTimestamp := time.Now().UTC().Add(10 * time.Second)
	batchSize := 10

	request := &persistence.GetTimerTasksRequest{
		ShardID:       s.mockShard.GetShardID(),
		MinTimestamp:  minTimestamp,
		MaxTimestamp:  maxTimestamp,
		BatchSize:     batchSize,
		NextPageToken: nil,
	}

	response := &persistence.GetTimerTasksResponse{
		Tasks: []tasks.Task{
			&tasks.ActivityTimeoutTask{
				WorkflowKey: definition.NewWorkflowKey(
					TestNamespaceId,
					"some random workflow ID",
					uuid.New(),
				),
				VisibilityTimestamp: time.Now().UTC().Add(-5 * time.Second),
				TaskID:              int64(59),
				TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
				EventID:             int64(28),
				Attempt:             1,
			},
		},
		NextPageToken: nil,
	}

	s.mockExecutionMgr.EXPECT().GetTimerTasks(request).Return(response, nil)

	timers, token, err := s.timerQueueAckMgr.getTimerTasks(minTimestamp, maxTimestamp, batchSize, request.NextPageToken)
	s.Nil(err)
	s.Equal(response.Tasks, timers)
	s.Empty(token)
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_NoLookAhead_NoNextPage() {
	ackLevel := s.timerQueueAckMgr.ackLevel
	minQueryLevel := s.timerQueueAckMgr.minQueryLevel
	token := s.timerQueueAckMgr.pageToken
	maxQueryLevel := s.timerQueueAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), minQueryLevel)
	s.Empty(token)
	s.Equal(minQueryLevel, maxQueryLevel)

	timer := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			TestNamespaceId,
			"some random workflow ID",
			uuid.New(),
		),
		VisibilityTimestamp: time.Now().UTC().Add(-5 * time.Second),
		TaskID:              int64(59),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             int64(28),
		Attempt:             1,
	}

	response := &persistence.GetTimerTasksResponse{
		Tasks:         []tasks.Task{timer},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetTimerTasks(gomock.Any()).Return(response, nil)
	s.mockExecutionMgr.EXPECT().GetTimerTasks(gomock.Any()).Return(&persistence.GetTimerTasksResponse{}, nil)
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]tasks.Task{timer}, filteredTasks)
	s.Nil(lookAheadTask)
	s.False(moreTasks)

	timerSequenceID := newTimerKey(timer.VisibilityTimestamp, timer.TaskID)
	s.Equal(map[timerKey]bool{*timerSequenceID: false}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
	s.Empty(s.timerQueueAckMgr.pageToken)
	s.Equal(s.timerQueueAckMgr.minQueryLevel, s.timerQueueAckMgr.maxQueryLevel)
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_NoLookAhead_HasNextPage() {
	ackLevel := s.timerQueueAckMgr.ackLevel
	minQueryLevel := s.timerQueueAckMgr.minQueryLevel
	token := s.timerQueueAckMgr.pageToken
	maxQueryLevel := s.timerQueueAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), minQueryLevel)
	s.Empty(token)
	s.Equal(minQueryLevel, maxQueryLevel)

	timer := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			TestNamespaceId,
			"some random workflow ID",
			uuid.New(),
		),
		VisibilityTimestamp: time.Now().UTC().Add(-5 * time.Second),
		TaskID:              int64(59),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             int64(28),
		Version:             int64(79),
		Attempt:             1,
	}

	response := &persistence.GetTimerTasksResponse{
		Tasks:         []tasks.Task{timer},
		NextPageToken: []byte("some random next page token"),
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetTimerTasks(gomock.Any()).Return(response, nil)
	readTimestamp := time.Now().UTC() // the approximate time of calling readTimerTasks
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]tasks.Task{timer}, filteredTasks)
	s.Nil(lookAheadTask)
	s.True(moreTasks)
	timerSequenceID := newTimerKey(timer.VisibilityTimestamp, timer.TaskID)
	s.Equal(map[timerKey]bool{*timerSequenceID: false}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
	s.Equal(minQueryLevel, s.timerQueueAckMgr.minQueryLevel)
	s.Equal(response.NextPageToken, s.timerQueueAckMgr.pageToken)
	s.True(s.timerQueueAckMgr.maxQueryLevel.After(readTimestamp))
	s.True(s.timerQueueAckMgr.maxQueryLevel.Before(readTimestamp.Add(s.mockShard.GetConfig().TimerProcessorMaxTimeShift()).Add(time.Second)))
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_HasLookAhead_NoNextPage() {
	ackLevel := s.timerQueueAckMgr.ackLevel
	minQueryLevel := s.timerQueueAckMgr.minQueryLevel
	token := s.timerQueueAckMgr.pageToken
	maxQueryLevel := s.timerQueueAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), minQueryLevel)
	s.Empty(token)
	s.Equal(minQueryLevel, maxQueryLevel)

	timer := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			TestNamespaceId,
			"some random workflow ID",
			uuid.New(),
		),
		VisibilityTimestamp: time.Now().UTC().Add(s.mockShard.GetConfig().TimerProcessorMaxTimeShift()),
		TaskID:              int64(59),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             int64(28),
		Attempt:             1,
	}

	response := &persistence.GetTimerTasksResponse{
		Tasks:         []tasks.Task{timer},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetTimerTasks(gomock.Any()).Return(response, nil)
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]tasks.Task{}, filteredTasks)
	s.Equal(timer, lookAheadTask)
	s.False(moreTasks)

	s.Equal(map[timerKey]bool{}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
	s.Equal(s.timerQueueAckMgr.maxQueryLevel, s.timerQueueAckMgr.minQueryLevel)
	s.Empty(s.timerQueueAckMgr.pageToken)
	s.Equal(timer.VisibilityTimestamp.UnixNano(), s.timerQueueAckMgr.maxQueryLevel.UnixNano())
}

func (s *timerQueueAckMgrSuite) TestReadTimerTasks_HasLookAhead_HasNextPage() {
	ackLevel := s.timerQueueAckMgr.ackLevel
	minQueryLevel := s.timerQueueAckMgr.minQueryLevel
	token := s.timerQueueAckMgr.pageToken
	maxQueryLevel := s.timerQueueAckMgr.maxQueryLevel

	// test ack && read level is initialized correctly
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), ackLevel.VisibilityTimestamp)
	s.Equal(s.mockShard.GetTimerClusterAckLevel(s.clusterName), minQueryLevel)
	s.Empty(token)
	s.Equal(minQueryLevel, maxQueryLevel)

	timer := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			TestNamespaceId,
			"some random workflow ID",
			uuid.New(),
		),
		VisibilityTimestamp: time.Now().UTC().Add(s.mockShard.GetConfig().TimerProcessorMaxTimeShift()),
		TaskID:              int64(59),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             int64(28),
		Version:             int64(79),
		Attempt:             1,
	}

	response := &persistence.GetTimerTasksResponse{
		Tasks:         []tasks.Task{timer},
		NextPageToken: []byte("some random next page token"),
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetTimerTasks(gomock.Any()).Return(response, nil)
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]tasks.Task{}, filteredTasks)
	s.Equal(timer, lookAheadTask)
	s.False(moreTasks)

	s.Equal(map[timerKey]bool{}, s.timerQueueAckMgr.outstandingTasks)
	s.Equal(ackLevel, s.timerQueueAckMgr.ackLevel)
	s.Equal(s.timerQueueAckMgr.maxQueryLevel, s.timerQueueAckMgr.minQueryLevel)
	s.Empty(s.timerQueueAckMgr.pageToken)
	s.Equal(timer.VisibilityTimestamp.UnixNano(), s.timerQueueAckMgr.maxQueryLevel.UnixNano())
}

func (s *timerQueueAckMgrSuite) TestReadCompleteUpdateTimerTasks() {
	// create 3 timers, timer1 < timer2 < timer3 < now
	timer1 := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			TestNamespaceId,
			"some random workflow ID",
			uuid.New(),
		),
		VisibilityTimestamp: time.Now().UTC().Add(-5 * time.Second),
		TaskID:              int64(59),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             int64(28),
		Attempt:             1,
	}

	timer2 := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			TestNamespaceId,
			"some random workflow ID",
			uuid.New(),
		),
		VisibilityTimestamp: timer1.VisibilityTimestamp,
		TaskID:              timer1.TaskID + 1,
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             int64(29),
		Attempt:             1,
	}
	timer3 := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			TestNamespaceId,
			"some random workflow ID",
			uuid.New(),
		),
		VisibilityTimestamp: timer2.VisibilityTimestamp.Add(time.Second),
		TaskID:              timer2.TaskID + 1,
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             int64(30),
		Attempt:             1,
	}
	response := &persistence.GetTimerTasksResponse{
		Tasks:         []tasks.Task{timer1, timer2, timer3},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetTimerTasks(gomock.Any()).Return(response, nil)
	s.mockExecutionMgr.EXPECT().GetTimerTasks(gomock.Any()).Return(&persistence.GetTimerTasksResponse{}, nil)
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]tasks.Task{timer1, timer2, timer3}, filteredTasks)
	s.Nil(lookAheadTask)
	s.False(moreTasks)

	// we are not testing shard context
	s.mockShardMgr.EXPECT().UpdateShard(gomock.Any()).Return(nil)
	timerSequenceID1 := newTimerKey(timer1.VisibilityTimestamp, timer1.TaskID)
	s.timerQueueAckMgr.completeTimerTask(timer1.VisibilityTimestamp, timer1.TaskID)
	s.True(s.timerQueueAckMgr.outstandingTasks[*timerSequenceID1])
	_ = s.timerQueueAckMgr.updateAckLevel()
	s.Equal(timer1.VisibilityTimestamp.UnixNano(), s.mockShard.GetTimerClusterAckLevel(s.clusterName).UnixNano())

	s.mockShardMgr.EXPECT().UpdateShard(gomock.Any()).Return(nil)
	timerSequenceID3 := newTimerKey(timer3.VisibilityTimestamp, timer3.TaskID)
	s.timerQueueAckMgr.completeTimerTask(timer3.VisibilityTimestamp, timer3.TaskID)
	s.True(s.timerQueueAckMgr.outstandingTasks[*timerSequenceID3])
	_ = s.timerQueueAckMgr.updateAckLevel()
	// ack level remains unchanged
	s.Equal(timer1.VisibilityTimestamp.UnixNano(), s.mockShard.GetTimerClusterAckLevel(s.clusterName).UnixNano())

	// we are not testing shard context
	s.mockShardMgr.EXPECT().UpdateShard(gomock.Any()).Return(nil)
	timerSequenceID2 := newTimerKey(timer2.VisibilityTimestamp, timer2.TaskID)
	s.timerQueueAckMgr.completeTimerTask(timer2.VisibilityTimestamp, timer2.TaskID)
	s.True(s.timerQueueAckMgr.outstandingTasks[*timerSequenceID2])
	_ = s.timerQueueAckMgr.updateAckLevel()
	s.Equal(timer3.VisibilityTimestamp.UnixNano(), s.mockShard.GetTimerClusterAckLevel(s.clusterName).UnixNano())
}

func (s *timerQueueAckMgrSuite) TestReadLookAheadTask() {
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(s.clusterName).AnyTimes()
	level := s.mockShard.UpdateTimerMaxReadLevel(s.clusterName)

	s.timerQueueAckMgr.minQueryLevel = level
	s.timerQueueAckMgr.maxQueryLevel = s.timerQueueAckMgr.minQueryLevel

	timer := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			TestNamespaceId,
			"some random workflow ID",
			uuid.New(),
		),
		VisibilityTimestamp: time.Now().UTC().Add(-5 * time.Second),
		TaskID:              int64(59),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             int64(28),
		Attempt:             1,
		Version:             int64(79),
	}

	response := &persistence.GetTimerTasksResponse{
		Tasks:         []tasks.Task{timer},
		NextPageToken: []byte("some random next page token"),
	}
	s.mockExecutionMgr.EXPECT().GetTimerTasks(gomock.Any()).Return(response, nil)
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

	config := tests.NewDynamicConfig()
	config.ShardUpdateMinInterval = dynamicconfig.GetDurationPropertyFn(0 * time.Second)

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 1,
				RangeId: 1,
				ClusterTimerAckLevel: map[string]*time.Time{
					cluster.TestCurrentClusterName:     timestamp.TimeNowPtrUtc(),
					cluster.TestAlternativeClusterName: timestamp.TimeNowPtrUtcAddSeconds(-10),
				}},
			TimerFailoverLevels: make(map[string]persistence.TimerFailoverLevel),
		},
		config,
	)

	s.mockShardMgr = s.mockShard.Resource.ShardMgr
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	s.namespaceID = "deadd0d0-c001-face-d00d-020000000000"
	s.minLevel = time.Now().UTC().Add(-10 * time.Minute)
	s.maxLevel = time.Now().UTC().Add(10 * time.Minute)
	s.timerQueueFailoverAckMgr = newTimerQueueFailoverAckMgr(
		s.mockShard,
		s.mockShard.GetMetricsClient(),
		s.minLevel,
		s.maxLevel,
		func() time.Time {
			return s.mockShard.GetCurrentTime(s.mockShard.Resource.GetClusterMetadata().GetCurrentClusterName())
		},
		func(ackLevel timerKey) error {
			return s.mockShard.UpdateTimerFailoverLevel(
				s.namespaceID,
				persistence.TimerFailoverLevel{
					MinLevel:     ackLevel.VisibilityTimestamp,
					MaxLevel:     ackLevel.VisibilityTimestamp,
					NamespaceIDs: map[string]struct{}{s.namespaceID: {}},
				},
			)
		},
		func() error {
			return s.mockShard.DeleteTimerFailoverLevel(s.namespaceID)
		},
		s.logger,
	)
}

func (s *timerQueueFailoverAckMgrSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *timerQueueFailoverAckMgrSuite) TestIsProcessNow() {
	// failover test to process whether to process a timer is use the current cluster's time
	now := s.mockShard.GetCurrentTime(s.mockShard.Resource.GetClusterMetadata().GetCurrentClusterName())
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

	timer1 := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			TestNamespaceId,
			"some random workflow ID",
			uuid.New(),
		),
		VisibilityTimestamp: time.Now().UTC().Add(-5 * time.Second),
		TaskID:              int64(59),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             int64(28),
		Attempt:             1,
	}

	timer2 := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			TestNamespaceId,
			"some random workflow ID",
			uuid.New(),
		),
		VisibilityTimestamp: time.Now().UTC().Add(-5 * time.Second),
		TaskID:              int64(60),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             int64(28),
		Attempt:             1,
	}

	response := &persistence.GetTimerTasksResponse{
		Tasks:         []tasks.Task{timer1, timer2},
		NextPageToken: []byte("some random next page token"),
	}

	s.mockExecutionMgr.EXPECT().GetTimerTasks(gomock.Any()).Return(response, nil)
	readTimestamp := time.Now().UTC() // the approximate time of calling readTimerTasks
	timers, lookAheadTimer, more, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]tasks.Task{timer1, timer2}, timers)
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

	response := &persistence.GetTimerTasksResponse{
		Tasks:         []tasks.Task{},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetTimerTasks(gomock.Any()).Return(response, nil)

	readTimestamp := time.Now().UTC() // the approximate time of calling readTimerTasks
	timers, lookAheadTimer, more, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]tasks.Task{}, timers)
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

	// when namespace failover happen, it is possible that remote cluster's time is after
	// current cluster's time
	maxQueryLevel := time.Now().UTC()
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
	from := time.Now().UTC().Add(-10 * time.Second)
	s.timerQueueFailoverAckMgr.minQueryLevel = from
	s.timerQueueFailoverAckMgr.maxQueryLevel = from
	s.timerQueueFailoverAckMgr.ackLevel = timerKey{VisibilityTimestamp: from}

	// create 3 timers, timer1 < timer2 < timer3 < now
	timer1 := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			TestNamespaceId,
			"some random workflow ID",
			uuid.New(),
		),
		VisibilityTimestamp: time.Now().UTC().Add(-5 * time.Second),
		TaskID:              int64(59),
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             int64(28),
		Attempt:             1,
	}

	timer2 := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			TestNamespaceId,
			"some random workflow ID",
			uuid.New(),
		),
		VisibilityTimestamp: timer1.VisibilityTimestamp,
		TaskID:              timer1.TaskID + 1,
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             int64(29),
		Attempt:             1,
	}
	timer3 := &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			TestNamespaceId,
			"some random workflow ID",
			uuid.New(),
		),
		VisibilityTimestamp: timer2.VisibilityTimestamp.Add(time.Second),
		TaskID:              timer2.TaskID + 1,
		TimeoutType:         enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		EventID:             int64(30),
		Attempt:             1,
	}
	response := &persistence.GetTimerTasksResponse{
		Tasks:         []tasks.Task{timer1, timer2, timer3},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetTimerTasks(gomock.Any()).Return(response, nil)
	filteredTasks, lookAheadTask, moreTasks, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]tasks.Task{timer1, timer2, timer3}, filteredTasks)
	s.Nil(lookAheadTask)
	s.False(moreTasks)

	timerSequenceID2 := newTimerKey(timer2.VisibilityTimestamp, timer2.TaskID)
	s.timerQueueFailoverAckMgr.completeTimerTask(timer2.VisibilityTimestamp, timer2.TaskID)
	s.True(s.timerQueueFailoverAckMgr.outstandingTasks[*timerSequenceID2])
	s.mockShardMgr.EXPECT().UpdateShard(gomock.Any()).Return(nil)
	_ = s.timerQueueFailoverAckMgr.updateAckLevel()
	select {
	case <-s.timerQueueFailoverAckMgr.getFinishedChan():
		s.Fail("timer queue ack mgr finished chan should not be fired")
	default:
	}

	timerSequenceID3 := newTimerKey(timer3.VisibilityTimestamp, timer3.TaskID)
	s.timerQueueFailoverAckMgr.completeTimerTask(timer3.VisibilityTimestamp, timer3.TaskID)
	s.True(s.timerQueueFailoverAckMgr.outstandingTasks[*timerSequenceID3])
	s.mockShardMgr.EXPECT().UpdateShard(gomock.Any()).Return(nil)
	_ = s.timerQueueFailoverAckMgr.updateAckLevel()
	select {
	case <-s.timerQueueFailoverAckMgr.getFinishedChan():
		s.Fail("timer queue ack mgr finished chan should not be fired")
	default:
	}

	timerSequenceID1 := newTimerKey(timer1.VisibilityTimestamp, timer1.TaskID)
	s.timerQueueFailoverAckMgr.completeTimerTask(timer1.VisibilityTimestamp, timer1.TaskID)
	s.True(s.timerQueueFailoverAckMgr.outstandingTasks[*timerSequenceID1])
	s.mockShardMgr.EXPECT().UpdateShard(gomock.Any()).Return(nil)
	_ = s.timerQueueFailoverAckMgr.updateAckLevel()
	select {
	case <-s.timerQueueFailoverAckMgr.getFinishedChan():
	default:
		s.Fail("timer queue ack mgr finished chan should be fired")
	}
}
