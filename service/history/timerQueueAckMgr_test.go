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

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
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
				QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{
					tasks.CategoryTimer.ID(): {
						ClusterAckLevel: map[string]int64{
							cluster.TestCurrentClusterName:     timestamp.TimeNowPtrUtcAddSeconds(-8).UnixNano(),
							cluster.TestAlternativeClusterName: timestamp.TimeNowPtrUtcAddSeconds(-10).UnixNano(),
						},
					},
				},
			},
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
		s.mockShard.GetQueueClusterAckLevel(tasks.CategoryTimer, s.clusterName).FireTime,
		func() time.Time {
			return s.mockShard.GetCurrentTime(s.clusterName)
		},
		func(ackLevel tasks.Key) error {
			return s.mockShard.UpdateQueueClusterAckLevel(
				tasks.CategoryTimer,
				s.clusterName,
				ackLevel,
			)
		},
		s.logger,
		s.clusterName,
		func(task tasks.Task) queues.Executable {
			return queues.NewExecutable(task, nil, nil, nil, nil, s.mockShard.GetTimeSource(), nil, nil, queues.QueueTypeActiveTimer, nil)
		},
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

	request := &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryTimer,
		InclusiveMinTaskKey: tasks.NewKey(minTimestamp, 0),
		ExclusiveMaxTaskKey: tasks.NewKey(maxTimestamp, 0),
		BatchSize:           batchSize,
		NextPageToken:       []byte("some random input next page token"),
	}

	response := &persistence.GetHistoryTasksResponse{
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

	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), request).Return(response, nil)

	timers, token, err := s.timerQueueAckMgr.getTimerTasks(minTimestamp, maxTimestamp, batchSize, request.NextPageToken)
	s.Nil(err)
	s.Equal(response.Tasks, timers)
	s.Equal(response.NextPageToken, token)
}

func (s *timerQueueAckMgrSuite) TestGetTimerTasks_NoMore() {
	minTimestamp := time.Now().UTC().Add(-10 * time.Second)
	maxTimestamp := time.Now().UTC().Add(10 * time.Second)
	batchSize := 10

	request := &persistence.GetHistoryTasksRequest{
		ShardID:             s.mockShard.GetShardID(),
		TaskCategory:        tasks.CategoryTimer,
		InclusiveMinTaskKey: tasks.NewKey(minTimestamp, 0),
		ExclusiveMaxTaskKey: tasks.NewKey(maxTimestamp, 0),
		BatchSize:           batchSize,
		NextPageToken:       nil,
	}

	response := &persistence.GetHistoryTasksResponse{
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

	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), request).Return(response, nil)

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
	s.Equal(s.mockShard.GetQueueClusterAckLevel(tasks.CategoryTimer, s.clusterName), ackLevel)
	s.Equal(s.mockShard.GetQueueClusterAckLevel(tasks.CategoryTimer, s.clusterName).FireTime, minQueryLevel)
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

	response := &persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{timer},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(response, nil)
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(&persistence.GetHistoryTasksResponse{}, nil)
	filteredExecutables, nextFireTime, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)

	filteredTasks := make([]tasks.Task, 0, len(filteredExecutables))
	for _, executable := range filteredExecutables {
		filteredTasks = append(filteredTasks, executable.GetTask())
	}
	s.Equal([]tasks.Task{timer}, filteredTasks)
	s.Equal(s.timerQueueAckMgr.maxQueryLevel.Add(s.timerQueueAckMgr.config.TimerProcessorMaxPollInterval()), *nextFireTime)
	s.False(moreTasks)

	timerSequenceID := tasks.NewKey(timer.VisibilityTimestamp, timer.TaskID)
	s.Len(s.timerQueueAckMgr.outstandingExecutables, 1)
	s.Equal(ctasks.TaskStatePending, s.timerQueueAckMgr.outstandingExecutables[timerSequenceID].State())
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
	s.Equal(s.mockShard.GetQueueClusterAckLevel(tasks.CategoryTimer, s.clusterName), ackLevel)
	s.Equal(s.mockShard.GetQueueClusterAckLevel(tasks.CategoryTimer, s.clusterName).FireTime, minQueryLevel)
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

	response := &persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{timer},
		NextPageToken: []byte("some random next page token"),
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(response, nil)
	readTimestamp := time.Now().UTC() // the approximate time of calling readTimerTasks
	filteredExecutables, nextFireTime, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)

	filteredTasks := make([]tasks.Task, 0, len(filteredExecutables))
	for _, executable := range filteredExecutables {
		filteredTasks = append(filteredTasks, executable.GetTask())
	}
	s.Equal([]tasks.Task{timer}, filteredTasks)
	s.Nil(nextFireTime)
	s.True(moreTasks)
	timerSequenceID := tasks.NewKey(timer.VisibilityTimestamp, timer.TaskID)
	s.Len(s.timerQueueAckMgr.outstandingExecutables, 1)
	s.Equal(ctasks.TaskStatePending, s.timerQueueAckMgr.outstandingExecutables[timerSequenceID].State())
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
	s.Equal(s.mockShard.GetQueueClusterAckLevel(tasks.CategoryTimer, s.clusterName), ackLevel)
	s.Equal(s.mockShard.GetQueueClusterAckLevel(tasks.CategoryTimer, s.clusterName).FireTime, minQueryLevel)
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

	response := &persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{timer},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(response, nil)
	filteredExecutables, nextFireTime, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)

	filteredTasks := make([]tasks.Task, 0, len(filteredExecutables))
	for _, executable := range filteredExecutables {
		filteredTasks = append(filteredTasks, executable.GetTask())
	}
	s.Equal([]tasks.Task{}, filteredTasks)
	s.Equal(timer.GetVisibilityTime(), *nextFireTime)
	s.False(moreTasks)

	s.Len(s.timerQueueAckMgr.outstandingExecutables, 0)
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
	s.Equal(s.mockShard.GetQueueClusterAckLevel(tasks.CategoryTimer, s.clusterName), ackLevel)
	s.Equal(s.mockShard.GetQueueClusterAckLevel(tasks.CategoryTimer, s.clusterName).FireTime, minQueryLevel)
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

	response := &persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{timer},
		NextPageToken: []byte("some random next page token"),
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(response, nil)
	filteredExecutables, nextFireTime, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)

	s.Equal([]queues.Executable{}, filteredExecutables)
	s.Equal(timer.GetVisibilityTime(), *nextFireTime)
	s.False(moreTasks)

	s.Len(s.timerQueueAckMgr.outstandingExecutables, 0)
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
	response := &persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{timer1, timer2, timer3},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(response, nil)
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(&persistence.GetHistoryTasksResponse{}, nil)
	filteredExecutables, nextFireTime, moreTasks, err := s.timerQueueAckMgr.readTimerTasks()
	s.Nil(err)

	filteredTasks := make([]tasks.Task, 0, len(filteredExecutables))
	for _, executable := range filteredExecutables {
		filteredTasks = append(filteredTasks, executable.GetTask())
	}
	s.Equal([]tasks.Task{timer1, timer2, timer3}, filteredTasks)
	s.Equal(s.timerQueueAckMgr.maxQueryLevel.Add(s.timerQueueAckMgr.config.TimerProcessorMaxPollInterval()), *nextFireTime)
	s.False(moreTasks)

	// we are not testing shard context
	s.mockShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil)
	timerSequenceID1 := tasks.NewKey(timer1.VisibilityTimestamp, timer1.TaskID)
	filteredExecutables[0].Ack()
	s.Equal(ctasks.TaskStateAcked, s.timerQueueAckMgr.outstandingExecutables[timerSequenceID1].State())
	_ = s.timerQueueAckMgr.updateAckLevel()
	s.Equal(timer1.VisibilityTimestamp.UnixNano(), s.mockShard.GetQueueClusterAckLevel(tasks.CategoryTimer, s.clusterName).FireTime.UnixNano())

	s.mockShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil)
	timerSequenceID3 := tasks.NewKey(timer3.VisibilityTimestamp, timer3.TaskID)
	filteredExecutables[2].Ack()
	s.Equal(ctasks.TaskStateAcked, s.timerQueueAckMgr.outstandingExecutables[timerSequenceID3].State())
	_ = s.timerQueueAckMgr.updateAckLevel()
	// ack level remains unchanged
	s.Equal(timer1.VisibilityTimestamp.UnixNano(), s.mockShard.GetQueueClusterAckLevel(tasks.CategoryTimer, s.clusterName).FireTime.UnixNano())

	// we are not testing shard context
	s.mockShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil)
	timerSequenceID2 := tasks.NewKey(timer2.VisibilityTimestamp, timer2.TaskID)
	filteredExecutables[1].Ack()
	s.Equal(ctasks.TaskStateAcked, s.timerQueueAckMgr.outstandingExecutables[timerSequenceID2].State())
	_ = s.timerQueueAckMgr.updateAckLevel()
	s.Equal(timer3.VisibilityTimestamp.UnixNano(), s.mockShard.GetQueueClusterAckLevel(tasks.CategoryTimer, s.clusterName).FireTime.UnixNano())
}

func (s *timerQueueAckMgrSuite) TestReadLookAheadTask() {
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(s.clusterName).AnyTimes()
	level := s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryTimer, s.clusterName).FireTime

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

	response := &persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{timer},
		NextPageToken: []byte("some random next page token"),
	}
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(response, nil)
	nextFireTime, err := s.timerQueueAckMgr.readLookAheadTask()
	s.Nil(err)
	s.Equal(timer.GetVisibilityTime(), *nextFireTime)
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
				QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{
					tasks.CategoryTimer.ID(): {
						ClusterAckLevel: map[string]int64{
							cluster.TestCurrentClusterName:     timestamp.TimeNowPtrUtc().UnixNano(),
							cluster.TestAlternativeClusterName: timestamp.TimeNowPtrUtcAddSeconds(-10).UnixNano(),
						},
					},
				},
			},
			FailoverLevels: make(map[tasks.Category]map[string]persistence.FailoverLevel),
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
		s.minLevel,
		s.maxLevel,
		func() time.Time {
			return s.mockShard.GetCurrentTime(s.mockShard.Resource.GetClusterMetadata().GetCurrentClusterName())
		},
		func(ackLevel tasks.Key) error {
			return s.mockShard.UpdateFailoverLevel(
				tasks.CategoryTimer,
				s.namespaceID,
				persistence.FailoverLevel{
					MinLevel:     ackLevel,
					MaxLevel:     ackLevel,
					NamespaceIDs: map[string]struct{}{s.namespaceID: {}},
				},
			)
		},
		func() error {
			return s.mockShard.DeleteFailoverLevel(tasks.CategoryTimer, s.namespaceID)
		},
		s.logger,
		func(task tasks.Task) queues.Executable {
			return queues.NewExecutable(task, nil, nil, nil, nil, s.mockShard.GetTimeSource(), nil, nil, queues.QueueTypeActiveTimer, nil)
		},
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
	s.Equal(s.minLevel, ackLevel.FireTime)
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

	response := &persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{timer1, timer2},
		NextPageToken: []byte("some random next page token"),
	}

	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(response, nil)
	readTimestamp := time.Now().UTC() // the approximate time of calling readTimerTasks
	filteredExecutables, lookAheadTimer, more, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)

	filteredTasks := make([]tasks.Task, 0, len(filteredExecutables))
	for _, executable := range filteredExecutables {
		filteredTasks = append(filteredTasks, executable.GetTask())
	}
	s.Equal([]tasks.Task{timer1, timer2}, filteredTasks)
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
	s.Equal(s.minLevel, ackLevel.FireTime)
	s.Equal(s.minLevel, minQueryLevel)
	s.Empty(token)
	s.Equal(s.maxLevel, maxQueryLevel)

	response := &persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(response, nil)

	readTimestamp := time.Now().UTC() // the approximate time of calling readTimerTasks
	timers, lookAheadTimer, more, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	s.Equal([]queues.Executable{}, timers)
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
	s.timerQueueFailoverAckMgr.ackLevel = tasks.NewKey(from, 0)

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
	response := &persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{timer1, timer2, timer3},
		NextPageToken: nil,
	}
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(response, nil)
	filteredExecutables, nextFireTime, moreTasks, err := s.timerQueueFailoverAckMgr.readTimerTasks()
	s.Nil(err)
	filteredTasks := make([]tasks.Task, 0, len(filteredExecutables))
	for _, executable := range filteredExecutables {
		filteredTasks = append(filteredTasks, executable.GetTask())
	}
	s.Equal([]tasks.Task{timer1, timer2, timer3}, filteredTasks)
	s.Nil(nextFireTime)
	s.False(moreTasks)

	timerSequenceID2 := tasks.NewKey(timer2.VisibilityTimestamp, timer2.TaskID)
	filteredExecutables[1].Ack()
	s.Equal(ctasks.TaskStateAcked, s.timerQueueFailoverAckMgr.outstandingExecutables[timerSequenceID2].State())
	s.mockShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil)
	_ = s.timerQueueFailoverAckMgr.updateAckLevel()
	select {
	case <-s.timerQueueFailoverAckMgr.getFinishedChan():
		s.Fail("timer queue ack mgr finished chan should not be fired")
	default:
	}

	timerSequenceID3 := tasks.NewKey(timer3.VisibilityTimestamp, timer3.TaskID)
	filteredExecutables[2].Ack()
	s.Equal(ctasks.TaskStateAcked, s.timerQueueFailoverAckMgr.outstandingExecutables[timerSequenceID3].State())
	s.mockShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil)
	_ = s.timerQueueFailoverAckMgr.updateAckLevel()
	select {
	case <-s.timerQueueFailoverAckMgr.getFinishedChan():
		s.Fail("timer queue ack mgr finished chan should not be fired")
	default:
	}

	timerSequenceID1 := tasks.NewKey(timer1.VisibilityTimestamp, timer1.TaskID)
	filteredExecutables[0].Ack()
	s.Equal(ctasks.TaskStateAcked, s.timerQueueFailoverAckMgr.outstandingExecutables[timerSequenceID1].State())
	s.mockShardMgr.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil)
	_ = s.timerQueueFailoverAckMgr.updateAckLevel()
	select {
	case <-s.timerQueueFailoverAckMgr.getFinishedChan():
	default:
		s.Fail("timer queue ack mgr finished chan should be fired")
	}
}
