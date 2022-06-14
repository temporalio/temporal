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

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	queueAckMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		mockShard  *shard.ContextTest

		mockProcessor *Mockprocessor

		logger      log.Logger
		queueAckMgr *queueAckMgrImpl
	}

	queueFailoverAckMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		mockShard  *shard.ContextTest

		mockProcessor *Mockprocessor

		logger              log.Logger
		queueFailoverAckMgr *queueAckMgrImpl
	}
)

func TestQueueAckMgrSuite(t *testing.T) {
	s := new(queueAckMgrSuite)
	suite.Run(t, s)
}

func TestQueueFailoverAckMgrSuite(t *testing.T) {
	s := new(queueFailoverAckMgrSuite)
	suite.Run(t, s)
}

func (s *queueAckMgrSuite) SetupSuite() {

}

func (s *queueAckMgrSuite) TearDownSuite() {

}

func (s *queueAckMgrSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	config := tests.NewDynamicConfig()
	config.ShardUpdateMinInterval = dynamicconfig.GetDurationPropertyFn(0 * time.Second)

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&p.ShardInfoWithFailover{
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

	s.mockProcessor = NewMockprocessor(s.controller)

	s.logger = s.mockShard.GetLogger()

	s.queueAckMgr = newQueueAckMgr(
		s.mockShard,
		&QueueProcessorOptions{
			MetricScope: metrics.TransferActiveQueueProcessorScope,
		},
		s.mockProcessor,
		0,
		s.logger,
		func(task tasks.Task) queues.Executable {
			return queues.NewExecutable(task, nil, nil, nil, nil, s.mockShard.GetTimeSource(), nil, nil, queues.QueueTypeActiveTransfer, nil)
		},
	)
}

func (s *queueAckMgrSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *queueAckMgrSuite) TestReadTimerTasks() {
	readLevel := s.queueAckMgr.readLevel
	// when the ack manager is first initialized, read == ack level
	s.Equal(s.queueAckMgr.getQueueAckLevel(), readLevel)

	moreInput := false
	taskID1 := int64(59)
	tasksInput := []tasks.Task{
		&tasks.WorkflowTask{
			WorkflowKey: definition.NewWorkflowKey(
				TestNamespaceId,
				"some random workflow ID",
				uuid.New(),
			),
			TaskID:           taskID1,
			TaskQueue:        "some random task queue",
			ScheduledEventID: 28,
		},
	}

	s.mockProcessor.EXPECT().readTasks(readLevel).Return(tasksInput, moreInput, nil)

	taskExecutables, moreOutput, err := s.queueAckMgr.readQueueTasks()
	s.Nil(err)

	tasksOutput := make([]tasks.Task, 0, len(taskExecutables))
	for _, executable := range taskExecutables {
		tasksOutput = append(tasksOutput, executable.GetTask())
		s.Equal(ctasks.TaskStatePending, s.queueAckMgr.outstandingExecutables[executable.GetTaskID()].State())
	}
	s.Equal(tasksOutput, tasksInput)
	s.Equal(moreOutput, moreInput)
	s.Len(s.queueAckMgr.outstandingExecutables, 1)

	moreInput = true
	taskID2 := int64(60)
	tasksInput = []tasks.Task{
		&tasks.WorkflowTask{
			WorkflowKey: definition.NewWorkflowKey(
				TestNamespaceId,
				"some random workflow ID",
				uuid.New(),
			),
			TaskID:           taskID2,
			TaskQueue:        "some random task queue",
			ScheduledEventID: 29,
		},
	}

	s.mockProcessor.EXPECT().readTasks(taskID1).Return(tasksInput, moreInput, nil)

	taskExecutables, moreOutput, err = s.queueAckMgr.readQueueTasks()
	s.Nil(err)

	tasksOutput = make([]tasks.Task, 0, len(taskExecutables))
	for _, executable := range taskExecutables {
		tasksOutput = append(tasksOutput, executable.GetTask())
		s.Equal(ctasks.TaskStatePending, s.queueAckMgr.outstandingExecutables[executable.GetTaskID()].State())
	}
	s.Equal(tasksOutput, tasksInput)
	s.Equal(moreOutput, moreInput)
	s.Len(s.queueAckMgr.outstandingExecutables, 2)
}

func (s *queueAckMgrSuite) TestReadCompleteTimerTasks() {
	readLevel := s.queueAckMgr.readLevel
	// when the ack manager is first initialized, read == ack level
	s.Equal(s.queueAckMgr.getQueueAckLevel(), readLevel)

	moreInput := false
	taskID := int64(59)
	tasksInput := []tasks.Task{
		&tasks.WorkflowTask{
			WorkflowKey: definition.NewWorkflowKey(
				TestNamespaceId,
				"some random workflow ID",
				uuid.New(),
			),
			TaskID:           taskID,
			TaskQueue:        "some random task queue",
			ScheduledEventID: 28,
		},
	}

	s.mockProcessor.EXPECT().readTasks(readLevel).Return(tasksInput, moreInput, nil)

	taskExecutables, moreOutput, err := s.queueAckMgr.readQueueTasks()
	s.Nil(err)

	tasksOutput := make([]tasks.Task, 0, len(taskExecutables))
	for _, executable := range taskExecutables {
		tasksOutput = append(tasksOutput, executable.GetTask())
		s.Equal(ctasks.TaskStatePending, s.queueAckMgr.outstandingExecutables[executable.GetTaskID()].State())
	}
	s.Equal(tasksOutput, tasksInput)
	s.Equal(moreOutput, moreInput)
	s.Len(s.queueAckMgr.outstandingExecutables, 1)

	taskExecutables[0].Ack()
	s.Equal(ctasks.TaskStateAcked, s.queueAckMgr.outstandingExecutables[taskID].State())
}

func (s *queueAckMgrSuite) TestReadCompleteUpdateTimerTasks() {
	readLevel := s.queueAckMgr.readLevel
	// when the ack manager is first initialized, read == ack level
	s.Equal(s.queueAckMgr.getQueueAckLevel(), readLevel)

	moreInput := true
	taskID1 := int64(59)
	taskID2 := int64(60)
	taskID3 := int64(61)
	tasksInput := []tasks.Task{
		&tasks.WorkflowTask{
			WorkflowKey: definition.NewWorkflowKey(
				TestNamespaceId,
				"some random workflow ID",
				uuid.New(),
			),
			TaskID:           taskID1,
			TaskQueue:        "some random task queue",
			ScheduledEventID: 28,
		},
		&tasks.WorkflowTask{
			WorkflowKey: definition.NewWorkflowKey(
				TestNamespaceId,
				"some random workflow ID",
				uuid.New(),
			),
			TaskID:           taskID2,
			TaskQueue:        "some random task queue",
			ScheduledEventID: 28,
		},
		&tasks.WorkflowTask{
			WorkflowKey: definition.NewWorkflowKey(
				TestNamespaceId,
				"some random workflow ID",
				uuid.New(),
			),
			TaskID:           taskID3,
			TaskQueue:        "some random task queue",
			ScheduledEventID: 28,
		},
	}

	s.mockProcessor.EXPECT().readTasks(readLevel).Return(tasksInput, moreInput, nil)

	taskExecutables, moreOutput, err := s.queueAckMgr.readQueueTasks()
	s.Nil(err)

	tasksOutput := make([]tasks.Task, 0, len(taskExecutables))
	for _, executable := range taskExecutables {
		tasksOutput = append(tasksOutput, executable.GetTask())
		s.Equal(ctasks.TaskStatePending, s.queueAckMgr.outstandingExecutables[executable.GetTaskID()].State())
	}
	s.Equal(tasksOutput, tasksInput)
	s.Equal(moreOutput, moreInput)
	s.Len(s.queueAckMgr.outstandingExecutables, 3)

	s.mockProcessor.EXPECT().updateAckLevel(taskID1).Return(nil)
	taskExecutables[0].Ack()
	s.queueAckMgr.updateQueueAckLevel()
	s.Equal(taskID1, s.queueAckMgr.getQueueAckLevel())

	s.mockProcessor.EXPECT().updateAckLevel(taskID1).Return(nil)
	taskExecutables[2].Ack()
	s.queueAckMgr.updateQueueAckLevel()
	s.Equal(taskID1, s.queueAckMgr.getQueueAckLevel())

	s.mockProcessor.EXPECT().updateAckLevel(taskID3).Return(nil)
	taskExecutables[1].Ack()
	s.queueAckMgr.updateQueueAckLevel()
	s.Equal(taskID3, s.queueAckMgr.getQueueAckLevel())
}

// Tests for failover ack manager
func (s *queueFailoverAckMgrSuite) SetupSuite() {

}

func (s *queueFailoverAckMgrSuite) TearDownSuite() {

}

func (s *queueFailoverAckMgrSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	config := tests.NewDynamicConfig()
	config.ShardUpdateMinInterval = dynamicconfig.GetDurationPropertyFn(0 * time.Second)

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&p.ShardInfoWithFailover{
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
		},
		config,
	)

	s.mockProcessor = NewMockprocessor(s.controller)

	s.logger = s.mockShard.GetLogger()

	s.queueFailoverAckMgr = newQueueFailoverAckMgr(
		s.mockShard,
		&QueueProcessorOptions{
			MetricScope: metrics.TransferQueueProcessorScope,
		},
		s.mockProcessor,
		0,
		s.logger,
		func(task tasks.Task) queues.Executable {
			return queues.NewExecutable(task, nil, nil, nil, nil, s.mockShard.GetTimeSource(), nil, nil, queues.QueueTypeActiveTransfer, nil)
		},
	)
}

func (s *queueFailoverAckMgrSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *queueFailoverAckMgrSuite) TestReadQueueTasks() {
	readLevel := s.queueFailoverAckMgr.readLevel
	// when the ack manager is first initialized, read == ack level
	s.Equal(s.queueFailoverAckMgr.getQueueAckLevel(), readLevel)

	moreInput := true
	taskID1 := int64(59)
	tasksInput := []tasks.Task{
		&tasks.WorkflowTask{
			WorkflowKey: definition.NewWorkflowKey(
				TestNamespaceId,
				"some random workflow ID",
				uuid.New(),
			),
			TaskID:           taskID1,
			TaskQueue:        "some random task queue",
			ScheduledEventID: 28,
		},
	}

	s.mockProcessor.EXPECT().readTasks(readLevel).Return(tasksInput, moreInput, nil)

	taskExecutables, moreOutput, err := s.queueFailoverAckMgr.readQueueTasks()
	s.Nil(err)

	tasksOutput := make([]tasks.Task, 0, len(taskExecutables))
	for _, executable := range taskExecutables {
		tasksOutput = append(tasksOutput, executable.GetTask())
		s.Equal(ctasks.TaskStatePending, s.queueFailoverAckMgr.outstandingExecutables[executable.GetTaskID()].State())
	}
	s.Equal(tasksOutput, tasksInput)
	s.Equal(moreOutput, moreInput)
	s.Len(s.queueFailoverAckMgr.outstandingExecutables, 1)
	s.False(s.queueFailoverAckMgr.isReadFinished)

	moreInput = false
	taskID2 := int64(60)
	tasksInput = []tasks.Task{
		&tasks.WorkflowTask{
			WorkflowKey: definition.NewWorkflowKey(
				TestNamespaceId,
				"some random workflow ID",
				uuid.New(),
			),
			TaskID:           taskID2,
			TaskQueue:        "some random task queue",
			ScheduledEventID: 29,
		},
	}

	s.mockProcessor.EXPECT().readTasks(taskID1).Return(tasksInput, moreInput, nil)

	taskExecutables, moreOutput, err = s.queueFailoverAckMgr.readQueueTasks()
	s.Nil(err)

	tasksOutput = make([]tasks.Task, 0, len(taskExecutables))
	for _, executable := range taskExecutables {
		tasksOutput = append(tasksOutput, executable.GetTask())
		s.Equal(ctasks.TaskStatePending, s.queueFailoverAckMgr.outstandingExecutables[executable.GetTaskID()].State())
	}
	s.Equal(tasksOutput, tasksInput)
	s.Equal(moreOutput, moreInput)
	s.Len(s.queueFailoverAckMgr.outstandingExecutables, 2)
	s.True(s.queueFailoverAckMgr.isReadFinished)
}

func (s *queueFailoverAckMgrSuite) TestReadCompleteQueueTasks() {
	readLevel := s.queueFailoverAckMgr.readLevel
	// when the ack manager is first initialized, read == ack level
	s.Equal(s.queueFailoverAckMgr.getQueueAckLevel(), readLevel)

	moreInput := false
	taskID1 := int64(59)
	taskID2 := int64(60)
	tasksInput := []tasks.Task{
		&tasks.WorkflowTask{
			WorkflowKey: definition.NewWorkflowKey(
				TestNamespaceId,
				"some random workflow ID",
				uuid.New(),
			),
			TaskID:           taskID1,
			TaskQueue:        "some random task queue",
			ScheduledEventID: 28,
		},
		&tasks.WorkflowTask{
			WorkflowKey: definition.NewWorkflowKey(
				TestNamespaceId,
				"some random workflow ID",
				uuid.New(),
			),
			TaskID:           taskID2,
			TaskQueue:        "some random task queue",
			ScheduledEventID: 29,
		},
	}

	s.mockProcessor.EXPECT().readTasks(readLevel).Return(tasksInput, moreInput, nil)

	taskExecutables, moreOutput, err := s.queueFailoverAckMgr.readQueueTasks()
	s.Nil(err)

	tasksOutput := make([]tasks.Task, 0, len(taskExecutables))
	for _, executable := range taskExecutables {
		tasksOutput = append(tasksOutput, executable.GetTask())
		s.Equal(ctasks.TaskStatePending, s.queueFailoverAckMgr.outstandingExecutables[executable.GetTaskID()].State())
	}
	s.Equal(tasksOutput, tasksInput)
	s.Equal(moreOutput, moreInput)
	s.Len(s.queueFailoverAckMgr.outstandingExecutables, 2)

	taskExecutables[1].Ack()
	s.Equal(ctasks.TaskStateAcked, s.queueFailoverAckMgr.outstandingExecutables[taskID2].State())
	s.mockProcessor.EXPECT().updateAckLevel(s.queueFailoverAckMgr.getQueueAckLevel()).Return(nil)
	s.queueFailoverAckMgr.updateQueueAckLevel()
	select {
	case <-s.queueFailoverAckMgr.getFinishedChan():
		s.Fail("finished channel should not fire")
	default:
	}

	taskExecutables[0].Ack()
	s.Equal(ctasks.TaskStateAcked, s.queueFailoverAckMgr.outstandingExecutables[taskID1].State())
	s.mockProcessor.EXPECT().queueShutdown().Return(nil)
	s.queueFailoverAckMgr.updateQueueAckLevel()
	select {
	case <-s.queueFailoverAckMgr.getFinishedChan():
	default:
		s.Fail("finished channel should fire")
	}
}
