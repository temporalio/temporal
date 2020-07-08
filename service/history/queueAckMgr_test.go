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

	"github.com/gogo/protobuf/types"

	"github.com/temporalio/temporal/api/persistenceblobs/v1"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	p "github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

type (
	queueAckMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		mockShard  *shardContextTest

		mockProcessor *MockProcessor

		logger      log.Logger
		queueAckMgr *queueAckMgrImpl
	}

	queueFailoverAckMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller *gomock.Controller
		mockShard  *shardContextTest

		mockProcessor *MockProcessor

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

	config := NewDynamicConfigForTest()
	config.ShardUpdateMinInterval = dynamicconfig.GetDurationPropertyFn(0 * time.Second)

	s.controller = gomock.NewController(s.T())
	s.mockShard = newTestShardContext(
		s.controller,
		&p.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardId: 0,
				RangeId: 1,
				ClusterTimerAckLevel: map[string]*types.Timestamp{
					cluster.TestCurrentClusterName:     gogoProtoTimestampNowAddDuration(-8),
					cluster.TestAlternativeClusterName: gogoProtoTimestampNowAddDuration(-10),
				}},
		},
		config,
	)

	s.mockProcessor = &MockProcessor{}

	s.logger = s.mockShard.GetLogger()

	s.queueAckMgr = newQueueAckMgr(s.mockShard, &QueueProcessorOptions{
		MetricScope: metrics.ReplicatorQueueProcessorScope,
	}, s.mockProcessor, 0, s.logger)
}

func (s *queueAckMgrSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
	s.mockProcessor.AssertExpectations(s.T())
}

func (s *queueAckMgrSuite) TestReadTimerTasks() {
	readLevel := s.queueAckMgr.readLevel
	// when the ack manager is first initialized, read == ack level
	s.Equal(s.queueAckMgr.getQueueAckLevel(), readLevel)

	moreInput := false
	taskID1 := int64(59)
	tasksInput := []queueTaskInfo{
		&persistenceblobs.TransferTaskInfo{
			NamespaceId: TestNamespaceId,
			WorkflowId:  "some random workflow ID",
			RunId:       uuid.New(),
			TaskId:      taskID1,
			TaskQueue:   "some random taskqueue",
			TaskType:    1,
			ScheduleId:  28,
		},
	}

	s.mockProcessor.On("readTasks", readLevel).Return(tasksInput, moreInput, nil).Once()

	tasksOutput, moreOutput, err := s.queueAckMgr.readQueueTasks()
	s.Nil(err)
	s.Equal(tasksOutput, tasksInput)
	s.Equal(moreOutput, moreInput)
	s.Equal(map[int64]bool{taskID1: false}, s.queueAckMgr.outstandingTasks)

	moreInput = true
	taskID2 := int64(60)
	tasksInput = []queueTaskInfo{
		&persistenceblobs.TransferTaskInfo{
			NamespaceId: TestNamespaceId,
			WorkflowId:  "some random workflow ID",
			RunId:       uuid.New(),
			TaskId:      taskID2,
			TaskQueue:   "some random taskqueue",
			TaskType:    1,
			ScheduleId:  29,
		},
	}

	s.mockProcessor.On("readTasks", taskID1).Return(tasksInput, moreInput, nil).Once()

	tasksOutput, moreOutput, err = s.queueAckMgr.readQueueTasks()
	s.Nil(err)
	s.Equal(tasksOutput, tasksInput)
	s.Equal(moreOutput, moreInput)
	s.Equal(map[int64]bool{taskID1: false, taskID2: false}, s.queueAckMgr.outstandingTasks)
}

func (s *queueAckMgrSuite) TestReadCompleteTimerTasks() {
	readLevel := s.queueAckMgr.readLevel
	// when the ack manager is first initialized, read == ack level
	s.Equal(s.queueAckMgr.getQueueAckLevel(), readLevel)

	moreInput := false
	taskID := int64(59)
	tasksInput := []queueTaskInfo{
		&persistenceblobs.TransferTaskInfo{
			NamespaceId: TestNamespaceId,
			WorkflowId:  "some random workflow ID",
			RunId:       uuid.New(),
			TaskId:      taskID,
			TaskQueue:   "some random taskqueue",
			TaskType:    1,
			ScheduleId:  28,
		},
	}

	s.mockProcessor.On("readTasks", readLevel).Return(tasksInput, moreInput, nil).Once()

	tasksOutput, moreOutput, err := s.queueAckMgr.readQueueTasks()
	s.Nil(err)
	s.Equal(tasksOutput, tasksInput)
	s.Equal(moreOutput, moreInput)
	s.Equal(map[int64]bool{taskID: false}, s.queueAckMgr.outstandingTasks)

	s.queueAckMgr.completeQueueTask(taskID)
	s.Equal(map[int64]bool{taskID: true}, s.queueAckMgr.outstandingTasks)
}

func (s *queueAckMgrSuite) TestReadCompleteUpdateTimerTasks() {
	readLevel := s.queueAckMgr.readLevel
	// when the ack manager is first initialized, read == ack level
	s.Equal(s.queueAckMgr.getQueueAckLevel(), readLevel)

	moreInput := true
	taskID1 := int64(59)
	taskID2 := int64(60)
	taskID3 := int64(61)
	tasksInput := []queueTaskInfo{
		&persistenceblobs.TransferTaskInfo{
			NamespaceId: TestNamespaceId,
			WorkflowId:  "some random workflow ID",
			RunId:       uuid.New(),
			TaskId:      taskID1,
			TaskQueue:   "some random taskqueue",
			TaskType:    1,
			ScheduleId:  28,
		},
		&persistenceblobs.TransferTaskInfo{
			NamespaceId: TestNamespaceId,
			WorkflowId:  "some random workflow ID",
			RunId:       uuid.New(),
			TaskId:      taskID2,
			TaskQueue:   "some random taskqueue",
			TaskType:    1,
			ScheduleId:  28,
		},
		&persistenceblobs.TransferTaskInfo{
			NamespaceId: TestNamespaceId,
			WorkflowId:  "some random workflow ID",
			RunId:       uuid.New(),
			TaskId:      taskID3,
			TaskQueue:   "some random taskqueue",
			TaskType:    1,
			ScheduleId:  28,
		},
	}

	s.mockProcessor.On("readTasks", readLevel).Return(tasksInput, moreInput, nil).Once()

	tasksOutput, moreOutput, err := s.queueAckMgr.readQueueTasks()
	s.Nil(err)
	s.Equal(tasksOutput, tasksInput)
	s.Equal(moreOutput, moreInput)
	s.Equal(map[int64]bool{taskID1: false, taskID2: false, taskID3: false}, s.queueAckMgr.outstandingTasks)

	s.mockProcessor.On("updateAckLevel", taskID1).Return(nil).Once()
	s.queueAckMgr.completeQueueTask(taskID1)
	s.queueAckMgr.updateQueueAckLevel()
	s.Equal(taskID1, s.queueAckMgr.getQueueAckLevel())

	s.mockProcessor.On("updateAckLevel", taskID1).Return(nil).Once()
	s.queueAckMgr.completeQueueTask(taskID3)
	s.queueAckMgr.updateQueueAckLevel()
	s.Equal(taskID1, s.queueAckMgr.getQueueAckLevel())

	s.mockProcessor.On("updateAckLevel", taskID3).Return(nil).Once()
	s.queueAckMgr.completeQueueTask(taskID2)
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

	config := NewDynamicConfigForTest()
	config.ShardUpdateMinInterval = dynamicconfig.GetDurationPropertyFn(0 * time.Second)

	s.controller = gomock.NewController(s.T())
	s.mockShard = newTestShardContext(
		s.controller,
		&p.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardId: 0,
				RangeId: 1,
				ClusterTimerAckLevel: map[string]*types.Timestamp{
					cluster.TestCurrentClusterName:     types.TimestampNow(),
					cluster.TestAlternativeClusterName: gogoProtoTimestampNowAddDuration(-10),
				}},
		},
		config,
	)

	s.mockProcessor = &MockProcessor{}

	s.logger = s.mockShard.GetLogger()

	s.queueFailoverAckMgr = newQueueFailoverAckMgr(s.mockShard, &QueueProcessorOptions{
		MetricScope: metrics.ReplicatorQueueProcessorScope,
	}, s.mockProcessor, 0, s.logger)
}

func (s *queueFailoverAckMgrSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
	s.mockProcessor.AssertExpectations(s.T())
}

func (s *queueFailoverAckMgrSuite) TestReadQueueTasks() {
	readLevel := s.queueFailoverAckMgr.readLevel
	// when the ack manager is first initialized, read == ack level
	s.Equal(s.queueFailoverAckMgr.getQueueAckLevel(), readLevel)

	moreInput := true
	taskID1 := int64(59)
	tasksInput := []queueTaskInfo{
		&persistenceblobs.TransferTaskInfo{
			NamespaceId: TestNamespaceId,
			WorkflowId:  "some random workflow ID",
			RunId:       uuid.New(),
			TaskId:      taskID1,
			TaskQueue:   "some random taskqueue",
			TaskType:    1,
			ScheduleId:  28,
		},
	}

	s.mockProcessor.On("readTasks", readLevel).Return(tasksInput, moreInput, nil).Once()

	tasksOutput, moreOutput, err := s.queueFailoverAckMgr.readQueueTasks()
	s.Nil(err)
	s.Equal(tasksOutput, tasksInput)
	s.Equal(moreOutput, moreInput)
	s.Equal(map[int64]bool{taskID1: false}, s.queueFailoverAckMgr.outstandingTasks)
	s.False(s.queueFailoverAckMgr.isReadFinished)

	moreInput = false
	taskID2 := int64(60)
	tasksInput = []queueTaskInfo{
		&persistenceblobs.TransferTaskInfo{
			NamespaceId: TestNamespaceId,
			WorkflowId:  "some random workflow ID",
			RunId:       uuid.New(),
			TaskId:      taskID2,
			TaskQueue:   "some random taskqueue",
			TaskType:    1,
			ScheduleId:  29,
		},
	}

	s.mockProcessor.On("readTasks", taskID1).Return(tasksInput, moreInput, nil).Once()

	tasksOutput, moreOutput, err = s.queueFailoverAckMgr.readQueueTasks()
	s.Nil(err)
	s.Equal(tasksOutput, tasksInput)
	s.Equal(moreOutput, moreInput)
	s.Equal(map[int64]bool{taskID1: false, taskID2: false}, s.queueFailoverAckMgr.outstandingTasks)
	s.True(s.queueFailoverAckMgr.isReadFinished)
}

func (s *queueFailoverAckMgrSuite) TestReadCompleteQueueTasks() {
	readLevel := s.queueFailoverAckMgr.readLevel
	// when the ack manager is first initialized, read == ack level
	s.Equal(s.queueFailoverAckMgr.getQueueAckLevel(), readLevel)

	moreInput := false
	taskID1 := int64(59)
	taskID2 := int64(60)
	tasksInput := []queueTaskInfo{
		&persistenceblobs.TransferTaskInfo{
			NamespaceId: TestNamespaceId,
			WorkflowId:  "some random workflow ID",
			RunId:       uuid.New(),
			TaskId:      taskID1,
			TaskQueue:   "some random taskqueue",
			TaskType:    1,
			ScheduleId:  28,
		},
		&persistenceblobs.TransferTaskInfo{
			NamespaceId: TestNamespaceId,
			WorkflowId:  "some random workflow ID",
			RunId:       uuid.New(),
			TaskId:      taskID2,
			TaskQueue:   "some random taskqueue",
			TaskType:    2,
			ScheduleId:  29,
		},
	}

	s.mockProcessor.On("readTasks", readLevel).Return(tasksInput, moreInput, nil).Once()

	tasksOutput, moreOutput, err := s.queueFailoverAckMgr.readQueueTasks()
	s.Nil(err)
	s.Equal(tasksOutput, tasksInput)
	s.Equal(moreOutput, moreInput)
	s.Equal(map[int64]bool{taskID1: false, taskID2: false}, s.queueFailoverAckMgr.outstandingTasks)

	s.queueFailoverAckMgr.completeQueueTask(taskID2)
	s.Equal(map[int64]bool{taskID1: false, taskID2: true}, s.queueFailoverAckMgr.outstandingTasks)
	s.mockProcessor.On("updateAckLevel", s.queueFailoverAckMgr.getQueueAckLevel()).Return(nil)
	s.queueFailoverAckMgr.updateQueueAckLevel()
	select {
	case <-s.queueFailoverAckMgr.getFinishedChan():
		s.Fail("finished channel should not fire")
	default:
	}

	s.queueFailoverAckMgr.completeQueueTask(taskID1)
	s.Equal(map[int64]bool{taskID1: true, taskID2: true}, s.queueFailoverAckMgr.outstandingTasks)
	s.mockProcessor.On("queueShutdown").Return(nil)
	s.queueFailoverAckMgr.updateQueueAckLevel()
	select {
	case <-s.queueFailoverAckMgr.getFinishedChan():
	default:
		s.Fail("finished channel should fire")
	}
}
