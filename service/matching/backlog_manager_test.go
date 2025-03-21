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

package matching

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
)

type BacklogManagerTestSuite struct {
	suite.Suite

	newMatcher bool
	logger     *testlogger.TestLogger
	blm        backlogManager
	controller *gomock.Controller
	cancelCtx  context.CancelFunc
	taskMgr    *testTaskManager
	ptqMgr     *MockphysicalTaskQueueManager
}

func TestBacklogManagerTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &BacklogManagerTestSuite{newMatcher: false})
}

func TestBacklogManagerWithNewMatcherTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &BacklogManagerTestSuite{newMatcher: true})
}

func (s *BacklogManagerTestSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
	s.taskMgr = newTestTaskManager(s.logger)

	cfg := NewConfig(dynamicconfig.NewNoopCollection())
	f, _ := tqid.NewTaskQueueFamily("", "test-queue")
	prtn := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(0)
	queue := UnversionedQueueKey(prtn)
	tlCfg := newTaskQueueConfig(prtn.TaskQueue(), cfg, "test-namespace")

	s.ptqMgr = NewMockphysicalTaskQueueManager(s.controller)
	s.ptqMgr.EXPECT().QueueKey().Return(queue).AnyTimes()
	s.ptqMgr.EXPECT().ProcessSpooledTask(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	var ctx context.Context
	ctx, s.cancelCtx = context.WithCancel(context.Background())
	s.T().Cleanup(s.cancelCtx)

	if s.newMatcher {
		s.blm = newPriBacklogManager(
			ctx,
			s.ptqMgr,
			tlCfg,
			s.taskMgr,
			s.logger,
			s.logger,
			nil,
			metrics.NoopMetricsHandler,
		)
	} else {
		s.blm = newBacklogManager(
			ctx,
			s.ptqMgr,
			tlCfg,
			s.taskMgr,
			s.logger,
			s.logger,
			nil,
			metrics.NoopMetricsHandler,
		)
	}
}

func (s *BacklogManagerTestSuite) TestReadLevelForAllExpiredTasksInBatch() {
	if s.newMatcher {
		s.T().Skip("not compatible with new backlog manager")
	}
	blm := s.blm.(*backlogManagerImpl)

	s.NoError(blm.taskWriter.initReadWriteState())
	s.Equal(int64(1), blm.getDB().rangeID)
	s.Equal(int64(0), blm.taskAckManager.getAckLevel())
	s.Equal(int64(0), blm.taskAckManager.getReadLevel())

	// Add all expired tasks
	tasks := []*persistencespb.AllocatedTaskInfo{
		{
			Data: &persistencespb.TaskInfo{
				ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(-60),
				CreateTime: timestamp.TimeNowPtrUtcAddSeconds(-60 * 60),
			},
			TaskId: 11,
		},
		{
			Data: &persistencespb.TaskInfo{
				ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(-60),
				CreateTime: timestamp.TimeNowPtrUtcAddSeconds(-60 * 60),
			},
			TaskId: 12,
		},
	}

	s.NoError(blm.taskReader.addTasksToBuffer(context.TODO(), tasks))
	s.Equal(int64(0), blm.taskAckManager.getAckLevel())
	s.Equal(int64(12), blm.taskAckManager.getReadLevel())

	// Now add a mix of valid and expired tasks
	s.NoError(blm.taskReader.addTasksToBuffer(context.TODO(), []*persistencespb.AllocatedTaskInfo{
		{
			Data: &persistencespb.TaskInfo{
				ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(-60),
				CreateTime: timestamp.TimeNowPtrUtcAddSeconds(-60 * 60),
			},
			TaskId: 13,
		},
		{
			Data: &persistencespb.TaskInfo{
				ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(-60),
				CreateTime: timestamp.TimeNowPtrUtcAddSeconds(-60 * 60),
			},
			TaskId: 14,
		},
	}))
	s.Equal(int64(0), blm.taskAckManager.getAckLevel())
	s.Equal(int64(14), blm.taskAckManager.getReadLevel())
}

func (s *BacklogManagerTestSuite) TestTaskWriterShutdown() {
	s.blm.Start()
	defer s.blm.Stop()
	s.NoError(s.blm.WaitUntilInitialized(context.Background()))

	s.ptqMgr.EXPECT().AddSpooledTask(gomock.Any()).MaxTimes(1)
	err := s.blm.SpoolTask(&persistencespb.TaskInfo{})
	s.NoError(err)

	s.cancelCtx()
	s.ptqMgr.EXPECT().UnloadFromPartitionManager(unloadCauseConflict).Times(1)

	err = s.blm.SpoolTask(&persistencespb.TaskInfo{})
	s.Error(err)
}

func (s *BacklogManagerTestSuite) TestReadBatchDone() {
	if s.newMatcher {
		s.T().Skip("not compatible with new backlog manager")
	}
	blm := s.blm.(*backlogManagerImpl)

	const rangeSize = 10
	const maxReadLevel = int64(120)
	blm.config.RangeSize = rangeSize

	blm.Start()
	defer blm.Stop()
	s.NoError(blm.WaitUntilInitialized(context.Background()))

	blm.taskAckManager.setReadLevel(0)
	blm.getDB().setMaxReadLevelForTesting(subqueueZero, maxReadLevel)
	batch, err := blm.taskReader.getTaskBatch(context.Background())
	s.NoError(err)
	s.Empty(batch.tasks)
	s.Equal(int64(rangeSize*10), batch.readLevel)
	s.False(batch.isReadBatchDone)
	s.NoError(err)

	blm.taskAckManager.setReadLevel(batch.readLevel)
	batch, err = blm.taskReader.getTaskBatch(context.Background())
	s.NoError(err)
	s.Empty(batch.tasks)
	s.Equal(maxReadLevel, batch.readLevel)
	s.True(batch.isReadBatchDone)
	s.NoError(err)
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_IncrementedByAppendTask() {
	if s.newMatcher {
		s.T().Skip("not compatible with new backlog manager")
	}
	blm := s.blm.(*backlogManagerImpl)

	// Add tasks on the taskWriters channel
	blm.taskWriter.appendCh <- &writeTaskRequest{
		taskInfo: &persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		},
		responseCh: make(chan<- error),
	}

	s.Equal(int64(0), blm.TotalApproximateBacklogCount())

	blm.taskWriter.Start()
	// Adding tasks to the buffer will increase the in-memory counter by 1
	// and this will be written to persistence
	s.Eventually(func() bool {
		return blm.TotalApproximateBacklogCount() == int64(1)
	}, time.Second*30, time.Millisecond)
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_DecrementedByCompleteTask() {
	if s.newMatcher {
		s.T().Skip("not compatible with new backlog manager")
	}
	blm := s.blm.(*backlogManagerImpl)

	_, err := blm.getDB().RenewLease(blm.tqCtx)
	s.NoError(err)

	blm.taskAckManager.addTask(int64(1))
	blm.taskAckManager.addTask(int64(2))
	blm.taskAckManager.addTask(int64(3))

	// Manually update the backlog size since adding tasks to the outstanding map does not increment the counter
	blm.getDB().updateBacklogStats(3, time.Time{})

	s.Equal(int64(3), blm.TotalApproximateBacklogCount(), "1 task in the backlog")
	s.Equal(int64(-1), blm.taskAckManager.getAckLevel(), "should only move ack level on completion")
	s.Equal(int64(3), blm.taskAckManager.getReadLevel(), "read level should be 1 since a task has been added")

	// Complete tasks
	ackLevel, numAcked := blm.taskAckManager.completeTask(2)
	s.Equal(int64(-1), ackLevel, "should not move the ack level")
	s.Equal(int64(0), numAcked, "should not decrease the backlog counter as ack level has not gone up")

	ackLevel, numAcked = blm.taskAckManager.completeTask(3)
	s.Equal(int64(-1), ackLevel, "should not move the ack level")
	s.Equal(int64(0), numAcked, "should not decrease the backlog counter as ack level has not gone up")

	ackLevel, numAcked = blm.taskAckManager.completeTask(1)
	s.Equal(int64(3), ackLevel, "should move the ack level")
	s.Equal(int64(3), numAcked, "should decrease the backlog counter to 0 as no more tasks in the backlog")
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_IncrementedBySpoolTask() {
	s.blm.Start()
	defer s.blm.Stop()

	taskCount := 10
	s.ptqMgr.EXPECT().AddSpooledTask(gomock.Any()).Return(nil).AnyTimes()
	for i := 0; i < taskCount; i++ {
		s.NoError(s.blm.SpoolTask(&persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		}))
	}
	s.Equal(int64(taskCount), s.blm.TotalApproximateBacklogCount(),
		"backlog count should match the number of tasks")
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_IncrementedBySpoolTask_ServiceError() {
	s.logger.Expect(testlogger.Error, "Persistent store operation failure")
	s.taskMgr.dbServiceError = true

	s.blm.Start()
	defer s.blm.Stop()

	taskCount := 10
	s.ptqMgr.EXPECT().AddSpooledTask(gomock.Any()).Return(nil).AnyTimes()
	for i := 0; i < taskCount; i++ {
		s.Error(s.blm.SpoolTask(&persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		}))
	}
	s.Equal(int64(taskCount), s.blm.TotalApproximateBacklogCount(),
		"backlog count should match the number of tasks despite the errors")
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_NotIncrementedBySpoolTask_CondFailedError() {
	s.logger.Expect(testlogger.Error, "Persistent store operation failure")
	s.taskMgr.dbCondFailedErr = true

	s.blm.Start()
	defer s.blm.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.ptqMgr.EXPECT().AddSpooledTask(gomock.Any()).Return(nil).AnyTimes()
	s.ptqMgr.EXPECT().UnloadFromPartitionManager(unloadCauseConflict).
		Do(func(_ any) { cancel() }).
		AnyTimes()

	s.Error(s.blm.SpoolTask(&persistencespb.TaskInfo{
		ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
		CreateTime: timestamp.TimeNowPtrUtc(),
	}))

	<-ctx.Done()

	s.Equal(int64(0), s.blm.TotalApproximateBacklogCount(),
		"backlog count should not be incremented")
}
