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

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
)

func TestReadLevelForAllExpiredTasksInBatch(t *testing.T) {
	controller := gomock.NewController(t)

	// TODO: do not create pq manager, directly create backlog manager
	tlm := mustCreateTestPhysicalTaskQueueManager(t, controller)
	blm := tlm.backlogMgr.(*backlogManagerImpl)
	require.NoError(t, blm.taskWriter.initReadWriteState())
	require.Equal(t, int64(1), blm.db.rangeID)
	require.Equal(t, int64(0), blm.taskAckManager.getAckLevel())
	require.Equal(t, int64(0), blm.taskAckManager.getReadLevel())

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

	require.NoError(t, blm.taskReader.addTasksToBuffer(context.TODO(), tasks))
	require.Equal(t, int64(0), blm.taskAckManager.getAckLevel())
	require.Equal(t, int64(12), blm.taskAckManager.getReadLevel())

	// Now add a mix of valid and expired tasks
	require.NoError(t, blm.taskReader.addTasksToBuffer(context.TODO(), []*persistencespb.AllocatedTaskInfo{
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
	require.Equal(t, int64(0), blm.taskAckManager.getAckLevel())
	require.Equal(t, int64(14), blm.taskAckManager.getReadLevel())
}

func TestTaskWriterShutdown(t *testing.T) {
	controller := gomock.NewController(t)

	tlm := mustCreateTestPhysicalTaskQueueManager(t, controller)
	tlm.Start()
	err := tlm.WaitUntilInitialized(context.Background())
	require.NoError(t, err)

	err = tlm.backlogMgr.SpoolTask(&persistencespb.TaskInfo{})
	require.NoError(t, err)

	// stop the task queue explicitly
	tlm.tqCtxCancel()

	// now attempt to add a task
	err = tlm.backlogMgr.SpoolTask(&persistencespb.TaskInfo{})
	require.Error(t, err)
}

func TestApproximateBacklogCountIncrement_taskWriterLoop(t *testing.T) {
	controller := gomock.NewController(t)
	backlogMgr := newBacklogMgr(t, controller, false)

	// Add tasks on the taskWriters channel
	backlogMgr.taskWriter.appendCh <- &writeTaskRequest{
		taskInfo: &persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		},
		responseCh: make(chan<- *writeTaskResponse),
	}

	require.Equal(t, int64(0), backlogMgr.TotalApproximateBacklogCount())

	backlogMgr.taskWriter.Start()
	// Adding tasks to the buffer will increase the in-memory counter by 1
	// and this will be written to persistence
	require.Eventually(t, func() bool { return backlogMgr.TotalApproximateBacklogCount() == int64(1) },
		time.Second*30, time.Millisecond)
}

func TestApproximateBacklogCounterDecrement_SingleTask(t *testing.T) {
	controller := gomock.NewController(t)
	backlogMgr := newBacklogMgr(t, controller, false)
	_, err := backlogMgr.db.RenewLease(backlogMgr.tqCtx)
	require.NoError(t, err)

	backlogMgr.taskAckManager.addTask(int64(1))
	// Manually update the backlog size since adding tasks to the outstanding map does not increment the counter
	backlogMgr.db.updateApproximateBacklogCount(int64(1))
	require.Equal(t, int64(1), backlogMgr.TotalApproximateBacklogCount(), "1 task in the backlog")
	require.Equal(t, int64(-1), backlogMgr.taskAckManager.getAckLevel(), "should only move ack level on completion")
	require.Equal(t, int64(1), backlogMgr.taskAckManager.getReadLevel(), "read level should be 1 since a task has been added")
	ackLevel, numAcked := backlogMgr.taskAckManager.completeTask(1)
	require.Equal(t, int64(1), ackLevel, "should move ack level and decrease backlog counter on completion")
	require.Equal(t, int64(1), numAcked, "should move ack level and decrease backlog counter on completion")

	require.Equal(t, int64(1), backlogMgr.taskAckManager.getAckLevel(), "should only move ack level on completion")
}

func TestApproximateBacklogCounterDecrement_MultipleTasks(t *testing.T) {
	controller := gomock.NewController(t)
	backlogMgr := newBacklogMgr(t, controller, false)
	_, err := backlogMgr.db.RenewLease(backlogMgr.tqCtx)
	require.NoError(t, err)

	backlogMgr.taskAckManager.addTask(int64(1))
	backlogMgr.taskAckManager.addTask(int64(2))
	backlogMgr.taskAckManager.addTask(int64(3))

	// Manually update the backlog size since adding tasks to the outstanding map does not increment the counter
	backlogMgr.db.updateApproximateBacklogCount(int64(3))

	require.Equal(t, int64(3), backlogMgr.TotalApproximateBacklogCount(), "1 task in the backlog")
	require.Equal(t, int64(-1), backlogMgr.taskAckManager.getAckLevel(), "should only move ack level on completion")
	require.Equal(t, int64(3), backlogMgr.taskAckManager.getReadLevel(), "read level should be 1 since a task has been added")

	// Complete tasks
	ackLevel, numAcked := backlogMgr.taskAckManager.completeTask(2)
	require.Equal(t, int64(-1), ackLevel, "should not move the ack level")
	require.Equal(t, int64(0), numAcked, "should not decrease the backlog counter as ack level has not gone up")

	ackLevel, numAcked = backlogMgr.taskAckManager.completeTask(3)
	require.Equal(t, int64(-1), ackLevel, "should not move the ack level")
	require.Equal(t, int64(0), numAcked, "should not decrease the backlog counter as ack level has not gone up")

	ackLevel, numAcked = backlogMgr.taskAckManager.completeTask(1)
	require.Equal(t, int64(3), ackLevel, "should move the ack level")
	require.Equal(t, int64(3), numAcked, "should decrease the backlog counter to 0 as no more tasks in the backlog")

}

// TestAddTasksValidateBacklogCounter uses the "backlogManager methods" to add a task to the backlog.
func TestAddSingleTaskValidateBacklogCounter(t *testing.T) {
	controller := gomock.NewController(t)
	backlogMgr := newBacklogMgr(t, controller, false)

	// only start the taskWriter for now!
	backlogMgr.taskWriter.Start()
	task := &persistencespb.TaskInfo{
		ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
		CreateTime: timestamp.TimeNowPtrUtc(),
	}
	err := backlogMgr.SpoolTask(task)
	require.NoError(t, err)
	require.Equal(t, int64(1), backlogMgr.TotalApproximateBacklogCount())
}

func TestAddTasksValidateBacklogCounter_ServiceError(t *testing.T) {
	controller := gomock.NewController(t)
	backlogMgr := newBacklogMgr(t, controller, true)

	// mock error signals
	logger := backlogMgr.logger.(*log.MockLogger) // nolint:revive
	logger.EXPECT().Error("Persistent store operation failure", gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).AnyTimes()

	// only start the taskWriter for now!
	backlogMgr.taskWriter.Start()
	taskCount := 10
	for i := 0; i < taskCount; i++ {
		// Create new tasks and spool them
		task := &persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		}
		err := backlogMgr.SpoolTask(task)
		require.Error(t, err)
	}
	require.Equal(t, int64(10), backlogMgr.TotalApproximateBacklogCount())
}

func TestAddMultipleTasksValidateBacklogCounter(t *testing.T) {
	controller := gomock.NewController(t)
	backlogMgr := newBacklogMgr(t, controller, false)

	// Only start the taskWriter for now!
	backlogMgr.taskWriter.Start()
	taskCount := 10
	for i := 0; i < taskCount; i++ {
		// Create new tasks and spool them
		task := &persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		}
		err := backlogMgr.SpoolTask(task)
		require.NoError(t, err)
	}
	require.Equal(t, int64(10), backlogMgr.TotalApproximateBacklogCount())
}

type cleanupper interface {
	Cleanup(func())
}

func newBacklogMgr(t cleanupper, controller *gomock.Controller, serviceError bool) *backlogManagerImpl {
	logger := log.NewMockLogger(controller)
	tm := newTestTaskManager(logger)
	if serviceError {
		tm.dbServiceError = true
	}

	pqMgr := NewMockphysicalTaskQueueManager(controller)

	matchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
	handler := metrics.NewMockHandler(controller)

	cfg := NewConfig(dynamicconfig.NewNoopCollection())
	f, _ := tqid.NewTaskQueueFamily("", "test-queue")
	prtn := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(0)
	queue := UnversionedQueueKey(prtn)
	tlCfg := newTaskQueueConfig(prtn.TaskQueue(), cfg, "test-namespace")

	// Set expected calls
	pqMgr.EXPECT().QueueKey().Return(queue).AnyTimes()
	pqMgr.EXPECT().ProcessSpooledTask(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	handler.EXPECT().Counter(gomock.Any()).Return(metrics.NoopCounterMetricFunc).AnyTimes()
	handler.EXPECT().Timer(gomock.Any()).Return(metrics.NoopTimerMetricFunc).AnyTimes()
	logger.EXPECT().Debug(gomock.Any(), gomock.Any()).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	return newBacklogManager(ctx, pqMgr, tlCfg, tm, logger, logger, matchingClient, handler)
}
