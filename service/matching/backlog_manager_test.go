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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/tqid"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/primitives/timestamp"
)

func TestDeliverBufferTasks(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	tests := []func(tlm *physicalTaskQueueManagerImpl){
		func(tlm *physicalTaskQueueManagerImpl) { close(tlm.backlogMgr.taskReader.taskBuffer) },
		func(tlm *physicalTaskQueueManagerImpl) { tlm.backlogMgr.taskReader.gorogrp.Cancel() },
		func(tlm *physicalTaskQueueManagerImpl) {
			rps := 0.1
			tlm.matcher.UpdateRatelimit(&rps)
			tlm.backlogMgr.taskReader.taskBuffer <- &persistencespb.AllocatedTaskInfo{
				Data: &persistencespb.TaskInfo{},
			}
			err := tlm.matcher.rateLimiter.Wait(context.Background()) // consume the token
			assert.NoError(t, err)
			tlm.backlogMgr.taskReader.gorogrp.Cancel()
		},
	}
	for _, test := range tests {
		// TODO: do not create pq manager, directly create backlog manager
		tlm := mustCreateTestPhysicalTaskQueueManager(t, controller)
		tlm.backlogMgr.taskReader.gorogrp.Go(tlm.backlogMgr.taskReader.dispatchBufferedTasks)
		test(tlm)
		// dispatchBufferedTasks should stop after invocation of the test function
		tlm.backlogMgr.taskReader.gorogrp.Wait()
	}
}

func TestDeliverBufferTasks_NoPollers(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// TODO: do not create pq manager, directly create backlog manager
	tlm := mustCreateTestPhysicalTaskQueueManager(t, controller)
	tlm.backlogMgr.taskReader.taskBuffer <- &persistencespb.AllocatedTaskInfo{
		Data: &persistencespb.TaskInfo{},
	}
	tlm.backlogMgr.taskReader.gorogrp.Go(tlm.backlogMgr.taskReader.dispatchBufferedTasks)
	time.Sleep(100 * time.Millisecond) // let go routine run first and block on tasksForPoll
	tlm.backlogMgr.taskReader.gorogrp.Cancel()
	tlm.backlogMgr.taskReader.gorogrp.Wait()
}

func TestReadLevelForAllExpiredTasksInBatch(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// TODO: do not create pq manager, directly create backlog manager
	tlm := mustCreateTestPhysicalTaskQueueManager(t, controller)
	tlm.backlogMgr.db.rangeID = int64(1)
	tlm.backlogMgr.db.ackLevel = int64(0)
	tlm.backlogMgr.taskAckManager.setAckLevel(tlm.backlogMgr.db.ackLevel)
	tlm.backlogMgr.taskAckManager.setReadLevel(tlm.backlogMgr.db.ackLevel)
	require.Equal(t, int64(0), tlm.backlogMgr.taskAckManager.getAckLevel())
	require.Equal(t, int64(0), tlm.backlogMgr.taskAckManager.getReadLevel())

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

	require.NoError(t, tlm.backlogMgr.taskReader.addTasksToBuffer(context.TODO(), tasks))
	require.Equal(t, int64(0), tlm.backlogMgr.taskAckManager.getAckLevel())
	require.Equal(t, int64(12), tlm.backlogMgr.taskAckManager.getReadLevel())

	// Now add a mix of valid and expired tasks
	require.NoError(t, tlm.backlogMgr.taskReader.addTasksToBuffer(context.TODO(), []*persistencespb.AllocatedTaskInfo{
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
	require.Equal(t, int64(0), tlm.backlogMgr.taskAckManager.getAckLevel())
	require.Equal(t, int64(14), tlm.backlogMgr.taskAckManager.getReadLevel())
}

func TestTaskWriterShutdown(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	tlm := mustCreateTestPhysicalTaskQueueManager(t, controller)
	tlm.Start()
	err := tlm.WaitUntilInitialized(context.Background())
	require.NoError(t, err)

	err = tlm.backlogMgr.SpoolTask(&persistencespb.TaskInfo{})
	require.NoError(t, err)

	// stop the task writer explicitly
	tlm.backlogMgr.taskWriter.Stop()

	// now attempt to add a task
	err = tlm.backlogMgr.SpoolTask(&persistencespb.TaskInfo{})
	require.Error(t, err)
}

//// Aims to test if ApproximateBacklogCount is being rightly updated during the
//// dispatchBufferedTasks call by taskReader
//func TestApproximateBacklogCount_dispatchBufferTasks(t *testing.T) {
//	controller := gomock.NewController(t)
//	defer controller.Finish()
//
//	logger := log.NewMockLogger(controller)
//	tm := newTestTaskManager(logger)
//
//	pqMgr := NewMockphysicalTaskQueueManager(controller)
//
//	matchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
//	handler := metrics.NewMockHandler(controller)
//
//	cfg := NewConfig(dynamicconfig.NewNoopCollection(), false, false)
//	f, err := tqid.NewTaskQueueFamily("", "test-queue")
//	require.NoError(t, err)
//	prtn := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(0)
//	queue := UnversionedQueueKey(prtn)
//	tlCfg := newTaskQueueConfig(prtn.TaskQueue(), cfg, "test-namespace")
//
//	// Expected calls
//	pqMgr.EXPECT().QueueKey().Return(queue).AnyTimes()
//	pqMgr.EXPECT().ProcessSpooledTask(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
//
//	backlogMgr := newBacklogManager(pqMgr, tlCfg, tm, logger, logger, matchingClient, handler, defaultContextInfoProvider)
//
//	// Adding tasks to the buffer
//	require.NoError(t, backlogMgr.taskReader.addTasksToBuffer(context.Background(), []*persistencespb.AllocatedTaskInfo{
//		{
//			Data: &persistencespb.TaskInfo{
//				ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
//				CreateTime: timestamp.TimeNowPtrUtc(),
//			},
//			TaskId: 13,
//		},
//	}))
//
//	// Setting the initial value to be 1 as dispatching of a task should
//	// decrease this counter
//	backlogMgr.db.updateApproximateBacklogCount(int64(1))
//	require.Equal(t, backlogMgr.db.getApproximateBacklogCount(), int64(1))
//
//	backlogMgr.taskReader.gorogrp.Go(backlogMgr.taskReader.dispatchBufferedTasks)
//	time.Sleep(30 * time.Second) // let go routine run first
//	backlogMgr.taskReader.gorogrp.Cancel()
//	backlogMgr.taskReader.gorogrp.Wait()
//
//	close(backlogMgr.taskReader.taskBuffer)
//	require.Equal(t, backlogMgr.db.getApproximateBacklogCount(), int64(0))
//}

// TODO Shivam: Add a unit test which shall decrement the counter when the ack level goes up

func TestApproximateBacklogCountIncrement_taskWriterLoop(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Setup calls
	logger := log.NewMockLogger(controller)
	tm := newTestTaskManager(logger)

	pqMgr := NewMockphysicalTaskQueueManager(controller)

	matchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
	handler := metrics.NewMockHandler(controller)

	cfg := NewConfig(dynamicconfig.NewNoopCollection(), false, false)
	f, err := tqid.NewTaskQueueFamily("", "test-queue")
	require.NoError(t, err)
	prtn := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(0)
	queue := UnversionedQueueKey(prtn)
	tlCfg := newTaskQueueConfig(prtn.TaskQueue(), cfg, "test-namespace")

	// Expected calls
	pqMgr.EXPECT().QueueKey().Return(queue).AnyTimes()
	pqMgr.EXPECT().ProcessSpooledTask(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	handler.EXPECT().Counter(gomock.Any()).Return(metrics.NoopCounterMetricFunc).AnyTimes()
	logger.EXPECT().Debug(gomock.Any(), gomock.Any()).AnyTimes()

	backlogMgr := newBacklogManager(pqMgr, tlCfg, tm, logger, logger, matchingClient, handler, defaultContextInfoProvider)

	// Adding tasks on the taskWriters channel
	backlogMgr.taskWriter.appendCh <- &writeTaskRequest{
		taskInfo: &persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		},
		responseCh: make(chan<- *writeTaskResponse),
	}

	require.Equal(t, backlogMgr.db.getApproximateBacklogCount(), int64(0))

	backlogMgr.taskWriter.Start()
	time.Sleep(30 * time.Second) // let go routine run first
	backlogMgr.taskWriter.Stop()

	// Adding tasks to the buffer shall increase the in-memory counter by 1
	// and this shall be written to persistence
	require.Equal(t, backlogMgr.db.getApproximateBacklogCount(), int64(1))

}

func defaultContextInfoProvider(ctx context.Context) context.Context {
	return ctx
}
