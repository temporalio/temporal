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
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/internal/goro"
)

var rpsInf = math.Inf(1)

func TestDeliverBufferTasks(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	tests := []func(tlm *taskQueueManagerImpl){
		func(tlm *taskQueueManagerImpl) { close(tlm.taskReader.taskBuffer) },
		func(tlm *taskQueueManagerImpl) { tlm.taskReader.gorogrp.Cancel() },
		func(tlm *taskQueueManagerImpl) {
			rps := 0.1
			tlm.matcher.UpdateRatelimit(&rps)
			tlm.taskReader.taskBuffer <- &persistencespb.AllocatedTaskInfo{}
			err := tlm.matcher.rateLimiter.Wait(context.Background()) // consume the token
			assert.NoError(t, err)
			tlm.taskReader.gorogrp.Cancel()
		},
	}
	for _, test := range tests {
		tlm := mustCreateTestTaskQueueManager(t, controller)
		tlm.taskReader.gorogrp.Go(tlm.taskReader.dispatchBufferedTasks)
		test(tlm)
		// dispatchBufferedTasks should stop after invocation of the test function
		tlm.taskReader.gorogrp.Wait()
	}
}

func TestDeliverBufferTasks_NoPollers(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	tlm := mustCreateTestTaskQueueManager(t, controller)
	tlm.taskReader.taskBuffer <- &persistencespb.AllocatedTaskInfo{}
	tlm.taskReader.gorogrp.Go(tlm.taskReader.dispatchBufferedTasks)
	time.Sleep(100 * time.Millisecond) // let go routine run first and block on tasksForPoll
	tlm.taskReader.gorogrp.Cancel()
	tlm.taskReader.gorogrp.Wait()
}

func TestReadLevelForAllExpiredTasksInBatch(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	tlm := mustCreateTestTaskQueueManager(t, controller)
	tlm.db.rangeID = int64(1)
	tlm.db.ackLevel = int64(0)
	tlm.taskAckManager.setAckLevel(tlm.db.ackLevel)
	tlm.taskAckManager.setReadLevel(tlm.db.ackLevel)
	require.Equal(t, int64(0), tlm.taskAckManager.getAckLevel())
	require.Equal(t, int64(0), tlm.taskAckManager.getReadLevel())

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

	require.NoError(t, tlm.taskReader.addTasksToBuffer(context.TODO(), tasks))
	require.Equal(t, int64(0), tlm.taskAckManager.getAckLevel())
	require.Equal(t, int64(12), tlm.taskAckManager.getReadLevel())

	// Now add a mix of valid and expired tasks
	require.NoError(t, tlm.taskReader.addTasksToBuffer(context.TODO(), []*persistencespb.AllocatedTaskInfo{
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
	require.Equal(t, int64(0), tlm.taskAckManager.getAckLevel())
	require.Equal(t, int64(14), tlm.taskAckManager.getReadLevel())
}

type testIDBlockAlloc struct {
	rid   int64
	alloc func() (taskQueueState, error)
}

func (a *testIDBlockAlloc) RangeID() int64 {
	return a.rid
}

func (a *testIDBlockAlloc) RenewLease(_ context.Context) (taskQueueState, error) {
	s, err := a.alloc()
	if err == nil {
		a.rid = s.rangeID
	}
	return s, err
}

func makeTestBlocAlloc(f func() (taskQueueState, error)) taskQueueManagerOpt {
	return withIDBlockAllocator(&testIDBlockAlloc{alloc: f})
}

func TestSyncMatchLeasingUnavailable(t *testing.T) {
	tqm := mustCreateTestTaskQueueManager(t, gomock.NewController(t),
		makeTestBlocAlloc(func() (taskQueueState, error) {
			// any error other than ConditionFailedError indicates an
			// availability problem at a lower layer so the TQM should NOT
			// unload itself because resilient sync match is enabled.
			return taskQueueState{}, errors.New(t.Name())
		}))
	tqm.Start()
	defer tqm.Stop()
	poller, _ := runOneShotPoller(context.Background(), tqm)
	defer poller.Cancel()

	sync, err := tqm.AddTask(context.TODO(), addTaskParams{
		execution: &commonpb.WorkflowExecution{},
		taskInfo:  &persistencespb.TaskInfo{},
		source:    enumsspb.TASK_SOURCE_HISTORY})
	require.NoError(t, err)
	require.True(t, sync)
}

func TestForeignPartitionOwnerCausesUnload(t *testing.T) {
	cc := dynamicconfig.NewMutableEphemeralClient()
	cfg := NewConfig(dynamicconfig.NewCollection(cc, log.NewTestLogger()))
	cfg.RangeSize = 1 // TaskID block size
	var leaseErr error = nil
	tqm := mustCreateTestTaskQueueManager(t, gomock.NewController(t),
		makeTestBlocAlloc(func() (taskQueueState, error) {
			return taskQueueState{rangeID: 1}, leaseErr
		}))
	tqm.Start()
	defer tqm.Stop()

	// TQM started succesfully with an ID block of size 1. Perform one send
	// without a poller to consume the one task ID from the reserved block.
	sync, err := tqm.AddTask(context.TODO(), addTaskParams{
		execution: &commonpb.WorkflowExecution{},
		taskInfo:  &persistencespb.TaskInfo{},
		source:    enumsspb.TASK_SOURCE_HISTORY})
	require.False(t, sync)
	require.NoError(t, err)

	// TQM's ID block should be empty so the next AddTask will trigger an
	// attempt to obtain more IDs. This specific error type indicates that
	// another service instance has become the owner of the partition
	leaseErr = &persistence.ConditionFailedError{Msg: "should kill the tqm"}

	sync, err = tqm.AddTask(context.TODO(), addTaskParams{
		execution: &commonpb.WorkflowExecution{},
		taskInfo:  &persistencespb.TaskInfo{},
		source:    enumsspb.TASK_SOURCE_HISTORY,
	})
	require.NoError(t, err)
	require.False(t, sync)
}

func TestReaderSignaling(t *testing.T) {
	readerNotifications := make(chan struct{}, 1)
	clearNotifications := func() {
		for len(readerNotifications) > 0 {
			<-readerNotifications
		}
	}
	tqm := mustCreateTestTaskQueueManager(t, gomock.NewController(t))

	// redirect taskReader signals into our local channel
	tqm.taskReader.notifyC = readerNotifications

	tqm.Start()
	defer tqm.Stop()

	// shut down the taskReader so it doesn't steal notifications from us
	tqm.taskReader.gorogrp.Cancel()
	tqm.taskReader.gorogrp.Wait()

	clearNotifications()

	sync, err := tqm.AddTask(context.TODO(), addTaskParams{
		execution: &commonpb.WorkflowExecution{},
		taskInfo:  &persistencespb.TaskInfo{},
		source:    enumsspb.TASK_SOURCE_HISTORY})
	require.NoError(t, err)
	require.False(t, sync)
	require.Len(t, readerNotifications, 1,
		"Sync match failure with successful db write should signal taskReader")

	clearNotifications()
	poller, _ := runOneShotPoller(context.Background(), tqm)
	defer poller.Cancel()

	sync, err = tqm.AddTask(context.TODO(), addTaskParams{
		execution: &commonpb.WorkflowExecution{},
		taskInfo:  &persistencespb.TaskInfo{},
		source:    enumsspb.TASK_SOURCE_HISTORY})
	require.NoError(t, err)
	require.True(t, sync)
	require.Len(t, readerNotifications, 0,
		"Sync match should not signal taskReader")
}

// runOneShotPoller spawns a goroutine to call tqm.GetTask on the provided tqm.
// The second return value is a channel of either error or *internalTask.
func runOneShotPoller(ctx context.Context, tqm taskQueueManager) (*goro.Handle, chan interface{}) {
	out := make(chan interface{}, 1)
	handle := goro.NewHandle(ctx).Go(func(ctx context.Context) error {
		task, err := tqm.GetTask(ctx, &rpsInf)
		if task == nil {
			out <- err
			return nil
		}
		task.finish(err)
		out <- task
		return nil
	})
	// tqm.GetTask() needs some time to attach the goro started above to the
	// internal task channel. Sorry for this but it appears unavoidable.
	time.Sleep(10 * time.Millisecond)
	return handle, out
}

func mustCreateTestTaskQueueManager(
	t *testing.T,
	controller *gomock.Controller,
	opts ...taskQueueManagerOpt,
) *taskQueueManagerImpl {
	t.Helper()
	return mustCreateTestTaskQueueManagerWithConfig(t, controller, defaultTestConfig(), opts...)
}

func mustCreateTestTaskQueueManagerWithConfig(
	t *testing.T,
	controller *gomock.Controller,
	cfg *Config,
	opts ...taskQueueManagerOpt,
) *taskQueueManagerImpl {
	t.Helper()
	tqm, err := createTestTaskQueueManagerWithConfig(controller, cfg, opts...)
	require.NoError(t, err)
	return tqm
}

func createTestTaskQueueManagerWithConfig(
	controller *gomock.Controller,
	cfg *Config,
	opts ...taskQueueManagerOpt,
) (*taskQueueManagerImpl, error) {
	logger := log.NewTestLogger()
	tm := newTestTaskManager(logger)
	mockNamespaceCache := namespace.NewMockRegistry(controller)
	mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()
	cmeta := cluster.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(false, true))
	me := newMatchingEngine(cfg, tm, nil, logger, mockNamespaceCache)
	tl := "tq"
	dID := namespace.ID("deadbeef-0000-4567-890a-bcdef0123456")
	tlID := newTestTaskQueueID(dID, tl, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	tlKind := enumspb.TASK_QUEUE_KIND_NORMAL
	tlMgr, err := newTaskQueueManager(me, tlID, tlKind, cfg, cmeta, opts...)
	if err != nil {
		return nil, err
	}
	me.taskQueues[*tlID] = tlMgr
	return tlMgr.(*taskQueueManagerImpl), nil
}

func TestIsTaskAddedRecently(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	tlm := mustCreateTestTaskQueueManager(t, controller)
	require.True(t, tlm.taskReader.isTaskAddedRecently(time.Now().UTC()))
	require.False(t, tlm.taskReader.isTaskAddedRecently(time.Now().UTC().Add(-tlm.config.MaxTaskqueueIdleTime())))
	require.True(t, tlm.taskReader.isTaskAddedRecently(time.Now().UTC().Add(1*time.Second)))
	require.False(t, tlm.taskReader.isTaskAddedRecently(time.Time{}))
}

func TestDescribeTaskQueue(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	startTaskID := int64(1)
	taskCount := int64(3)
	PollerIdentity := "test-poll"

	// Create taskQueue Manager and set taskQueue state
	tlm := mustCreateTestTaskQueueManager(t, controller)
	tlm.db.rangeID = int64(1)
	tlm.db.ackLevel = int64(0)
	tlm.taskAckManager.setAckLevel(tlm.db.ackLevel)

	for i := int64(0); i < taskCount; i++ {
		tlm.taskAckManager.addTask(startTaskID + i)
	}

	includeTaskStatus := false
	descResp := tlm.DescribeTaskQueue(includeTaskStatus)
	require.Equal(t, 0, len(descResp.GetPollers()))
	require.Nil(t, descResp.GetTaskQueueStatus())

	includeTaskStatus = true
	taskQueueStatus := tlm.DescribeTaskQueue(includeTaskStatus).GetTaskQueueStatus()
	require.NotNil(t, taskQueueStatus)
	require.Zero(t, taskQueueStatus.GetAckLevel())
	require.Equal(t, taskCount, taskQueueStatus.GetReadLevel())
	require.Equal(t, taskCount, taskQueueStatus.GetBacklogCountHint())
	taskIDBlock := taskQueueStatus.GetTaskIdBlock()
	require.Equal(t, int64(1), taskIDBlock.GetStartId())
	require.Equal(t, tlm.config.RangeSize, taskIDBlock.GetEndId())

	// Add a poller and complete all tasks
	tlm.pollerHistory.updatePollerInfo(pollerIdentity(PollerIdentity), nil)
	for i := int64(0); i < taskCount; i++ {
		tlm.taskAckManager.completeTask(startTaskID + i)
	}

	descResp = tlm.DescribeTaskQueue(includeTaskStatus)
	require.Equal(t, 1, len(descResp.GetPollers()))
	require.Equal(t, PollerIdentity, descResp.Pollers[0].GetIdentity())
	require.NotEmpty(t, descResp.Pollers[0].GetLastAccessTime())

	rps := 5.0
	tlm.pollerHistory.updatePollerInfo(pollerIdentity(PollerIdentity), &rps)
	descResp = tlm.DescribeTaskQueue(includeTaskStatus)
	require.Equal(t, 1, len(descResp.GetPollers()))
	require.Equal(t, PollerIdentity, descResp.Pollers[0].GetIdentity())
	require.True(t, descResp.Pollers[0].GetRatePerSecond() > 4.0 && descResp.Pollers[0].GetRatePerSecond() < 6.0)

	taskQueueStatus = descResp.GetTaskQueueStatus()
	require.NotNil(t, taskQueueStatus)
	require.Equal(t, taskCount, taskQueueStatus.GetAckLevel())
	require.Zero(t, taskQueueStatus.GetBacklogCountHint())
}

func TestCheckIdleTaskQueue(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	cfg := NewConfig(dynamicconfig.NewNoopCollection())
	cfg.IdleTaskqueueCheckInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueueInfo(2 * time.Second)

	// Idle
	tlm := mustCreateTestTaskQueueManagerWithConfig(t, controller, cfg)
	tlm.Start()
	time.Sleep(1 * time.Second)
	require.Equal(t, common.DaemonStatusStarted, atomic.LoadInt32(&tlm.status))

	// Active poll-er
	tlm = mustCreateTestTaskQueueManagerWithConfig(t, controller, cfg)
	tlm.Start()
	tlm.pollerHistory.updatePollerInfo(pollerIdentity("test-poll"), nil)
	require.Equal(t, 1, len(tlm.GetAllPollerInfo()))
	time.Sleep(1 * time.Second)
	require.Equal(t, common.DaemonStatusStarted, atomic.LoadInt32(&tlm.status))
	tlm.Stop()
	require.Equal(t, common.DaemonStatusStopped, atomic.LoadInt32(&tlm.status))

	// Active adding task
	tlm = mustCreateTestTaskQueueManagerWithConfig(t, controller, cfg)
	tlm.Start()
	require.Equal(t, 0, len(tlm.GetAllPollerInfo()))
	tlm.taskReader.Signal()
	time.Sleep(1 * time.Second)
	require.Equal(t, common.DaemonStatusStarted, atomic.LoadInt32(&tlm.status))
	tlm.Stop()
	require.Equal(t, common.DaemonStatusStopped, atomic.LoadInt32(&tlm.status))
}

func TestAddTaskStandby(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	tlm := mustCreateTestTaskQueueManagerWithConfig(
		t,
		controller,
		NewConfig(dynamicconfig.NewNoopCollection()),
		func(tqm *taskQueueManagerImpl) {
			// we need to override the mockNamespaceCache to return a passive namespace
			mockNamespaceCache := namespace.NewMockRegistry(controller)
			mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).
				Return(namespace.NewGlobalNamespaceForTest(
					&persistencespb.NamespaceInfo{},
					&persistencespb.NamespaceConfig{},
					&persistencespb.NamespaceReplicationConfig{
						ActiveClusterName: cluster.TestAlternativeClusterName,
					},
					cluster.TestAlternativeClusterInitialFailoverVersion,
				), nil).AnyTimes()
			tqm.namespaceRegistry = mockNamespaceCache
		},
	)
	tlm.Start()
	// stop taskWriter so that we can check if there's any call to it
	// otherwise the task persist process is async and hard to test
	tlm.taskWriter.Stop()
	<-tlm.taskWriter.writeLoop.Done()

	addTaskParam := addTaskParams{
		execution: &commonpb.WorkflowExecution{},
		taskInfo:  &persistencespb.TaskInfo{},
		source:    enumsspb.TASK_SOURCE_HISTORY,
	}

	syncMatch, err := tlm.AddTask(context.Background(), addTaskParam)
	require.Equal(t, errShutdown, err) // task writer was stopped above
	require.False(t, syncMatch)

	addTaskParam.forwardedFrom = "from child partition"
	syncMatch, err = tlm.AddTask(context.Background(), addTaskParam)
	require.Equal(t, errRemoteSyncMatchFailed, err) // should not persist the task
	require.False(t, syncMatch)
}
