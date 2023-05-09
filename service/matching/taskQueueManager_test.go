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
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
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

const defaultNamespaceId = namespace.ID("deadbeef-0000-4567-890a-bcdef0123456")
const defaultRootTqID = "tq"

type tqmTestOpts struct {
	config             *Config
	tqId               *taskQueueID
	matchingClientMock *matchingservicemock.MockMatchingServiceClient
}

func defaultTqmTestOpts(controller *gomock.Controller) *tqmTestOpts {
	return &tqmTestOpts{
		config:             defaultTestConfig(),
		tqId:               defaultTqId(),
		matchingClientMock: matchingservicemock.NewMockMatchingServiceClient(controller),
	}
}

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
	cfg := NewConfig(dynamicconfig.NewNoopCollection())
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
		task, err := tqm.GetTask(ctx, &pollMetadata{ratePerSecond: &rpsInf})
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

func defaultTqId() *taskQueueID {
	return newTestTaskQueueID(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
}

func mustCreateTestTaskQueueManager(
	t *testing.T,
	controller *gomock.Controller,
	opts ...taskQueueManagerOpt,
) *taskQueueManagerImpl {
	t.Helper()
	return mustCreateTestTaskQueueManagerWithConfig(t, controller, defaultTqmTestOpts(controller), opts...)
}

func mustCreateTestTaskQueueManagerWithConfig(
	t *testing.T,
	controller *gomock.Controller,
	testOpts *tqmTestOpts,
	opts ...taskQueueManagerOpt,
) *taskQueueManagerImpl {
	t.Helper()
	tqm, err := createTestTaskQueueManagerWithConfig(controller, testOpts, opts...)
	require.NoError(t, err)
	return tqm
}

func createTestTaskQueueManagerWithConfig(
	controller *gomock.Controller,
	testOpts *tqmTestOpts,
	opts ...taskQueueManagerOpt,
) (*taskQueueManagerImpl, error) {
	logger := log.NewTestLogger()
	tm := newTestTaskManager(logger)
	mockNamespaceCache := namespace.NewMockRegistry(controller)
	mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(namespace.Name("ns-name"), nil).AnyTimes()
	cmeta := cluster.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(false, true))
	me := newMatchingEngine(testOpts.config, tm, nil, logger, mockNamespaceCache, testOpts.matchingClientMock)
	tlKind := enumspb.TASK_QUEUE_KIND_NORMAL
	tlMgr, err := newTaskQueueManager(me, testOpts.tqId, tlKind, testOpts.config, cmeta, opts...)
	if err != nil {
		return nil, err
	}
	me.taskQueues[*testOpts.tqId] = tlMgr
	return tlMgr.(*taskQueueManagerImpl), nil
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
	tlm.pollerHistory.updatePollerInfo(pollerIdentity(PollerIdentity), &pollMetadata{})
	for i := int64(0); i < taskCount; i++ {
		tlm.taskAckManager.completeTask(startTaskID + i)
	}

	descResp = tlm.DescribeTaskQueue(includeTaskStatus)
	require.Equal(t, 1, len(descResp.GetPollers()))
	require.Equal(t, PollerIdentity, descResp.Pollers[0].GetIdentity())
	require.NotEmpty(t, descResp.Pollers[0].GetLastAccessTime())

	rps := 5.0
	tlm.pollerHistory.updatePollerInfo(pollerIdentity(PollerIdentity), &pollMetadata{ratePerSecond: &rps})
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
	cfg.MaxTaskQueueIdleTime = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueueInfo(2 * time.Second)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.config = cfg

	// Idle
	tlm := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg)
	tlm.Start()
	time.Sleep(1 * time.Second)
	require.Equal(t, common.DaemonStatusStarted, atomic.LoadInt32(&tlm.status))

	// Active poll-er
	tlm = mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg)
	tlm.Start()
	tlm.pollerHistory.updatePollerInfo(pollerIdentity("test-poll"), &pollMetadata{})
	require.Equal(t, 1, len(tlm.GetAllPollerInfo()))
	time.Sleep(1 * time.Second)
	require.Equal(t, common.DaemonStatusStarted, atomic.LoadInt32(&tlm.status))
	tlm.Stop()
	require.Equal(t, common.DaemonStatusStopped, atomic.LoadInt32(&tlm.status))

	// Active adding task
	tlm = mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg)
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
		defaultTqmTestOpts(controller),
		func(tqm *taskQueueManagerImpl) {
			ns := namespace.NewGlobalNamespaceForTest(
				&persistencespb.NamespaceInfo{},
				&persistencespb.NamespaceConfig{},
				&persistencespb.NamespaceReplicationConfig{
					ActiveClusterName: cluster.TestAlternativeClusterName,
				},
				cluster.TestAlternativeClusterInitialFailoverVersion,
			)

			// we need to override the mockNamespaceCache to return a passive namespace
			mockNamespaceCache := namespace.NewMockRegistry(controller)
			mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil).AnyTimes()
			mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(ns.Name(), nil).AnyTimes()
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

/* FIXME
func TestTaskQueuePartitionFetchesUserDataFromRootPartitionOnInit(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId

	data := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}
	asResp := &matchingservice.GetTaskQueueUserDataResponse{
		TaskQueueHasUserData: true,
		UserData:             data,
	}

	subTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			mockMatchingClient.EXPECT().GetTaskQueueUserData(gomock.Any(),
				gomock.Eq(&matchingservice.GetTaskQueueUserDataRequest{
					NamespaceId:              defaultNamespaceId.String(),
					TaskQueue:                defaultRootTqID,
					LastKnownUserDataVersion: 0,
				})).
				Return(asResp, nil)
			tqm.matchingClient = mockMatchingClient
		})
	subTq.Start()
	require.NoError(t, subTq.WaitUntilInitialized(ctx))
	userData, err := subTq.GetUserData(ctx)
	require.NoError(t, err)
	require.Equal(t, data, userData)
	subTq.Stop()
}
*/

/* FIXME
func TestTaskQueuePartitionSendsLastKnownVersionOfUserDataWhenFetching(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId

	data := &persistencespb.VersionedTaskQueueUserData{
		Version: 2,
		Data:    mkUserData(1),
	}
	asResp := &matchingservice.GetTaskQueueUserDataResponse{
		TaskQueueHasUserData: true,
		UserData:             data,
	}

	subTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			mockMatchingClient.EXPECT().GetTaskQueueUserData(gomock.Any(),
				gomock.Eq(&matchingservice.GetTaskQueueUserDataRequest{
					NamespaceId:              defaultNamespaceId.String(),
					TaskQueue:                defaultRootTqID,
					LastKnownUserDataVersion: 0,
				})).
				Return(asResp, nil)
			tqm.matchingClient = mockMatchingClient
		})
	// Don't start it. Just explicitly call fetching function.
	res, err := subTq.fetchUserDataFromRootPartition(ctx)
	require.NotNil(t, res)
	require.NoError(t, err)
	assert.Equal(t, data, subTq.db.userData)
}
*/

/* FIXME
type invalidateMatcher struct {
	matchesTaskQType enumspb.TaskQueueType
}

func (m invalidateMatcher) Matches(x interface{}) bool {
	v, ok := x.(*matchingservice.InvalidateTaskQueueUserDataRequest)
	return ok && v.GetTaskQueueType() == m.matchesTaskQType
}
func (m invalidateMatcher) String() string {
	return fmt.Sprintf("%#v", m)
}

func TestTaskQueueRootPartitionNotifiesChildrenOfInvalidation(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()

	rootTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, defaultTqmTestOpts(controller),
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			mockMatchingClient.EXPECT().InvalidateTaskQueueUserData(
				gomock.Any(), invalidateMatcher{matchesTaskQType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}).
				Return(&matchingservice.InvalidateTaskQueueUserDataResponse{}, nil).
				Times(tqm.config.NumReadPartitions() - 1)
			mockMatchingClient.EXPECT().InvalidateTaskQueueUserData(
				gomock.Any(), invalidateMatcher{matchesTaskQType: enumspb.TASK_QUEUE_TYPE_ACTIVITY}).
				Return(&matchingservice.InvalidateTaskQueueUserDataResponse{}, nil).
				// Not minus 1 here because root activity partition gets invalidated
				Times(tqm.config.NumReadPartitions())
			tqm.matchingClient = mockMatchingClient
		})

	rootTq.Start()
	require.NoError(t, rootTq.WaitUntilInitialized(ctx))
	// Make a change, mock verifies children are invalidated
	require.NoError(t, rootTq.UpdateUserData(ctx, false, func(vd *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, error) {
		return mkUserData(1), nil
	}))
	rootTq.Stop()
}
*/

/* FIXME
func TestTaskQueueSubPartitionPollsPeriodically(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId
	tqCfg.config.UserDataPollFrequency = func() time.Duration {
		return time.Millisecond * 10
	}

	asResp := &matchingservice.GetTaskQueueUserDataResponse{
		TaskQueueHasUserData: true,
		UserData: &persistencespb.VersionedTaskQueueUserData{
			Version: 1,
			Data:    mkUserData(1),
		},
	}

	subTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			mockMatchingClient.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).
				Return(asResp, nil).MinTimes(3)
			tqm.matchingClient = mockMatchingClient
		})
	res, err := subTq.fetchUserDataFromRootPartition(ctx)
	require.NotNil(t, res)
	require.NoError(t, err)
	// Wait a bit to make sure we poll a few times
	time.Sleep(time.Millisecond * 25)
	subTq.Stop()
}
*/

/* FIXME
func TestTaskQueueSubPartitionDoesNotPollIfNoDataThenPollsWhenInvalidated(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId
	tqCfg.config.UserDataPollFrequency = func() time.Duration {
		return time.Millisecond * 10
	}

	nilDatResp := &matchingservice.GetTaskQueueUserDataResponse{
		TaskQueueHasUserData: false,
	}
	userData := mkUserData(1)
	verionedUserData := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    userData,
	}
	hasDatResp := &matchingservice.GetTaskQueueUserDataResponse{
		TaskQueueHasUserData: true,
		UserData:             verionedUserData,
	}

	subTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			mockMatchingClient.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).
				Return(nilDatResp, nil).Times(2)
			mockMatchingClient.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).
				Return(hasDatResp, nil).MinTimes(1)
			tqm.matchingClient = mockMatchingClient
		})
	res, err := subTq.fetchUserDataFromRootPartition(ctx)
	require.Nil(t, res)
	require.NoError(t, err)
	// Wait a bit to make sure we *don't* end up polling (if we do, mock will fail with >2 fetches)
	time.Sleep(time.Millisecond * 25)
	// Explicitly try to get versioning data. Since we don't have any cached, it'll explicitly fetch.
	existingData, err := subTq.GetUserData(ctx)
	require.NoError(t, err)
	require.Nil(t, existingData)
	// Now invalidate, the poll loop should be started, so we'll see at least one more mock call
	err = subTq.InvalidateUserData(&matchingservice.InvalidateTaskQueueUserDataRequest{
		UserData: verionedUserData,
	})
	require.NoError(t, err)
	time.Sleep(time.Millisecond * 20)
}
*/

func TestTaskQueueManagerWaitInitFailThenPass(t *testing.T) {
	controller := gomock.NewController(t)
	ctx := context.Background()

	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId
	tqCfg.config.UserDataPollFrequency = func() time.Duration {
		return time.Millisecond * 10
	}

	data := mkUserData(1)
	versionedData := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    data,
	}
	asResp := &matchingservice.GetTaskQueueUserDataResponse{
		TaskQueueHasUserData: true,
		UserData:             versionedData,
	}

	mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
	tq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).
				Return(nil, errors.New("some error")).Times(1)
			mockMatchingClient.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).
				Return(asResp, nil).Times(1)
			tqm.matchingClient = mockMatchingClient
		})

	tq.Start()
	// This does not error even if initial user data fetch fails (and it does, here)
	require.NoError(t, tq.WaitUntilInitialized(ctx))
	// Wait long enough for poller retry to happen
	time.Sleep(time.Millisecond * 15)
	// Need to make sure both calls have happened *before* calling to get data, as it would make a call if the second
	// call hasn't happened yet.
	controller.Finish()
	// Get the data and see it's set
	newData, _, err := tq.GetUserData(ctx)
	require.NoError(t, err)
	require.Equal(t, versionedData, newData)
	tq.Stop()
}

/* FIXME
func TestFetchingUserDataErrorsIfNeverFetchedFromRootSuccessfully(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId
	tqCfg.config.UserDataPollFrequency = func() time.Duration {
		return time.Millisecond * 10
	}

	subTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			mockMatchingClient.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).
				Return(nil, errors.New("fetching broken!")).AnyTimes()
			tqm.matchingClient = mockMatchingClient
		})
	subTq.Start()
	require.NoError(t, subTq.WaitUntilInitialized(ctx))
	for i := 0; i < 10; i++ {
		_, err := subTq.GetUserData(ctx)
		require.Error(t, err)
		time.Sleep(time.Millisecond * 2)
	}
}
*/

/* FIXME
func TestActivityQueueGetsUserDataFromWorkflowQueue(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()

	data := mkUserData(1)
	versionedData := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    data,
	}
	asResp := &matchingservice.GetTaskQueueUserDataResponse{
		TaskQueueHasUserData: true,
		UserData:             versionedData,
	}

	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId.taskType = enumspb.TASK_QUEUE_TYPE_ACTIVITY
	actTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			mockMatchingClient.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).
				Return(asResp, nil).Times(1)
			tqm.matchingClient = mockMatchingClient
		})
	actTq.Start()
	require.NoError(t, actTq.WaitUntilInitialized(ctx))

	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_ACTIVITY, 1)
	require.NoError(t, err)
	tqCfg = defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId
	actTqPart := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			mockMatchingClient.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).
				Return(asResp, nil).Times(1)
			tqm.matchingClient = mockMatchingClient
		})
	actTqPart.Start()
	require.NoError(t, actTqPart.WaitUntilInitialized(ctx))

	actTqData, err := actTq.GetUserData(ctx)
	require.NoError(t, err)
	require.Equal(t, versionedData, actTqData)
	actTqData, err = actTqPart.GetUserData(ctx)
	require.NoError(t, err)
	require.Equal(t, versionedData, actTqData)

	actTq.Stop()
	actTqPart.Stop()
}
*/

func TestUpdateOnNonRootFails(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()

	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId
	subTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg)
	err = subTq.UpdateUserData(ctx, false, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, error) {
		return data, nil
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errUserDataNoMutateNonRoot)

	actTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_ACTIVITY, 0)
	require.NoError(t, err)
	actTqCfg := defaultTqmTestOpts(controller)
	actTqCfg.tqId = actTqId
	actTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, actTqCfg)
	err = actTq.UpdateUserData(ctx, false, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, error) {
		return data, nil
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errUserDataNoMutateNonRoot)
}
