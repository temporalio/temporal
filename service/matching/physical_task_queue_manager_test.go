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

	"go.temporal.io/server/common/clock"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/internal/goro"
)

var rpsInf = math.Inf(1)

const (
	defaultNamespaceId = "deadbeef-0000-4567-890a-bcdef0123456"
	defaultRootTqID    = "tq"
)

type tqmTestOpts struct {
	config             *Config
	dbq                *PhysicalTaskQueueKey
	matchingClientMock *matchingservicemock.MockMatchingServiceClient
}

func defaultTqmTestOpts(controller *gomock.Controller) *tqmTestOpts {
	return &tqmTestOpts{
		config:             defaultTestConfig(),
		dbq:                defaultTqId(),
		matchingClientMock: matchingservicemock.NewMockMatchingServiceClient(controller),
	}
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

func withIDBlockAllocator(ibl idBlockAllocator) taskQueueManagerOpt {
	return func(tqm *physicalTaskQueueManagerImpl) {
		tqm.backlogMgr.taskWriter.idAlloc = ibl
	}
}

func TestSyncMatchLeasingUnavailable(t *testing.T) {
	tqm := mustCreateTestPhysicalTaskQueueManager(t, gomock.NewController(t),
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
		taskInfo: &persistencespb.TaskInfo{},
		source:   enumsspb.TASK_SOURCE_HISTORY})
	require.NoError(t, err)
	require.True(t, sync)
}

// addTasks is a helper which adds numberOfTasks to a taskTracker
func trackTasksHelper(tr *taskTracker, numberOfTasks int) {
	for i := 0; i < numberOfTasks; i++ {
		// adding a bunch of tasks
		tr.incrementTaskCount()
	}
}

func TestAddTasksRate(t *testing.T) {
	// define a fake clock and it's time for testing
	timeSource := clock.NewEventTimeSource()
	currentTime := time.Now()
	timeSource.Update(currentTime)

	tr := newTaskTracker(timeSource)

	// mini windows will have the following format : (start time, end time)
	// (0 - 4), (5 - 9), (10 - 14), (15 - 19), (20 - 24), (25 - 29), (30 - 34), ...

	// tasks should be placed in the first mini-window
	timeSource.Advance(1 * time.Second) // time: 1 second
	trackTasksHelper(tr, 100)
	require.InEpsilon(t, float32(100), tr.rate(), 0.001) // 100 tasks added in 1 second = 100 / 1 = 100

	// tasks should be placed in the second mini-window with 6 total seconds elapsed
	timeSource.Advance(5 * time.Second)
	trackTasksHelper(tr, 200)                           // time: 6 second
	require.InEpsilon(t, float32(50), tr.rate(), 0.001) // (100 + 200) tasks added in 6 seconds = 300/6 = 50

	timeSource.Advance(24 * time.Second) // time: 30 second
	trackTasksHelper(tr, 300)
	require.InEpsilon(t, float32(20), tr.rate(), 0.001) // (100 + 200 + 300) tasks added in (30 + 0 (current window)) seconds = 600/30 = 20

	// this should clear out the first mini-window of 100 tasks
	timeSource.Advance(5 * time.Second) // time: 35 second
	trackTasksHelper(tr, 10)
	require.InEpsilon(t, float32(17), tr.rate(), 0.001) // (10 + 200 + 300) tasks added in (30 + 0 (current window)) seconds = 510/30 = 17

	// this should clear out the second and third mini-windows
	timeSource.Advance(15 * time.Second) // time: 50 second
	trackTasksHelper(tr, 10)
	require.InEpsilon(t, float32(10.666667), tr.rate(), 0.001) // (10 + 10 + 300) tasks added in (30 + 0 (current window)) seconds = 320/30 = 10.66

	// a minute passes and no tasks are added
	timeSource.Advance(60 * time.Second)
	require.Equal(t, float32(0), tr.rate()) // 0 tasks have been added in the last 30 seconds
}

func TestForeignPartitionOwnerCausesUnload(t *testing.T) {
	cfg := NewConfig(dynamicconfig.NewNoopCollection())
	cfg.RangeSize = 1 // TaskID block size
	var leaseErr error
	tqm := mustCreateTestPhysicalTaskQueueManager(t, gomock.NewController(t),
		makeTestBlocAlloc(func() (taskQueueState, error) {
			return taskQueueState{rangeID: 1}, leaseErr
		}))
	tqm.Start()
	defer tqm.Stop()

	// TQM started succesfully with an ID block of size 1. Perform one send
	// without a poller to consume the one task ID from the reserved block.
	sync, err := tqm.AddTask(context.TODO(), addTaskParams{
		taskInfo: &persistencespb.TaskInfo{
			CreateTime: timestamp.TimePtr(time.Now().UTC()),
		},
		source: enumsspb.TASK_SOURCE_HISTORY})
	require.False(t, sync)
	require.NoError(t, err)

	// TQM's ID block should be empty so the next AddTask will trigger an
	// attempt to obtain more IDs. This specific error type indicates that
	// another service instance has become the owner of the partition
	leaseErr = &persistence.ConditionFailedError{Msg: "should kill the tqm"}

	sync, err = tqm.AddTask(context.TODO(), addTaskParams{
		taskInfo: &persistencespb.TaskInfo{
			CreateTime: timestamp.TimePtr(time.Now().UTC()),
		},
		source: enumsspb.TASK_SOURCE_HISTORY,
	})
	require.NoError(t, err)
	require.False(t, sync)
}

// TODO: this test probably should go to backlog_manager_test
func TestReaderSignaling(t *testing.T) {
	readerNotifications := make(chan struct{}, 1)
	clearNotifications := func() {
		for len(readerNotifications) > 0 {
			<-readerNotifications
		}
	}
	tqm := mustCreateTestPhysicalTaskQueueManager(t, gomock.NewController(t))

	// redirect taskReader signals into our local channel
	tqm.backlogMgr.taskReader.notifyC = readerNotifications

	tqm.Start()
	defer tqm.Stop()

	// shut down the taskReader so it doesn't steal notifications from us
	tqm.backlogMgr.taskReader.gorogrp.Cancel()
	tqm.backlogMgr.taskReader.gorogrp.Wait()

	clearNotifications()

	sync, err := tqm.AddTask(context.TODO(), addTaskParams{
		taskInfo: &persistencespb.TaskInfo{
			CreateTime: timestamp.TimePtr(time.Now().UTC()),
		},
		source: enumsspb.TASK_SOURCE_HISTORY})
	require.NoError(t, err)
	require.False(t, sync)
	require.Len(t, readerNotifications, 1,
		"Sync match failure with successful db write should signal taskReader")

	clearNotifications()
	poller, _ := runOneShotPoller(context.Background(), tqm)
	defer poller.Cancel()

	sync, err = tqm.AddTask(context.TODO(), addTaskParams{
		taskInfo: &persistencespb.TaskInfo{
			CreateTime: timestamp.TimePtr(time.Now().UTC()),
		},
		source: enumsspb.TASK_SOURCE_HISTORY})
	require.NoError(t, err)
	require.True(t, sync)
	require.Len(t, readerNotifications, 0,
		"Sync match should not signal taskReader")
}

// runOneShotPoller spawns a goroutine to call tqm.PollTask on the provided tqm.
// The second return value is a channel of either error or *internalTask.
func runOneShotPoller(ctx context.Context, tqm physicalTaskQueueManager) (*goro.Handle, chan interface{}) {
	out := make(chan interface{}, 1)
	handle := goro.NewHandle(ctx).Go(func(ctx context.Context) error {
		task, err := tqm.PollTask(ctx, &pollMetadata{ratePerSecond: &rpsInf})
		if task == nil {
			out <- err
			return nil
		}
		task.finish(err)
		out <- task
		return nil
	})
	// tqm.PollTask() needs some time to attach the goro started above to the
	// internal task channel. Sorry for this but it appears unavoidable.
	time.Sleep(10 * time.Millisecond)
	return handle, out
}

func defaultTqId() *PhysicalTaskQueueKey {
	return newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0)
}

func mustCreateTestPhysicalTaskQueueManager(
	t *testing.T,
	controller *gomock.Controller,
	opts ...taskQueueManagerOpt,
) *physicalTaskQueueManagerImpl {
	t.Helper()
	return mustCreateTestTaskQueueManagerWithConfig(t, controller, defaultTqmTestOpts(controller), opts...)
}

func mustCreateTestTaskQueueManagerWithConfig(
	t *testing.T,
	controller *gomock.Controller,
	testOpts *tqmTestOpts,
	opts ...taskQueueManagerOpt,
) *physicalTaskQueueManagerImpl {
	t.Helper()
	tqm, err := createTestTaskQueueManagerWithConfig(controller, testOpts, opts...)
	require.NoError(t, err)
	return tqm
}

func createTestTaskQueueManagerWithConfig(
	controller *gomock.Controller,
	testOpts *tqmTestOpts,
	opts ...taskQueueManagerOpt,
) (*physicalTaskQueueManagerImpl, error) {
	nsName := namespace.Name("ns-name")
	ns, registry := createMockNamespaceCache(controller, nsName)
	me := createTestMatchingEngine(controller, testOpts.config, testOpts.matchingClientMock, registry)
	partition := testOpts.dbq.Partition()
	tqConfig := newTaskQueueConfig(partition.TaskQueue(), me.config, nsName)
	userDataManager := newUserDataManager(me.taskManager, me.matchingRawClient, partition, tqConfig, me.logger, me.namespaceRegistry)
	pm := createTestTaskQueuePartitionManager(ns, partition, tqConfig, me, userDataManager)
	tlMgr, err := newPhysicalTaskQueueManager(pm, testOpts.dbq, opts...)
	pm.defaultQueue = tlMgr
	if err != nil {
		return nil, err
	}
	return tlMgr, nil
}

func createTestTaskQueuePartitionManager(ns *namespace.Namespace, partition tqid.Partition, tqConfig *taskQueueConfig, me *matchingEngineImpl, userDataManager userDataManager) *taskQueuePartitionManagerImpl {
	pm := &taskQueuePartitionManagerImpl{
		engine:               me,
		partition:            partition,
		config:               tqConfig,
		ns:                   ns,
		logger:               me.logger,
		matchingClient:       me.matchingRawClient,
		taggedMetricsHandler: me.metricsHandler,
		userDataManager:      userDataManager,
	}

	me.partitions[partition.Key()] = pm
	return pm
}

func TestLegacyDescribeTaskQueue(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	startTaskID := int64(1)
	taskCount := int64(3)
	PollerIdentity := "test-poll"

	// Create queue Manager and set queue state
	tlm := mustCreateTestPhysicalTaskQueueManager(t, controller)
	tlm.backlogMgr.db.rangeID = int64(1)
	tlm.backlogMgr.db.ackLevel = int64(0)
	tlm.backlogMgr.taskAckManager.setAckLevel(tlm.backlogMgr.db.ackLevel)

	for i := int64(0); i < taskCount; i++ {
		tlm.backlogMgr.taskAckManager.addTask(startTaskID + i)
	}

	// Manually increasing the backlog counter since it does not get incremented by taskAckManager.addTask
	// Only doing this for the purpose of this test
	tlm.backlogMgr.db.updateApproximateBacklogCount(taskCount)

	includeTaskStatus := false
	descResp := tlm.LegacyDescribeTaskQueue(includeTaskStatus)
	require.Equal(t, 0, len(descResp.DescResponse.GetPollers()))
	require.Nil(t, descResp.DescResponse.GetTaskQueueStatus())

	includeTaskStatus = true
	taskQueueStatus := tlm.LegacyDescribeTaskQueue(includeTaskStatus).DescResponse.GetTaskQueueStatus()
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
		tlm.backlogMgr.taskAckManager.completeTask(startTaskID + i)
	}

	descResp = tlm.LegacyDescribeTaskQueue(includeTaskStatus)
	require.Equal(t, 1, len(descResp.DescResponse.GetPollers()))
	require.Equal(t, PollerIdentity, descResp.DescResponse.Pollers[0].GetIdentity())
	require.NotEmpty(t, descResp.DescResponse.Pollers[0].GetLastAccessTime())

	rps := 5.0
	tlm.pollerHistory.updatePollerInfo(pollerIdentity(PollerIdentity), &pollMetadata{ratePerSecond: &rps})
	descResp = tlm.LegacyDescribeTaskQueue(includeTaskStatus)
	require.Equal(t, 1, len(descResp.DescResponse.GetPollers()))
	require.Equal(t, PollerIdentity, descResp.DescResponse.Pollers[0].GetIdentity())
	require.True(t, descResp.DescResponse.Pollers[0].GetRatePerSecond() > 4.0 && descResp.DescResponse.Pollers[0].GetRatePerSecond() < 6.0)

	taskQueueStatus = descResp.DescResponse.GetTaskQueueStatus()
	require.NotNil(t, taskQueueStatus)
	require.Equal(t, taskCount, taskQueueStatus.GetAckLevel())
	require.Zero(t, taskQueueStatus.GetBacklogCountHint()) // should be 0 since AckManager.CompleteTask decrements the updated backlog counter
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
	tlm.pollerHistory.updatePollerInfo("test-poll", &pollMetadata{})
	require.Equal(t, 1, len(tlm.GetAllPollerInfo()))
	time.Sleep(1 * time.Second)
	require.Equal(t, common.DaemonStatusStarted, atomic.LoadInt32(&tlm.status))
	tlm.Stop()
	require.Equal(t, common.DaemonStatusStopped, atomic.LoadInt32(&tlm.status))

	// Active adding task
	tlm = mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg)
	tlm.Start()
	require.Equal(t, 0, len(tlm.GetAllPollerInfo()))
	tlm.backlogMgr.taskReader.Signal()
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
		func(tqm *physicalTaskQueueManagerImpl) {
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
	tlm.backlogMgr.taskWriter.Stop()
	<-tlm.backlogMgr.taskWriter.writeLoop.Done()

	addTaskParam := addTaskParams{
		taskInfo: &persistencespb.TaskInfo{
			CreateTime: timestamp.TimePtr(time.Now().UTC()),
		},
		source: enumsspb.TASK_SOURCE_HISTORY,
	}

	syncMatch, err := tlm.AddTask(context.Background(), addTaskParam)
	require.Equal(t, errShutdown, err) // task writer was stopped above
	require.False(t, syncMatch)

	addTaskParam.forwardedFrom = "from child partition"
	syncMatch, err = tlm.AddTask(context.Background(), addTaskParam)
	require.Equal(t, errRemoteSyncMatchFailed, err) // should not persist the task
	require.False(t, syncMatch)
}

func TestTQMDoesFinalUpdateOnIdleUnload(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)

	cfg := NewConfig(dynamicconfig.NewNoopCollection())
	cfg.MaxTaskQueueIdleTime = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueueInfo(1 * time.Second)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.config = cfg

	tqm := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg)
	tm, ok := tqm.partitionMgr.engine.taskManager.(*testTaskManager)
	require.True(t, ok)

	tqm.Start()
	time.Sleep(2 * time.Second) // will unload due to idleness
	require.Equal(t, 1, tm.getUpdateCount(tqCfg.dbq))
}

func TestTQMDoesNotDoFinalUpdateOnOwnershipLost(t *testing.T) {
	// TODO: use mocks instead of testTaskManager so we can do synchronization better instead of sleeps
	t.Parallel()

	controller := gomock.NewController(t)

	cfg := NewConfig(dynamicconfig.NewNoopCollection())
	cfg.UpdateAckInterval = dynamicconfig.GetDurationPropertyFnFilteredByTaskQueueInfo(2 * time.Second)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.config = cfg

	tqm := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg)
	tm, ok := tqm.partitionMgr.engine.taskManager.(*testTaskManager)
	require.True(t, ok)

	tqm.Start()
	time.Sleep(1 * time.Second)

	// simulate ownership lost
	ttm := tm.getQueueManager(tqCfg.dbq)
	ttm.Lock()
	ttm.rangeID++
	ttm.Unlock()

	time.Sleep(2 * time.Second) // will attempt to update and fail and not try again

	require.Equal(t, 1, tm.getUpdateCount(tqCfg.dbq))
}
