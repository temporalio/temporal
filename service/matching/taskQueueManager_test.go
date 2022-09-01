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
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
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

func TestSyncMatchLeasingUnavailable(t *testing.T) {
	tqm := mustCreateTestTaskQueueManager(t, gomock.NewController(t))
	tqm.Start()
	err := tqm.WaitUntilInitialized(context.TODO())
	require.Nil(t, err)
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
	tqm := mustCreateTestTaskQueueManager(t, gomock.NewController(t))
	tqm.Start()
	err := tqm.WaitUntilInitialized(context.TODO())
	require.Nil(t, err)
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
	sync, err = tqm.AddTask(context.TODO(), addTaskParams{
		execution: &commonpb.WorkflowExecution{},
		taskInfo:  &persistencespb.TaskInfo{},
		source:    enumsspb.TASK_SOURCE_HISTORY,
	})
	require.NoError(t, err)
	require.False(t, sync)
}

func TestReadLevelAckLevel(t *testing.T) {
	tqm := mustCreateTestTaskQueueManager(t, gomock.NewController(t))
	tqm.Start()
	tqm.WaitUntilInitialized(context.TODO())
	defer tqm.Stop()
	readLevel := tqm.dbTaskManager.taskReader.getReadLevel()
	ackLevel := tqm.dbTaskManager.taskReader.getReadLevel()
	require.Equal(t, int64(0), readLevel)
	require.Equal(t, int64(0), ackLevel)
	sync, err := tqm.AddTask(context.TODO(), addTaskParams{
		execution: &commonpb.WorkflowExecution{},
		taskInfo:  &persistencespb.TaskInfo{},
		source:    enumsspb.TASK_SOURCE_HISTORY})
	require.NoError(t, err)
	require.False(t, sync)
	sync, err = tqm.AddTask(context.TODO(), addTaskParams{
		execution: &commonpb.WorkflowExecution{},
		taskInfo:  &persistencespb.TaskInfo{},
		source:    enumsspb.TASK_SOURCE_HISTORY})
	require.NoError(t, err)
	require.False(t, sync)
	poller, out := runOneShotPoller(context.Background(), tqm)
	<-out
	defer poller.Cancel()
	readLevel = tqm.dbTaskManager.taskReader.getReadLevel()
	require.Equal(t, int64(2), readLevel)
	_, out = runOneShotPoller(context.Background(), tqm)
	<-out
	ackLevel = tqm.dbTaskManager.taskReader.getAckLevel()
	require.Equal(t, int64(0), ackLevel)
	time.Sleep(time.Minute) // dbTaskUpdateAckInterval
	ackLevel = tqm.dbTaskManager.taskReader.getAckLevel()
	require.Equal(t, int64(2), ackLevel)
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

	taskCount := int64(3)
	PollerIdentity := "test-poll"

	// Create taskQueue Manager and set taskQueue state
	tlm := mustCreateTestTaskQueueManager(t, controller)
	tlm.Start()
	tlm.WaitUntilInitialized(context.TODO())
	defer tlm.Stop()

	for i := int64(0); i < taskCount; i++ {
		tlm.AddTask(context.TODO(), addTaskParams{
			execution: &commonpb.WorkflowExecution{},
			taskInfo:  &persistencespb.TaskInfo{},
			source:    enumsspb.TASK_SOURCE_HISTORY})
	}

	includeTaskStatus := false
	descResp := tlm.DescribeTaskQueue(includeTaskStatus)
	require.Equal(t, 0, len(descResp.GetPollers()))
	require.Nil(t, descResp.GetTaskQueueStatus())

	includeTaskStatus = true
	taskQueueStatus := tlm.DescribeTaskQueue(includeTaskStatus).GetTaskQueueStatus()
	require.NotNil(t, taskQueueStatus)
	require.Zero(t, taskQueueStatus.GetAckLevel())
	taskIDBlock := taskQueueStatus.GetTaskIdBlock()
	require.Equal(t, int64(1), taskIDBlock.GetStartId())
	require.Equal(t, tlm.config.RangeSize, taskIDBlock.GetEndId())

	// Add a poller and complete all tasks
	tlm.pollerHistory.updatePollerInfo(pollerIdentity(PollerIdentity), nil)
	for i := int64(0); i < taskCount; i++ {
		poller, out := runOneShotPoller(context.Background(), tlm)
		<-out
		poller.Cancel()
	}
	time.Sleep(time.Minute) // dbTaskUpdateAckInterval

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
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.config = cfg

	// Idle
	tlm := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg)
	tlm.Start()
	tlm.WaitUntilInitialized(context.TODO())
	time.Sleep(1 * time.Second)
	require.Equal(t, common.DaemonStatusStarted, atomic.LoadInt32(&tlm.status))

	// Active poll-er
	tlm = mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg)
	tlm.Start()
	tlm.pollerHistory.updatePollerInfo(pollerIdentity("test-poll"), nil)
	require.Equal(t, 1, len(tlm.GetAllPollerInfo()))
	time.Sleep(1 * time.Second)
	require.Equal(t, common.DaemonStatusStarted, atomic.LoadInt32(&tlm.status))
	tlm.Stop()
	require.Equal(t, common.DaemonStatusStopped, atomic.LoadInt32(&tlm.status))

	// Active adding task
	tlm = mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg)
	tlm.Start()
	require.Equal(t, 0, len(tlm.GetAllPollerInfo()))
	tlm.dbTaskManager.signalDispatch()
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
	tlm.WaitUntilInitialized(context.TODO())
	// stop taskWriter so that we can check if there's any call to it
	// otherwise the task persist process is async and hard to test
	tlm.dbTaskManager.Stop()
	<-tlm.dbTaskManager.shutdownChan

	addTaskParam := addTaskParams{
		execution: &commonpb.WorkflowExecution{},
		taskInfo:  &persistencespb.TaskInfo{},
		source:    enumsspb.TASK_SOURCE_HISTORY,
	}

	syncMatch, err := tlm.AddTask(context.Background(), addTaskParam)
	require.Equal(t, errDBTaskManagerNotReady, err) // task writer was stopped above
	require.False(t, syncMatch)

	addTaskParam.forwardedFrom = "from child partition"
	syncMatch, err = tlm.AddTask(context.Background(), addTaskParam)
	require.Equal(t, errRemoteSyncMatchFailed, err) // should not persist the task
	require.False(t, syncMatch)
}

func TestTaskQueueSubParitionFetchesVersioningInfoFromRootPartitionOnInit(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId

	data := &persistencespb.VersioningData{
		CurrentDefault: mkVerIdNode("0"),
	}
	asResp := &matchingservice.GetTaskQueueMetadataResponse{
		VersioningDataResp: &matchingservice.GetTaskQueueMetadataResponse_VersioningData{
			VersioningData: data,
		},
	}

	subTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			mockMatchingClient.EXPECT().GetTaskQueueMetadata(gomock.Any(),
				gomock.Eq(&matchingservice.GetTaskQueueMetadataRequest{
					NamespaceId: defaultNamespaceId.String(),
					TaskQueue:   defaultRootTqID,
					// Nonsense value since it has nothing on startup
					WantVersioningDataCurhash: []byte{0},
				})).
				Return(asResp, nil)
			tqm.matchingClient = mockMatchingClient
		})
	subTq.Start()
	subTq.WaitUntilInitialized(context.TODO())
	verDat, err := subTq.GetVersioningData(ctx)
	require.NoError(t, err)
	require.Equal(t, data, verDat)
	subTq.Stop()
}

func TestTaskQueueSubParitionSendsCurrentHashOfVersioningDataWhenFetching(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId

	data := &persistencespb.VersioningData{
		CurrentDefault: mkVerIdNode("0"),
	}
	asResp := &matchingservice.GetTaskQueueMetadataResponse{
		VersioningDataResp: &matchingservice.GetTaskQueueMetadataResponse_VersioningData{
			VersioningData: data,
		},
	}
	dataHash := HashVersioningData(data)

	subTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			mockMatchingClient.EXPECT().GetTaskQueueMetadata(gomock.Any(),
				gomock.Eq(&matchingservice.GetTaskQueueMetadataRequest{
					NamespaceId:               defaultNamespaceId.String(),
					TaskQueue:                 defaultRootTqID,
					WantVersioningDataCurhash: dataHash,
				})).
				Return(asResp, nil)
			tqm.matchingClient = mockMatchingClient
		})
	subTq.dbTaskManager.Start()
	subTq.dbTaskManager.taskQueueOwnership.setVersioningDataForNonRootPartition(data)
	res, err := subTq.fetchMetadataFromRootPartition(ctx)
	require.NotNil(t, res)
	require.NoError(t, err)
}

type invalidateMatcher struct {
	matchesTaskQType enumspb.TaskQueueType
}

func (m invalidateMatcher) Matches(x interface{}) bool {
	v, ok := x.(*matchingservice.InvalidateTaskQueueMetadataRequest)
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
			mockMatchingClient.EXPECT().InvalidateTaskQueueMetadata(
				gomock.Any(), invalidateMatcher{matchesTaskQType: enumspb.TASK_QUEUE_TYPE_WORKFLOW}).
				Return(&matchingservice.InvalidateTaskQueueMetadataResponse{}, nil).
				Times(tqm.config.NumReadPartitions() - 1)
			mockMatchingClient.EXPECT().InvalidateTaskQueueMetadata(
				gomock.Any(), invalidateMatcher{matchesTaskQType: enumspb.TASK_QUEUE_TYPE_ACTIVITY}).
				Return(&matchingservice.InvalidateTaskQueueMetadataResponse{}, nil).
				// Not minus 1 here because root activity partition gets invalidated
				Times(tqm.config.NumReadPartitions())
			tqm.matchingClient = mockMatchingClient
		})

	rootTq.Start()
	rootTq.WaitUntilInitialized(context.TODO())
	// Make a change, mock verifies children are invalidated
	require.NoError(t, rootTq.MutateVersioningData(ctx, func(vd *persistencespb.VersioningData) error {
		*vd = persistencespb.VersioningData{
			CurrentDefault: mkVerIdNode("0"),
		}
		return nil
	}))
	rootTq.Stop()
}

func TestTaskQueueSubPartitionPollsPeriodically(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId
	tqCfg.config.MetadataPollFrequency = func() time.Duration {
		return time.Millisecond * 10
	}

	asResp := &matchingservice.GetTaskQueueMetadataResponse{
		VersioningDataResp: &matchingservice.GetTaskQueueMetadataResponse_VersioningData{
			VersioningData: &persistencespb.VersioningData{
				CurrentDefault: mkVerIdNode("0"),
			},
		},
	}

	subTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			mockMatchingClient.EXPECT().GetTaskQueueMetadata(gomock.Any(), gomock.Any()).
				Return(asResp, nil).MinTimes(3)
			tqm.matchingClient = mockMatchingClient
		})
	subTq.dbTaskManager.Start()
	res, err := subTq.fetchMetadataFromRootPartition(ctx)
	require.NotNil(t, res)
	require.NoError(t, err)
	// Wait a bit to make sure we poll a few times
	time.Sleep(time.Millisecond * 25)
	subTq.Stop()
}

func TestTaskQueueSubPartitionDoesNotPollIfNoDataThenPollsWhenInvalidated(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId
	tqCfg.config.MetadataPollFrequency = func() time.Duration {
		return time.Millisecond * 10
	}

	nilDatResp := &matchingservice.GetTaskQueueMetadataResponse{
		VersioningDataResp: &matchingservice.GetTaskQueueMetadataResponse_VersioningData{
			VersioningData: nil,
		},
	}
	verDat := &persistencespb.VersioningData{
		CurrentDefault: mkVerIdNode("0"),
	}
	hasDatResp := &matchingservice.GetTaskQueueMetadataResponse{
		VersioningDataResp: &matchingservice.GetTaskQueueMetadataResponse_VersioningData{
			VersioningData: verDat,
		},
	}

	subTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			mockMatchingClient.EXPECT().GetTaskQueueMetadata(gomock.Any(), gomock.Any()).
				Return(nilDatResp, nil).Times(2)
			mockMatchingClient.EXPECT().GetTaskQueueMetadata(gomock.Any(), gomock.Any()).
				Return(hasDatResp, nil).MinTimes(1)
			tqm.matchingClient = mockMatchingClient
		})
	subTq.dbTaskManager.Start()
	res, err := subTq.fetchMetadataFromRootPartition(ctx)
	require.Nil(t, res)
	require.NoError(t, err)
	// Wait a bit to make sure we *don't* end up polling (if we do, mock will fail with >2 fetches)
	time.Sleep(time.Millisecond * 25)
	// Explicitly try to get versioning data. Since we don't have any cached, it'll explicitly fetch.
	vDat, err := subTq.GetVersioningData(ctx)
	require.NoError(t, err)
	require.Nil(t, vDat)
	// Now invalidate, the poll loop should be started, so we'll see at least one more mock call
	err = subTq.InvalidateMetadata(&matchingservice.InvalidateTaskQueueMetadataRequest{
		VersioningData: verDat,
	})
	require.NoError(t, err)
	time.Sleep(time.Millisecond * 20)
}

func TestTaskQueueManagerWaitInitFailThenPass(t *testing.T) {
	controller := gomock.NewController(t)
	ctx := context.Background()

	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId
	tqCfg.config.MetadataPollFrequency = func() time.Duration {
		return time.Millisecond * 10
	}

	data := &persistencespb.VersioningData{
		CurrentDefault: mkVerIdNode("0"),
	}
	asResp := &matchingservice.GetTaskQueueMetadataResponse{
		VersioningDataResp: &matchingservice.GetTaskQueueMetadataResponse_VersioningData{
			VersioningData: data,
		},
	}

	mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
	tq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient.EXPECT().GetTaskQueueMetadata(gomock.Any(), gomock.Any()).
				Return(nil, errors.New("some error")).Times(1)
			mockMatchingClient.EXPECT().GetTaskQueueMetadata(gomock.Any(), gomock.Any()).
				Return(asResp, nil).Times(1)
			tqm.matchingClient = mockMatchingClient
		})

	tq.Start()
	// This does not error even if initial metadata fetch fails (and it does, here)
	tq.WaitUntilInitialized(context.TODO())
	// Wait long enough for poller retry to happen
	time.Sleep(time.Millisecond * 15)
	// Need to make sure both calls have happened *before* calling to get data, as it would make a call if the second
	// call hasn't happened yet.
	controller.Finish()
	// Get the data and see it's set
	dat, err := tq.GetVersioningData(ctx)
	require.NoError(t, err)
	require.Equal(t, data, dat)
	tq.Stop()
}

func TestFetchingVersioningDataErrorsIfNeverFetchedFromRootSuccessfully(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId
	tqCfg.config.MetadataPollFrequency = func() time.Duration {
		return time.Millisecond * 10
	}

	subTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			mockMatchingClient.EXPECT().GetTaskQueueMetadata(gomock.Any(), gomock.Any()).
				Return(nil, errors.New("fetching broken!")).AnyTimes()
			tqm.matchingClient = mockMatchingClient
		})
	subTq.Start()
	subTq.WaitUntilInitialized(context.TODO())
	for i := 0; i < 10; i++ {
		_, err := subTq.GetVersioningData(ctx)
		require.Error(t, err)
		time.Sleep(time.Millisecond * 2)
	}
}

func TestActivityQueueGetsVersioningDataFromWorkflowQueue(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()

	data := &persistencespb.VersioningData{
		CurrentDefault: mkVerIdNode("0"),
	}
	asResp := &matchingservice.GetTaskQueueMetadataResponse{
		VersioningDataResp: &matchingservice.GetTaskQueueMetadataResponse_VersioningData{
			VersioningData: data,
		},
	}

	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId.taskType = enumspb.TASK_QUEUE_TYPE_ACTIVITY
	actTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			mockMatchingClient.EXPECT().GetTaskQueueMetadata(gomock.Any(), gomock.Any()).
				Return(asResp, nil).Times(1)
			tqm.matchingClient = mockMatchingClient
		})
	actTq.Start()
	actTq.WaitUntilInitialized(context.TODO())

	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_ACTIVITY, 1)
	require.NoError(t, err)
	tqCfg = defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId
	actTqPart := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg,
		func(tqm *taskQueueManagerImpl) {
			mockMatchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			mockMatchingClient.EXPECT().GetTaskQueueMetadata(gomock.Any(), gomock.Any()).
				Return(asResp, nil).Times(1)
			tqm.matchingClient = mockMatchingClient
		})
	actTqPart.Start()
	actTqPart.WaitUntilInitialized(context.TODO())

	verDat, err := actTq.GetVersioningData(ctx)
	require.NoError(t, err)
	require.Equal(t, data, verDat)
	verDat, err = actTqPart.GetVersioningData(ctx)
	require.NoError(t, err)
	require.Equal(t, data, verDat)

	actTq.Stop()
	actTqPart.Stop()
}

func TestMutateOnNonRootFails(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()

	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId
	subTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, tqCfg)
	subTq.dbTaskManager.Start()
	err = subTq.MutateVersioningData(ctx, func(data *persistencespb.VersioningData) error { return nil })
	require.Error(t, err)
	require.ErrorIs(t, err, errVersioningDataNoMutateNonRoot)

	actTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_ACTIVITY, 0)
	require.NoError(t, err)
	actTqCfg := defaultTqmTestOpts(controller)
	actTqCfg.tqId = actTqId
	actTq := mustCreateTestTaskQueueManagerWithConfig(t, controller, actTqCfg)
	actTq.dbTaskManager.Start()
	err = actTq.MutateVersioningData(ctx, func(data *persistencespb.VersioningData) error { return nil })
	require.Error(t, err)
	require.ErrorIs(t, err, errVersioningDataNoMutateNonRoot)
}
