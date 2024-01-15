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

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/common/future"
	"google.golang.org/grpc"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
)

func createTestTaskQueuePartitionManager(controller *gomock.Controller, testOpts *tqmTestOpts, ) *taskQueuePartitionManagerImpl {
	logger := log.NewTestLogger()
	ns := namespace.Name("ns-name")
	tm := newTestTaskManager(logger)
	mockNamespaceCache := namespace.NewMockRegistry(controller)
	mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(ns, nil).AnyTimes()
	mockVisibilityManager := manager.NewMockVisibilityManager(controller)
	mockVisibilityManager.EXPECT().Close().AnyTimes()
	mockHistoryClient := historyservicemock.NewMockHistoryServiceClient(controller)
	mockHistoryClient.EXPECT().IsWorkflowTaskValid(gomock.Any(), gomock.Any()).Return(&historyservice.IsWorkflowTaskValidResponse{IsValid: true}, nil).AnyTimes()
	mockHistoryClient.EXPECT().IsActivityTaskValid(gomock.Any(), gomock.Any()).Return(&historyservice.IsActivityTaskValidResponse{IsValid: true}, nil).AnyTimes()
	me := newMatchingEngine(testOpts.config, tm, mockHistoryClient, logger, mockNamespaceCache, testOpts.matchingClientMock, mockVisibilityManager)
	pm := &taskQueuePartitionManagerImpl{
		engine:       me,
		taskQueueID:  testOpts.tqId,
		stickyInfo:   normalStickyInfo,
		config:       newTaskQueueConfig(testOpts.tqId, me.config, ns),
		namespaceRegistry: me.namespaceRegistry,
		logger: logger,
		matchingClient: me.matchingRawClient,
		taggedMetricsHandler: me.metricsHandler,
		userDataReady:        future.NewFuture[struct{}](),
	}

	me.taskQueues[*testOpts.tqId] = pm
	return pm
}

func createTestTaskQueuePartitionManagerWithDefaultQueue(
	t *testing.T,
	controller *gomock.Controller,
	testOpts *tqmTestOpts,
) *taskQueuePartitionManagerImpl {
	t.Helper()
	pm := createTestTaskQueuePartitionManager(controller, testOpts)
	dq, err := newTaskQueueManager(pm, pm.taskQueueID)
	require.NoError(t, err)
	pm.defaultQueue = dq
	pm.db = dq.db
	return pm
}

func TestUserData_LoadOnInit(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	tqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = tqId

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	tq := createTestTaskQueuePartitionManagerWithDefaultQueue(t, controller, tqCfg)

	require.NoError(t, tq.engine.taskManager.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: defaultNamespaceId.String(),
			TaskQueue:   defaultRootTqID,
			UserData:    data1,
		}))
	data1.Version++

	tq.Start()
	require.NoError(t, tq.WaitUntilInitialized(ctx))
	userData, _, err := tq.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	tq.Stop()
}

func TestUserData_LoadOnInit_OnlyOnceWhenNoData(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	tqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = tqId

	tq := createTestTaskQueuePartitionManagerWithDefaultQueue(t, controller, tqCfg)
	tm := tq.engine.taskManager.(*testTaskManager)

	require.Equal(t, 0, tm.getGetUserDataCount(tqId))

	tq.Start()
	require.NoError(t, tq.WaitUntilInitialized(ctx))

	require.Equal(t, 1, tm.getGetUserDataCount(tqId))

	userData, _, err := tq.GetUserData()
	require.NoError(t, err)
	require.Nil(t, userData)

	require.Equal(t, 1, tm.getGetUserDataCount(tqId))

	userData, _, err = tq.GetUserData()
	require.NoError(t, err)
	require.Nil(t, userData)

	require.Equal(t, 1, tm.getGetUserDataCount(tqId))

	tq.Stop()
}

func TestUserData_FetchesOnInit(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	tqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = tqId

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId.String(),
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false, // first fetch is not long poll
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			TaskQueueHasUserData: true,
			UserData:             data1,
		}, nil)

	tq := createTestTaskQueuePartitionManagerWithDefaultQueue(t, controller, tqCfg)
	tq.config.GetUserDataMinWaitTime = 10 * time.Second // only one fetch

	tq.Start()
	require.NoError(t, tq.WaitUntilInitialized(ctx))
	userData, _, err := tq.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	tq.Stop()
}

func TestUserData_FetchesAndFetchesAgain(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	// note: using activity here
	tqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_ACTIVITY, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = tqId

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}
	data2 := &persistencespb.VersionedTaskQueueUserData{
		Version: 2,
		Data:    mkUserData(2),
	}

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId.String(),
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false, // first is not long poll
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			TaskQueueHasUserData: true,
			UserData:             data1,
		}, nil)

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId.String(),
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 1,
			WaitNewData:              true, // second is long poll
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			TaskQueueHasUserData: true,
			UserData:             data2,
		}, nil)

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId.String(),
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 2,
			WaitNewData:              true,
		}).
		Return(nil, serviceerror.NewUnavailable("hold on")).AnyTimes()

	tq := createTestTaskQueuePartitionManagerWithDefaultQueue(t, controller, tqCfg)
	tq.config.GetUserDataMinWaitTime = 10 * time.Millisecond // fetch again quickly
	tq.Start()
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, tq.WaitUntilInitialized(ctx))
	userData, _, err := tq.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data2, userData)
	tq.Stop()
}

func TestUserData_RetriesFetchOnUnavailable(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	tqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = tqId

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	ch := make(chan struct{})

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId.String(),
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false,
		}).
		DoAndReturn(func(ctx context.Context, in *matchingservice.GetTaskQueueUserDataRequest, opts ...grpc.CallOption) (*matchingservice.GetTaskQueueUserDataResponse, error) {
			<-ch
			return nil, serviceerror.NewUnavailable("wait a sec")
		}).Times(3)

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId.String(),
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false,
		}).
		DoAndReturn(func(ctx context.Context, in *matchingservice.GetTaskQueueUserDataRequest, opts ...grpc.CallOption) (*matchingservice.GetTaskQueueUserDataResponse, error) {
			<-ch
			return &matchingservice.GetTaskQueueUserDataResponse{
				TaskQueueHasUserData: true,
				UserData:             data1,
			}, nil
		})

	tq := createTestTaskQueuePartitionManagerWithDefaultQueue(t, controller, tqCfg)
	tq.config.GetUserDataMinWaitTime = 10 * time.Second // wait on success
	tq.config.GetUserDataRetryPolicy = backoff.NewExponentialRetryPolicy(50 * time.Millisecond).
		WithMaximumInterval(50 * time.Millisecond) // faster retry on failure

	tq.Start()

	ch <- struct{}{}
	ch <- struct{}{}

	// at this point it should have tried two times and gotten unavailable. it should not be ready yet.
	require.False(t, tq.userDataReady.Ready())

	ch <- struct{}{}
	ch <- struct{}{}
	time.Sleep(100 * time.Millisecond) // time to return

	// now it should be ready
	require.NoError(t, tq.WaitUntilInitialized(ctx))
	userData, _, err := tq.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	tq.Stop()
}

func TestUserData_RetriesFetchOnUnImplemented(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	tqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = tqId

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	ch := make(chan struct{})

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId.String(),
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false,
		}).
		DoAndReturn(func(ctx context.Context, in *matchingservice.GetTaskQueueUserDataRequest, opts ...grpc.CallOption) (*matchingservice.GetTaskQueueUserDataResponse, error) {
			<-ch
			return nil, serviceerror.NewUnimplemented("older version")
		}).Times(3)

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId.String(),
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false,
		}).
		DoAndReturn(func(ctx context.Context, in *matchingservice.GetTaskQueueUserDataRequest, opts ...grpc.CallOption) (*matchingservice.GetTaskQueueUserDataResponse, error) {
			<-ch
			return &matchingservice.GetTaskQueueUserDataResponse{
				TaskQueueHasUserData: true,
				UserData:             data1,
			}, nil
		})

	tq := createTestTaskQueuePartitionManagerWithDefaultQueue(t, controller, tqCfg)
	tq.config.GetUserDataMinWaitTime = 10 * time.Second // wait on success
	tq.config.GetUserDataRetryPolicy = backoff.NewExponentialRetryPolicy(50 * time.Millisecond).
		WithMaximumInterval(50 * time.Millisecond) // faster retry on failure

	tq.Start()

	ch <- struct{}{}
	ch <- struct{}{}

	// at this point it should have tried once and gotten unimplemented. it should be ready already.
	require.NoError(t, tq.WaitUntilInitialized(ctx))

	userData, _, err := tq.GetUserData()
	require.Nil(t, userData)
	require.NoError(t, err)

	ch <- struct{}{}
	ch <- struct{}{}
	time.Sleep(100 * time.Millisecond) // time to return

	userData, _, err = tq.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	tq.Stop()
}

func TestUserData_FetchesUpTree(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	tqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 31)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.config.ForwarderMaxChildrenPerNode = dynamicconfig.GetIntPropertyFilteredByTaskQueueInfo(3)
	tqCfg.tqId = tqId

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId.String(),
			TaskQueue:                tqId.Name.WithPartition(10).FullName(),
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false,
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			TaskQueueHasUserData: true,
			UserData:             data1,
		}, nil)

	tq := createTestTaskQueuePartitionManagerWithDefaultQueue(t, controller, tqCfg)
	tq.config.GetUserDataMinWaitTime = 10 * time.Second // wait on success
	tq.Start()
	require.NoError(t, tq.WaitUntilInitialized(ctx))
	userData, _, err := tq.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	tq.Stop()
}

func TestUserData_FetchesActivityToWorkflow(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	// note: activity root
	tqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_ACTIVITY, 0)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = tqId

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId.String(),
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false,
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			TaskQueueHasUserData: true,
			UserData:             data1,
		}, nil)

	tq := createTestTaskQueuePartitionManagerWithDefaultQueue(t, controller, tqCfg)
	tq.config.GetUserDataMinWaitTime = 10 * time.Second // wait on success
	tq.Start()
	require.NoError(t, tq.WaitUntilInitialized(ctx))
	userData, _, err := tq.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	tq.Stop()
}

func TestUserData_FetchesStickyToNormal(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	tqCfg := defaultTqmTestOpts(controller)

	normalName := "normal-queue"
	stickyName := uuid.New()

	tqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, stickyName, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0)
	require.NoError(t, err)
	tqCfg.tqId = tqId

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId.String(),
			TaskQueue:                normalName,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false,
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			TaskQueueHasUserData: true,
			UserData:             data1,
		}, nil)

	// have to create manually to get sticky
	logger := log.NewTestLogger()
	tm := newTestTaskManager(logger)
	mockNamespaceCache := namespace.NewMockRegistry(controller)
	mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(namespace.Name("ns-name"), nil).AnyTimes()
	mockVisibilityManager := manager.NewMockVisibilityManager(controller)
	mockVisibilityManager.EXPECT().Close().AnyTimes()
	me := newMatchingEngine(tqCfg.config, tm, nil, logger, mockNamespaceCache, tqCfg.matchingClientMock, mockVisibilityManager)
	stickyInfo := stickyInfo{
		kind:       enumspb.TASK_QUEUE_KIND_STICKY,
		normalName: normalName,
	}

	tqi, err := newTaskQueuePartitionManager(me, tqCfg.tqId, stickyInfo, tqCfg.config)
	require.NoError(t, err)
	tq := tqi.(*taskQueuePartitionManagerImpl)

	tq.config.GetUserDataMinWaitTime = 10 * time.Second // wait on success
	tq.Start()
	require.NoError(t, tq.WaitUntilInitialized(ctx))
	userData, _, err := tq.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	tq.Stop()
}

func TestUserData_UpdateOnNonRootFails(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()

	subTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	require.NoError(t, err)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.tqId = subTqId
	subTq := createTestTaskQueuePartitionManagerWithDefaultQueue(t, controller, tqCfg)
	err = subTq.UpdateUserData(ctx, UserDataUpdateOptions{}, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
		return data, false, nil
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errUserDataNoMutateNonRoot)

	actTqId, err := newTaskQueueIDWithPartition(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_ACTIVITY, 0)
	require.NoError(t, err)
	actTqCfg := defaultTqmTestOpts(controller)
	actTqCfg.tqId = actTqId
	actTq := createTestTaskQueuePartitionManagerWithDefaultQueue(t, controller, actTqCfg)
	err = actTq.UpdateUserData(ctx, UserDataUpdateOptions{}, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
		return data, false, nil
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errUserDataNoMutateNonRoot)
}
