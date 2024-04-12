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
	"google.golang.org/grpc"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
)

func createUserDataManager(
	t *testing.T,
	controller *gomock.Controller,
	testOpts *tqmTestOpts,
) *userDataManagerImpl {
	t.Helper()

	logger := log.NewTestLogger()
	ns := namespace.Name("ns-name")
	tm := newTestTaskManager(logger)
	mockNamespaceCache := namespace.NewMockRegistry(controller)
	mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(ns, nil).AnyTimes()
	return newUserDataManager(tm, testOpts.matchingClientMock, testOpts.dbq.Partition(), newTaskQueueConfig(testOpts.dbq.Partition().TaskQueue(), testOpts.config, ns), logger, mockNamespaceCache)
}

func TestUserData_LoadOnInit(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	dbq := newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.dbq = dbq

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	m := createUserDataManager(t, controller, tqCfg)

	require.NoError(t, m.store.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: defaultNamespaceId,
			TaskQueue:   defaultRootTqID,
			UserData:    data1,
		}))
	data1.Version++

	m.Start()
	require.NoError(t, m.WaitUntilInitialized(ctx))
	userData, _, err := m.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	m.Stop()
}

func TestUserData_LoadOnInit_OnlyOnceWhenNoData(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	dbq := newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.dbq = dbq

	m := createUserDataManager(t, controller, tqCfg)
	tm, ok := m.store.(*testTaskManager)
	require.True(t, ok)

	require.Equal(t, 0, tm.getGetUserDataCount(dbq))

	m.Start()
	require.NoError(t, m.WaitUntilInitialized(ctx))

	require.Equal(t, 1, tm.getGetUserDataCount(dbq))

	userData, _, err := m.GetUserData()
	require.NoError(t, err)
	require.Nil(t, userData)

	require.Equal(t, 1, tm.getGetUserDataCount(dbq))

	userData, _, err = m.GetUserData()
	require.NoError(t, err)
	require.Nil(t, userData)

	require.Equal(t, 1, tm.getGetUserDataCount(dbq))

	m.Stop()
}

func TestUserData_FetchesOnInit(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	dbq := newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.dbq = dbq

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId,
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false, // first fetch is not long poll
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			UserData: data1,
		}, nil)

	m := createUserDataManager(t, controller, tqCfg)
	m.config.GetUserDataMinWaitTime = 10 * time.Second // only one fetch

	m.Start()
	require.NoError(t, m.WaitUntilInitialized(ctx))
	userData, _, err := m.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	m.Stop()
}

func TestUserData_FetchesAndFetchesAgain(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	// note: using activity here
	dbq := newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_ACTIVITY, 1)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.dbq = dbq

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
			NamespaceId:              defaultNamespaceId,
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false, // first is not long poll
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			UserData: data1,
		}, nil)

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId,
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 1,
			WaitNewData:              true, // second is long poll
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			UserData: data2,
		}, nil)

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId,
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 2,
			WaitNewData:              true,
		}).
		Return(nil, serviceerror.NewUnavailable("hold on")).AnyTimes()

	m := createUserDataManager(t, controller, tqCfg)
	m.config.GetUserDataMinWaitTime = 10 * time.Millisecond // fetch again quickly
	m.Start()
	time.Sleep(100 * time.Millisecond)
	require.NoError(t, m.WaitUntilInitialized(ctx))
	userData, _, err := m.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data2, userData)
	m.Stop()
}

func TestUserData_RetriesFetchOnUnavailable(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	dbq := newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.dbq = dbq

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	ch := make(chan struct{})

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId,
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
			NamespaceId:              defaultNamespaceId,
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false,
		}).
		DoAndReturn(func(ctx context.Context, in *matchingservice.GetTaskQueueUserDataRequest, opts ...grpc.CallOption) (*matchingservice.GetTaskQueueUserDataResponse, error) {
			<-ch
			return &matchingservice.GetTaskQueueUserDataResponse{
				UserData: data1,
			}, nil
		})

	m := createUserDataManager(t, controller, tqCfg)
	m.config.GetUserDataMinWaitTime = 10 * time.Second // wait on success
	m.config.GetUserDataRetryPolicy = backoff.NewExponentialRetryPolicy(50 * time.Millisecond).
		WithMaximumInterval(50 * time.Millisecond) // faster retry on failure

	m.Start()

	ch <- struct{}{}
	ch <- struct{}{}

	// at this point it should have tried two times and gotten unavailable. it should not be ready yet.
	require.False(t, m.userDataReady.Ready())

	ch <- struct{}{}
	ch <- struct{}{}
	time.Sleep(100 * time.Millisecond) // time to return

	// now it should be ready
	require.NoError(t, m.WaitUntilInitialized(ctx))
	userData, _, err := m.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	m.Stop()
}

func TestUserData_RetriesFetchOnUnImplemented(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	dbq := newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.dbq = dbq

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	ch := make(chan struct{})

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId,
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
			NamespaceId:              defaultNamespaceId,
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false,
		}).
		DoAndReturn(func(ctx context.Context, in *matchingservice.GetTaskQueueUserDataRequest, opts ...grpc.CallOption) (*matchingservice.GetTaskQueueUserDataResponse, error) {
			<-ch
			return &matchingservice.GetTaskQueueUserDataResponse{
				UserData: data1,
			}, nil
		})

	m := createUserDataManager(t, controller, tqCfg)
	m.config.GetUserDataMinWaitTime = 10 * time.Second // wait on success
	m.config.GetUserDataRetryPolicy = backoff.NewExponentialRetryPolicy(50 * time.Millisecond).
		WithMaximumInterval(50 * time.Millisecond) // faster retry on failure

	m.Start()

	ch <- struct{}{}
	ch <- struct{}{}

	// at this point it should have tried once and gotten unimplemented. it should be ready already.
	require.NoError(t, m.WaitUntilInitialized(ctx))

	userData, _, err := m.GetUserData()
	require.Nil(t, userData)
	require.NoError(t, err)

	ch <- struct{}{}
	ch <- struct{}{}
	time.Sleep(100 * time.Millisecond) // time to return

	userData, _, err = m.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	m.Stop()
}

func TestUserData_FetchesUpTree(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	taskQueue := newTestTaskQueue(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	dbq := UnversionedQueueKey(taskQueue.NormalPartition(31))
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.config.ForwarderMaxChildrenPerNode = dynamicconfig.GetIntPropertyFnFilteredByTaskQueue(3)
	tqCfg.dbq = dbq

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId,
			TaskQueue:                taskQueue.NormalPartition(10).RpcName(),
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false,
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			UserData: data1,
		}, nil)

	m := createUserDataManager(t, controller, tqCfg)
	m.config.GetUserDataMinWaitTime = 10 * time.Second // wait on success
	m.Start()
	require.NoError(t, m.WaitUntilInitialized(ctx))
	userData, _, err := m.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	m.Stop()
}

func TestUserData_FetchesActivityToWorkflow(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	// note: activity root
	dbq := newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_ACTIVITY, 0)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.dbq = dbq

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId,
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false,
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			UserData: data1,
		}, nil)

	m := createUserDataManager(t, controller, tqCfg)
	m.config.GetUserDataMinWaitTime = 10 * time.Second // wait on success
	m.Start()
	require.NoError(t, m.WaitUntilInitialized(ctx))
	userData, _, err := m.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	m.Stop()
}

func TestUserData_FetchesStickyToNormal(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()
	tqCfg := defaultTqmTestOpts(controller)

	normalName := "normal-queue"
	stickyName := uuid.New()

	normalTq := newTestTaskQueue(defaultNamespaceId, normalName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	stickyTq := normalTq.StickyPartition(stickyName)
	tqCfg.dbq = UnversionedQueueKey(stickyTq)

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId,
			TaskQueue:                normalName,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false,
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			UserData: data1,
		}, nil)

	m := createUserDataManager(t, controller, tqCfg)
	m.config.GetUserDataMinWaitTime = 10 * time.Second // wait on success
	m.Start()
	require.NoError(t, m.WaitUntilInitialized(ctx))
	userData, _, err := m.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	m.Stop()
}

func TestUserData_UpdateOnNonRootFails(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	defer controller.Finish()
	ctx := context.Background()

	subTqId := newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.dbq = subTqId
	subTq := createUserDataManager(t, controller, tqCfg)
	err := subTq.UpdateUserData(ctx, UserDataUpdateOptions{}, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
		return data, false, nil
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errUserDataNoMutateNonRoot)

	actTqId := newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_ACTIVITY, 0)
	actTqCfg := defaultTqmTestOpts(controller)
	actTqCfg.dbq = actTqId
	actTq := createUserDataManager(t, controller, actTqCfg)
	err = actTq.UpdateUserData(ctx, UserDataUpdateOptions{}, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
		return data, false, nil
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errUserDataNoMutateNonRoot)
}

func newTestUnversionedPhysicalQueueKey(namespaceId string, name string, taskType enumspb.TaskQueueType, partition int) *PhysicalTaskQueueKey {
	return UnversionedQueueKey(newTestTaskQueue(namespaceId, name, taskType).NormalPartition(partition))
}
