package matching

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/util"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type tqmTestOpts struct {
	config              *Config
	dbq                 *PhysicalTaskQueueKey
	matchingClientMock  *matchingservicemock.MockMatchingServiceClient
	expectUserDataError bool
}

func createUserDataManager(
	t *testing.T,
	controller *gomock.Controller,
	testOpts *tqmTestOpts,
) (m *userDataManagerImpl) {
	t.Helper()

	logger := log.NewTestLogger()
	ns := namespace.Name("ns-name")
	tm := newTestTaskManager(logger)
	mockNamespaceCache := namespace.NewMockRegistry(controller)
	mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()
	mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(ns, nil).AnyTimes()

	var onFatalErr func(unloadCause)
	if testOpts.expectUserDataError {
		var fatalErrCalled atomic.Bool
		onFatalErr = func(unloadCause) {
			fatalErrCalled.Store(true)
			m.Stop() // this would normally go through the engine+pm but just skip to Stop in test
		}
		t.Cleanup(func() {
			if !fatalErrCalled.Load() {
				t.Fatal("user data manager did not call onFatalErr")
			}
		})
	} else {
		onFatalErr = func(unloadCause) { t.Fatal("user data manager called onFatalErr") }
	}

	return newUserDataManager(tm, testOpts.matchingClientMock, onFatalErr, nil, testOpts.dbq.Partition(), newTaskQueueConfig(testOpts.dbq.Partition().TaskQueue(), testOpts.config, ns), logger, mockNamespaceCache)
}

func TestUserData_LoadOnInit(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
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
			Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
				defaultRootTqID: &persistence.SingleTaskQueueUserDataUpdate{
					UserData: data1,
				},
			},
		}))
	data1.Version++

	m.Start()
	require.NoError(t, m.WaitUntilInitialized(ctx))
	userData, _, err := m.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	m.Stop()
	m.goroGroup.Wait() // ensure gomock doesn't complain about calls after the test returns
}

func TestUserData_LoadOnInit_Refresh(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	ctx := context.Background()
	dbq := newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.dbq = dbq
	tqCfg.expectUserDataError = false

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	m := createUserDataManager(t, controller, tqCfg)
	m.config.GetUserDataInitialRefresh = time.Millisecond
	m.config.GetUserDataRefresh = dynamicconfig.GetDurationPropertyFn(time.Millisecond)
	tm := m.store.(*testTaskManager)

	require.NoError(t, m.store.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: defaultNamespaceId,
			Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
				defaultRootTqID: &persistence.SingleTaskQueueUserDataUpdate{
					UserData: data1,
				},
			},
		}))
	data1.Version++

	m.Start()

	require.NoError(t, m.WaitUntilInitialized(ctx))

	// should be refreshing quickly
	require.Eventually(t, func() bool {
		return tm.getGetUserDataCount(dbq) >= 5
	}, time.Second, time.Millisecond)

	// should still have version 2
	userData, _, err := m.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)

	// pretend someone else managed to update data
	data2 := &persistencespb.VersionedTaskQueueUserData{
		Version: 2,
		Data:    mkUserData(2),
	}
	require.NoError(t, m.store.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: defaultNamespaceId,
			Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
				defaultRootTqID: &persistence.SingleTaskQueueUserDataUpdate{
					UserData: data2,
				},
			},
		}))
	data2.Version++

	// eventually it will notice and reload the data
	require.Eventually(t, func() bool {
		userData, _, err := m.GetUserData()
		return err == nil && userData.Version == data2.Version
	}, time.Second, time.Millisecond)

	m.Stop()
	m.goroGroup.Wait() // ensure gomock doesn't complain about calls after the test returns
}

func TestUserData_LoadOnInit_Refresh_Backwards(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	ctx := context.Background()
	dbq := newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 0)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.dbq = dbq
	tqCfg.expectUserDataError = true

	data5 := &persistencespb.VersionedTaskQueueUserData{
		Version: 5,
		Data:    mkUserData(5),
	}

	m := createUserDataManager(t, controller, tqCfg)
	m.config.GetUserDataInitialRefresh = time.Millisecond
	m.config.GetUserDataRefresh = dynamicconfig.GetDurationPropertyFn(time.Millisecond)
	tm := m.store.(*testTaskManager)

	require.NoError(t, m.store.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: defaultNamespaceId,
			Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
				defaultRootTqID: &persistence.SingleTaskQueueUserDataUpdate{
					UserData: data5,
				},
			},
		}))
	data5.Version++

	m.Start()

	require.NoError(t, m.WaitUntilInitialized(ctx))

	// should be refreshing quickly
	require.Eventually(t, func() bool {
		return tm.getGetUserDataCount(dbq) >= 5
	}, time.Second, time.Millisecond)

	// should still have version 6
	userData, _, err := m.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data5, userData)

	// data in db has older version
	data4 := &persistencespb.VersionedTaskQueueUserData{
		Version: 4,
		Data:    mkUserData(4),
	}
	require.NoError(t, m.store.UpdateTaskQueueUserData(context.Background(),
		&persistence.UpdateTaskQueueUserDataRequest{
			NamespaceID: defaultNamespaceId,
			Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
				defaultRootTqID: &persistence.SingleTaskQueueUserDataUpdate{
					UserData: data4,
				},
			},
		}))
	data4.Version++

	// it should unload the task queue
	require.Eventually(t, func() bool {
		_, _, err := m.GetUserData()
		return errors.Is(err, errTaskQueueClosed)
	}, time.Second, time.Millisecond)
}

func TestUserData_LoadOnInit_OnlyOnceWhenNoData(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
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
	m.goroGroup.Wait() // ensure gomock doesn't complain about calls after the test returns
}

func TestUserData_FetchesOnInit(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
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
			UserData: data1,
		}, nil).MaxTimes(maxFastUserDataFetches + 1)

	m := createUserDataManager(t, controller, tqCfg)
	m.config.GetUserDataMinWaitTime = 10 * time.Second // only one fetch

	m.Start()
	require.NoError(t, m.WaitUntilInitialized(ctx))
	userData, _, err := m.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	m.Stop()
	m.goroGroup.Wait() // ensure gomock doesn't complain about calls after the test returns
}

func TestUserData_FetchesAndFetchesAgain(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
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
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_ACTIVITY,
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
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_ACTIVITY,
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
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_ACTIVITY,
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
	m.goroGroup.Wait() // ensure gomock doesn't complain about calls after the test returns
}

func TestUserData_RetriesFetchOnUnavailable(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
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

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId,
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 1,
			WaitNewData:              true, // after first successful poll, there would be long polls
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			UserData: data1,
		}, nil).
		// +3 because the counter resets when version changes so the calls with error do not count
		MaxTimes(maxFastUserDataFetches + 3)

	m := createUserDataManager(t, controller, tqCfg)
	m.config.GetUserDataMinWaitTime = 10 * time.Second // wait on success
	m.config.GetUserDataRetryPolicy = backoff.NewExponentialRetryPolicy(50 * time.Millisecond).
		WithMaximumInterval(50 * time.Millisecond).WithExpirationInterval(backoff.NoInterval) // faster retry on failure

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
	m.goroGroup.Wait() // ensure gomock doesn't complain about calls after the test returns
}

func TestUserData_RetriesFetchOnUnImplemented(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
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

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId,
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 1,
			WaitNewData:              true, // after first successful poll, there would be long polls
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			UserData: data1,
		}, nil).
		// +3 because the counter resets when version changes so the calls with error do not count
		MaxTimes(maxFastUserDataFetches + 3)

	m := createUserDataManager(t, controller, tqCfg)
	m.config.GetUserDataMinWaitTime = 10 * time.Second // wait on success
	m.config.GetUserDataRetryPolicy = backoff.NewExponentialRetryPolicy(50 * time.Millisecond).
		WithMaximumInterval(50 * time.Millisecond).WithExpirationInterval(backoff.NoInterval) // faster retry on failure

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
	m.goroGroup.Wait() // ensure gomock doesn't complain about calls after the test returns
}

func TestUserData_FetchesUpTree(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
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

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId,
			TaskQueue:                taskQueue.NormalPartition(10).RpcName(),
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 1,
			WaitNewData:              true, // after first successful poll, there would be long polls
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			UserData: data1,
		}, nil).MaxTimes(maxFastUserDataFetches + 1)

	m := createUserDataManager(t, controller, tqCfg)
	m.config.GetUserDataMinWaitTime = 10 * time.Second // wait on success
	m.Start()
	require.NoError(t, m.WaitUntilInitialized(ctx))
	userData, _, err := m.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	m.Stop()
	m.goroGroup.Wait() // ensure gomock doesn't complain about calls after the test returns
}

func TestUserData_FetchesActivityToWorkflow(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
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

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId,
			TaskQueue:                defaultRootTqID,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 1,
			WaitNewData:              true, // after first successful poll, there would be long polls
		}).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			UserData: data1,
		}, nil).MaxTimes(maxFastUserDataFetches + 1)

	m := createUserDataManager(t, controller, tqCfg)
	m.config.GetUserDataMinWaitTime = 10 * time.Second // wait on success
	m.Start()
	require.NoError(t, m.WaitUntilInitialized(ctx))
	userData, _, err := m.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	m.Stop()
	m.goroGroup.Wait() // ensure gomock doesn't complain about calls after the test returns
}

func TestUserData_FetchesStickyToNormal(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	ctx := context.Background()
	tqCfg := defaultTqmTestOpts(controller)

	normalName := "normal-queue"
	stickyName := uuid.NewString()

	normalTq := newTestTaskQueue(defaultNamespaceId, normalName, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	stickyTq := normalTq.StickyPartition(stickyName)
	tqCfg.dbq = UnversionedQueueKey(stickyTq)

	data1 := &persistencespb.VersionedTaskQueueUserData{
		Version: 1,
		Data:    mkUserData(1),
	}

	matchGetTaskQueueUserDataRequest := func(expectedReq *matchingservice.GetTaskQueueUserDataRequest, saveTq func(any)) gomock.Matcher {
		expectedReq = common.CloneProto(expectedReq)
		expectedTQ := expectedReq.TaskQueue
		expectedReq.TaskQueue = ""
		return gomock.Cond(func(req *matchingservice.GetTaskQueueUserDataRequest) bool {
			saveTq(req.TaskQueue)
			// must be some partition of expected name, just use substring match
			if !strings.Contains(req.TaskQueue, expectedTQ) {
				return false
			}
			// check the rest matches
			req = common.CloneProto(req)
			req.TaskQueue = ""
			return proto.Equal(req, expectedReq)
		})
	}

	var firstPartition, secondPartition atomic.Value
	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		matchGetTaskQueueUserDataRequest(&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId,
			TaskQueue:                normalName,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 0,
			WaitNewData:              false,
		}, firstPartition.Store)).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			UserData: data1,
		}, nil)

	tqCfg.matchingClientMock.EXPECT().GetTaskQueueUserData(
		gomock.Any(),
		matchGetTaskQueueUserDataRequest(&matchingservice.GetTaskQueueUserDataRequest{
			NamespaceId:              defaultNamespaceId,
			TaskQueue:                normalName,
			TaskQueueType:            enumspb.TASK_QUEUE_TYPE_WORKFLOW,
			LastKnownUserDataVersion: 1,
			WaitNewData:              true, // after first successful poll, there would be long polls
		}, secondPartition.Store)).
		Return(&matchingservice.GetTaskQueueUserDataResponse{
			UserData: data1,
		}, nil).MaxTimes(maxFastUserDataFetches + 1)

	m := createUserDataManager(t, controller, tqCfg)
	m.config.GetUserDataMinWaitTime = 10 * time.Second // wait on success
	m.Start()
	require.NoError(t, m.WaitUntilInitialized(ctx))
	userData, _, err := m.GetUserData()
	require.NoError(t, err)
	require.Equal(t, data1, userData)
	require.Eventually(t, func() bool { return firstPartition.Load() == secondPartition.Load() }, 5*time.Second, time.Millisecond)
	m.Stop()
	m.goroGroup.Wait() // ensure gomock doesn't complain about calls after the test returns
}

func TestUserData_UpdateOnNonRootFails(t *testing.T) {
	t.Parallel()

	controller := gomock.NewController(t)
	ctx := context.Background()

	subTqId := newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
	tqCfg := defaultTqmTestOpts(controller)
	tqCfg.dbq = subTqId
	subTq := createUserDataManager(t, controller, tqCfg)
	_, err := subTq.UpdateUserData(ctx, UserDataUpdateOptions{}, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
		return data, false, nil
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errUserDataNoMutateNonRoot)

	actTqId := newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_ACTIVITY, 0)
	actTqCfg := defaultTqmTestOpts(controller)
	actTqCfg.dbq = actTqId
	actTq := createUserDataManager(t, controller, actTqCfg)
	_, err = actTq.UpdateUserData(ctx, UserDataUpdateOptions{}, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
		return data, false, nil
	})
	require.Error(t, err)
	require.ErrorIs(t, err, errUserDataNoMutateNonRoot)
}

func newTestUnversionedPhysicalQueueKey(namespaceId string, name string, taskType enumspb.TaskQueueType, partition int) *PhysicalTaskQueueKey {
	return UnversionedQueueKey(newTestTaskQueue(namespaceId, name, taskType).NormalPartition(partition))
}

func TestUserData_Propagation(t *testing.T) {
	t.Parallel()

	const N = 7

	ctx := context.Background()
	controller := gomock.NewController(t)
	opts := defaultTqmTestOpts(controller)

	keys := make([]*PhysicalTaskQueueKey, N)
	for i := range keys {
		keys[i] = newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, i)
	}

	managers := make([]*userDataManagerImpl, N)
	var tm *testTaskManager
	for i := range managers {
		optsi := *opts // share config and mock client
		optsi.dbq = keys[i]
		managers[i] = createUserDataManager(t, controller, &optsi)
		if i == 0 {
			// only the root uses persistence
			tm = managers[0].store.(*testTaskManager)
		}
		// use two levels
		managers[i].config.ForwarderMaxChildrenPerNode = dynamicconfig.GetIntPropertyFn(3)
		// override timeouts to run much faster
		managers[i].config.GetUserDataLongPollTimeout = dynamicconfig.GetDurationPropertyFn(100 * time.Millisecond)
		managers[i].config.GetUserDataMinWaitTime = 10 * time.Millisecond
		managers[i].config.GetUserDataReturnBudget = 10 * time.Millisecond
		managers[i].config.GetUserDataRetryPolicy = backoff.NewExponentialRetryPolicy(100 * time.Millisecond).WithMaximumInterval(1 * time.Second).WithExpirationInterval(backoff.NoInterval)
		managers[i].logger = log.With(managers[i].logger, tag.HostID(fmt.Sprintf("%d", i)))
	}

	// hook up "rpcs"
	opts.matchingClientMock.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *matchingservice.GetTaskQueueUserDataRequest, opts ...grpc.CallOption) (*matchingservice.GetTaskQueueUserDataResponse, error) {
			// inject failures
			if rand.Float64() < 0.1 {
				return nil, serviceerror.NewUnavailable("timeout")
			}
			p, err := tqid.NormalPartitionFromRpcName(req.TaskQueue, req.NamespaceId, req.TaskQueueType)
			require.NoError(t, err)
			require.Equal(t, enumspb.TASK_QUEUE_TYPE_WORKFLOW, p.TaskType())
			res, err := managers[p.PartitionId()].HandleGetUserDataRequest(ctx, req)
			return res, err
		},
	).AnyTimes()
	opts.matchingClientMock.EXPECT().UpdateTaskQueueUserData(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *matchingservice.UpdateTaskQueueUserDataRequest, opts ...grpc.CallOption) (*matchingservice.UpdateTaskQueueUserDataResponse, error) {
			err := tm.UpdateTaskQueueUserData(ctx, &persistence.UpdateTaskQueueUserDataRequest{
				NamespaceID: req.NamespaceId,
				Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
					req.TaskQueue: &persistence.SingleTaskQueueUserDataUpdate{
						UserData:        req.UserData,
						BuildIdsAdded:   req.BuildIdsAdded,
						BuildIdsRemoved: req.BuildIdsRemoved,
					},
				},
			})
			return &matchingservice.UpdateTaskQueueUserDataResponse{}, err
		},
	).AnyTimes()

	defer time.Sleep(50 * time.Millisecond) // extra buffer to let goroutines exit after manager.Stop()
	for i := range managers {
		managers[i].Start()
		defer managers[i].Stop()
	}

	const iters = 5
	for iter := 0; iter < iters; iter++ {
		newVersion, err := managers[0].UpdateUserData(ctx, UserDataUpdateOptions{}, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
			return data, false, nil
		})
		require.NoError(t, err)
		require.Equal(t, int64(iter+1), newVersion)
		start := time.Now()
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			for i := 1; i < N; i++ {
				d, _, err := managers[i].GetUserData()
				require.NoError(c, err, "number", i)
				require.Equal(c, iter+1, int(d.GetVersion()), "number", i)
			}
		}, 5*time.Second, 10*time.Millisecond, "failed to propagate")
		t.Log("Propagation time:", time.Since(start))
	}
}

func TestUserData_CheckPropagation(t *testing.T) {
	t.Parallel()

	const N = 7

	ctx := context.Background()
	controller := gomock.NewController(t)
	opts := defaultTqmTestOpts(controller)

	keys := make([]*PhysicalTaskQueueKey, N)
	for i := range keys {
		keys[i] = newTestUnversionedPhysicalQueueKey(defaultNamespaceId, defaultRootTqID, enumspb.TASK_QUEUE_TYPE_WORKFLOW, i)
	}

	managers := make([]*userDataManagerImpl, N)
	var tm *testTaskManager
	for i := range managers {
		optsi := *opts // share config and mock client
		optsi.dbq = keys[i]
		managers[i] = createUserDataManager(t, controller, &optsi)
		if i == 0 {
			// only the root uses persistence
			tm = managers[0].store.(*testTaskManager)
		}
		// use two levels
		managers[i].config.ForwarderMaxChildrenPerNode = dynamicconfig.GetIntPropertyFn(3)
		// override timeouts to run faster
		managers[i].config.GetUserDataMinWaitTime = 5 * time.Millisecond
		managers[i].config.GetUserDataRetryPolicy = backoff.NewConstantDelayRetryPolicy(5 * time.Millisecond)
		managers[i].logger = log.With(managers[i].logger, tag.HostID(fmt.Sprintf("%d", i)))
	}

	type checkKey struct{}
	ctxFromCheck := context.WithValue(ctx, checkKey{}, true)

	var failing atomic.Bool
	failing.Store(true)

	// hook up "rpcs"
	opts.matchingClientMock.EXPECT().GetTaskQueueUserData(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *matchingservice.GetTaskQueueUserDataRequest, opts ...grpc.CallOption) (*matchingservice.GetTaskQueueUserDataResponse, error) {
			// inject failures for propagation rpcs but not check rpcs
			if ctx.Value(checkKey{}) == nil && failing.Load() {
				util.InterruptibleSleep(ctx, 10*time.Millisecond)
				return nil, serviceerror.NewUnavailable("timeout")
			}
			p, err := tqid.NormalPartitionFromRpcName(req.TaskQueue, req.NamespaceId, req.TaskQueueType)
			require.NoError(t, err)
			require.Equal(t, enumspb.TASK_QUEUE_TYPE_WORKFLOW, p.TaskType())
			res, err := managers[p.PartitionId()].HandleGetUserDataRequest(ctx, req)
			return res, err
		},
	).AnyTimes()
	opts.matchingClientMock.EXPECT().UpdateTaskQueueUserData(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *matchingservice.UpdateTaskQueueUserDataRequest, opts ...grpc.CallOption) (*matchingservice.UpdateTaskQueueUserDataResponse, error) {
			err := tm.UpdateTaskQueueUserData(ctx, &persistence.UpdateTaskQueueUserDataRequest{
				NamespaceID: req.NamespaceId,
				Updates: map[string]*persistence.SingleTaskQueueUserDataUpdate{
					req.TaskQueue: &persistence.SingleTaskQueueUserDataUpdate{
						UserData:        req.UserData,
						BuildIdsAdded:   req.BuildIdsAdded,
						BuildIdsRemoved: req.BuildIdsRemoved,
					},
				},
			})
			return &matchingservice.UpdateTaskQueueUserDataResponse{}, err
		},
	).AnyTimes()

	defer time.Sleep(50 * time.Millisecond) //nolint:forbidigo // extra buffer to let goroutines exit after manager.Stop()
	for i := range managers {
		managers[i].Start()
		defer managers[i].Stop() //nolint:revive
	}

	newVersion, err := managers[0].UpdateUserData(ctx, UserDataUpdateOptions{}, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
		return data, false, nil
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), newVersion)

	checkReturned := make(chan error)
	go func() {
		// only check workflow, not activity partitions
		checkReturned <- managers[0].CheckTaskQueueUserDataPropagation(ctxFromCheck, newVersion, N, 0)
	}()

	// CheckTaskQueueUserDataPropagation should not return within 100ms
	time.Sleep(100 * time.Millisecond) //nolint:forbidigo // need to sleep for negative check
	select {
	case <-checkReturned:
		t.Fatal("check should not have returned")
	default:
	}

	// unblock propagation
	failing.Store(false)

	// CheckTaskQueueUserDataPropagation should return soon
	require.Eventually(t, func() bool {
		select {
		case err := <-checkReturned:
			return err == nil
		default:
			return false
		}
	}, 100*time.Millisecond, 5*time.Millisecond, "CheckTaskQueueUserDataPropagation did not return fast enough")
}

func defaultTqmTestOpts(controller *gomock.Controller) *tqmTestOpts {
	return &tqmTestOpts{
		config:             defaultTestConfig(),
		dbq:                defaultTqId(),
		matchingClientMock: matchingservicemock.NewMockMatchingServiceClient(controller),
	}
}
