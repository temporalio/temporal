package nexus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protoassert"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testMocks struct {
	config         *EndpointRegistryConfig
	matchingClient *matchingservicemock.MockMatchingServiceClient
	persistence    *persistence.MockNexusEndpointManager
}

func TestGet(t *testing.T) {
	t.Parallel()

	testEntry := newEndpointEntry(t.Name())
	mocks := newTestMocks(t)

	// initial load
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), gomock.Any()).Return(&matchingservice.ListNexusEndpointsResponse{
		Entries:       []*persistencespb.NexusEndpointEntry{testEntry},
		TableVersion:  1,
		NextPageToken: nil,
	}, nil)

	// first long poll
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), &matchingservice.ListNexusEndpointsRequest{
		PageSize:              int32(100),
		LastKnownTableVersion: int64(1),
		Wait:                  true,
	}).DoAndReturn(func(context.Context, *matchingservice.ListNexusEndpointsRequest, ...interface{}) (*matchingservice.ListNexusEndpointsResponse, error) {
		time.Sleep(20 * time.Millisecond)
		return &matchingservice.ListNexusEndpointsResponse{TableVersion: int64(1)}, nil
	}).AnyTimes()

	reg := NewEndpointRegistry(mocks.config, mocks.matchingClient, mocks.persistence, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	reg.StartLifecycle()
	defer reg.StopLifecycle()

	endpoint, err := reg.GetByID(context.Background(), testEntry.Id)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, testEntry, endpoint)

	endpoint, err = reg.GetByName(context.Background(), "ignored", testEntry.Endpoint.Spec.Name)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, testEntry, endpoint)

	reg.dataLock.RLock()
	defer reg.dataLock.RUnlock()
	assert.Equal(t, int64(1), reg.tableVersion)
}

func TestGetNotFound(t *testing.T) {
	t.Parallel()

	testEntry := newEndpointEntry(t.Name())
	mocks := newTestMocks(t)

	// initial load
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), gomock.Any()).Return(&matchingservice.ListNexusEndpointsResponse{
		Entries:       []*persistencespb.NexusEndpointEntry{},
		TableVersion:  1,
		NextPageToken: nil,
	}, nil)

	// first long poll
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), &matchingservice.ListNexusEndpointsRequest{
		PageSize:              int32(100),
		LastKnownTableVersion: int64(1),
		Wait:                  true,
	}).DoAndReturn(func(context.Context, *matchingservice.ListNexusEndpointsRequest, ...interface{}) (*matchingservice.ListNexusEndpointsResponse, error) {
		time.Sleep(20 * time.Millisecond)
		return &matchingservice.ListNexusEndpointsResponse{TableVersion: int64(1)}, nil
	}).AnyTimes()

	// readthrough
	mocks.persistence.EXPECT().GetNexusEndpoint(gomock.Any(), &persistence.GetNexusEndpointRequest{ID: testEntry.Id}).Return(testEntry, nil)
	sentinelErr := errors.New("sentinel")
	mocks.persistence.EXPECT().GetNexusEndpoint(gomock.Any(), gomock.Any()).Return(nil, sentinelErr)

	reg := NewEndpointRegistry(mocks.config, mocks.matchingClient, mocks.persistence, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	reg.StartLifecycle()
	defer reg.StopLifecycle()

	var notFound *serviceerror.NotFound

	// Readthrough success
	endpoint, err := reg.GetByID(context.Background(), testEntry.Id)
	assert.NoError(t, err)
	assert.Equal(t, testEntry, endpoint)

	// Readthrough is cached (mock will verify only one call)
	endpoint, err = reg.GetByID(context.Background(), testEntry.Id)
	assert.NoError(t, err)
	assert.Equal(t, testEntry, endpoint)

	// Readthrough fail
	endpoint, err = reg.GetByID(context.Background(), uuid.NewString())
	assert.Equal(t, sentinelErr, err)
	assert.Nil(t, endpoint)

	endpoint, err = reg.GetByName(context.Background(), "ignored", uuid.NewString())
	assert.ErrorAs(t, err, &notFound)
	assert.Nil(t, endpoint)

	reg.dataLock.RLock()
	defer reg.dataLock.RUnlock()
	assert.Equal(t, int64(1), reg.tableVersion)
}

func TestInitializationFallback(t *testing.T) {
	t.Parallel()

	testEndpoint := newEndpointEntry(t.Name())
	mocks := newTestMocks(t)

	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("matching unavailable test error")).MinTimes(1)
	mocks.persistence.EXPECT().ListNexusEndpoints(gomock.Any(), gomock.Any()).Return(&persistence.ListNexusEndpointsResponse{
		TableVersion:  int64(1),
		NextPageToken: nil,
		Entries:       []*persistencespb.NexusEndpointEntry{testEndpoint},
	}, nil)

	reg := NewEndpointRegistry(mocks.config, mocks.matchingClient, mocks.persistence, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	reg.StartLifecycle()
	defer reg.StopLifecycle()

	endpoint, err := reg.GetByID(context.Background(), testEndpoint.Id)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, testEndpoint, endpoint)

	reg.dataLock.RLock()
	defer reg.dataLock.RUnlock()
	assert.Equal(t, int64(1), reg.tableVersion)
}

func TestEnableDisableEnable(t *testing.T) {
	t.Parallel()

	testEntry := newEndpointEntry(t.Name())
	mocks := newTestMocks(t)

	mocks.config.refreshMinWait = dynamicconfig.GetDurationPropertyFn(time.Millisecond)
	var callback func(bool) // capture callback to call later
	mocks.config.refreshEnabled = func(cb func(bool)) (bool, func()) {
		callback = cb
		return false, func() {}
	}

	// start disabled
	reg := NewEndpointRegistry(mocks.config, mocks.matchingClient, mocks.persistence, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	reg.StartLifecycle()
	defer reg.StopLifecycle()

	// check waitUntilInitialized
	quickCtx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, reg.waitUntilInitialized(quickCtx), ErrNexusDisabled)

	// mocks for initial load
	inLongPoll := make(chan struct{})
	closeOnce := sync.OnceFunc(func() { close(inLongPoll) })
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), gomock.Any()).Return(&matchingservice.ListNexusEndpointsResponse{
		Entries:       []*persistencespb.NexusEndpointEntry{testEntry},
		TableVersion:  1,
		NextPageToken: nil,
	}, nil)
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), &matchingservice.ListNexusEndpointsRequest{
		PageSize:              int32(100),
		LastKnownTableVersion: int64(1),
		Wait:                  true,
	}).DoAndReturn(func(context.Context, *matchingservice.ListNexusEndpointsRequest, ...interface{}) (*matchingservice.ListNexusEndpointsResponse, error) {
		closeOnce()
		time.Sleep(100 * time.Millisecond)
		return &matchingservice.ListNexusEndpointsResponse{TableVersion: int64(1)}, nil
	})

	// enable
	callback(true)
	<-inLongPoll

	// check waitUntilInitialized
	quickCtx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	require.NoError(t, reg.waitUntilInitialized(quickCtx))

	// now disable
	callback(false)

	quickCtx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	require.ErrorIs(t, reg.waitUntilInitialized(quickCtx), ErrNexusDisabled)

	// enable again, should not crash

	inLongPoll = make(chan struct{})
	closeOnce = sync.OnceFunc(func() { close(inLongPoll) })
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), gomock.Any()).Return(&matchingservice.ListNexusEndpointsResponse{
		Entries:       []*persistencespb.NexusEndpointEntry{testEntry},
		TableVersion:  1,
		NextPageToken: nil,
	}, nil)
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), &matchingservice.ListNexusEndpointsRequest{
		PageSize:              int32(100),
		LastKnownTableVersion: int64(1),
		Wait:                  true,
	}).DoAndReturn(func(context.Context, *matchingservice.ListNexusEndpointsRequest, ...interface{}) (*matchingservice.ListNexusEndpointsResponse, error) {
		closeOnce()
		time.Sleep(100 * time.Millisecond)
		return &matchingservice.ListNexusEndpointsResponse{TableVersion: int64(1)}, nil
	})
	callback(true)
	<-inLongPoll
}

func TestTableVersionErrorResetsMatchingPagination(t *testing.T) {
	t.Parallel()

	testEntry0 := newEndpointEntry(t.Name() + "-0")
	testEntry1 := newEndpointEntry(t.Name() + "-1")

	mocks := newTestMocks(t)
	mocks.config.refreshPageSize = dynamicconfig.GetIntPropertyFn(1)

	// endpoint data initialization mocks
	// successfully get first page
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), &matchingservice.ListNexusEndpointsRequest{
		NextPageToken:         nil,
		PageSize:              int32(1),
		LastKnownTableVersion: int64(0),
		Wait:                  false,
	}).Return(&matchingservice.ListNexusEndpointsResponse{
		Entries:       []*persistencespb.NexusEndpointEntry{testEntry0},
		TableVersion:  int64(2),
		NextPageToken: []byte(testEntry0.Id),
	}, nil)
	// persistence.ErrNexusTableVersionConflict error on second page
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), &matchingservice.ListNexusEndpointsRequest{
		NextPageToken:         []byte(testEntry0.Id),
		PageSize:              int32(1),
		LastKnownTableVersion: int64(2),
		Wait:                  false,
	}).Return(nil, serviceerror.NewFailedPrecondition(persistence.ErrNexusTableVersionConflict.Error()))
	// request first page again
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), &matchingservice.ListNexusEndpointsRequest{
		NextPageToken:         nil,
		PageSize:              int32(1),
		LastKnownTableVersion: int64(0),
		Wait:                  false,
	}).Return(&matchingservice.ListNexusEndpointsResponse{
		Entries:       []*persistencespb.NexusEndpointEntry{testEntry0},
		TableVersion:  int64(3),
		NextPageToken: []byte(testEntry0.Id),
	}, nil)
	// successfully get second page
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), &matchingservice.ListNexusEndpointsRequest{
		NextPageToken:         []byte(testEntry0.Id),
		PageSize:              int32(1),
		LastKnownTableVersion: int64(3),
		Wait:                  false,
	}).Return(&matchingservice.ListNexusEndpointsResponse{
		Entries:       []*persistencespb.NexusEndpointEntry{testEntry1},
		TableVersion:  int64(3),
		NextPageToken: nil,
	}, nil)

	// mock first long poll
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), &matchingservice.ListNexusEndpointsRequest{
		PageSize:              int32(1),
		LastKnownTableVersion: int64(3),
		Wait:                  true,
	}).DoAndReturn(func(context.Context, *matchingservice.ListNexusEndpointsRequest, ...interface{}) (*matchingservice.ListNexusEndpointsResponse, error) {
		time.Sleep(20 * time.Millisecond)
		return &matchingservice.ListNexusEndpointsResponse{TableVersion: int64(1)}, nil
	}).MaxTimes(1)

	reg := NewEndpointRegistry(mocks.config, mocks.matchingClient, mocks.persistence, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	reg.StartLifecycle()
	defer reg.StopLifecycle()

	entry, err := reg.GetByID(context.Background(), testEntry0.Id)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, testEntry0, entry)

	entry, err = reg.GetByID(context.Background(), testEntry1.Id)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, testEntry1, entry)

	reg.dataLock.RLock()
	defer reg.dataLock.RUnlock()
	assert.Equal(t, int64(3), reg.tableVersion)
}

func TestTableVersionErrorResetsPersistencePagination(t *testing.T) {
	t.Parallel()

	testEntry0 := newEndpointEntry(t.Name() + "-0")
	testEntry1 := newEndpointEntry(t.Name() + "-1")

	mocks := newTestMocks(t)
	mocks.config.refreshPageSize = dynamicconfig.GetIntPropertyFn(1)

	// mock unavailable matching service
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("matching unavailable test error")).MinTimes(1)

	// fallback endpoint data initialization mocks
	// successfully get first page
	mocks.persistence.EXPECT().ListNexusEndpoints(gomock.Any(), &persistence.ListNexusEndpointsRequest{
		NextPageToken:         nil,
		PageSize:              1,
		LastKnownTableVersion: int64(0),
	}).Return(&persistence.ListNexusEndpointsResponse{
		Entries:       []*persistencespb.NexusEndpointEntry{testEntry0},
		TableVersion:  int64(2),
		NextPageToken: []byte(testEntry0.Id),
	}, nil)
	// persistence.ErrNexusTableVersionConflict error on second page
	mocks.persistence.EXPECT().ListNexusEndpoints(gomock.Any(), &persistence.ListNexusEndpointsRequest{
		NextPageToken:         []byte(testEntry0.Id),
		PageSize:              1,
		LastKnownTableVersion: int64(2),
	}).Return(&persistence.ListNexusEndpointsResponse{TableVersion: int64(3)}, persistence.ErrNexusTableVersionConflict)
	// request first page again
	mocks.persistence.EXPECT().ListNexusEndpoints(gomock.Any(), &persistence.ListNexusEndpointsRequest{
		NextPageToken:         nil,
		PageSize:              1,
		LastKnownTableVersion: int64(0),
	}).Return(&persistence.ListNexusEndpointsResponse{
		Entries:       []*persistencespb.NexusEndpointEntry{testEntry0},
		TableVersion:  int64(3),
		NextPageToken: []byte(testEntry0.Id),
	}, nil)
	// successfully get second page
	mocks.persistence.EXPECT().ListNexusEndpoints(gomock.Any(), &persistence.ListNexusEndpointsRequest{
		NextPageToken:         []byte(testEntry0.Id),
		PageSize:              1,
		LastKnownTableVersion: int64(3),
	}).Return(&persistence.ListNexusEndpointsResponse{
		Entries:       []*persistencespb.NexusEndpointEntry{testEntry1},
		TableVersion:  int64(3),
		NextPageToken: nil,
	}, nil)

	reg := NewEndpointRegistry(mocks.config, mocks.matchingClient, mocks.persistence, log.NewNoopLogger(), metrics.NoopMetricsHandler)
	reg.StartLifecycle()
	defer reg.StopLifecycle()

	entry, err := reg.GetByID(context.Background(), testEntry0.Id)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, testEntry0, entry)

	entry, err = reg.GetByID(context.Background(), testEntry1.Id)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, testEntry1, entry)

	reg.dataLock.RLock()
	defer reg.dataLock.RUnlock()
	assert.Equal(t, int64(3), reg.tableVersion)
}

func newTestMocks(t *testing.T) *testMocks {
	ctrl := gomock.NewController(t)
	testConfig := NewEndpointRegistryConfig(dynamicconfig.NewNoopCollection())
	testConfig.refreshEnabled = func(func(bool)) (bool, func()) {
		return true, func() {}
	}
	return &testMocks{
		config:         testConfig,
		matchingClient: matchingservicemock.NewMockMatchingServiceClient(ctrl),
		persistence:    persistence.NewMockNexusEndpointManager(ctrl),
	}
}

func newEndpointEntry(name string) *persistencespb.NexusEndpointEntry {
	id := uuid.NewString()
	return &persistencespb.NexusEndpointEntry{
		Version: 1,
		Id:      id,
		Endpoint: &persistencespb.NexusEndpoint{
			Clock:       hybrid_logical_clock.Zero(1),
			CreatedTime: timestamppb.Now(),
			Spec: &persistencespb.NexusEndpointSpec{
				Name: name,
				Target: &persistencespb.NexusEndpointTarget{
					Variant: &persistencespb.NexusEndpointTarget_Worker_{
						Worker: &persistencespb.NexusEndpointTarget_Worker{
							NamespaceId: uuid.NewString(),
							TaskQueue:   name + "-task-queue",
						},
					},
				},
			},
		},
	}
}

func publicToInternalEndpointTarget(target *nexuspb.EndpointTarget) *persistencespb.NexusEndpointTarget {
	switch v := target.Variant.(type) {
	case *nexuspb.EndpointTarget_Worker_:
		return &persistencespb.NexusEndpointTarget{
			Variant: &persistencespb.NexusEndpointTarget_Worker_{
				Worker: &persistencespb.NexusEndpointTarget_Worker{
					NamespaceId: "TODO",
					TaskQueue:   v.Worker.TaskQueue,
				},
			},
		}
	case *nexuspb.EndpointTarget_External_:
		return &persistencespb.NexusEndpointTarget{
			Variant: &persistencespb.NexusEndpointTarget_External_{
				External: &persistencespb.NexusEndpointTarget_External{
					Url: v.External.Url,
				},
			},
		}
	}
	panic(fmt.Errorf("invalid target: %v", target))
}
