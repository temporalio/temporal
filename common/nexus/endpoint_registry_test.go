// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package nexus

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protoassert"
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
		Entries:       []*persistencepb.NexusEndpointEntry{testEntry},
		TableVersion:  1,
		NextPageToken: nil,
	}, nil)

	// first long poll
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), &matchingservice.ListNexusEndpointsRequest{
		PageSize:              int32(1000),
		LastKnownTableVersion: int64(1),
		Wait:                  true,
	}).DoAndReturn(func(context.Context, *matchingservice.ListNexusEndpointsRequest, ...interface{}) (*matchingservice.ListNexusEndpointsResponse, error) {
		time.Sleep(20 * time.Millisecond)
		return &matchingservice.ListNexusEndpointsResponse{TableVersion: int64(1)}, nil
	}).MaxTimes(1)

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
		Entries:       []*persistencepb.NexusEndpointEntry{},
		TableVersion:  1,
		NextPageToken: nil,
	}, nil)

	// first long poll
	mocks.matchingClient.EXPECT().ListNexusEndpoints(gomock.Any(), &matchingservice.ListNexusEndpointsRequest{
		PageSize:              int32(1000),
		LastKnownTableVersion: int64(1),
		Wait:                  true,
	}).DoAndReturn(func(context.Context, *matchingservice.ListNexusEndpointsRequest, ...interface{}) (*matchingservice.ListNexusEndpointsResponse, error) {
		time.Sleep(20 * time.Millisecond)
		return &matchingservice.ListNexusEndpointsResponse{TableVersion: int64(1)}, nil
	}).MaxTimes(1)

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
		Entries:       []*persistencepb.NexusEndpointEntry{testEndpoint},
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
		Entries:       []*persistencepb.NexusEndpointEntry{testEntry0},
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
		Entries:       []*persistencepb.NexusEndpointEntry{testEntry0},
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
		Entries:       []*persistencepb.NexusEndpointEntry{testEntry1},
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
		Entries:       []*persistencepb.NexusEndpointEntry{testEntry0},
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
		Entries:       []*persistencepb.NexusEndpointEntry{testEntry0},
		TableVersion:  int64(3),
		NextPageToken: []byte(testEntry0.Id),
	}, nil)
	// successfully get second page
	mocks.persistence.EXPECT().ListNexusEndpoints(gomock.Any(), &persistence.ListNexusEndpointsRequest{
		NextPageToken:         []byte(testEntry0.Id),
		PageSize:              1,
		LastKnownTableVersion: int64(3),
	}).Return(&persistence.ListNexusEndpointsResponse{
		Entries:       []*persistencepb.NexusEndpointEntry{testEntry1},
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
	testConfig.refreshEnabled = dynamicconfig.GetBoolPropertyFn(true)
	return &testMocks{
		config:         testConfig,
		matchingClient: matchingservicemock.NewMockMatchingServiceClient(ctrl),
		persistence:    persistence.NewMockNexusEndpointManager(ctrl),
	}
}

func newEndpointEntry(name string) *persistencepb.NexusEndpointEntry {
	id := uuid.NewString()
	return &persistencepb.NexusEndpointEntry{
		Version: 1,
		Id:      id,
		Endpoint: &persistencepb.NexusEndpoint{
			Clock:       hybrid_logical_clock.Zero(1),
			CreatedTime: timestamppb.Now(),
			Spec: &persistencepb.NexusEndpointSpec{
				Name: name,
				Target: &persistencepb.NexusEndpointTarget{
					Variant: &persistencepb.NexusEndpointTarget_Worker_{
						Worker: &persistencepb.NexusEndpointTarget_Worker{
							NamespaceId: uuid.NewString(),
							TaskQueue:   name + "-task-queue",
						},
					},
				},
			},
		},
	}
}

func publicToInternalEndpointTarget(target *nexus.EndpointTarget) *persistencepb.NexusEndpointTarget {
	switch v := target.Variant.(type) {
	case *nexus.EndpointTarget_Worker_:
		return &persistencepb.NexusEndpointTarget{
			Variant: &persistencepb.NexusEndpointTarget_Worker_{
				Worker: &persistencepb.NexusEndpointTarget_Worker{
					NamespaceId: "TODO",
					TaskQueue:   v.Worker.TaskQueue,
				},
			},
		}
	case *nexus.EndpointTarget_External_:
		return &persistencepb.NexusEndpointTarget{
			Variant: &persistencepb.NexusEndpointTarget_External_{
				External: &persistencepb.NexusEndpointTarget_External{
					Url: v.External.Url,
				},
			},
		}
	}
	panic(fmt.Errorf("invalid target: %v", target))
}
