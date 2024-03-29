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
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protoassert"
)

type testMocks struct {
	config         *IncomingServiceRegistryConfig
	matchingClient *matchingservicemock.MockMatchingServiceClient
	persistence    *persistence.MockNexusIncomingServiceManager
}

func TestGet(t *testing.T) {
	t.Parallel()

	testService := newIncomingService(t.Name())
	mocks := newTestMocks(t)

	// lazy load
	mocks.matchingClient.EXPECT().ListNexusIncomingServices(gomock.Any(), gomock.Any()).Return(&matchingservice.ListNexusIncomingServicesResponse{
		Services:      []*nexus.IncomingService{testService},
		TableVersion:  1,
		NextPageToken: nil,
	}, nil)

	// first long poll
	mocks.matchingClient.EXPECT().ListNexusIncomingServices(gomock.Any(), &matchingservice.ListNexusIncomingServicesRequest{
		PageSize:              int32(1000),
		LastKnownTableVersion: int64(1),
		Wait:                  true,
	}).DoAndReturn(func(context.Context, *matchingservice.ListNexusIncomingServicesRequest, ...interface{}) (*matchingservice.ListNexusIncomingServicesResponse, error) {
		time.Sleep(20 * time.Millisecond)
		return &matchingservice.ListNexusIncomingServicesResponse{TableVersion: int64(1)}, nil
	}).MaxTimes(1)

	reg := NewIncomingServiceRegistry(mocks.config, mocks.matchingClient, mocks.persistence, log.NewNoopLogger())
	reg.Start()
	defer reg.Stop()

	service, err := reg.Get(context.Background(), testService.Id)
	require.NoError(t, err)
	assert.Equal(t, int64(1), reg.tableVersion)
	protoassert.ProtoEqual(t, testService, service)
}

func TestGetNotFound(t *testing.T) {
	t.Parallel()

	testService := newIncomingService(t.Name())
	mocks := newTestMocks(t)

	// lazy load
	mocks.matchingClient.EXPECT().ListNexusIncomingServices(gomock.Any(), gomock.Any()).Return(&matchingservice.ListNexusIncomingServicesResponse{
		Services:      []*nexus.IncomingService{testService},
		TableVersion:  1,
		NextPageToken: nil,
	}, nil)

	// first long poll
	mocks.matchingClient.EXPECT().ListNexusIncomingServices(gomock.Any(), &matchingservice.ListNexusIncomingServicesRequest{
		PageSize:              int32(1000),
		LastKnownTableVersion: int64(1),
		Wait:                  true,
	}).DoAndReturn(func(context.Context, *matchingservice.ListNexusIncomingServicesRequest, ...interface{}) (*matchingservice.ListNexusIncomingServicesResponse, error) {
		time.Sleep(20 * time.Millisecond)
		return &matchingservice.ListNexusIncomingServicesResponse{TableVersion: int64(1)}, nil
	}).MaxTimes(1)

	reg := NewIncomingServiceRegistry(mocks.config, mocks.matchingClient, mocks.persistence, log.NewNoopLogger())
	reg.Start()
	defer reg.Stop()

	service, err := reg.Get(context.Background(), uuid.NewString())
	var notFound *serviceerror.NotFound
	assert.ErrorAs(t, err, &notFound)
	assert.Nil(t, service)
	assert.Equal(t, int64(1), reg.tableVersion)
	assert.NotEmpty(t, reg.services)
}

func TestLazyLoadFallback(t *testing.T) {
	t.Parallel()

	testService := newIncomingService(t.Name())
	mocks := newTestMocks(t)

	mocks.matchingClient.EXPECT().ListNexusIncomingServices(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("matching unavailable test error")).MinTimes(1)
	mocks.persistence.EXPECT().ListNexusIncomingServices(gomock.Any(), gomock.Any()).Return(&persistence.ListNexusIncomingServicesResponse{
		TableVersion:  int64(1),
		NextPageToken: nil,
		Entries:       []*persistencepb.NexusIncomingServiceEntry{serviceToEntry(testService)},
	}, nil)

	reg := NewIncomingServiceRegistry(mocks.config, mocks.matchingClient, mocks.persistence, log.NewNoopLogger())
	reg.Start()
	defer reg.Stop()

	service, err := reg.Get(context.Background(), testService.Id)
	require.NoError(t, err)
	assert.Equal(t, int64(1), reg.tableVersion)
	protoassert.ProtoEqual(t, testService, service)
}

func TestTableVersionErrorResetsMatchingPagination(t *testing.T) {
	t.Parallel()

	testService0 := newIncomingService(t.Name() + "-0")
	testService1 := newIncomingService(t.Name() + "-1")

	mocks := newTestMocks(t)
	mocks.config.refreshPageSize = dynamicconfig.GetIntPropertyFn(1)

	// service data initialization mocks
	// successfully get first page
	mocks.matchingClient.EXPECT().ListNexusIncomingServices(gomock.Any(), &matchingservice.ListNexusIncomingServicesRequest{
		NextPageToken:         nil,
		PageSize:              int32(1),
		LastKnownTableVersion: int64(0),
		Wait:                  false,
	}).Return(&matchingservice.ListNexusIncomingServicesResponse{
		Services:      []*nexus.IncomingService{testService0},
		TableVersion:  int64(2),
		NextPageToken: []byte(testService0.Id),
	}, nil)
	// persistence.ErrNexusTableVersionConflict error on second page
	mocks.matchingClient.EXPECT().ListNexusIncomingServices(gomock.Any(), &matchingservice.ListNexusIncomingServicesRequest{
		NextPageToken:         []byte(testService0.Id),
		PageSize:              int32(1),
		LastKnownTableVersion: int64(2),
		Wait:                  false,
	}).Return(&matchingservice.ListNexusIncomingServicesResponse{TableVersion: int64(3)}, persistence.ErrNexusTableVersionConflict)
	// request first page again
	mocks.matchingClient.EXPECT().ListNexusIncomingServices(gomock.Any(), &matchingservice.ListNexusIncomingServicesRequest{
		NextPageToken:         nil,
		PageSize:              int32(1),
		LastKnownTableVersion: int64(0),
		Wait:                  false,
	}).Return(&matchingservice.ListNexusIncomingServicesResponse{
		Services:      []*nexus.IncomingService{testService0},
		TableVersion:  int64(3),
		NextPageToken: []byte(testService0.Id),
	}, nil)
	// successfully get second page
	mocks.matchingClient.EXPECT().ListNexusIncomingServices(gomock.Any(), &matchingservice.ListNexusIncomingServicesRequest{
		NextPageToken:         []byte(testService0.Id),
		PageSize:              int32(1),
		LastKnownTableVersion: int64(3),
		Wait:                  false,
	}).Return(&matchingservice.ListNexusIncomingServicesResponse{
		Services:      []*nexus.IncomingService{testService1},
		TableVersion:  int64(3),
		NextPageToken: nil,
	}, nil)

	// mock first long poll
	mocks.matchingClient.EXPECT().ListNexusIncomingServices(gomock.Any(), &matchingservice.ListNexusIncomingServicesRequest{
		PageSize:              int32(1),
		LastKnownTableVersion: int64(3),
		Wait:                  true,
	}).DoAndReturn(func(context.Context, *matchingservice.ListNexusIncomingServicesRequest, ...interface{}) (*matchingservice.ListNexusIncomingServicesResponse, error) {
		time.Sleep(20 * time.Millisecond)
		return &matchingservice.ListNexusIncomingServicesResponse{TableVersion: int64(1)}, nil
	}).MaxTimes(1)

	reg := NewIncomingServiceRegistry(mocks.config, mocks.matchingClient, mocks.persistence, log.NewNoopLogger())
	reg.Start()
	defer reg.Stop()

	service, err := reg.Get(context.Background(), testService0.Id)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, testService0, service)

	service, err = reg.Get(context.Background(), testService1.Id)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, testService1, service)

	assert.Equal(t, int64(3), reg.tableVersion)
}

func TestTableVersionErrorResetsPersistencePagination(t *testing.T) {
	t.Parallel()

	testService0 := newIncomingService(t.Name() + "-0")
	testService1 := newIncomingService(t.Name() + "-1")

	mocks := newTestMocks(t)
	mocks.config.refreshPageSize = dynamicconfig.GetIntPropertyFn(1)

	// mock unavailable matching service
	mocks.matchingClient.EXPECT().ListNexusIncomingServices(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("matching unavailable test error")).MinTimes(1)

	// fallback service data initialization mocks
	// successfully get first page
	mocks.persistence.EXPECT().ListNexusIncomingServices(gomock.Any(), &persistence.ListNexusIncomingServicesRequest{
		NextPageToken:         nil,
		PageSize:              1,
		LastKnownTableVersion: int64(0),
	}).Return(&persistence.ListNexusIncomingServicesResponse{
		Entries:       []*persistencepb.NexusIncomingServiceEntry{serviceToEntry(testService0)},
		TableVersion:  int64(2),
		NextPageToken: []byte(testService0.Id),
	}, nil)
	// persistence.ErrNexusTableVersionConflict error on second page
	mocks.persistence.EXPECT().ListNexusIncomingServices(gomock.Any(), &persistence.ListNexusIncomingServicesRequest{
		NextPageToken:         []byte(testService0.Id),
		PageSize:              1,
		LastKnownTableVersion: int64(2),
	}).Return(&persistence.ListNexusIncomingServicesResponse{TableVersion: int64(3)}, persistence.ErrNexusTableVersionConflict)
	// request first page again
	mocks.persistence.EXPECT().ListNexusIncomingServices(gomock.Any(), &persistence.ListNexusIncomingServicesRequest{
		NextPageToken:         nil,
		PageSize:              1,
		LastKnownTableVersion: int64(0),
	}).Return(&persistence.ListNexusIncomingServicesResponse{
		Entries:       []*persistencepb.NexusIncomingServiceEntry{serviceToEntry(testService0)},
		TableVersion:  int64(3),
		NextPageToken: []byte(testService0.Id),
	}, nil)
	// successfully get second page
	mocks.persistence.EXPECT().ListNexusIncomingServices(gomock.Any(), &persistence.ListNexusIncomingServicesRequest{
		NextPageToken:         []byte(testService0.Id),
		PageSize:              1,
		LastKnownTableVersion: int64(3),
	}).Return(&persistence.ListNexusIncomingServicesResponse{
		Entries:       []*persistencepb.NexusIncomingServiceEntry{serviceToEntry(testService1)},
		TableVersion:  int64(3),
		NextPageToken: nil,
	}, nil)

	reg := NewIncomingServiceRegistry(mocks.config, mocks.matchingClient, mocks.persistence, log.NewNoopLogger())
	reg.Start()
	defer reg.Stop()

	service, err := reg.Get(context.Background(), testService0.Id)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, testService0, service)

	service, err = reg.Get(context.Background(), testService1.Id)
	require.NoError(t, err)
	protoassert.ProtoEqual(t, testService1, service)

	assert.Equal(t, int64(3), reg.tableVersion)
}

func newTestMocks(t *testing.T) *testMocks {
	ctrl := gomock.NewController(t)
	return &testMocks{
		config:         NewIncomingServiceRegistryConfig(dynamicconfig.NewNoopCollection()),
		matchingClient: matchingservicemock.NewMockMatchingServiceClient(ctrl),
		persistence:    persistence.NewMockNexusIncomingServiceManager(ctrl),
	}
}

func newIncomingService(name string) *nexus.IncomingService {
	id := uuid.NewString()
	return &nexus.IncomingService{
		Version:     1,
		Id:          id,
		CreatedTime: timestamppb.Now(),
		UrlPrefix:   "/" + Routes().DispatchNexusTaskByService.Path(id),
		Spec: &nexus.IncomingServiceSpec{
			Name:      name,
			Namespace: name + "-namespace",
			TaskQueue: name + "-task-queue",
		},
	}
}

func serviceToEntry(service *nexus.IncomingService) *persistencepb.NexusIncomingServiceEntry {
	return &persistencepb.NexusIncomingServiceEntry{
		Version: service.Version,
		Id:      service.Id,
		Service: &persistencepb.NexusIncomingService{Spec: service.Spec, CreatedTime: service.CreatedTime},
	}
}
