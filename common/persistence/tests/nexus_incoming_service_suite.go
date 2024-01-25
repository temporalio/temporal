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

package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql"
)

func RunNexusIncomingServiceTestSuite(t *testing.T, store persistence.NexusServiceStore, tableVersion *atomic.Int64) {
	// NB: These tests cannot be run in parallel because of concurrent updates to the table version by different tests
	t.Run("TestNexusIncomingServicesSteadyState", func(t *testing.T) {
		testNexusIncomingServicesStoreSteadyState(t, store, tableVersion)
	})
	t.Run("TestCreateOrUpdateNexusIncomingServiceExpectedErrors", func(t *testing.T) {
		testCreateOrUpdateNexusIncomingServiceExpectedErrors(t, store, tableVersion)
	})
	t.Run("TestListNexusIncomingServicesExpectedErrors", func(t *testing.T) {
		testListNexusIncomingServicesExpectedErrors(t, store, tableVersion)
	})
	t.Run("TestDeleteNexusIncomingServiceExpectedErrors", func(t *testing.T) {
		testDeleteNexusIncomingServiceExpectedErrors(t, store, tableVersion)
	})
}

func RunNexusIncomingServiceTestSuiteForSQL(t *testing.T, factory *sql.Factory) {
	tableVersion := atomic.Int64{}
	t.Run("Generic", func(t *testing.T) {
		store, err := factory.NewNexusServiceStore()
		require.NoError(t, err)
		RunNexusIncomingServiceTestSuite(t, store, &tableVersion)
	})
}

func testNexusIncomingServicesStoreSteadyState(t *testing.T, store persistence.NexusServiceStore, tableVersion *atomic.Int64) {
	t.Run("SteadyState", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		data := &commonpb.DataBlob{
			Data:         []byte("dummy service data"),
			EncodingType: enums.ENCODING_TYPE_PROTO3,
		}

		// List when table is empty
		resp, err := store.ListNexusIncomingServices(ctx, &persistence.ListNexusIncomingServicesRequest{PageSize: 10})
		require.NoError(t, err)
		require.Len(t, resp.Services, 0)
		require.Equal(t, tableVersion.Load(), resp.TableVersion)

		// Create a service
		firstService := persistence.InternalNexusIncomingService{ID: uuid.NewString(), Version: 0, Data: data}
		err = store.CreateOrUpdateNexusIncomingService(ctx, &persistence.InternalCreateOrUpdateNexusIncomingServiceRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Service:               firstService,
		})
		require.NoError(t, err)
		tableVersion.Add(1)
		firstService.Version++

		// List one
		resp, err = store.ListNexusIncomingServices(ctx, &persistence.ListNexusIncomingServicesRequest{PageSize: 10})
		require.NoError(t, err)
		require.Contains(t, resp.Services, firstService)
		require.Equal(t, resp.TableVersion, tableVersion.Load())

		// Create a second service
		secondService := persistence.InternalNexusIncomingService{ID: uuid.NewString(), Version: 0, Data: data}
		err = store.CreateOrUpdateNexusIncomingService(ctx, &persistence.InternalCreateOrUpdateNexusIncomingServiceRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Service:               secondService,
		})
		require.NoError(t, err)
		tableVersion.Add(1)
		secondService.Version++

		// List multiple
		resp, err = store.ListNexusIncomingServices(ctx, &persistence.ListNexusIncomingServicesRequest{PageSize: 10})
		require.NoError(t, err)
		require.Contains(t, resp.Services, firstService)
		require.Contains(t, resp.Services, secondService)
		require.Equal(t, resp.TableVersion, tableVersion.Load())

		// Create a third service
		thirdServiceID := uuid.New().String()
		err = store.CreateOrUpdateNexusIncomingService(ctx, &persistence.InternalCreateOrUpdateNexusIncomingServiceRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Service:               persistence.InternalNexusIncomingService{ID: thirdServiceID, Data: data},
		})
		require.NoError(t, err)
		tableVersion.Add(1)

		// Update a service
		err = store.CreateOrUpdateNexusIncomingService(ctx, &persistence.InternalCreateOrUpdateNexusIncomingServiceRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Service:               persistence.InternalNexusIncomingService{ID: thirdServiceID, Version: 1, Data: data},
		})
		require.NoError(t, err)
		tableVersion.Add(1)

		// List in pages (page 1)
		resp, err = store.ListNexusIncomingServices(ctx, &persistence.ListNexusIncomingServicesRequest{PageSize: 2})
		require.NoError(t, err)
		require.Len(t, resp.Services, 2)
		require.Equal(t, resp.TableVersion, tableVersion.Load())
		require.NotNil(t, resp.NextPageToken)

		// List in pages (page 2)
		resp, err = store.ListNexusIncomingServices(ctx, &persistence.ListNexusIncomingServicesRequest{
			PageSize:              2,
			NextPageToken:         resp.NextPageToken,
			LastKnownTableVersion: resp.TableVersion,
		})
		require.NoError(t, err)
		require.Len(t, resp.Services, 1)
		require.Equal(t, resp.TableVersion, tableVersion.Load())
		require.Nil(t, resp.NextPageToken)

		// Delete a service
		err = store.DeleteNexusIncomingService(ctx, &persistence.DeleteNexusIncomingServiceRequest{
			ServiceID:             firstService.ID,
			LastKnownTableVersion: tableVersion.Load(),
		})
		require.NoError(t, err)
		tableVersion.Add(1)

		// List services with table version
		resp, err = store.ListNexusIncomingServices(ctx, &persistence.ListNexusIncomingServicesRequest{
			PageSize:              10,
			NextPageToken:         nil,
			LastKnownTableVersion: tableVersion.Load(),
		})
		require.NoError(t, err)
		require.Len(t, resp.Services, 2)

		// Delete remaining services
		err = store.DeleteNexusIncomingService(ctx, &persistence.DeleteNexusIncomingServiceRequest{
			ServiceID:             secondService.ID,
			LastKnownTableVersion: tableVersion.Load(),
		})
		require.NoError(t, err)
		tableVersion.Add(1)
		err = store.DeleteNexusIncomingService(ctx, &persistence.DeleteNexusIncomingServiceRequest{
			ServiceID:             thirdServiceID,
			LastKnownTableVersion: tableVersion.Load(),
		})
		require.NoError(t, err)
		tableVersion.Add(1)

		// List services when table empty and expected version non-zero
		resp, err = store.ListNexusIncomingServices(ctx, &persistence.ListNexusIncomingServicesRequest{
			PageSize:              10,
			NextPageToken:         nil,
			LastKnownTableVersion: tableVersion.Load(),
		})
		require.NoError(t, err)
		require.Len(t, resp.Services, 0)
	})
}

func testCreateOrUpdateNexusIncomingServiceExpectedErrors(t *testing.T, store persistence.NexusServiceStore, tableVersion *atomic.Int64) {
	t.Run("ExpectedErrors", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		data := &commonpb.DataBlob{
			Data:         []byte("dummy service data"),
			EncodingType: enums.ENCODING_TYPE_PROTO3,
		}

		// Valid create
		serviceID := uuid.New().String()
		err := store.CreateOrUpdateNexusIncomingService(ctx, &persistence.InternalCreateOrUpdateNexusIncomingServiceRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Service:               persistence.InternalNexusIncomingService{ID: serviceID, Version: 0, Data: data},
		})
		require.NoError(t, err)
		tableVersion.Add(1)

		// Valid update
		err = store.CreateOrUpdateNexusIncomingService(ctx, &persistence.InternalCreateOrUpdateNexusIncomingServiceRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Service:               persistence.InternalNexusIncomingService{ID: serviceID, Version: 1, Data: data},
		})
		require.NoError(t, err)
		tableVersion.Add(1)

		// Create request (version=0) when service already exists
		err = store.CreateOrUpdateNexusIncomingService(ctx, &persistence.InternalCreateOrUpdateNexusIncomingServiceRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Service:               persistence.InternalNexusIncomingService{ID: serviceID, Version: 0, Data: data},
		})
		require.ErrorContains(t, err, "nexus incoming service version mismatch")

		// Update request version mismatch
		err = store.CreateOrUpdateNexusIncomingService(ctx, &persistence.InternalCreateOrUpdateNexusIncomingServiceRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Service:               persistence.InternalNexusIncomingService{ID: serviceID, Version: 10, Data: data},
		})
		require.ErrorContains(t, err, "nexus incoming service version mismatch")

		// Create request table version mismatch
		err = store.CreateOrUpdateNexusIncomingService(ctx, &persistence.InternalCreateOrUpdateNexusIncomingServiceRequest{
			LastKnownTableVersion: 10,
			Service:               persistence.InternalNexusIncomingService{ID: uuid.NewString(), Version: 0, Data: data},
		})
		require.ErrorContains(t, err, "nexus incoming services table version mismatch")

		// Update request table version mismatch
		err = store.CreateOrUpdateNexusIncomingService(ctx, &persistence.InternalCreateOrUpdateNexusIncomingServiceRequest{
			LastKnownTableVersion: 10,
			Service:               persistence.InternalNexusIncomingService{ID: serviceID, Version: 2, Data: data},
		})
		require.ErrorContains(t, err, "nexus incoming services table version mismatch")
	})
}

func testListNexusIncomingServicesExpectedErrors(t *testing.T, store persistence.NexusServiceStore, tableVersion *atomic.Int64) {
	t.Run("ExpectedErrors", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		data := &commonpb.DataBlob{
			Data:         []byte("dummy service data"),
			EncodingType: enums.ENCODING_TYPE_PROTO3,
		}

		// Create two services
		firstService := persistence.InternalNexusIncomingService{ID: uuid.NewString(), Version: 0, Data: data}
		err := store.CreateOrUpdateNexusIncomingService(ctx, &persistence.InternalCreateOrUpdateNexusIncomingServiceRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Service:               firstService,
		})
		require.NoError(t, err)
		tableVersion.Add(1)
		firstService.Version = 1

		secondService := persistence.InternalNexusIncomingService{ID: uuid.NewString(), Version: 0, Data: data}
		err = store.CreateOrUpdateNexusIncomingService(ctx, &persistence.InternalCreateOrUpdateNexusIncomingServiceRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Service:               secondService,
		})
		require.NoError(t, err)
		tableVersion.Add(1)
		secondService.Version = 1

		// Valid list
		resp, err := store.ListNexusIncomingServices(ctx, &persistence.ListNexusIncomingServicesRequest{PageSize: 10, LastKnownTableVersion: tableVersion.Load()})
		require.NoError(t, err)
		require.Contains(t, resp.Services, firstService)
		require.Contains(t, resp.Services, secondService)
		require.Equal(t, resp.TableVersion, tableVersion.Load())

		// Table version mismatch
		_, err = store.ListNexusIncomingServices(ctx, &persistence.ListNexusIncomingServicesRequest{PageSize: 10, LastKnownTableVersion: 100})
		require.ErrorContains(t, err, "nexus incoming services table version mismatch")
	})
}

func testDeleteNexusIncomingServiceExpectedErrors(t *testing.T, store persistence.NexusServiceStore, tableVersion *atomic.Int64) {
	t.Run("ExpectedErrors", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		data := &commonpb.DataBlob{
			Data:         []byte("dummy service data"),
			EncodingType: enums.ENCODING_TYPE_PROTO3,
		}

		// Create a service
		serviceID := uuid.New().String()
		err := store.CreateOrUpdateNexusIncomingService(ctx, &persistence.InternalCreateOrUpdateNexusIncomingServiceRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Service:               persistence.InternalNexusIncomingService{ID: serviceID, Version: 0, Data: data},
		})
		require.NoError(t, err)
		tableVersion.Add(1)

		// Table version mismatch
		err = store.DeleteNexusIncomingService(ctx, &persistence.DeleteNexusIncomingServiceRequest{
			ServiceID:             serviceID,
			LastKnownTableVersion: 100,
		})
		require.ErrorContains(t, err, "nexus incoming services table version mismatch")

		// Delete non-existent service
		err = store.DeleteNexusIncomingService(ctx, &persistence.DeleteNexusIncomingServiceRequest{
			ServiceID:             uuid.NewString(),
			LastKnownTableVersion: tableVersion.Load(),
		})
		require.ErrorContains(t, err, "nexus incoming service not found")

		// Valid delete
		err = store.DeleteNexusIncomingService(ctx, &persistence.DeleteNexusIncomingServiceRequest{
			ServiceID:             serviceID,
			LastKnownTableVersion: tableVersion.Load(),
		})
		require.NoError(t, err)
		tableVersion.Add(1)
	})
}
