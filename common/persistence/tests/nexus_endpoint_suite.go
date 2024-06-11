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

func RunNexusEndpointTestSuite(t *testing.T, store persistence.NexusEndpointStore, tableVersion *atomic.Int64) {
	// NB: These tests cannot be run in parallel because of concurrent updates to the table version by different tests
	t.Run("TestNexusEndpointsSteadyState", func(t *testing.T) {
		testNexusEndpointsStoreSteadyState(t, store, tableVersion)
	})
	t.Run("TestCreateOrUpdateNexusEndpointExpectedErrors", func(t *testing.T) {
		testCreateOrUpdateNexusEndpointExpectedErrors(t, store, tableVersion)
	})
	t.Run("TestListNexusEndpointsExpectedErrors", func(t *testing.T) {
		testListNexusEndpointsExpectedErrors(t, store, tableVersion)
	})
	t.Run("TestDeleteNexusEndpointExpectedErrors", func(t *testing.T) {
		testDeleteNexusEndpointExpectedErrors(t, store, tableVersion)
	})
}

func RunNexusEndpointTestSuiteForSQL(t *testing.T, factory *sql.Factory) {
	tableVersion := atomic.Int64{}
	t.Run("Generic", func(t *testing.T) {
		store, err := factory.NewNexusEndpointStore()
		require.NoError(t, err)
		RunNexusEndpointTestSuite(t, store, &tableVersion)
	})
}

func testNexusEndpointsStoreSteadyState(t *testing.T, store persistence.NexusEndpointStore, tableVersion *atomic.Int64) {
	t.Run("SteadyState", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		data := &commonpb.DataBlob{
			Data:         []byte("dummy endpoint data"),
			EncodingType: enums.ENCODING_TYPE_PROTO3,
		}

		// Get endpoint by ID when table is empty
		endpoint, err := store.GetNexusEndpoint(ctx, &persistence.GetNexusEndpointRequest{ID: uuid.NewString()})
		require.ErrorContains(t, err, "not found")
		require.Nil(t, endpoint)

		// List when table is empty
		resp, err := store.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{PageSize: 10})
		require.NoError(t, err)
		require.Len(t, resp.Endpoints, 0)
		require.Equal(t, tableVersion.Load(), resp.TableVersion)

		// Create an endpoint
		firstEndpoint := persistence.InternalNexusEndpoint{ID: uuid.NewString(), Version: 0, Data: data}
		err = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Endpoint:              firstEndpoint,
		})
		require.NoError(t, err)
		tableVersion.Add(1)
		firstEndpoint.Version++

		// Get endpoint by ID
		endpoint, err = store.GetNexusEndpoint(ctx, &persistence.GetNexusEndpointRequest{ID: firstEndpoint.ID})
		require.NoError(t, err)
		require.Equal(t, firstEndpoint.ID, endpoint.ID)
		require.Equal(t, firstEndpoint.Version, endpoint.Version)

		// List one
		resp, err = store.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{PageSize: 10})
		require.NoError(t, err)
		require.Contains(t, resp.Endpoints, firstEndpoint)
		require.Equal(t, resp.TableVersion, tableVersion.Load())

		// Create a second endpoint
		secondEndpoint := persistence.InternalNexusEndpoint{ID: uuid.NewString(), Version: 0, Data: data}
		err = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Endpoint:              secondEndpoint,
		})
		require.NoError(t, err)
		tableVersion.Add(1)
		secondEndpoint.Version++

		// List multiple
		resp, err = store.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{PageSize: 10})
		require.NoError(t, err)
		require.Contains(t, resp.Endpoints, firstEndpoint)
		require.Contains(t, resp.Endpoints, secondEndpoint)
		require.Equal(t, resp.TableVersion, tableVersion.Load())

		// Create a third endpoints
		thirdEndpointID := uuid.New().String()
		err = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Endpoint:              persistence.InternalNexusEndpoint{ID: thirdEndpointID, Data: data},
		})
		require.NoError(t, err)
		tableVersion.Add(1)

		// Update an endpoint
		err = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Endpoint:              persistence.InternalNexusEndpoint{ID: thirdEndpointID, Version: 1, Data: data},
		})
		require.NoError(t, err)
		tableVersion.Add(1)

		// List in pages (page 1)
		resp, err = store.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{PageSize: 2})
		require.NoError(t, err)
		require.Len(t, resp.Endpoints, 2)
		require.Equal(t, resp.TableVersion, tableVersion.Load())
		require.NotNil(t, resp.NextPageToken)

		// List in pages (page 2)
		resp, err = store.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{
			PageSize:              2,
			NextPageToken:         resp.NextPageToken,
			LastKnownTableVersion: resp.TableVersion,
		})
		require.NoError(t, err)
		require.Len(t, resp.Endpoints, 1)
		require.Equal(t, resp.TableVersion, tableVersion.Load())
		require.Nil(t, resp.NextPageToken)

		// Delete an endpoint
		err = store.DeleteNexusEndpoint(ctx, &persistence.DeleteNexusEndpointRequest{
			ID:                    firstEndpoint.ID,
			LastKnownTableVersion: tableVersion.Load(),
		})
		require.NoError(t, err)
		tableVersion.Add(1)

		// List endpoints with table version
		resp, err = store.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{
			PageSize:              10,
			NextPageToken:         nil,
			LastKnownTableVersion: tableVersion.Load(),
		})
		require.NoError(t, err)
		require.Len(t, resp.Endpoints, 2)

		// Delete remaining endpoints
		err = store.DeleteNexusEndpoint(ctx, &persistence.DeleteNexusEndpointRequest{
			ID:                    secondEndpoint.ID,
			LastKnownTableVersion: tableVersion.Load(),
		})
		require.NoError(t, err)
		tableVersion.Add(1)
		err = store.DeleteNexusEndpoint(ctx, &persistence.DeleteNexusEndpointRequest{
			ID:                    thirdEndpointID,
			LastKnownTableVersion: tableVersion.Load(),
		})
		require.NoError(t, err)
		tableVersion.Add(1)

		// List endpoints when table empty and expected version non-zero
		resp, err = store.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{
			PageSize:              10,
			NextPageToken:         nil,
			LastKnownTableVersion: tableVersion.Load(),
		})
		require.NoError(t, err)
		require.Len(t, resp.Endpoints, 0)
	})
}

func testCreateOrUpdateNexusEndpointExpectedErrors(t *testing.T, store persistence.NexusEndpointStore, tableVersion *atomic.Int64) {
	t.Run("ExpectedErrors", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		data := &commonpb.DataBlob{
			Data:         []byte("dummy endpoint data"),
			EncodingType: enums.ENCODING_TYPE_PROTO3,
		}

		// Valid create
		endpointID := uuid.New().String()
		err := store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Endpoint:              persistence.InternalNexusEndpoint{ID: endpointID, Version: 0, Data: data},
		})
		require.NoError(t, err)
		tableVersion.Add(1)

		// Valid update
		err = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Endpoint:              persistence.InternalNexusEndpoint{ID: endpointID, Version: 1, Data: data},
		})
		require.NoError(t, err)
		tableVersion.Add(1)

		// Create request (version=0) when endpoint already exists
		err = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Endpoint:              persistence.InternalNexusEndpoint{ID: endpointID, Version: 0, Data: data},
		})
		require.ErrorContains(t, err, "nexus endpoint version mismatch")

		// Update request version mismatch
		err = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Endpoint:              persistence.InternalNexusEndpoint{ID: endpointID, Version: 10, Data: data},
		})
		require.ErrorContains(t, err, "nexus endpoint version mismatch")

		// Create request table version mismatch
		err = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: 10,
			Endpoint:              persistence.InternalNexusEndpoint{ID: uuid.NewString(), Version: 0, Data: data},
		})
		require.ErrorContains(t, err, "nexus endpoints table version mismatch")

		// Update request table version mismatch
		err = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: 10,
			Endpoint:              persistence.InternalNexusEndpoint{ID: endpointID, Version: 2, Data: data},
		})
		require.ErrorContains(t, err, "nexus endpoints table version mismatch")
	})
}

func testListNexusEndpointsExpectedErrors(t *testing.T, store persistence.NexusEndpointStore, tableVersion *atomic.Int64) {
	t.Run("ExpectedErrors", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		data := &commonpb.DataBlob{
			Data:         []byte("dummy endpoint data"),
			EncodingType: enums.ENCODING_TYPE_PROTO3,
		}

		// Create two endpoints
		firstEndpoint := persistence.InternalNexusEndpoint{ID: uuid.NewString(), Version: 0, Data: data}
		err := store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Endpoint:              firstEndpoint,
		})
		require.NoError(t, err)
		tableVersion.Add(1)
		firstEndpoint.Version = 1

		secondEndpoint := persistence.InternalNexusEndpoint{ID: uuid.NewString(), Version: 0, Data: data}
		err = store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Endpoint:              secondEndpoint,
		})
		require.NoError(t, err)
		tableVersion.Add(1)
		secondEndpoint.Version = 1

		// Valid list
		resp, err := store.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{PageSize: 10, LastKnownTableVersion: tableVersion.Load()})
		require.NoError(t, err)
		require.Contains(t, resp.Endpoints, firstEndpoint)
		require.Contains(t, resp.Endpoints, secondEndpoint)
		require.Equal(t, resp.TableVersion, tableVersion.Load())

		// Table version mismatch
		_, err = store.ListNexusEndpoints(ctx, &persistence.ListNexusEndpointsRequest{PageSize: 10, LastKnownTableVersion: 100})
		require.ErrorContains(t, err, "nexus endpoints table version mismatch")
	})
}

func testDeleteNexusEndpointExpectedErrors(t *testing.T, store persistence.NexusEndpointStore, tableVersion *atomic.Int64) {
	t.Run("ExpectedErrors", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		data := &commonpb.DataBlob{
			Data:         []byte("dummy endpoint data"),
			EncodingType: enums.ENCODING_TYPE_PROTO3,
		}

		// Create an endpoint
		id := uuid.New().String()
		err := store.CreateOrUpdateNexusEndpoint(ctx, &persistence.InternalCreateOrUpdateNexusEndpointRequest{
			LastKnownTableVersion: tableVersion.Load(),
			Endpoint:              persistence.InternalNexusEndpoint{ID: id, Version: 0, Data: data},
		})
		require.NoError(t, err)
		tableVersion.Add(1)

		// Table version mismatch
		err = store.DeleteNexusEndpoint(ctx, &persistence.DeleteNexusEndpointRequest{
			ID:                    id,
			LastKnownTableVersion: 100,
		})
		require.ErrorContains(t, err, "nexus endpoints table version mismatch")

		// Delete non-existent endpoint
		err = store.DeleteNexusEndpoint(ctx, &persistence.DeleteNexusEndpointRequest{
			ID:                    uuid.NewString(),
			LastKnownTableVersion: tableVersion.Load(),
		})
		require.ErrorContains(t, err, "nexus endpoint not found")

		// Valid delete
		err = store.DeleteNexusEndpoint(ctx, &persistence.DeleteNexusEndpointRequest{
			ID:                    id,
			LastKnownTableVersion: tableVersion.Load(),
		})
		require.NoError(t, err)
		tableVersion.Add(1)
	})
}
