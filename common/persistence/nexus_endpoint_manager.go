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

package persistence

import (
	"context"
	"fmt"

	"go.temporal.io/api/enums/v1"

	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/serialization"
)

var (
	ErrNexusTableVersionConflict = &ConditionFailedError{
		Msg: "nexus endpoints table version mismatch",
	}
	ErrNexusEndpointVersionConflict = &ConditionFailedError{
		Msg: "nexus endpoint version mismatch",
	}
	ErrNexusEndpointNotFound = &ConditionFailedError{
		Msg: "nexus endpoint not found",
	}
	ErrNegativeListNexusEndpointsPageSize = &InvalidPersistenceRequestError{
		Msg: "received negative page size for listing Nexus endpoints",
	}
)

type (
	nexusEndpointManagerImpl struct {
		persistence NexusEndpointStore
		serializer  serialization.Serializer
		logger      log.Logger
	}
)

func NewNexusEndpointManager(
	persistence NexusEndpointStore,
	serializer serialization.Serializer,
	logger log.Logger,
) NexusEndpointManager {
	return &nexusEndpointManagerImpl{
		persistence: persistence,
		serializer:  serializer,
		logger:      logger,
	}
}

func (m *nexusEndpointManagerImpl) GetName() string {
	return m.persistence.GetName()
}

func (m *nexusEndpointManagerImpl) Close() {
	m.persistence.Close()
}

func (m *nexusEndpointManagerImpl) GetNexusEndpoint(
	ctx context.Context,
	request *GetNexusEndpointRequest,
) (*persistencepb.NexusEndpointEntry, error) {
	internalEndpoint, err := m.persistence.GetNexusEndpoint(ctx, request)
	if err != nil {
		return nil, err
	}

	endpoint, err := m.serializer.NexusEndpointFromBlob(internalEndpoint.Data)
	if err != nil {
		m.logger.Error(fmt.Sprintf("error deserializing nexus endpoint with ID:%v", internalEndpoint.ID), tag.Error(err))
		return nil, err
	}

	return &persistencepb.NexusEndpointEntry{
		Id:       internalEndpoint.ID,
		Version:  internalEndpoint.Version,
		Endpoint: endpoint,
	}, nil
}

func (m *nexusEndpointManagerImpl) ListNexusEndpoints(
	ctx context.Context,
	request *ListNexusEndpointsRequest,
) (*ListNexusEndpointsResponse, error) {
	if request.PageSize < 0 {
		return nil, ErrNegativeListNexusEndpointsPageSize
	}

	result := &ListNexusEndpointsResponse{}

	resp, err := m.persistence.ListNexusEndpoints(ctx, request)
	if resp != nil {
		result.TableVersion = resp.TableVersion
	}
	if err != nil {
		return result, err
	}

	entries := make([]*persistencepb.NexusEndpointEntry, len(resp.Endpoints))
	for i, entry := range resp.Endpoints {
		endpoint, err := m.serializer.NexusEndpointFromBlob(entry.Data)
		if err != nil {
			m.logger.Error(fmt.Sprintf("error deserializing nexus endpoint with ID: %v", entry.ID), tag.Error(err))
			return nil, err
		}
		entries[i] = &persistencepb.NexusEndpointEntry{
			Id:       entry.ID,
			Version:  entry.Version,
			Endpoint: endpoint,
		}
	}

	result.NextPageToken = resp.NextPageToken
	result.Entries = entries
	return result, nil
}

func (m *nexusEndpointManagerImpl) CreateOrUpdateNexusEndpoint(
	ctx context.Context,
	request *CreateOrUpdateNexusEndpointRequest,
) (*CreateOrUpdateNexusEndpointResponse, error) {
	blob, err := m.serializer.NexusEndpointToBlob(request.Entry.Endpoint, enums.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}

	err = m.persistence.CreateOrUpdateNexusEndpoint(ctx, &InternalCreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: request.LastKnownTableVersion,
		Endpoint: InternalNexusEndpoint{
			ID:      request.Entry.Id,
			Version: request.Entry.Version,
			Data:    blob,
		},
	})
	if err != nil {
		return nil, err
	}

	return &CreateOrUpdateNexusEndpointResponse{Version: request.Entry.Version + 1}, nil
}

func (m *nexusEndpointManagerImpl) DeleteNexusEndpoint(
	ctx context.Context,
	request *DeleteNexusEndpointRequest,
) error {
	return m.persistence.DeleteNexusEndpoint(ctx, request)
}
