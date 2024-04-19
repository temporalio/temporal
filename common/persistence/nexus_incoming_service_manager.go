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
		Msg: "nexus incoming services table version mismatch",
	}
	ErrNexusIncomingServiceVersionConflict = &ConditionFailedError{
		Msg: "nexus incoming service version mismatch",
	}
	ErrNexusIncomingServiceNotFound = &ConditionFailedError{
		Msg: "nexus incoming service not found",
	}
	ErrNegativeListNexusIncomingServicesPageSize = &InvalidPersistenceRequestError{
		Msg: "received negative page size for listing Nexus incoming services",
	}
)

type (
	nexusIncomingServiceManagerImpl struct {
		persistence NexusIncomingServiceStore
		serializer  serialization.Serializer
		logger      log.Logger
	}
)

func NewNexusIncomingServiceManager(
	persistence NexusIncomingServiceStore,
	serializer serialization.Serializer,
	logger log.Logger,
) NexusIncomingServiceManager {
	return &nexusIncomingServiceManagerImpl{
		persistence: persistence,
		serializer:  serializer,
		logger:      logger,
	}
}

func (m *nexusIncomingServiceManagerImpl) GetName() string {
	return m.persistence.GetName()
}

func (m *nexusIncomingServiceManagerImpl) Close() {
	m.persistence.Close()
}

func (m *nexusIncomingServiceManagerImpl) GetNexusIncomingService(
	ctx context.Context,
	request *GetNexusIncomingServiceRequest,
) (*persistencepb.NexusIncomingServiceEntry, error) {
	internalService, err := m.persistence.GetNexusIncomingService(ctx, request)
	if err != nil {
		return nil, err
	}

	service, err := m.serializer.NexusIncomingServiceFromBlob(internalService.Data)
	if err != nil {
		m.logger.Error(fmt.Sprintf("error deserializing nexus incoming service with ID:%v", internalService.ServiceID), tag.Error(err))
		return nil, err
	}

	return &persistencepb.NexusIncomingServiceEntry{
		Id:      internalService.ServiceID,
		Version: internalService.Version,
		Service: service,
	}, nil
}

func (m *nexusIncomingServiceManagerImpl) ListNexusIncomingServices(
	ctx context.Context,
	request *ListNexusIncomingServicesRequest,
) (*ListNexusIncomingServicesResponse, error) {
	if request.PageSize < 0 {
		return nil, ErrNegativeListNexusIncomingServicesPageSize
	}

	result := &ListNexusIncomingServicesResponse{}

	resp, err := m.persistence.ListNexusIncomingServices(ctx, request)
	if resp != nil {
		result.TableVersion = resp.TableVersion
	}
	if err != nil {
		return result, err
	}

	entries := make([]*persistencepb.NexusIncomingServiceEntry, len(resp.Services))
	for i, entry := range resp.Services {
		service, err := m.serializer.NexusIncomingServiceFromBlob(entry.Data)
		if err != nil {
			m.logger.Error(fmt.Sprintf("error deserializing nexus incoming service with ID:%v", entry.ServiceID), tag.Error(err))
			return nil, err
		}
		entries[i] = &persistencepb.NexusIncomingServiceEntry{
			Id:      entry.ServiceID,
			Version: entry.Version,
			Service: service,
		}
	}

	result.NextPageToken = resp.NextPageToken
	result.Entries = entries
	return result, nil
}

func (m *nexusIncomingServiceManagerImpl) CreateOrUpdateNexusIncomingService(
	ctx context.Context,
	request *CreateOrUpdateNexusIncomingServiceRequest,
) (*CreateOrUpdateNexusIncomingServiceResponse, error) {
	serviceBlob, err := m.serializer.NexusIncomingServiceToBlob(request.Entry.Service, enums.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}

	err = m.persistence.CreateOrUpdateNexusIncomingService(ctx, &InternalCreateOrUpdateNexusIncomingServiceRequest{
		LastKnownTableVersion: request.LastKnownTableVersion,
		Service: InternalNexusIncomingService{
			ServiceID: request.Entry.Id,
			Version:   request.Entry.Version,
			Data:      serviceBlob,
		},
	})
	if err != nil {
		return nil, err
	}

	return &CreateOrUpdateNexusIncomingServiceResponse{Version: request.Entry.Version + 1}, nil
}

func (m *nexusIncomingServiceManagerImpl) DeleteNexusIncomingService(
	ctx context.Context,
	request *DeleteNexusIncomingServiceRequest,
) error {
	return m.persistence.DeleteNexusIncomingService(ctx, request)
}
