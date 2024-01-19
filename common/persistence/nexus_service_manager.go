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

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
)

var (
	ErrTableVersionConflict = &ConditionFailedError{
		Msg: "nexus incoming services table version mismatch",
	}
	ErrNexusIncomingServiceVersionConflict = &ConditionFailedError{
		Msg: "nexus incoming service version mismatch",
	}
	ErrNexusIncomingServiceNotFound = &ConditionFailedError{
		Msg: "nexus incoming service not found",
	}
	ErrNonPositiveListNexusIncomingServicesPageSize = &InvalidPersistenceRequestError{
		Msg: "received non-positive page size for listing Nexus incoming services",
	}
)

type (
	nexusServiceManagerImpl struct {
		persistence NexusServiceStore
		serializer  serialization.Serializer
	}
)

func NewNexusServiceManager(
	persistence NexusServiceStore,
) NexusServiceManager {
	return &nexusServiceManagerImpl{
		persistence: persistence,
	}
}

func (m *nexusServiceManagerImpl) GetName() string {
	return m.persistence.GetName()
}

func (m *nexusServiceManagerImpl) Close() {
	m.persistence.Close()
}

func (m *nexusServiceManagerImpl) ListNexusIncomingServices(
	ctx context.Context,
	request *ListNexusIncomingServicesRequest,
) (*ListNexusIncomingServicesResponse, error) {
	if request.PageSize <= 0 {
		return nil, ErrNonPositiveListNexusIncomingServicesPageSize
	}

	result := &ListNexusIncomingServicesResponse{}

	resp, err := m.persistence.ListNexusIncomingServices(ctx, &InternalListNexusIncomingServicesRequest{ // TODO: this request and response are just a copy of the request and response params. should simplify
		PageSize:              request.PageSize,
		NextPageToken:         request.NextPageToken,
		LastKnownTableVersion: request.LastKnownTableVersion,
	})
	// TODO: this logic looks duplicated with store-level logic
	if resp != nil {
		result.TableVersion = resp.TableVersion
	}
	if err != nil {
		return result, err
	}

	services := make([]*persistence.VersionedNexusIncomingService, len(resp.Services))
	for i := range services {
		service, err := m.serializer.NexusIncomingServiceDataFromBlob(resp.Services[i].Data)
		if err != nil {
			// TODO: improve error handling here
			return result, err
		}
		services[i] = &persistence.VersionedNexusIncomingService{
			Version: resp.Services[i].Version,
			Service: service, // TODO: fix serialization / data types
		}
	}

	result.NextPageToken = resp.NextPageToken
	result.Services = services

	return result, nil
}

func (m *nexusServiceManagerImpl) DeleteNexusIncomingService(
	ctx context.Context,
	request *DeleteNexusIncomingServiceRequest,
) error {
	return m.persistence.DeleteNexusIncomingService(ctx, &InternalDeleteNexusIncomingServiceRequest{
		LastKnownTableVersion: request.LastKnownTableVersion,
		ServiceID:             request.ID,
	})
}

func (m *nexusServiceManagerImpl) CreateOrUpdateNexusIncomingService(
	ctx context.Context,
	request *CreateOrUpdateNexusIncomingServiceRequest,
) (*CreateOrUpdateNexusIncomingServiceResponse, error) {
	if request.Service.Version == 0 {
		request.Service.Service.Id = uuid.NewString()
	}

	serviceDataBlob, err := m.serializer.NexusIncomingServiceDataToBlob(request.Service.Service.Data, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}

	err = m.persistence.CreateOrUpdateNexusIncomingService(ctx, &InternalCreateOrUpdateNexusIncomingServiceRequest{
		LastKnownTableVersion: request.LastKnownTableVersion,
		Service: InternalNexusIncomingService{
			ServiceID: request.Service.Service.Id,
			Version:   request.Service.Version,
			Data:      serviceDataBlob,
		},
	})
	if err != nil {
		return nil, err
	}

	return &CreateOrUpdateNexusIncomingServiceResponse{
		ServiceID: request.Service.Service.Id,
		Version:   request.Service.Version + 1,
	}, nil
}
