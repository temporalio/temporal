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

	enumspb "go.temporal.io/api/enums/v1"

	persistencepb "go.temporal.io/server/api/persistence/v1"
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
	serializer serialization.Serializer,
) NexusServiceManager {
	return &nexusServiceManagerImpl{
		persistence: persistence,
		serializer:  serializer,
	}
}

func (m *nexusServiceManagerImpl) GetName() string {
	return m.persistence.GetName()
}

func (m *nexusServiceManagerImpl) Close() {
	m.persistence.Close()
}

func (m *nexusServiceManagerImpl) CreateOrUpdateNexusIncomingService(
	ctx context.Context,
	request *CreateOrUpdateNexusIncomingServiceRequest,
) (*CreateOrUpdateNexusIncomingServiceResponse, error) {
	serviceData := &persistencepb.NexusIncomingServiceData{
		Name:        request.Service.Name,
		NamespaceId: request.Service.Namespace,
		TaskQueue:   request.Service.TaskQueue,
		Metadata:    request.Service.Metadata,
	}
	serviceBlob, err := m.serializer.NexusIncomingServiceDataToBlob(serviceData, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}

	internalRequest := &InternalCreateOrUpdateNexusIncomingServiceRequest{
		LastKnownTableVersion: request.LastKnownTableVersion,
		Service: InternalNexusIncomingService{
			ServiceID: request.ServiceID,
			Version:   request.Service.Version,
			Data:      serviceBlob,
		},
	}
	service, err := m.persistence.CreateOrUpdateNexusIncomingService(ctx, internalRequest)
	if err != nil {
		return nil, err
	}

	return &CreateOrUpdateNexusIncomingServiceResponse{Service: service}, nil
}

func (m *nexusServiceManagerImpl) DeleteNexusIncomingService(
	ctx context.Context,
	request *DeleteNexusIncomingServiceRequest,
) error {
	return m.persistence.DeleteNexusIncomingService(ctx, &InternalDeleteNexusIncomingServiceRequest{
		LastKnownTableVersion: request.LastKnownTableVersion,
		ServiceID:             request.ServiceID,
	})
}

func (m *nexusServiceManagerImpl) ListNexusIncomingServices(
	ctx context.Context,
	request *ListNexusIncomingServicesRequest,
) (*ListNexusIncomingServicesResponse, error) {
	if request.PageSize <= 0 {
		return nil, ErrNonPositiveListNexusIncomingServicesPageSize
	}

	result := &ListNexusIncomingServicesResponse{}
	resp, err := m.persistence.ListNexusIncomingServices(ctx, &InternalListNexusIncomingServicesRequest{
		LastKnownTableVersion: request.LastKnownTableVersion,
		NextPageToken:         request.NextPageToken,
		PageSize:              request.PageSize,
	})
	if resp != nil {
		result.TableVersion = resp.TableVersion
	}
	if err != nil {
		return result, nil
	}

	services := make([]*persistencepb.VersionedNexusIncomingService, len(resp.Services))
	for i, service := range resp.Services {
		serviceData, err := m.serializer.NexusIncomingServiceDataFromBlob(service.Data)
		if err != nil {
			return result, err
		}
		services[i] = &persistencepb.VersionedNexusIncomingService{
			Version: service.Version,
			Service: &persistencepb.NexusIncomingService{
				Id:   service.ServiceID,
				Data: serviceData,
			},
		}
	}

	result.Services = services
	result.NextPageToken = resp.NextPageToken

	return result, nil
}
