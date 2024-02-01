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
	ErrNonPositiveListNexusIncomingServicesPageSize = &InvalidPersistenceRequestError{
		Msg: "received non-positive page size for listing Nexus incoming services",
	}
)

type (
	nexusServiceManagerImpl struct {
		persistence NexusServiceStore
		serializer  serialization.Serializer
		logger      log.Logger
	}
)

func NewNexusServiceManager(
	persistence NexusServiceStore,
	serializer serialization.Serializer,
	logger log.Logger,
) NexusServiceManager {
	return &nexusServiceManagerImpl{
		persistence: persistence,
		serializer:  serializer,
		logger:      logger,
	}
}

func (m *nexusServiceManagerImpl) GetName() string {
	return m.persistence.GetName()
}

func (m *nexusServiceManagerImpl) Close() {
	m.persistence.Close()
}

// GetNexusIncomingServicesTableVersion is a convenience method for getting the current nexus_incoming_services table
// version by calling ListNexusIncomingServices with LastKnownTableVersion=0 and PageSize=0
func (m *nexusServiceManagerImpl) GetNexusIncomingServicesTableVersion(ctx context.Context) (int64, error) {
	tableVersion := int64(0)
	resp, err := m.ListNexusIncomingServices(ctx, &ListNexusIncomingServicesRequest{
		LastKnownTableVersion: 0,
		PageSize:              0,
	})
	if resp != nil {
		tableVersion = resp.TableVersion
	}
	return tableVersion, err
}

func (m *nexusServiceManagerImpl) ListNexusIncomingServices(
	ctx context.Context,
	request *ListNexusIncomingServicesRequest,
) (*ListNexusIncomingServicesResponse, error) {
	if request.PageSize < 0 {
		return nil, ErrNonPositiveListNexusIncomingServicesPageSize
	}

	result := &ListNexusIncomingServicesResponse{}

	resp, err := m.persistence.ListNexusIncomingServices(ctx, request)
	if resp != nil {
		result.TableVersion = resp.TableVersion
	}
	if err != nil {
		return result, err
	}

	services := make([]*persistencepb.VersionedNexusIncomingService, len(resp.Services))
	for i, service := range resp.Services {
		serviceInfo, err := m.serializer.NexusIncomingServiceFromBlob(service.Data)
		if err != nil {
			m.logger.Error(fmt.Sprintf("error deserializing nexus incoming service with ID:%v", service.ServiceID), tag.Error(err))
			return nil, err
		}
		services[i].Id = service.ServiceID
		services[i].Version = service.Version
		services[i].ServiceInfo = serviceInfo
	}

	result.NextPageToken = resp.NextPageToken
	result.Services = services
	return result, nil
}

func (m *nexusServiceManagerImpl) CreateOrUpdateNexusIncomingService(
	ctx context.Context,
	request *CreateOrUpdateNexusIncomingServiceRequest,
) (*CreateOrUpdateNexusIncomingServiceResponse, error) {
	serviceBlob, err := m.serializer.NexusIncomingServiceToBlob(request.Service.ServiceInfo, enums.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}

	err = m.persistence.CreateOrUpdateNexusIncomingService(ctx, &InternalCreateOrUpdateNexusIncomingServiceRequest{
		LastKnownTableVersion: request.LastKnownTableVersion,
		Service: InternalNexusIncomingService{
			ServiceID: request.Service.Id,
			Version:   request.Service.Version,
			Data:      serviceBlob,
		},
	})
	if err != nil {
		return nil, err
	}

	request.Service.Version++
	return &CreateOrUpdateNexusIncomingServiceResponse{Service: request.Service}, nil
}

func (m *nexusServiceManagerImpl) DeleteNexusIncomingService(
	ctx context.Context,
	request *DeleteNexusIncomingServiceRequest,
) error {
	return m.persistence.DeleteNexusIncomingService(ctx, request)
}
