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

package matching

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/nexus"
	p "go.temporal.io/server/common/persistence"
)

const (
	// loadServicesPageSize is the page size to use when initially loading services from persistence
	loadServicesPageSize = 100 // TODO: should be dynamic config?
)

type (
	// incomingServiceManager manages cache and persistence access for Nexus incoming services.
	// incomingServiceManager also contains a RWLock to enforce serial updates to prevent
	// nexus_incoming_services table version conflicts.
	//
	// incomingServiceManager should only be used within matching service because it assumes
	// that it is running on the matching node that owns the nexus_incoming_services table.
	// There is no explicit listener for membership changes because table ownership changes
	// will be detected by version conflicts and eventually settle through retries.
	//
	// Not to be confused with persistence.NexusServiceManager which is responsible for persistence-layer
	// CRUD APIs for Nexus incoming services.
	incomingServiceManager interface {
		CreateOrUpdateNexusService(ctx context.Context, request *matchingservice.CreateOrUpdateNexusServiceRequest, clusterID int64, timeSource clock.TimeSource) (*matchingservice.CreateOrUpdateNexusServiceResponse, error)
		DeleteNexusService(ctx context.Context, request *matchingservice.DeleteNexusServiceRequest) (*matchingservice.DeleteNexusServiceResponse, error)
		ListNexusServices(ctx context.Context, request *matchingservice.ListNexusServicesRequest) (*matchingservice.ListNexusServicesResponse, chan struct{}, error)
	}

	incomingServiceManagerImpl struct {
		hasLoadedServices atomic.Bool

		sync.RWMutex        // protects tableVersion, servicesByName, and tableVersionChanged
		tableVersion        int64
		servicesByName      map[string]*persistencepb.VersionedNexusIncomingService
		tableVersionChanged chan struct{}

		persistence p.NexusServiceManager
	}
)

func newIncomingServiceManager(
	persistence p.NexusServiceManager,
) incomingServiceManager {
	return &incomingServiceManagerImpl{
		persistence: persistence,
	}
}

func (m *incomingServiceManagerImpl) CreateOrUpdateNexusService(ctx context.Context, request *matchingservice.CreateOrUpdateNexusServiceRequest, clusterID int64, timeSource clock.TimeSource) (*matchingservice.CreateOrUpdateNexusServiceResponse, error) {
	if !m.hasLoadedServices.Load() {
		// services must be loaded into memory before CreateOrUpdate, so we know whether
		// this service name is in use and if so, what its UUID is
		if err := m.loadServices(ctx); err != nil {
			return nil, fmt.Errorf("error loading nexus incoming services cache: %w", err)
		}
	}

	m.Lock()
	defer m.Unlock()

	previous := &persistencepb.VersionedNexusIncomingService{}
	exists := false

	previous, exists = m.servicesByName[request.Service.Name]
	if !exists {
		previous.Version = 0
		previous.Id = uuid.NewString()
		previous.ServiceInfo.Clock = hlc.Zero(clusterID)
	}

	if request.Service.Version != previous.Version {
		// TODO: could improve error handling. handle already exists during create and not found during update
		return nil, fmt.Errorf("%w received: %v expected: %v", p.ErrNexusIncomingServiceVersionConflict, request.Service.Version, previous.Version)
	}

	versioned := nexus.IncomingServiceExternalToPersisted(request.Service, previous.Id, hlc.Next(previous.ServiceInfo.Clock, timeSource))

	resp, err := m.persistence.CreateOrUpdateNexusIncomingService(ctx, &p.CreateOrUpdateNexusIncomingServiceRequest{
		LastKnownTableVersion: m.tableVersion,
		Service:               versioned,
	})
	if err != nil {
		// TODO: special handling for table version conflicts? means table ownership changed
		return nil, err
	}

	m.tableVersion++
	m.servicesByName[resp.Service.ServiceInfo.Name] = resp.Service
	close(m.tableVersionChanged)
	m.tableVersionChanged = make(chan struct{})

	return &matchingservice.CreateOrUpdateNexusServiceResponse{Service: resp.Service}, nil
}

func (m *incomingServiceManagerImpl) DeleteNexusService(ctx context.Context, request *matchingservice.DeleteNexusServiceRequest) (*matchingservice.DeleteNexusServiceResponse, error) {
	if !m.hasLoadedServices.Load() {
		// services must be loaded into memory before deletion so that the service UUID can be looked up
		if err := m.loadServices(ctx); err != nil {
			return nil, fmt.Errorf("error loading nexus incoming services cache: %w", err)
		}
	}

	m.Lock()
	defer m.Unlock()

	service, ok := m.servicesByName[request.Name]
	if !ok {
		return nil, p.ErrNexusIncomingServiceNotFound
	}

	err := m.persistence.DeleteNexusIncomingService(ctx, &p.DeleteNexusIncomingServiceRequest{
		LastKnownTableVersion: m.tableVersion,
		ServiceID:             service.Id,
	})
	if err != nil {
		// TODO: special handling for table version conflicts? means table ownership changed
		return nil, err
	}

	m.tableVersion++
	delete(m.servicesByName, request.Name)
	close(m.tableVersionChanged)
	m.tableVersionChanged = make(chan struct{})

	return &matchingservice.DeleteNexusServiceResponse{}, nil
}

// ListNexusServices returns all cached Nexus incoming services. If no services have been loaded, it first tries to
// load all services from persistence.
// TODO: currently does not support pagination. go map iteration is no deterministic, so this will require sorting first.
func (m *incomingServiceManagerImpl) ListNexusServices(ctx context.Context, request *matchingservice.ListNexusServicesRequest) (*matchingservice.ListNexusServicesResponse, chan struct{}, error) {
	if !m.hasLoadedServices.Load() {
		if err := m.loadServices(ctx); err != nil {
			return nil, nil, fmt.Errorf("error loading nexus incoming services cache: %w", err)
		}
	}

	m.RLock()
	defer m.RUnlock()

	if request.LastKnownTableVersion != 0 && request.LastKnownTableVersion != m.tableVersion {
		return nil, nil, fmt.Errorf("%w received: %v expected: %v", p.ErrNexusTableVersionConflict, request.LastKnownTableVersion, m.tableVersion)
	}

	services := make([]*persistencepb.VersionedNexusIncomingService, len(m.servicesByName))
	i := 0
	for _, service := range m.servicesByName {
		services[i] = service
		i++
	}

	resp := &matchingservice.ListNexusServicesResponse{
		TableVersion: m.tableVersion,
		Services:     services,
	}

	return resp, m.tableVersionChanged, nil
}

func (m *incomingServiceManagerImpl) loadServices(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()

	if m.hasLoadedServices.Load() {
		// check whether services were loaded while waiting for write lock
		return nil
	}

	// reset cached view since we will be paging from the start
	m.servicesByName = make(map[string]*persistencepb.VersionedNexusIncomingService)

	finishedPaging := false
	var pageToken []byte

	for ctx.Err() == nil && !finishedPaging {
		resp, err := m.persistence.ListNexusIncomingServices(ctx, &p.ListNexusIncomingServicesRequest{
			LastKnownTableVersion: m.tableVersion,
			NextPageToken:         pageToken,
			PageSize:              loadServicesPageSize,
		})
		if err != nil {
			return err
		}

		pageToken = resp.NextPageToken
		m.tableVersion = resp.TableVersion
		for _, service := range resp.Services {
			m.servicesByName[service.ServiceInfo.Name] = service
		}

		finishedPaging = pageToken == nil
	}

	m.hasLoadedServices.Store(finishedPaging)
	return ctx.Err()
}
