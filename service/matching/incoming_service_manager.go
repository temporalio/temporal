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
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	nexuspb "go.temporal.io/api/nexus/v1"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
)

const (
	// loadServicesPageSize is the page size to use when initially loading services from persistence
	loadServicesPageSize = 100
)

type (
	internalCreateOrUpdateRequest struct {
		service     *nexuspb.IncomingService
		namespaceID string
		clusterID   int64
		timeSource  clock.TimeSource
	}

	// incomingServiceManager manages cache and persistence access for Nexus incoming services.
	// incomingServiceManager contains a RWLock to enforce serial updates to prevent
	// nexus_incoming_services table version conflicts.
	//
	// incomingServiceManager should only be used within matching service because it assumes
	// that it is running on the matching node that owns the nexus_incoming_services table.
	// There is no explicit listener for membership changes because table ownership changes
	// will be detected by version conflicts and eventually settle through retries.
	//
	// Not to be confused with persistence.NexusIncomingServiceManager which is responsible for
	// persistence-layer CRUD APIs for Nexus incoming services.
	incomingServiceManager struct {
		hasLoadedServices atomic.Bool

		sync.RWMutex        // protects tableVersion, services, servicesByName, and tableVersionChanged
		tableVersion        int64
		services            []*persistencepb.NexusIncomingServiceEntry // sorted by serviceID to support pagination during ListNexusIncomingServices
		servicesByName      map[string]*persistencepb.NexusIncomingServiceEntry
		tableVersionChanged chan struct{}

		persistence p.NexusIncomingServiceManager
	}
)

func newIncomingServiceManager(
	persistence p.NexusIncomingServiceManager,
) *incomingServiceManager {
	return &incomingServiceManager{
		persistence: persistence,
	}
}

func (m *incomingServiceManager) CreateOrUpdateNexusIncomingService(
	ctx context.Context,
	request *internalCreateOrUpdateRequest,
) (*matchingservice.CreateOrUpdateNexusIncomingServiceResponse, error) {
	if !m.hasLoadedServices.Load() {
		// services must be loaded into memory before CreateOrUpdate, so we know whether
		// this service name is in use and if so, what its UUID is
		if err := m.loadServices(ctx); err != nil {
			return nil, fmt.Errorf("error loading nexus incoming services cache: %w", err)
		}
	}

	m.Lock()
	defer m.Unlock()

	previous := &persistencepb.NexusIncomingServiceEntry{}
	exists := false

	previous, exists = m.servicesByName[request.service.Name]
	if !exists {
		previous.Version = 0
		previous.Id = uuid.NewString()
		previous.Service.Clock = hlc.Zero(request.clusterID)
	}

	if request.service.Version == 0 && exists {
		return nil, fmt.Errorf("%w service_name: %v service_version: %v", p.ErrNexusIncomingServiceAlreadyExists, previous.Service.Name, previous.Version)
	} else if request.service.Version != 0 && !exists {
		return nil, fmt.Errorf("%w service_name: %v", p.ErrNexusIncomingServiceNotFound, request.service.Name)
	} else if request.service.Version != previous.Version {
		return nil, fmt.Errorf("%w received: %v expected: %v", p.ErrNexusIncomingServiceVersionConflict, request.service.Version, previous.Version)
	}

	entry := &persistencepb.NexusIncomingServiceEntry{
		Version: previous.Version,
		Id:      previous.Id,
		Service: &persistencepb.NexusIncomingService{
			Clock:       hlc.Next(previous.Service.Clock, request.timeSource),
			Name:        request.service.Name,
			NamespaceId: request.namespaceID,
			TaskQueue:   request.service.TaskQueue,
			Metadata:    request.service.Metadata,
		},
	}

	resp, err := m.persistence.CreateOrUpdateNexusIncomingService(ctx, &p.CreateOrUpdateNexusIncomingServiceRequest{
		LastKnownTableVersion: m.tableVersion,
		Entry:                 entry,
	})
	if err != nil {
		// TODO: special handling for table version conflicts? means table ownership changed
		return nil, err
	}

	entry.Version = resp.Version
	m.tableVersion++
	m.servicesByName[entry.Service.Name] = entry
	m.insertServiceLocked(entry)
	close(m.tableVersionChanged)
	m.tableVersionChanged = make(chan struct{})

	return &matchingservice.CreateOrUpdateNexusIncomingServiceResponse{Entry: entry}, nil
}

func (m *incomingServiceManager) insertServiceLocked(entry *persistencepb.NexusIncomingServiceEntry) {
	idx, found := slices.BinarySearchFunc(m.services, entry, func(a *persistencepb.NexusIncomingServiceEntry, b *persistencepb.NexusIncomingServiceEntry) int {
		return cmp.Compare(a.Id, b.Id)
	})

	if found {
		m.services[idx] = entry
	} else {
		m.services = slices.Insert(m.services, idx, entry)
	}
}

func (m *incomingServiceManager) DeleteNexusIncomingService(
	ctx context.Context,
	request *matchingservice.DeleteNexusIncomingServiceRequest,
) (*matchingservice.DeleteNexusIncomingServiceResponse, error) {
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
		return nil, fmt.Errorf("error deleting nexus incoming service with name %v: %w", request.Name, p.ErrNexusIncomingServiceNotFound)
	}

	err := m.persistence.DeleteNexusIncomingService(ctx, &p.DeleteNexusIncomingServiceRequest{
		LastKnownTableVersion: m.tableVersion,
		ServiceID:             service.Id,
	})
	if err != nil {
		return nil, err
	}

	m.tableVersion++
	delete(m.servicesByName, request.Name)
	m.services = slices.DeleteFunc(m.services, func(entry *persistencepb.NexusIncomingServiceEntry) bool {
		return entry.Service.Name == request.Name
	})
	close(m.tableVersionChanged)
	m.tableVersionChanged = make(chan struct{})

	return &matchingservice.DeleteNexusIncomingServiceResponse{}, nil
}

func (m *incomingServiceManager) ListNexusIncomingServices(
	ctx context.Context,
	request *matchingservice.ListNexusIncomingServicesRequest,
) (*matchingservice.ListNexusIncomingServicesResponse, chan struct{}, error) {
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

	startIdx := 0
	if request.NextPageToken != nil {
		nextServiceID := primitives.UUIDString(request.NextPageToken)

		startFound := false
		startIdx, startFound = slices.BinarySearchFunc(
			m.services,
			&persistencepb.NexusIncomingServiceEntry{Id: nextServiceID},
			func(a *persistencepb.NexusIncomingServiceEntry, b *persistencepb.NexusIncomingServiceEntry) int {
				return cmp.Compare(a.Id, b.Id)
			})

		if !startFound {
			return nil, nil, fmt.Errorf("could not find service indicated by nexus list incoming services next page token: %w", p.ErrNexusIncomingServiceNotFound)
		}
	}

	endIdx := startIdx + int(request.PageSize)
	if endIdx > len(m.services) {
		endIdx = len(m.services)
	}

	var nextPageToken []byte
	if endIdx < len(m.services) {
		nextPageToken = []byte(m.services[endIdx].Id)
	}

	resp := &matchingservice.ListNexusIncomingServicesResponse{
		TableVersion:  m.tableVersion,
		NextPageToken: nextPageToken,
		Entries:       m.services[startIdx:endIdx],
	}

	return resp, m.tableVersionChanged, nil
}

func (m *incomingServiceManager) loadServices(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()

	if m.hasLoadedServices.Load() {
		// check whether services were loaded while waiting for write lock
		return nil
	}

	// reset cached view since we will be paging from the start
	m.services = []*persistencepb.NexusIncomingServiceEntry{}
	m.servicesByName = make(map[string]*persistencepb.NexusIncomingServiceEntry)

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
		for _, entry := range resp.Entries {
			m.services = append(m.services, entry)
			m.servicesByName[entry.Service.Name] = entry
		}

		finishedPaging = pageToken == nil
	}

	m.hasLoadedServices.Store(finishedPaging)
	return ctx.Err()
}
