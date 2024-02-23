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
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	p "go.temporal.io/server/common/persistence"
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

	// incomingNexusServiceManager manages cache and persistence access for Nexus incoming services.
	// incomingNexusServiceManager contains a RWLock to enforce serial updates to prevent
	// nexus_incoming_services table version conflicts.
	//
	// incomingNexusServiceManager should only be used within matching service because it assumes
	// that it is running on the matching node that owns the nexus_incoming_services table.
	// There is no explicit listener for membership changes because table ownership changes
	// will be detected by version conflicts and eventually settle through retries.
	//
	// Not to be confused with persistence.NexusIncomingServiceManager which is responsible for
	// persistence-layer CRUD APIs for Nexus incoming services.
	incomingNexusServiceManager struct {
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
) *incomingNexusServiceManager {
	return &incomingNexusServiceManager{
		persistence:         persistence,
		tableVersionChanged: make(chan struct{}),
	}
}

func (m *incomingNexusServiceManager) CreateOrUpdateNexusIncomingService(
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

	previous, exists := m.servicesByName[request.service.Name]
	if !exists {
		previous = &persistencepb.NexusIncomingServiceEntry{
			Version: 0,
			Id:      uuid.NewString(),
			Service: &persistencepb.NexusIncomingService{
				Clock: hlc.Zero(request.clusterID),
			},
		}
	}

	if request.service.Version == 0 && exists {
		return nil, serviceerror.NewAlreadyExist(fmt.Sprintf("error creating Nexus incoming service. service with name %v already registered with version %v", previous.Service.Name, previous.Version))
	}
	if request.service.Version != 0 && !exists {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("error updating Nexus incoming service. service name %v not found", request.service.Name))
	}
	if request.service.Version != previous.Version {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("nexus incoming service version mismatch. received: %v expected %v", request.service.Version, previous.Version))
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
		return nil, err
	}

	entry.Version = resp.Version
	m.tableVersion++
	m.servicesByName[entry.Service.Name] = entry
	m.insertServiceLocked(entry)
	ch := m.tableVersionChanged
	m.tableVersionChanged = make(chan struct{})
	close(ch)

	return &matchingservice.CreateOrUpdateNexusIncomingServiceResponse{Entry: entry}, nil
}

func (m *incomingNexusServiceManager) insertServiceLocked(entry *persistencepb.NexusIncomingServiceEntry) {
	idx, found := slices.BinarySearchFunc(m.services, entry, func(a *persistencepb.NexusIncomingServiceEntry, b *persistencepb.NexusIncomingServiceEntry) int {
		return cmp.Compare(a.Id, b.Id)
	})

	if found {
		m.services[idx] = entry
	} else {
		m.services = slices.Insert(m.services, idx, entry)
	}
}

func (m *incomingNexusServiceManager) DeleteNexusIncomingService(
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
		return nil, serviceerror.NewNotFound(fmt.Sprintf("error deleting nexus incoming service with name: %v", request.Name))
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
	ch := m.tableVersionChanged
	m.tableVersionChanged = make(chan struct{})
	close(ch)

	return &matchingservice.DeleteNexusIncomingServiceResponse{}, nil
}

func (m *incomingNexusServiceManager) ListNexusIncomingServices(
	ctx context.Context,
	request *matchingservice.ListNexusIncomingServicesRequest,
) (*matchingservice.ListNexusIncomingServicesResponse, chan struct{}, error) {
	if request.LastKnownTableVersion > m.tableVersion {
		// indicates we may have lost table ownership, so need to reload from persistence
		m.hasLoadedServices.Store(false)
	}
	if !m.hasLoadedServices.Load() {
		if err := m.loadServices(ctx); err != nil {
			return nil, nil, fmt.Errorf("error loading nexus incoming services cache: %w", err)
		}
	}

	m.RLock()
	defer m.RUnlock()

	if request.LastKnownTableVersion != 0 && request.LastKnownTableVersion != m.tableVersion {
		return nil, nil, serviceerror.NewFailedPrecondition(fmt.Sprintf("nexus incoming service table version mismatch. received: %v expected %v", request.LastKnownTableVersion, m.tableVersion))
	}

	startIdx := 0
	if request.NextPageToken != nil {
		nextServiceID := string(request.NextPageToken)

		startFound := false
		startIdx, startFound = slices.BinarySearchFunc(
			m.services,
			&persistencepb.NexusIncomingServiceEntry{Id: nextServiceID},
			func(a *persistencepb.NexusIncomingServiceEntry, b *persistencepb.NexusIncomingServiceEntry) int {
				return cmp.Compare(a.Id, b.Id)
			})

		if !startFound {
			return nil, nil, serviceerror.NewFailedPrecondition("could not find service indicated by nexus list incoming services next page token")
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

func (m *incomingNexusServiceManager) loadServices(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()

	if m.hasLoadedServices.Load() {
		// check whether services were loaded while waiting for write lock
		return nil
	}

	// reset cached view since we will be paging from the start
	m.tableVersion = 0
	m.services = []*persistencepb.NexusIncomingServiceEntry{}
	m.servicesByName = make(map[string]*persistencepb.NexusIncomingServiceEntry)

	var pageToken []byte

	for ctx.Err() == nil {
		resp, err := m.persistence.ListNexusIncomingServices(ctx, &p.ListNexusIncomingServicesRequest{
			LastKnownTableVersion: m.tableVersion,
			NextPageToken:         pageToken,
			PageSize:              loadServicesPageSize,
		})
		if err != nil {
			if errors.Is(err, p.ErrNexusTableVersionConflict) {
				// indicates table was updated during paging, so reset and start from the beginning
				pageToken = nil
				m.tableVersion = 0
				m.services = []*persistencepb.NexusIncomingServiceEntry{}
				m.servicesByName = make(map[string]*persistencepb.NexusIncomingServiceEntry)
				continue
			}
			return err
		}

		pageToken = resp.NextPageToken
		m.tableVersion = resp.TableVersion
		for _, entry := range resp.Entries {
			m.services = append(m.services, entry)
			m.servicesByName[entry.Service.Name] = entry
		}

		if pageToken == nil {
			break
		}
	}

	m.hasLoadedServices.Store(ctx.Err() == nil)
	return ctx.Err()
}
