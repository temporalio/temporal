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
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	commonnexus "go.temporal.io/server/common/nexus"
	p "go.temporal.io/server/common/persistence"
)

const (
	// loadServicesPageSize is the page size to use when initially loading services from persistence
	loadServicesPageSize = 100
)

type (
	internalUpdateRequest struct {
		serviceID   string
		version     int64
		spec        *nexuspb.IncomingServiceSpec
		namespaceID string
		clusterID   int64
		timeSource  clock.TimeSource
	}

	internalCreateRequest struct {
		spec        *nexuspb.IncomingServiceSpec
		namespaceID string
		clusterID   int64
		timeSource  clock.TimeSource
	}

	// nexusIncomingServiceClient manages cache and persistence access for Nexus incoming services.
	// nexusIncomingServiceClient contains a RWLock to enforce serial updates to prevent
	// nexus_incoming_services table version conflicts.
	//
	// nexusIncomingServiceClient should only be used within matching service because it assumes
	// that it is running on the matching node that owns the nexus_incoming_services table.
	// There is no explicit listener for membership changes because table ownership changes
	// will be detected by version conflicts and eventually settle through retries.
	nexusIncomingServiceClient struct {
		hasLoadedServices atomic.Bool

		sync.RWMutex        // protects tableVersion, services, servicesByID, servicesByName, and tableVersionChanged
		tableVersion        int64
		services            []*persistencepb.NexusIncomingServiceEntry // sorted by serviceID to support pagination during ListNexusIncomingServices
		servicesByID        map[string]*persistencepb.NexusIncomingServiceEntry
		servicesByName      map[string]*persistencepb.NexusIncomingServiceEntry
		tableVersionChanged chan struct{}

		persistence p.NexusIncomingServiceManager
	}
)

func newIncomingServiceClient(
	persistence p.NexusIncomingServiceManager,
) *nexusIncomingServiceClient {
	return &nexusIncomingServiceClient{
		persistence:         persistence,
		tableVersionChanged: make(chan struct{}),
	}
}

func (m *nexusIncomingServiceClient) CreateNexusIncomingService(
	ctx context.Context,
	request *internalCreateRequest,
) (*matchingservice.CreateNexusIncomingServiceResponse, error) {
	if !m.hasLoadedServices.Load() {
		// Services must be loaded into memory before Create so we know whether this service name is in use and that we
		// have the last known table version to update persistence.
		if err := m.loadServices(ctx); err != nil {
			return nil, fmt.Errorf("error loading nexus incoming services cache: %w", err)
		}
	}

	m.Lock()
	defer m.Unlock()

	if _, exists := m.servicesByName[request.spec.GetName()]; exists {
		return nil, serviceerror.NewAlreadyExist(fmt.Sprintf("error creating Nexus incoming service. service with name %v already registered", request.spec.GetName()))
	}

	entry := &persistencepb.NexusIncomingServiceEntry{
		Version: 0,
		Id:      uuid.NewString(),
		Service: &persistencepb.NexusIncomingService{
			Clock:       hlc.Zero(request.clusterID),
			Spec:        request.spec,
			CreatedTime: timestamppb.New(request.timeSource.Now().UTC()),
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
	m.servicesByID[entry.Id] = entry
	m.servicesByName[entry.Service.Spec.Name] = entry
	m.insertServiceLocked(entry)
	ch := m.tableVersionChanged
	m.tableVersionChanged = make(chan struct{})
	close(ch)

	return &matchingservice.CreateNexusIncomingServiceResponse{
		Service: commonnexus.IncomingServicePersistedEntryToExternalAPI(entry),
	}, nil
}

func (m *nexusIncomingServiceClient) UpdateNexusIncomingService(
	ctx context.Context,
	request *internalUpdateRequest,
) (*matchingservice.UpdateNexusIncomingServiceResponse, error) {
	if !m.hasLoadedServices.Load() {
		// Services must be loaded into memory before Update, since we need to check the previous entry and we need the
		// last known table version to update persistence.
		if err := m.loadServices(ctx); err != nil {
			return nil, fmt.Errorf("error loading nexus incoming services cache: %w", err)
		}
	}

	m.Lock()
	defer m.Unlock()

	previous, exists := m.servicesByID[request.serviceID]
	if !exists {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("error updating Nexus incoming service. service ID %v not found", request.serviceID))
	}

	if request.version != previous.Version {
		return nil, serviceerror.NewFailedPrecondition(fmt.Sprintf("nexus incoming service version mismatch. received: %v expected %v", request.version, previous.Version))
	}

	entry := &persistencepb.NexusIncomingServiceEntry{
		Version: previous.Version,
		Id:      previous.Id,
		Service: &persistencepb.NexusIncomingService{
			Clock: hlc.Next(previous.Service.Clock, request.timeSource),
			Spec:  request.spec,
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
	m.servicesByID[entry.Id] = entry
	m.servicesByName[entry.Service.Spec.Name] = entry
	m.insertServiceLocked(entry)
	ch := m.tableVersionChanged
	m.tableVersionChanged = make(chan struct{})
	close(ch)

	return &matchingservice.UpdateNexusIncomingServiceResponse{
		Service: commonnexus.IncomingServicePersistedEntryToExternalAPI(entry),
	}, nil
}

func (m *nexusIncomingServiceClient) insertServiceLocked(entry *persistencepb.NexusIncomingServiceEntry) {
	idx, found := slices.BinarySearchFunc(m.services, entry, func(a *persistencepb.NexusIncomingServiceEntry, b *persistencepb.NexusIncomingServiceEntry) int {
		return bytes.Compare([]byte(a.Id), []byte(b.Id))
	})

	if found {
		m.services[idx] = entry
	} else {
		m.services = slices.Insert(m.services, idx, entry)
	}
}

func (m *nexusIncomingServiceClient) DeleteNexusIncomingService(
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

	entry, ok := m.servicesByID[request.Id]
	if !ok {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("error deleting nexus incoming service with ID: %v", request.Id))
	}

	err := m.persistence.DeleteNexusIncomingService(ctx, &p.DeleteNexusIncomingServiceRequest{
		LastKnownTableVersion: m.tableVersion,
		ServiceID:             entry.Id,
	})
	if err != nil {
		return nil, err
	}

	m.tableVersion++
	delete(m.servicesByID, request.Id)
	delete(m.servicesByName, entry.Service.Spec.Name)
	m.services = slices.DeleteFunc(m.services, func(entry *persistencepb.NexusIncomingServiceEntry) bool {
		return entry.Id == request.Id
	})
	ch := m.tableVersionChanged
	m.tableVersionChanged = make(chan struct{})
	close(ch)

	return &matchingservice.DeleteNexusIncomingServiceResponse{}, nil
}

func (m *nexusIncomingServiceClient) ListNexusIncomingServices(
	ctx context.Context,
	request *matchingservice.ListNexusIncomingServicesRequest,
) (*matchingservice.ListNexusIncomingServicesResponse, chan struct{}, error) {
	m.RLock()
	if request.LastKnownTableVersion > m.tableVersion {
		// indicates we may have lost table ownership, so need to reload from persistence
		m.hasLoadedServices.Store(false)
	}
	m.RUnlock()

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
				return bytes.Compare([]byte(a.Id), []byte(b.Id))
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

	services := make([]*nexuspb.IncomingService, endIdx-startIdx)
	for i := 0; i < endIdx-startIdx; i++ {
		entry := m.services[i+startIdx]
		services[i] = commonnexus.IncomingServicePersistedEntryToExternalAPI(entry)
	}

	resp := &matchingservice.ListNexusIncomingServicesResponse{
		TableVersion:  m.tableVersion,
		NextPageToken: nextPageToken,
		Services:      services,
	}

	return resp, m.tableVersionChanged, nil
}

func (m *nexusIncomingServiceClient) loadServices(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()

	if m.hasLoadedServices.Load() {
		// check whether services were loaded while waiting for write lock
		return nil
	}

	// reset cached view since we will be paging from the start
	m.resetCacheStateLocked()

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
				m.resetCacheStateLocked()
				pageToken = nil
				continue
			}
			return err
		}

		pageToken = resp.NextPageToken
		m.tableVersion = resp.TableVersion
		for _, entry := range resp.Entries {
			m.services = append(m.services, entry)
			m.servicesByID[entry.Id] = entry
			m.servicesByName[entry.Service.Spec.Name] = entry
		}

		if pageToken == nil {
			break
		}
	}

	m.hasLoadedServices.Store(ctx.Err() == nil)
	return ctx.Err()
}

func (m *nexusIncomingServiceClient) resetCacheStateLocked() {
	m.tableVersion = 0
	m.services = []*persistencepb.NexusIncomingServiceEntry{}
	m.servicesByID = make(map[string]*persistencepb.NexusIncomingServiceEntry)
	m.servicesByName = make(map[string]*persistencepb.NexusIncomingServiceEntry)
}
