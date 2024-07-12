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
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	p "go.temporal.io/server/common/persistence"
)

const (
	// loadEndpointsPageSize is the page size to use when initially loading endpoints from persistence
	loadEndpointsPageSize = 100
)

type (
	internalUpdateNexusEndpointRequest struct {
		endpointID string
		version    int64
		spec       *persistencespb.NexusEndpointSpec
		clusterID  int64
		timeSource clock.TimeSource
	}

	internalCreateNexusEndpointRequest struct {
		spec       *persistencespb.NexusEndpointSpec
		clusterID  int64
		timeSource clock.TimeSource
	}

	// nexusEndpointClient manages cache and persistence access for Nexus endpoints.
	// nexusEndpointClient contains a RWLock to enforce serial updates to prevent
	// nexus_endpoints table version conflicts.
	//
	// nexusEndpointClient should only be used within matching service because it assumes
	// that it is running on the matching node that owns the nexus_endpoints table.
	// There is no explicit listener for membership changes because table ownership changes
	// will be detected by version conflicts and eventually settle through retries.
	nexusEndpointClient struct {
		hasLoadedEndpoints atomic.Bool

		sync.RWMutex        // protects tableVersion, endpoints, endpointsByID, endpointsByName, and tableVersionChanged
		tableVersion        int64
		endpointEntries     []*persistencespb.NexusEndpointEntry // sorted by ID to support pagination during ListNexusEndpoints
		endpointsByID       map[string]*persistencespb.NexusEndpointEntry
		endpointsByName     map[string]*persistencespb.NexusEndpointEntry
		tableVersionChanged chan struct{}

		persistence p.NexusEndpointManager
	}
)

func newEndpointClient(
	persistence p.NexusEndpointManager,
) *nexusEndpointClient {
	return &nexusEndpointClient{
		persistence:         persistence,
		tableVersionChanged: make(chan struct{}),
	}
}

func (m *nexusEndpointClient) CreateNexusEndpoint(
	ctx context.Context,
	request *internalCreateNexusEndpointRequest,
) (*matchingservice.CreateNexusEndpointResponse, error) {
	if !m.hasLoadedEndpoints.Load() {
		// Endpoints must be loaded into memory before Create so we know whether this endpoint name is in use and that we
		// have the last known table version to update persistence.
		if err := m.loadEndpoints(ctx); err != nil {
			return nil, fmt.Errorf("error loading nexus endpoints cache: %w", err)
		}
	}

	m.Lock()
	defer m.Unlock()

	if _, exists := m.endpointsByName[request.spec.GetName()]; exists {
		return nil, serviceerror.NewAlreadyExist(fmt.Sprintf("error creating Nexus endpoint. Endpoint with name %v already registered", request.spec.GetName()))
	}

	entry := &persistencespb.NexusEndpointEntry{
		Version: 0,
		Id:      uuid.NewString(),
		Endpoint: &persistencespb.NexusEndpoint{
			Clock:       hlc.Zero(request.clusterID),
			Spec:        request.spec,
			CreatedTime: timestamppb.New(request.timeSource.Now().UTC()),
		},
	}

	resp, err := m.persistence.CreateOrUpdateNexusEndpoint(ctx, &p.CreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: m.tableVersion,
		Entry:                 entry,
	})
	if err != nil {
		return nil, err
	}

	entry.Version = resp.Version
	m.tableVersion++
	m.endpointsByID[entry.Id] = entry
	m.endpointsByName[entry.Endpoint.Spec.Name] = entry
	m.insertEndpointLocked(entry)
	ch := m.tableVersionChanged
	m.tableVersionChanged = make(chan struct{})
	close(ch)

	return &matchingservice.CreateNexusEndpointResponse{
		Entry: entry,
	}, nil
}

func (m *nexusEndpointClient) UpdateNexusEndpoint(
	ctx context.Context,
	request *internalUpdateNexusEndpointRequest,
) (*matchingservice.UpdateNexusEndpointResponse, error) {
	if !m.hasLoadedEndpoints.Load() {
		// Endpoints must be loaded into memory before Update, since we need to check the previous entry and we need the
		// last known table version to update persistence.
		if err := m.loadEndpoints(ctx); err != nil {
			return nil, fmt.Errorf("error loading nexus endpoint cache: %w", err)
		}
	}

	m.Lock()
	defer m.Unlock()

	previous, exists := m.endpointsByID[request.endpointID]
	if !exists {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("error updating Nexus endpoint. endpoint ID %v not found", request.endpointID))
	}

	if request.version != previous.Version {
		return nil, serviceerror.NewFailedPrecondition(fmt.Sprintf("nexus endpoint version mismatch. received: %v expected %v", request.version, previous.Version))
	}

	entry := &persistencespb.NexusEndpointEntry{
		Version: previous.Version,
		Id:      previous.Id,
		Endpoint: &persistencespb.NexusEndpoint{
			Clock: hlc.Next(previous.Endpoint.Clock, request.timeSource),
			Spec:  request.spec,
		},
	}

	resp, err := m.persistence.CreateOrUpdateNexusEndpoint(ctx, &p.CreateOrUpdateNexusEndpointRequest{
		LastKnownTableVersion: m.tableVersion,
		Entry:                 entry,
	})
	if err != nil {
		return nil, err
	}

	entry.Version = resp.Version
	m.tableVersion++
	m.endpointsByID[entry.Id] = entry
	m.endpointsByName[entry.Endpoint.Spec.Name] = entry
	m.insertEndpointLocked(entry)
	ch := m.tableVersionChanged
	m.tableVersionChanged = make(chan struct{})
	close(ch)

	return &matchingservice.UpdateNexusEndpointResponse{
		Entry: entry,
	}, nil
}

func (m *nexusEndpointClient) insertEndpointLocked(entry *persistencespb.NexusEndpointEntry) {
	idx, found := slices.BinarySearchFunc(m.endpointEntries, entry, func(a *persistencespb.NexusEndpointEntry, b *persistencespb.NexusEndpointEntry) int {
		return bytes.Compare([]byte(a.Id), []byte(b.Id))
	})

	if found {
		m.endpointEntries[idx] = entry
	} else {
		m.endpointEntries = slices.Insert(m.endpointEntries, idx, entry)
	}
}

func (m *nexusEndpointClient) DeleteNexusEndpoint(
	ctx context.Context,
	request *matchingservice.DeleteNexusEndpointRequest,
) (*matchingservice.DeleteNexusEndpointResponse, error) {
	if !m.hasLoadedEndpoints.Load() {
		// Endpoints must be loaded into memory before deletion so that the endpoint UUID can be looked up
		if err := m.loadEndpoints(ctx); err != nil {
			return nil, fmt.Errorf("error loading nexus endpoints cache: %w", err)
		}
	}

	m.Lock()
	defer m.Unlock()

	entry, ok := m.endpointsByID[request.Id]
	if !ok {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("error deleting nexus endpoint with ID: %v", request.Id))
	}

	err := m.persistence.DeleteNexusEndpoint(ctx, &p.DeleteNexusEndpointRequest{
		LastKnownTableVersion: m.tableVersion,
		ID:                    entry.Id,
	})
	if err != nil {
		return nil, err
	}

	m.tableVersion++
	delete(m.endpointsByID, request.Id)
	delete(m.endpointsByName, entry.Endpoint.Spec.Name)
	m.endpointEntries = slices.DeleteFunc(m.endpointEntries, func(entry *persistencespb.NexusEndpointEntry) bool {
		return entry.Id == request.Id
	})
	ch := m.tableVersionChanged
	m.tableVersionChanged = make(chan struct{})
	close(ch)

	return &matchingservice.DeleteNexusEndpointResponse{}, nil
}

func (m *nexusEndpointClient) ListNexusEndpoints(
	ctx context.Context,
	request *matchingservice.ListNexusEndpointsRequest,
) (*matchingservice.ListNexusEndpointsResponse, chan struct{}, error) {
	m.RLock()
	if request.LastKnownTableVersion > m.tableVersion {
		// indicates we may have lost table ownership, so need to reload from persistence
		m.hasLoadedEndpoints.Store(false)
	}
	m.RUnlock()

	if !m.hasLoadedEndpoints.Load() {
		if err := m.loadEndpoints(ctx); err != nil {
			return nil, nil, fmt.Errorf("error loading nexus endpoints cache: %w", err)
		}
	}

	m.RLock()
	defer m.RUnlock()

	if request.LastKnownTableVersion != 0 && request.LastKnownTableVersion != m.tableVersion {
		return nil, nil, serviceerror.NewFailedPrecondition(fmt.Sprintf("nexus endpoints table version mismatch. received: %v expected %v", request.LastKnownTableVersion, m.tableVersion))
	}

	startIdx := 0
	if request.NextPageToken != nil {
		nextEndpointID := string(request.NextPageToken)

		startFound := false
		startIdx, startFound = slices.BinarySearchFunc(
			m.endpointEntries,
			&persistencespb.NexusEndpointEntry{Id: nextEndpointID},
			func(a *persistencespb.NexusEndpointEntry, b *persistencespb.NexusEndpointEntry) int {
				return bytes.Compare([]byte(a.Id), []byte(b.Id))
			})

		if !startFound {
			return nil, nil, serviceerror.NewFailedPrecondition("could not find endpoint indicated by nexus list endpoints next page token")
		}
	}

	endIdx := min(startIdx+int(request.PageSize), len(m.endpointEntries))

	var nextPageToken []byte
	if endIdx < len(m.endpointEntries) {
		nextPageToken = []byte(m.endpointEntries[endIdx].Id)
	}

	resp := &matchingservice.ListNexusEndpointsResponse{
		TableVersion:  m.tableVersion,
		NextPageToken: nextPageToken,
		Entries:       slices.Clone(m.endpointEntries[startIdx:endIdx]),
	}

	return resp, m.tableVersionChanged, nil
}

func (m *nexusEndpointClient) loadEndpoints(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()

	if m.hasLoadedEndpoints.Load() {
		// check whether endpoints were loaded while waiting for write lock
		return nil
	}

	// reset cached view since we will be paging from the start
	m.resetCacheStateLocked()

	var pageToken []byte

	for ctx.Err() == nil {
		resp, err := m.persistence.ListNexusEndpoints(ctx, &p.ListNexusEndpointsRequest{
			LastKnownTableVersion: m.tableVersion,
			NextPageToken:         pageToken,
			PageSize:              loadEndpointsPageSize,
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
			m.endpointEntries = append(m.endpointEntries, entry)
			m.endpointsByID[entry.Id] = entry
			m.endpointsByName[entry.Endpoint.Spec.Name] = entry
		}

		if len(pageToken) == 0 {
			break
		}
	}

	m.hasLoadedEndpoints.Store(ctx.Err() == nil)
	return ctx.Err()
}

func (m *nexusEndpointClient) resetCacheStateLocked() {
	m.tableVersion = 0
	m.endpointEntries = []*persistencespb.NexusEndpointEntry{}
	m.endpointsByID = make(map[string]*persistencespb.NexusEndpointEntry)
	m.endpointsByName = make(map[string]*persistencespb.NexusEndpointEntry)
}
