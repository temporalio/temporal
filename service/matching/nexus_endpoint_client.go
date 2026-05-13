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
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/headers"
	commonnexus "go.temporal.io/server/common/nexus"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/util"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	// It serializes updates via lookupCache's RWMutex to prevent nexus_endpoints table
	// version conflicts.
	//
	// nexusEndpointClient should only be used within matching service because it assumes
	// that it is running on the matching node that owns the nexus_endpoints table.
	// There is no explicit listener for membership changes because table ownership changes
	// will be detected by version conflicts and eventually settle through retries.
	nexusEndpointClient struct {
		hasLoadedEndpoints atomic.Bool

		// endpointEntries and tableVersionChanged are protected by lookupCache's RWMutex.
		endpointEntries     []*persistencespb.NexusEndpointEntry // sorted by ID to support pagination during ListNexusEndpoints
		tableVersionChanged chan struct{}

		refreshLock              sync.Mutex // protects refreshHandle which is updated whenever node gains/loses ownership
		refreshHandle            *goro.Handle
		endpointsRefreshInterval dynamicconfig.DurationPropertyFn

		persistence p.NexusEndpointManager
		lookupCache *commonnexus.EndpointLookupCache
	}
)

func newEndpointClient(
	endpointsRefreshInterval dynamicconfig.DurationPropertyFn,
	persistence p.NexusEndpointManager,
	lookupCache *commonnexus.EndpointLookupCache,
) *nexusEndpointClient {
	return &nexusEndpointClient{
		endpointsRefreshInterval: endpointsRefreshInterval,
		persistence:              persistence,
		lookupCache:              lookupCache,
		tableVersionChanged:      make(chan struct{}),
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

	m.lookupCache.Lock()
	defer m.lookupCache.Unlock()

	if _, _, exists := m.lookupCache.GetByNameLocked(request.spec.GetName()); exists {
		return nil, serviceerror.NewAlreadyExistsf("error creating Nexus endpoint. Endpoint with name %v already registered", request.spec.GetName())
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
		LastKnownTableVersion: m.lookupCache.VersionLocked(),
		Entry:                 entry,
	})
	if err != nil {
		return nil, err
	}

	entry.Version = resp.Version
	m.lookupCache.ApplyChangeLocked(nil, entry)
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

	m.lookupCache.Lock()
	defer m.lookupCache.Unlock()

	previous, _, exists := m.lookupCache.GetByIDLocked(request.endpointID)
	if !exists {
		return nil, serviceerror.NewNotFoundf("error updating Nexus endpoint. endpoint ID %v not found", request.endpointID)
	}

	if request.version != previous.Version {
		return nil, serviceerror.NewFailedPreconditionf("nexus endpoint version mismatch. received: %v expected %v", request.version, previous.Version)
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
		LastKnownTableVersion: m.lookupCache.VersionLocked(),
		Entry:                 entry,
	})
	if err != nil {
		return nil, err
	}

	entry.Version = resp.Version
	m.lookupCache.ApplyChangeLocked(previous, entry)
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

	m.lookupCache.Lock()
	defer m.lookupCache.Unlock()

	entry, _, ok := m.lookupCache.GetByIDLocked(request.Id)
	if !ok {
		return nil, serviceerror.NewNotFoundf("error deleting nexus endpoint with ID: %v", request.Id)
	}

	err := m.persistence.DeleteNexusEndpoint(ctx, &p.DeleteNexusEndpointRequest{
		LastKnownTableVersion: m.lookupCache.VersionLocked(),
		ID:                    entry.Id,
	})
	if err != nil {
		return nil, err
	}

	m.lookupCache.ApplyChangeLocked(entry, nil)
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
	m.lookupCache.RLock()
	if request.LastKnownTableVersion > m.lookupCache.VersionLocked() {
		// indicates we may have lost table ownership, so need to reload from persistence
		m.hasLoadedEndpoints.Store(false)
	}
	m.lookupCache.RUnlock()

	if !m.hasLoadedEndpoints.Load() {
		if err := m.loadEndpoints(ctx); err != nil {
			return nil, nil, fmt.Errorf("error loading nexus endpoints cache: %w", err)
		}
	}

	m.lookupCache.RLock()
	defer m.lookupCache.RUnlock()

	tableVersion := m.lookupCache.VersionLocked()
	if request.LastKnownTableVersion != 0 && request.LastKnownTableVersion != tableVersion {
		return nil, nil, serviceerror.NewFailedPreconditionf("nexus endpoints table version mismatch. received: %v expected %v", request.LastKnownTableVersion, tableVersion)
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
		TableVersion:  tableVersion,
		NextPageToken: nextPageToken,
		Entries:       slices.Clone(m.endpointEntries[startIdx:endIdx]),
	}

	return resp, m.tableVersionChanged, nil
}

func (m *nexusEndpointClient) loadEndpoints(ctx context.Context) error {
	m.lookupCache.Lock()
	defer m.lookupCache.Unlock()

	if m.hasLoadedEndpoints.Load() {
		// check whether endpoints were loaded while waiting for write lock
		return nil
	}

	// reset cached view since we will be paging from the start
	m.resetCacheStateLocked()

	var pageToken []byte
	var tableVersion int64

	for ctx.Err() == nil {
		resp, err := m.persistence.ListNexusEndpoints(ctx, &p.ListNexusEndpointsRequest{
			LastKnownTableVersion: tableVersion,
			NextPageToken:         pageToken,
			PageSize:              loadEndpointsPageSize,
		})
		if err != nil {
			if errors.Is(err, p.ErrNexusTableVersionConflict) {
				// indicates table was updated during paging, so reset and start from the beginning
				m.resetCacheStateLocked()
				pageToken = nil
				tableVersion = 0
				continue
			}
			return err
		}

		pageToken = resp.NextPageToken
		tableVersion = resp.TableVersion
		m.endpointEntries = append(m.endpointEntries, resp.Entries...)

		if len(pageToken) == 0 {
			break
		}
	}

	m.hasLoadedEndpoints.Store(ctx.Err() == nil)
	if ctx.Err() == nil {
		m.lookupCache.ReplaceAllLocked(tableVersion, m.endpointEntries)
	}
	return ctx.Err()
}

func (m *nexusEndpointClient) resetCacheStateLocked() {
	m.endpointEntries = []*persistencespb.NexusEndpointEntry{}
	m.lookupCache.ClearLocked()
}

// notifyOwnershipChanged starts or stops a background routine which watches the Nexus endpoints table version for
// changes. This is only expected to be called from matchingEngineImpl.notifyNexusEndpointsOwnershipChange()
func (m *nexusEndpointClient) notifyOwnershipChanged(isOwner bool) {
	var oldHandle *goro.Handle

	m.refreshLock.Lock()
	if isOwner && m.refreshHandle == nil {
		// Just acquired ownership. Start refresh loop on table version to catch any updates from previous owner.
		backgroundCtx := headers.SetCallerInfo(
			context.Background(),
			headers.SystemBackgroundHighCallerInfo,
		)
		m.refreshHandle = goro.NewHandle(backgroundCtx)
		m.refreshHandle.Go(m.refreshTableVersion)
	} else if !isOwner && m.refreshHandle != nil {
		// Just lost ownership. Stop table version refresh loop.
		oldHandle = m.refreshHandle
		m.refreshHandle = nil
	}
	m.refreshLock.Unlock()

	if oldHandle != nil {
		oldHandle.Cancel()
		<-oldHandle.Done()
	}
}

func (m *nexusEndpointClient) refreshTableVersion(ctx context.Context) error {
	for ctx.Err() == nil {
		m.checkTableVersion(ctx)
		util.InterruptibleSleep(ctx, backoff.Jitter(m.endpointsRefreshInterval(), 0.2))
	}
	return ctx.Err()
}

func (m *nexusEndpointClient) checkTableVersion(ctx context.Context) {
	// Acquire lock to make sure we are not in the middle of an update.
	m.lookupCache.Lock()
	defer m.lookupCache.Unlock()

	resp, err := m.persistence.ListNexusEndpoints(ctx, &p.ListNexusEndpointsRequest{
		LastKnownTableVersion: 0,
		PageSize:              0,
	})
	if err != nil || resp.TableVersion != m.lookupCache.VersionLocked() {
		m.hasLoadedEndpoints.Store(false)
		ch := m.tableVersionChanged
		m.tableVersionChanged = make(chan struct{})
		close(ch)
	}
}
