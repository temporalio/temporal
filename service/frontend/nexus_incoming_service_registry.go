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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nexus_incoming_service_registry_mock.go

package frontend

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/headers"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/internal/goro"
)

const (
	// loadServicesPageSize is the page size to use when initially loading services from persistence
	loadServicesPageSize = 100
)

type (
	// NexusIncomingServiceRegistry is the frontend interface for interacting with Nexus incoming service CRUD APIs.
	// A separate interface definition is required only for mockgen to work so operator_handler_test can be built.
	NexusIncomingServiceRegistry interface {
		Start()
		Stop()
		CreateOrUpdateNexusIncomingService(context.Context, *operatorservice.CreateOrUpdateNexusIncomingServiceRequest) (*operatorservice.CreateOrUpdateNexusIncomingServiceResponse, error)
		DeleteNexusIncomingService(context.Context, *operatorservice.DeleteNexusIncomingServiceRequest) (*operatorservice.DeleteNexusIncomingServiceResponse, error)
		GetNexusIncomingService(context.Context, *operatorservice.GetNexusIncomingServiceRequest) (*operatorservice.GetNexusIncomingServiceResponse, error)
		ListNexusIncomingServices(context.Context, *operatorservice.ListNexusIncomingServicesRequest) (*operatorservice.ListNexusIncomingServicesResponse, error)
	}

	registry struct {
		status           int32
		serviceDataReady *future.FutureImpl[struct{}]

		sync.RWMutex     // protects tableVersion, services, serviceIDsSorted, and servicesByName
		tableVersion     int64
		services         []*nexus.IncomingService // must maintain same sort order as serviceIDsSorted
		serviceIDsSorted []string                 // parallel array to services of sorted serviceIDs to support pagination during ListNexusIncomingServices
		servicesByName   map[string]*nexus.IncomingService

		refreshPoller                  *goro.Handle
		refreshServicesLongPollTimeout dynamicconfig.DurationPropertyFn
		refreshServicesLongPollMinWait dynamicconfig.DurationPropertyFn
		refreshServicesRetryPolicy     backoff.RetryPolicy

		matchingClient matchingservice.MatchingServiceClient
		persistence    p.NexusIncomingServiceManager
	}
)

func newNexusIncomingServiceRegistry(
	listNexusIncomingServicesLongPollTimeout dynamicconfig.DurationPropertyFn,
	listNexusIncomingServicesLongPollMinWait dynamicconfig.DurationPropertyFn,
	listNexusIncomingServicesRetryPolicy backoff.RetryPolicy,
	matchingClient matchingservice.MatchingServiceClient,
	persistence p.NexusIncomingServiceManager,
) NexusIncomingServiceRegistry {
	return &registry{
		status:                         common.DaemonStatusInitialized,
		serviceDataReady:               future.NewFuture[struct{}](),
		refreshServicesLongPollTimeout: listNexusIncomingServicesLongPollTimeout,
		refreshServicesLongPollMinWait: listNexusIncomingServicesLongPollMinWait,
		refreshServicesRetryPolicy:     listNexusIncomingServicesRetryPolicy,
		matchingClient:                 matchingClient,
		persistence:                    persistence,
	}
}

func (r *registry) Start() {
	if !atomic.CompareAndSwapInt32(&r.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	refreshCtx := headers.SetCallerInfo(
		context.Background(),
		headers.SystemBackgroundCallerInfo,
	)

	r.refreshPoller = goro.NewHandle(refreshCtx).Go(r.refreshServices)
}

func (r *registry) Stop() {
	if !atomic.CompareAndSwapInt32(&r.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	r.refreshPoller.Cancel()
	<-r.refreshPoller.Done()
}

func (r *registry) CreateOrUpdateNexusIncomingService(
	ctx context.Context,
	request *operatorservice.CreateOrUpdateNexusIncomingServiceRequest,
) (*operatorservice.CreateOrUpdateNexusIncomingServiceResponse, error) {
	resp, err := r.matchingClient.CreateOrUpdateNexusIncomingService(ctx, &matchingservice.CreateOrUpdateNexusIncomingServiceRequest{
		Service: request.Service,
	})
	if err != nil {
		return nil, err
	}

	return &operatorservice.CreateOrUpdateNexusIncomingServiceResponse{Service: entryToService(resp.Entry)}, nil
}

func (r *registry) DeleteNexusIncomingService(
	ctx context.Context,
	request *operatorservice.DeleteNexusIncomingServiceRequest,
) (*operatorservice.DeleteNexusIncomingServiceResponse, error) {
	_, err := r.matchingClient.DeleteNexusIncomingService(ctx, &matchingservice.DeleteNexusIncomingServiceRequest{
		Name: request.Name,
	})
	if err != nil {
		return nil, err
	}

	return &operatorservice.DeleteNexusIncomingServiceResponse{}, nil
}

func (r *registry) GetNexusIncomingService(
	ctx context.Context,
	request *operatorservice.GetNexusIncomingServiceRequest,
) (*operatorservice.GetNexusIncomingServiceResponse, error) {
	if !r.isTableVersionCurrent(ctx) {
		r.invalidateCachedServices()
		if err := r.fetchServices(ctx); err != nil {
			return nil, fmt.Errorf("error loading nexus incoming services frontend cache: %w", err)
		}
	}

	r.RLock()
	defer r.RUnlock()

	service, ok := r.servicesByName[request.Name]
	if !ok {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("frontend could not find cached service with name %v", request.Name))
	}

	return &operatorservice.GetNexusIncomingServiceResponse{Service: service}, nil
}

func (r *registry) ListNexusIncomingServices(
	ctx context.Context,
	request *operatorservice.ListNexusIncomingServicesRequest,
) (*operatorservice.ListNexusIncomingServicesResponse, error) {
	if !r.isTableVersionCurrent(ctx) {
		r.invalidateCachedServices()
		if err := r.fetchServices(ctx); err != nil {
			return nil, fmt.Errorf("error loading nexus incoming services frontend cache: %w", err)
		}
	}

	r.RLock()
	defer r.RUnlock()

	startIdx := 0
	if request.NextPageToken != nil {
		nextServiceID := string(request.NextPageToken)

		startFound := false
		startIdx, startFound = slices.BinarySearch(r.serviceIDsSorted, nextServiceID)

		if !startFound {
			return nil, serviceerror.NewFailedPrecondition("could not find service indicated by nexus list incoming services next page token")
		}
	}

	endIdx := startIdx + int(request.PageSize)
	if endIdx > len(r.serviceIDsSorted) {
		endIdx = len(r.serviceIDsSorted)
	}

	var nextPageToken []byte
	if endIdx < len(r.services) {
		nextPageToken = []byte(r.serviceIDsSorted[endIdx])
	}

	return &operatorservice.ListNexusIncomingServicesResponse{
		NextPageToken: nextPageToken,
		Services:      r.services[startIdx:endIdx],
	}, nil
}

func (r *registry) isTableVersionCurrent(ctx context.Context) bool {
	actualTableVersion, err := r.persistence.GetNexusIncomingServicesTableVersion(ctx)
	if err != nil {
		return false
	}

	r.RLock()
	defer r.RUnlock()
	return r.tableVersion == actualTableVersion
}

func (r *registry) fetchServices(ctx context.Context) error {
	var currentPageToken []byte
	pagingWasReset := false // used to ensure write lock is only acquired once

	for ctx.Err() == nil {
		r.RLock()
		requestTableVersion := r.tableVersion
		r.RUnlock()

		shouldWait := currentPageToken == nil && requestTableVersion != 0

		resp, err := r.matchingClient.ListNexusIncomingServices(ctx, &matchingservice.ListNexusIncomingServicesRequest{
			LastKnownTableVersion: requestTableVersion,
			NextPageToken:         currentPageToken,
			PageSize:              loadServicesPageSize,
			Wait:                  shouldWait,
		})
		if err != nil {
			if errors.Is(err, p.ErrNexusTableVersionConflict) {
				// indicates table was updated during paging, so reset and start from the beginning
				currentPageToken = nil
				pagingWasReset = true
				r.invalidateCachedServicesLocked()
				continue
			}
			// for all other errors, fall back to reading directly from persistence
			if currentPageToken == nil && !pagingWasReset {
				r.Lock()
			}
			persistenceErr := r.loadServicesLocked(ctx)
			if persistenceErr == nil {
				r.setServiceDataReady(persistenceErr)
			}
			r.Unlock()
			return persistenceErr
		}

		if shouldWait && requestTableVersion == resp.TableVersion {
			// long poll returned with no changes
			return nil
		}

		if currentPageToken == nil && !pagingWasReset {
			// acquire lock once upon getting first page and release once all pagination is complete
			r.Lock()
		}

		if shouldWait {
			// long poll returned new data, starting with first page
			r.invalidateCachedServicesLocked()
		}

		currentPageToken = resp.NextPageToken
		r.tableVersion = resp.TableVersion
		r.updateCachedServicesLocked(resp.Entries)

		if resp.NextPageToken == nil {
			break
		}
	}

	r.setServiceDataReady(ctx.Err())
	r.Unlock()
	return ctx.Err()
}

// Loads all incoming services into memory from persistence.
// Used as a fallback if matching service is unavailable.
// Always replaces entire in-memory view by paging from the start.
func (r *registry) loadServicesLocked(ctx context.Context) error {
	r.invalidateCachedServicesLocked()

	var currentPageToken []byte

	for ctx.Err() == nil {
		resp, err := r.persistence.ListNexusIncomingServices(ctx, &p.ListNexusIncomingServicesRequest{
			LastKnownTableVersion: r.tableVersion,
			NextPageToken:         currentPageToken,
			PageSize:              loadServicesPageSize,
		})
		if err != nil {
			if errors.Is(err, p.ErrNexusTableVersionConflict) {
				// indicates table was updated during paging, so reset and start from the beginning
				currentPageToken = nil
				r.invalidateCachedServicesLocked()
				continue
			}
			return err
		}

		currentPageToken = resp.NextPageToken
		r.tableVersion = resp.TableVersion
		r.updateCachedServicesLocked(resp.Entries)

		if resp.NextPageToken == nil {
			break
		}
	}

	return ctx.Err()
}

func (r *registry) refreshServices(ctx context.Context) error {
	refreshFn := func(ctx context.Context) error {
		_, err := r.serviceDataReady.Get(ctx)
		if err != nil {
			return err
		}

		return r.fetchServices(ctx)
	}

	minWaitTime := r.refreshServicesLongPollMinWait()

	for ctx.Err() == nil {
		start := time.Now()
		_ = backoff.ThrottleRetryContext(ctx, refreshFn, r.refreshServicesRetryPolicy, nil)
		elapsed := time.Since(start)

		// In general, we want to start a new call immediately on completion of the previous
		// one. But if the remote is broken and returns success immediately, we might end up
		// spinning. So enforce a minimum wait time that increases as long as we keep getting
		// very fast replies.
		if elapsed < minWaitTime {
			common.InterruptibleSleep(ctx, minWaitTime-elapsed)
			// Don't let this get near our call timeout, otherwise we can't tell the difference
			// between a fast reply and a timeout.
			minWaitTime = min(minWaitTime*2, r.refreshServicesLongPollTimeout()/2)
		} else {
			minWaitTime = r.refreshServicesLongPollMinWait()
		}
	}

	return ctx.Err()
}

func (r *registry) setServiceDataReady(futureErr error) {
	if !r.serviceDataReady.Ready() {
		r.serviceDataReady.Set(struct{}{}, futureErr)
	}
}

func (r *registry) updateCachedServicesLocked(entries []*persistencepb.NexusIncomingServiceEntry) {
	// the following assumes entries are already sorted by serviceID
	r.services = slices.Grow(r.services, len(entries))
	r.serviceIDsSorted = slices.Grow(r.serviceIDsSorted, len(entries))
	for _, entry := range entries {
		service := entryToService(entry)
		r.services = append(r.services, service)
		r.servicesByName[entry.Service.Name] = service
		r.serviceIDsSorted = append(r.serviceIDsSorted, entry.Id)
	}
}

func (r *registry) invalidateCachedServices() {
	r.Lock()
	defer r.Unlock()
	r.invalidateCachedServicesLocked()
}

func (r *registry) invalidateCachedServicesLocked() {
	r.tableVersion = 0
	r.services = []*nexus.IncomingService{}
	r.serviceIDsSorted = []string{}
	r.servicesByName = make(map[string]*nexus.IncomingService)
}

func entryToService(entry *persistencepb.NexusIncomingServiceEntry) *nexus.IncomingService {
	return &nexus.IncomingService{
		Version:   entry.Version,
		Name:      entry.Service.Name,
		Namespace: entry.Service.NamespaceId,
		TaskQueue: entry.Service.TaskQueue,
		Metadata:  entry.Service.Metadata,
	}
}
