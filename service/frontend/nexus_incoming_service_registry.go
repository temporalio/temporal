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
	"cmp"
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
		status            int32
		hasLoadedServices atomic.Bool

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

	r.refreshPoller = goro.NewHandle(refreshCtx).Go(r.fetchServices)
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

	service := entryToService(resp.Entry)

	if r.hasLoadedServices.Load() {
		// Only update in-memory view if we have already done the initial load.
		// Otherwise, any changes will get overwritten on first read anyway.
		r.insertService(service, resp.Entry.Id)
	}

	return &operatorservice.CreateOrUpdateNexusIncomingServiceResponse{Service: service}, nil
}

func (r *registry) insertService(service *nexus.IncomingService, serviceID string) {
	r.Lock()
	defer r.Unlock()

	idx, found := slices.BinarySearch(r.serviceIDsSorted, serviceID)
	if found {
		r.services[idx] = service
	} else {
		r.services = slices.Insert(r.services, idx, service)
		r.serviceIDsSorted = slices.Insert(r.serviceIDsSorted, idx, serviceID)
	}
	r.servicesByName[service.Name] = service
	r.tableVersion++
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

	if r.hasLoadedServices.Load() {
		// Only update in-memory view if we have already done the initial load.
		// Otherwise, any changes will get overwritten on first read anyway.
		r.deleteService(request.Name)
	}

	return &operatorservice.DeleteNexusIncomingServiceResponse{}, nil
}

func (r *registry) deleteService(name string) {
	r.Lock()
	defer r.Unlock()

	r.tableVersion++
	delete(r.servicesByName, name)

	idx, found := slices.BinarySearchFunc(r.services, &nexus.IncomingService{Name: name}, func(a *nexus.IncomingService, b *nexus.IncomingService) int {
		return cmp.Compare(a.Name, b.Name)
	})
	if !found {
		return
	}

	slices.Delete(r.services, idx, idx)
	slices.Delete(r.serviceIDsSorted, idx, idx)
}

func (r *registry) GetNexusIncomingService(
	ctx context.Context,
	request *operatorservice.GetNexusIncomingServiceRequest,
) (*operatorservice.GetNexusIncomingServiceResponse, error) {
	if !r.hasLoadedServices.Load() {
		if err := r.loadServices(ctx); err != nil {
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
	if !r.hasLoadedServices.Load() {
		if err := r.loadServices(ctx); err != nil {
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

// Loads all incoming services into memory from persistence.
// Called once, on first read after startup.
func (r *registry) loadServices(ctx context.Context) error {
	r.Lock()
	defer r.Unlock()

	if r.hasLoadedServices.Load() {
		// check whether services were loaded while waiting for write lock
		return nil
	}

	r.tableVersion = 0
	r.services = []*nexus.IncomingService{}
	r.serviceIDsSorted = []string{}
	r.servicesByName = make(map[string]*nexus.IncomingService)

	var pageToken []byte

	for ctx.Err() == nil {
		resp, err := r.persistence.ListNexusIncomingServices(ctx, &p.ListNexusIncomingServicesRequest{
			LastKnownTableVersion: r.tableVersion,
			NextPageToken:         pageToken,
			PageSize:              loadServicesPageSize,
		})
		if err != nil {
			if errors.Is(err, p.ErrNexusTableVersionConflict) {
				// indicates table was updated during paging, so reset and start from the beginning
				pageToken = nil
				r.invalidateCachedServicesLocked()
				continue
			}
			return err
		}

		pageToken = resp.NextPageToken
		r.tableVersion = resp.TableVersion
		for _, entry := range resp.Entries {
			service := entryToService(entry)
			r.services = append(r.services, service)
			r.serviceIDsSorted = append(r.serviceIDsSorted, entry.Id)
			r.servicesByName[entry.Service.Name] = service
		}

		if pageToken == nil {
			break
		}
	}

	r.hasLoadedServices.Store(ctx.Err() == nil)
	return ctx.Err()
}

// Long polls matching service to refresh cached services.
func (r *registry) fetchServices(ctx context.Context) error {
	// currentPageToken is the most recently returned page token from ListNexusIncomingServices.
	// It is used in combination with r.tableVersion to determine whether to do a long poll or a simple get.
	var currentPageToken []byte

	fetchFn := func(ctx context.Context) error {
		if !r.hasLoadedServices.Load() {
			// Wait for at least one read to trigger lazy loading from
			// persistence before long polling matching for changes
			return nil
		}

		r.RLock()
		requestTableVersion := r.tableVersion
		r.RUnlock()

		callCtx, cancel := context.WithTimeout(ctx, r.refreshServicesLongPollTimeout())
		defer cancel()

		shouldWait := currentPageToken == nil && requestTableVersion != 0

		resp, err := r.matchingClient.ListNexusIncomingServices(callCtx, &matchingservice.ListNexusIncomingServicesRequest{
			LastKnownTableVersion: requestTableVersion,
			NextPageToken:         currentPageToken,
			PageSize:              loadServicesPageSize,
			Wait:                  shouldWait,
		})

		if err != nil {
			if errors.Is(err, p.ErrNexusTableVersionConflict) {
				// indicates table was updated during paging, so reset and start from the beginning
				currentPageToken = nil
				r.invalidateCachedServices()
				return nil
			}
			return err
		}

		if shouldWait && resp.TableVersion == requestTableVersion {
			// long poll returned with no changes
			return nil
		}

		r.Lock()
		defer r.Unlock()

		if shouldWait {
			// long poll returned new data, starting with first page
			r.invalidateCachedServicesLocked()
		}

		currentPageToken = resp.NextPageToken
		r.tableVersion = resp.TableVersion

		// the following assumes services returned by matching are already sorted
		startIdx := len(r.services)
		r.services = slices.Grow(r.services, len(resp.Entries))
		r.serviceIDsSorted = slices.Grow(r.serviceIDsSorted, len(resp.Entries))
		for i, entry := range resp.Entries {
			service := entryToService(entry)
			r.services[startIdx+i] = service
			r.servicesByName[entry.Service.Name] = service
			r.serviceIDsSorted[startIdx+i] = entry.Id
		}

		return nil
	}

	minWaitTime := r.refreshServicesLongPollMinWait()

	for ctx.Err() == nil {
		start := time.Now()
		_ = backoff.ThrottleRetryContext(ctx, fetchFn, r.refreshServicesRetryPolicy, nil)
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
