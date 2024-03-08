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

	r.refreshPoller = goro.NewHandle(refreshCtx).Go(r.refreshServicesLoop)
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
		Service: &nexus.IncomingService{
			Version:   request.Version,
			Name:      request.Name,
			Namespace: request.Namespace,
			TaskQueue: request.TaskQueue,
			Metadata:  request.Metadata,
		},
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
	if !r.serviceDataReady.Ready() {
		// ensure data has been initialized before any reads
		err := r.initializeServices(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		// ensure data is not stale
		err := r.checkTableVersionAndMaybeRefresh(ctx)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("error retrieving Nexus incoming services: %v", err))
		}
	}

	r.RLock()
	defer r.RUnlock()

	service, ok := r.servicesByName[request.Name]
	if !ok {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("could not find service with name %v", request.Name))
	}

	return &operatorservice.GetNexusIncomingServiceResponse{Service: service}, nil
}

func (r *registry) ListNexusIncomingServices(
	ctx context.Context,
	request *operatorservice.ListNexusIncomingServicesRequest,
) (*operatorservice.ListNexusIncomingServicesResponse, error) {
	if !r.serviceDataReady.Ready() {
		// ensure data has been initialized before any reads
		err := r.initializeServices(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		// ensure data is not stale
		err := r.checkTableVersionAndMaybeRefresh(ctx)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("error retrieving Nexus incoming services: %v", err))
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

func (r *registry) checkTableVersionAndMaybeRefresh(ctx context.Context) error {
	r.RLock()
	actualTableVersion, err := r.persistence.GetNexusIncomingServicesTableVersion(ctx)
	if err != nil {
		r.RUnlock()
		return err
	}
	if r.tableVersion == actualTableVersion {
		r.RUnlock()
		return nil
	}
	r.RUnlock()

	return r.fetchServicesAndUpdate(ctx, actualTableVersion)
}

func (r *registry) fetchServicesAndUpdate(ctx context.Context, expectedTableVersion int64) error {
	r.Lock()
	defer r.Unlock()

	if r.tableVersion >= expectedTableVersion {
		// service data was updated while waiting for write lock
		return nil
	}

	tableVersion, services, serviceIDsSorted, servicesByName, err := r.getAllServicesMatching(ctx)
	if err != nil {
		return err
	}

	r.tableVersion = tableVersion
	r.services = services
	r.serviceIDsSorted = serviceIDsSorted
	r.servicesByName = servicesByName

	return nil
}

func (r *registry) initializeServices(ctx context.Context) error {
	r.Lock()
	defer r.Unlock()

	if r.serviceDataReady.Ready() {
		// check if data was initialized while waiting for write lock
		return nil
	}

	tableVersion, services, serviceIDsSorted, servicesByName, err := r.getAllServicesMatching(ctx)
	if err != nil {
		// fallback to directly loading from persistence on initial load only
		tableVersion, services, serviceIDsSorted, servicesByName, err = r.getAllServicesPersistence(ctx)
		if err != nil {
			return serviceerror.NewUnavailable(fmt.Sprintf("error initializing Nexus incoming services: %v", err))
		}
	}

	r.tableVersion = tableVersion
	r.services = services
	r.serviceIDsSorted = serviceIDsSorted
	r.servicesByName = servicesByName

	r.setServiceDataReady(nil)
	return nil
}

// Paginates over all services returned by matching and returns the accumulated result
func (r *registry) getAllServicesMatching(ctx context.Context) (int64, []*nexus.IncomingService, []string, map[string]*nexus.IncomingService, error) {
	currentTableVersion := int64(0)
	var services []*nexus.IncomingService
	var serviceIDsSorted []string
	servicesByName := make(map[string]*nexus.IncomingService)

	var currentPageToken []byte

	for ctx.Err() == nil {
		resp, err := r.matchingClient.ListNexusIncomingServices(ctx, &matchingservice.ListNexusIncomingServicesRequest{
			LastKnownTableVersion: currentTableVersion,
			NextPageToken:         currentPageToken,
			PageSize:              loadServicesPageSize,
			Wait:                  false,
		})
		if err != nil {
			if errors.Is(err, p.ErrNexusTableVersionConflict) {
				// indicates table was updated during paging, so reset and start from the beginning
				currentPageToken = nil
				currentTableVersion = 0
				services = []*nexus.IncomingService{}
				serviceIDsSorted = []string{}
				servicesByName = make(map[string]*nexus.IncomingService)
				continue
			}
			return 0, nil, nil, nil, err
		}

		currentPageToken = resp.NextPageToken
		currentTableVersion = resp.TableVersion
		services, serviceIDsSorted, servicesByName = appendServiceEntries(resp.Entries, services, serviceIDsSorted, servicesByName)

		if resp.NextPageToken == nil {
			return currentTableVersion, services, serviceIDsSorted, servicesByName, nil
		}
	}

	return 0, nil, nil, nil, ctx.Err()
}

// Paginates over all services returned by persistence and returns the accumulated result
func (r *registry) getAllServicesPersistence(ctx context.Context) (int64, []*nexus.IncomingService, []string, map[string]*nexus.IncomingService, error) {
	currentTableVersion := int64(0)
	var services []*nexus.IncomingService
	var serviceIDsSorted []string
	servicesByName := make(map[string]*nexus.IncomingService)

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
				currentTableVersion = 0
				services = []*nexus.IncomingService{}
				serviceIDsSorted = []string{}
				servicesByName = make(map[string]*nexus.IncomingService)
				continue
			}
			return 0, nil, nil, nil, err
		}

		currentPageToken = resp.NextPageToken
		currentTableVersion = resp.TableVersion
		services, serviceIDsSorted, servicesByName = appendServiceEntries(resp.Entries, services, serviceIDsSorted, servicesByName)

		if resp.NextPageToken == nil {
			break
		}
	}

	if ctx.Err() != nil {
		return 0, nil, nil, nil, ctx.Err()
	}

	return currentTableVersion, services, serviceIDsSorted, servicesByName, nil
}

func (r *registry) refreshServices(ctx context.Context) error {
	// wait for data to be initialized before long polling
	_, err := r.serviceDataReady.Get(ctx)
	if err != nil {
		return err
	}

	r.RLock()
	currentTableVersion := r.tableVersion
	r.RUnlock()

	var currentPageToken []byte

	// long poll to wait for table updates
	resp, err := r.matchingClient.ListNexusIncomingServices(ctx, &matchingservice.ListNexusIncomingServicesRequest{
		LastKnownTableVersion: currentTableVersion,
		NextPageToken:         currentPageToken,
		PageSize:              loadServicesPageSize,
		Wait:                  true,
	})
	if err != nil {
		return err
	}

	if currentTableVersion == resp.TableVersion {
		// long poll returned with no changes
		return nil
	}

	r.Lock()
	defer r.Unlock()

	if r.tableVersion >= resp.TableVersion {
		// service data was updated while waiting for the write lock
		return nil
	}

	var services []*nexus.IncomingService
	var serviceIDsSorted []string
	servicesByName := make(map[string]*nexus.IncomingService)

	services, serviceIDsSorted, servicesByName = appendServiceEntries(resp.Entries, services, serviceIDsSorted, servicesByName)
	currentTableVersion = resp.TableVersion
	currentPageToken = resp.NextPageToken

	for currentPageToken != nil && ctx.Err() == nil {
		// iterate over remaining pages without long polling
		resp, err = r.matchingClient.ListNexusIncomingServices(ctx, &matchingservice.ListNexusIncomingServicesRequest{
			LastKnownTableVersion: currentTableVersion,
			NextPageToken:         currentPageToken,
			PageSize:              loadServicesPageSize,
			Wait:                  false,
		})
		if err != nil {
			if errors.Is(err, p.ErrNexusTableVersionConflict) {
				// indicates table was updated during paging, so reset and start from the beginning
				currentTableVersion, services, serviceIDsSorted, servicesByName, err = r.getAllServicesMatching(ctx)
				if err != nil {
					return err
				}
			}
		}

		services, serviceIDsSorted, servicesByName = appendServiceEntries(resp.Entries, services, serviceIDsSorted, servicesByName)
		currentPageToken = resp.NextPageToken
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	r.tableVersion = currentTableVersion
	r.services = services
	r.serviceIDsSorted = serviceIDsSorted
	r.servicesByName = servicesByName

	return nil
}

func (r *registry) refreshServicesLoop(ctx context.Context) error {
	minWaitTime := r.refreshServicesLongPollMinWait()

	for ctx.Err() == nil {
		start := time.Now()
		_ = backoff.ThrottleRetryContext(ctx, r.refreshServices, r.refreshServicesRetryPolicy, nil)
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

func appendServiceEntries(
	entries []*persistencepb.NexusIncomingServiceEntry,
	services []*nexus.IncomingService,
	serviceIDsSorted []string,
	servicesByName map[string]*nexus.IncomingService,
) ([]*nexus.IncomingService, []string, map[string]*nexus.IncomingService) {
	// the following assumes entries are already sorted by serviceID
	services = slices.Grow(services, len(entries))
	serviceIDsSorted = slices.Grow(serviceIDsSorted, len(entries))
	for _, entry := range entries {
		service := entryToService(entry)
		services = append(services, service)
		servicesByName[entry.Service.Name] = service
		serviceIDsSorted = append(serviceIDsSorted, entry.Id)
	}
	return services, serviceIDsSorted, servicesByName
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
