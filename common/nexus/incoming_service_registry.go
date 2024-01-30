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

package nexus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/nexus/v1"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/internal/goro"
)

const (
	stopped int32 = iota
	starting
	running
	stopping
)

type (
	IncomingServiceRegistry interface {
		Start()
		Stop()
		WaitUntilInitialized(ctx context.Context) error
		CreateOrUpdateService(ctx context.Context, service *nexus.IncomingService) (*persistencespb.VersionedNexusIncomingService, error)
		DeleteService(ctx context.Context, name string) error
		GetService(name string) (*persistencespb.VersionedNexusIncomingService, error)
		ListServices(request *p.ListNexusIncomingServicesRequest) (*p.ListNexusIncomingServicesResponse, chan struct{}, error)
	}

	IncomingServiceRegistryConfig struct {
		ListServicesLongPollTimeout dynamicconfig.DurationPropertyFn
		ListServicesMinWaitTime     dynamicconfig.DurationPropertyFn
		ListServicesRetryPolicy     backoff.RetryPolicy
		ListServicesPageSize        dynamicconfig.IntPropertyFn
	}

	registry struct {
		status int32

		sync.RWMutex        // protects curTableVersion, servicesByName, and tableVersionChanged
		curTableVersion     int64
		servicesByName      map[string]*persistencespb.VersionedNexusIncomingService
		tableVersionChanged chan struct{}

		refreshPoller    *goro.Handle
		serviceDataReady *future.FutureImpl[struct{}]

		hostService primitives.ServiceName // the Temporal service that this registry instance is on
		config      IncomingServiceRegistryConfig

		matchingClient matchingservice.MatchingServiceClient
		persistence    p.NexusServiceManager

		metricsHandler metrics.Handler
		logger         log.Logger
	}
)

func NewIncomingServiceRegistry(
	owningService primitives.ServiceName,
	config IncomingServiceRegistryConfig,
	matchingClient matchingservice.MatchingServiceClient,
	persistence p.NexusServiceManager,
	metricsHandler metrics.Handler,
	logger log.Logger,
) IncomingServiceRegistry {
	return &registry{
		hostService:         owningService,
		config:              config,
		matchingClient:      matchingClient,
		persistence:         persistence,
		metricsHandler:      metricsHandler,
		logger:              logger,
		servicesByName:      make(map[string]*persistencespb.VersionedNexusIncomingService),
		tableVersionChanged: make(chan struct{}),
		serviceDataReady:    future.NewFuture[struct{}](),
	}
}

func (r *registry) Start() {
	if !atomic.CompareAndSwapInt32(&r.status, stopped, starting) {
		return
	}
	defer atomic.StoreInt32(&r.status, running)

	refreshCtx := headers.SetCallerInfo(
		context.Background(),
		headers.SystemBackgroundCallerInfo,
	)

	if r.hostService == primitives.MatchingService {
		r.refreshPoller = goro.NewHandle(refreshCtx).Go(r.loadServices)
	} else {
		r.refreshPoller = goro.NewHandle(refreshCtx).Go(r.fetchServices)
	}
}

func (r *registry) Stop() {
	if !atomic.CompareAndSwapInt32(&r.status, running, stopping) {
		return
	}
	defer atomic.StoreInt32(&r.status, stopped)

	r.refreshPoller.Cancel()
	<-r.refreshPoller.Done()

	r.persistence.Close()
}

func (r *registry) WaitUntilInitialized(ctx context.Context) error {
	_, err := r.serviceDataReady.Get(ctx)
	return err
}

func (r *registry) CreateOrUpdateService(ctx context.Context, service *nexus.IncomingService) (*persistencespb.VersionedNexusIncomingService, error) {
	curService, err := r.GetService(service.Name)
	if err != nil {
		return nil, err
	}
	if curService == nil {
		curService = &persistencespb.VersionedNexusIncomingService{
			Version:     0,
			Id:          uuid.NewString(),
			ServiceInfo: &persistencespb.NexusIncomingService{},
		}
	}
	if curService.Version != service.Version {
		return nil, fmt.Errorf("%w. expected %v. got %v", p.ErrNexusIncomingServiceVersionConflict, curService.Version, service.Version)
	}

	var updated *persistencespb.VersionedNexusIncomingService
	if r.hostService == primitives.MatchingService {
		updated, err = r.persistCreateOrUpdate(ctx, curService.Id, service)
	} else {
		updated, err = r.forwardCreateOrUpdate(ctx, service)
	}
	if err != nil {
		return nil, err
	}

	r.Lock()
	defer r.Unlock()

	r.curTableVersion++
	r.servicesByName[updated.ServiceInfo.Name] = updated
	close(r.tableVersionChanged)
	r.tableVersionChanged = make(chan struct{})

	return updated, nil
}

func (r *registry) persistCreateOrUpdate(ctx context.Context, serviceID string, service *nexus.IncomingService) (*persistencespb.VersionedNexusIncomingService, error) {
	resp, err := r.persistence.CreateOrUpdateNexusIncomingService(ctx, &p.CreateOrUpdateNexusIncomingServiceRequest{
		LastKnownTableVersion: r.curTableVersion,
		ServiceID:             serviceID,
		Service:               service,
	})
	if err != nil {
		return nil, err
	}
	return resp.Service, nil
}

func (r *registry) forwardCreateOrUpdate(ctx context.Context, service *nexus.IncomingService) (*persistencespb.VersionedNexusIncomingService, error) {
	resp, err := r.matchingClient.CreateOrUpdateNexusService(ctx, &matchingservice.CreateOrUpdateNexusServiceRequest{
		Service: service,
	})
	if err != nil {
		return nil, err
	}
	return resp.Service, nil
}

func (r *registry) DeleteService(ctx context.Context, name string) error {
	service, err := r.GetService(name)
	if err != nil {
		return err
	}
	if service == nil {
		return p.ErrNexusIncomingServiceNotFound
	}

	if r.hostService == primitives.MatchingService {
		err = r.persistDelete(ctx, service.Id)
	} else {
		err = r.forwardDelete(ctx, name)
	}
	if err != nil {
		return err
	}

	r.Lock()
	defer r.Unlock()

	r.curTableVersion++
	delete(r.servicesByName, name)
	close(r.tableVersionChanged)
	r.tableVersionChanged = make(chan struct{})

	return nil
}

func (r *registry) persistDelete(ctx context.Context, serviceID string) error {
	return r.persistence.DeleteNexusIncomingService(ctx, &p.DeleteNexusIncomingServiceRequest{
		LastKnownTableVersion: r.curTableVersion,
		ServiceID:             serviceID,
	})
}

func (r *registry) forwardDelete(ctx context.Context, serviceName string) error {
	_, err := r.matchingClient.DeleteNexusService(ctx, &matchingservice.DeleteNexusServiceRequest{
		Name: serviceName,
	})
	return err
}

func (r *registry) GetService(name string) (*persistencespb.VersionedNexusIncomingService, error) {
	r.RLock()
	defer r.RUnlock()

	service, ok := r.servicesByName[name]
	if !ok {
		return nil, p.ErrNexusIncomingServiceNotFound
	}

	return service, nil
}

// ListServices returns a persistence.ListNexusIncomingServicesResponse containing all currently in-memory services and
// the current table version, and a channel which will be closed when any change is made to the table.
// TODO: support pagination. golang map iteration is non-deterministic, so this will require sorting by serviceID.
func (r *registry) ListServices(request *p.ListNexusIncomingServicesRequest) (*p.ListNexusIncomingServicesResponse, chan struct{}, error) {
	r.RLock()
	defer r.RUnlock()

	if request.LastKnownTableVersion != 0 && request.LastKnownTableVersion != r.curTableVersion {
		return nil, nil, p.ErrNexusTableVersionConflict
	}

	resp := &p.ListNexusIncomingServicesResponse{
		TableVersion: r.curTableVersion,
		Services:     r.listAllServicesLocked(),
	}

	return resp, r.tableVersionChanged, nil
}

func (r *registry) listAllServicesLocked() []*persistencespb.VersionedNexusIncomingService {
	services := make([]*persistencespb.VersionedNexusIncomingService, len(r.servicesByName))

	idx := 0
	for _, service := range r.servicesByName {
		services[idx] = service
		idx++
	}

	return services
}

func (r *registry) setServiceDataReady(err error) {
	if !r.serviceDataReady.Ready() {
		r.serviceDataReady.Set(struct{}{}, err)
	}
}

// Load a page of services directly from persistence.
// Should only be used on startup or if the owning matching node is unreachable.
func (r *registry) loadServiceDataPage(ctx context.Context, nextPageToken []byte) ([]byte, error) {
	resp, err := r.persistence.ListNexusIncomingServices(ctx, &p.ListNexusIncomingServicesRequest{
		PageSize:              r.config.ListServicesPageSize(),
		NextPageToken:         nextPageToken,
		LastKnownTableVersion: r.curTableVersion,
	})
	if err != nil {
		return nil, err
	}

	if nextPageToken == nil {
		r.setServiceData(resp.TableVersion, resp.Services)
	} else {
		r.appendServices(resp.Services)
	}

	return resp.NextPageToken, nil
}

// initializeServiceData loads all pages of services into memory from persistence
func (r *registry) initializeServiceData(ctx context.Context) error {
	finishedPaging := false
	var curPageToken []byte

	for ctx.Err() == nil && !finishedPaging {
		nextPageToken, err := r.loadServiceDataPage(ctx, curPageToken)
		if err != nil {
			r.setServiceDataReady(err)
			return err
		}

		curPageToken = nextPageToken
		finishedPaging = nextPageToken == nil
	}

	r.setServiceDataReady(ctx.Err())
	return ctx.Err()
}

// loadServices trys to initialize service data by loading from persistence.
// If loading fails, it will retry every ListServicesLongPollTimeout
// Used by owning matching node, so after services are loaded, in-memory view should always be correct.
func (r *registry) loadServices(ctx context.Context) error {

	hasLoadedServiceData := false

	for ctx.Err() == nil {
		if !hasLoadedServiceData {
			err := r.initializeServiceData(ctx)
			hasLoadedServiceData = err == nil
		} else {
			r.setServiceDataReady(nil)
		}
		common.InterruptibleSleep(ctx, r.config.ListServicesLongPollTimeout())
	}

	return nil
}

// fetchServices first initializes service data from persistence, then long-polls matching for updates.
func (r *registry) fetchServices(ctx context.Context) error {

	// currentPageToken is the most recently returned page token from ListNexusServices.
	// It is used in combination with r.curTableVersion to determine whether to do a long poll or a simple get.
	var currentPageToken []byte

	refreshFn := func(ctx context.Context) error {
		callCtx, cancel := context.WithTimeout(ctx, r.config.ListServicesLongPollTimeout())
		defer cancel()

		shouldWait := currentPageToken == nil && r.curTableVersion != 0

		resp, err := r.matchingClient.ListNexusServices(callCtx, &matchingservice.ListNexusServicesRequest{
			LastKnownTableVersion: r.curTableVersion,
			NextPageToken:         currentPageToken,
			PageSize:              int32(r.config.ListServicesPageSize()),
			Wait:                  shouldWait,
		})

		if err != nil {
			if errors.Is(err, p.ErrNexusTableVersionConflict) {
				r.invalidateServiceData()
				currentPageToken = nil
			} else if isRetryableClientError(err) {
				return err
			} else {
				// Fallback to querying persistence directly when matching returns a non-retryable error
				nextPageToken, persistErr := r.loadServiceDataPage(callCtx, currentPageToken)
				currentPageToken = nextPageToken
				return persistErr
			}
		}

		if currentPageToken == nil {
			// Got first page, need to replace in-memory service data
			r.setServiceData(resp.TableVersion, resp.Services)
		} else {
			// Got non-first page, add services to in-memory service data. Table version should not have changed.
			r.appendServices(resp.Services)
		}

		currentPageToken = resp.NextPageToken
		return nil
	}

	hasLoadedServiceData := false
	minWaitTime := r.config.ListServicesMinWaitTime()

	for ctx.Err() == nil {
		start := time.Now()
		if !hasLoadedServiceData {
			// initialize data by loading directly from persistence
			err := backoff.ThrottleRetryContext(ctx, r.initializeServiceData, r.config.ListServicesRetryPolicy, nil)
			hasLoadedServiceData = err == nil
		} else {
			// long poll request to matching to be notified of table updates
			_ = backoff.ThrottleRetryContext(ctx, refreshFn, r.config.ListServicesRetryPolicy, nil)
		}
		elapsed := time.Since(start)

		// In general, we want to start a new call immediately on completion of the previous
		// one. But if the remote is broken and returns success immediately, we might end up
		// spinning. So enforce a minimum wait time that increases as long as we keep getting
		// very fast replies.
		if elapsed < minWaitTime {
			common.InterruptibleSleep(ctx, minWaitTime-elapsed)
			// Don't let this get near our call timeout, otherwise we can't tell the difference
			// between a fast reply and a timeout.
			minWaitTime = min(minWaitTime*2, r.config.ListServicesLongPollTimeout()/2)
		} else {
			minWaitTime = r.config.ListServicesMinWaitTime()
		}
	}

	return ctx.Err()
}

func (r *registry) invalidateServiceData() {
	r.Lock()
	defer r.Unlock()
	r.curTableVersion = 0
	r.servicesByName = nil
}

func (r *registry) setServiceData(tableVersion int64, services []*persistencespb.VersionedNexusIncomingService) {
	servicesByName := make(map[string]*persistencespb.VersionedNexusIncomingService, len(services))

	for _, service := range services {
		servicesByName[service.ServiceInfo.Name] = service
	}

	r.Lock()
	defer r.Unlock()
	r.curTableVersion = tableVersion
	r.servicesByName = servicesByName
}

func (r *registry) appendServices(services []*persistencespb.VersionedNexusIncomingService) {
	r.Lock()
	defer r.Unlock()

	for _, service := range services {
		r.servicesByName[service.ServiceInfo.Name] = service
	}
}

func isRetryableClientError(err error) bool {
	if err == nil {
		return false
	}
	if common.IsServiceClientTransientError(err) {
		return true
	}
	var conditionFailedError *p.ConditionFailedError
	return errors.As(err, &conditionFailedError)
}
