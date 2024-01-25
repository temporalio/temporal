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
	"go.temporal.io/server/common/membership"
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
		ListServices(ctx context.Context, request *p.ListNexusIncomingServicesRequest) (*p.ListNexusIncomingServicesResponse, chan struct{}, error)
	}

	IncomingServiceRegistryConfig struct {
		ListServicesLongPollTimeout dynamicconfig.DurationPropertyFn
		ListServicesMinWaitTime     dynamicconfig.DurationPropertyFn
		ListServicesRetryPolicy     backoff.RetryPolicy
		ListServicesPageSize        dynamicconfig.IntPropertyFn
	}

	registry struct {
		status int32

		sync.RWMutex          // protects curTableVersion, and servicesByName
		curTableVersion int64 //TODO: maybe make this atomic
		servicesByName  map[string]*persistencespb.VersionedNexusIncomingService

		tableVersionChanged chan struct{}

		refreshPoller    *goro.Handle
		serviceDataReady *future.FutureImpl[struct{}]

		config IncomingServiceRegistryConfig
		client *incomingServiceClient

		metricsHandler metrics.Handler
		logger         log.Logger
	}
)

func NewIncomingServiceRegistry(
	config IncomingServiceRegistryConfig,
	serviceName primitives.ServiceName,
	hostInfoProvider membership.HostInfoProvider,
	matchingServiceResolver membership.ServiceResolver,
	matchingClient matchingservice.MatchingServiceClient,
	persistence p.NexusServiceManager,
	metricsHandler metrics.Handler,
	logger log.Logger,
) IncomingServiceRegistry {
	client := newNexusIncomingServiceClient(
		serviceName,
		hostInfoProvider,
		matchingServiceResolver,
		matchingClient,
		persistence,
		logger)

	return &registry{
		config:              config,
		client:              client,
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

	r.client.Start()

	refreshCtx := headers.SetCallerInfo(
		context.Background(),
		headers.SystemBackgroundCallerInfo,
	)

	r.refreshPoller = goro.NewHandle(refreshCtx).Go(r.refreshServices)
}

func (r *registry) Stop() {
	if !atomic.CompareAndSwapInt32(&r.status, running, stopping) {
		return
	}
	defer atomic.StoreInt32(&r.status, stopped)

	r.refreshPoller.Cancel()
	<-r.refreshPoller.Done()

	r.client.Stop()
}

func (r *registry) WaitUntilInitialized(ctx context.Context) error {
	_, err := r.serviceDataReady.Get(ctx)
	return err
}

func (r *registry) CreateOrUpdateService(ctx context.Context, service *nexus.IncomingService) (*persistencespb.VersionedNexusIncomingService, error) {
	var serviceID string
	currentServiceRecord, err := r.GetService(service.Name)
	if err != nil {
		if service.Version == 0 {
			serviceID = uuid.NewString()
		} else {
			return nil, err // TODO: error handling. this means trying to create service with a name that already exists
		}
	} else {
		serviceID = currentServiceRecord.Id
	}

	versioned, err := r.client.CreateOrUpdateNexusIncomingService(ctx, &p.CreateOrUpdateNexusIncomingServiceRequest{
		LastKnownTableVersion: r.curTableVersion,
		ServiceID:             serviceID,
		Service:               service,
	})
	if err != nil {
		return nil, err
	}

	r.Lock()
	defer r.Unlock()

	r.curTableVersion++
	r.servicesByName[versioned.ServiceInfo.Name] = versioned
	close(r.tableVersionChanged)
	r.tableVersionChanged = make(chan struct{})

	return versioned, nil
}

func (r *registry) DeleteService(ctx context.Context, name string) error {
	service, err := r.GetService(name)
	if err != nil {
		return err
	}

	err = r.client.DeleteNexusIncomingService(ctx, r.curTableVersion, service.Id, name)
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

func (r *registry) GetService(name string) (*persistencespb.VersionedNexusIncomingService, error) {
	r.RLock()
	defer r.RUnlock()

	service, ok := r.servicesByName[name]
	if !ok {
		return nil, nil // TODO: error handling
	}

	return service, nil
}

func (r *registry) ListServices(ctx context.Context, request *p.ListNexusIncomingServicesRequest) (*p.ListNexusIncomingServicesResponse, chan struct{}, error) {
	r.RLock()
	defer r.RUnlock()

	services, err := r.listServicesLocked()
	if err != nil {
		return nil, nil, err //TODO: error handling
	}

	// TODO: how to handle fallback to persistence from here?

	resp := &p.ListNexusIncomingServicesResponse{
		TableVersion: r.curTableVersion,
		Services:     services,
	}

	return resp, r.tableVersionChanged, nil
}

func (r *registry) listServicesLocked() ([]*persistencespb.VersionedNexusIncomingService, error) {
	// TODO: how to handle pagination when reading from memory

	services := make([]*persistencespb.VersionedNexusIncomingService, len(r.servicesByName))

	idx := 0
	for _, service := range r.servicesByName {
		services[idx] = service
		idx++
	}

	return services, nil
}

func (r *registry) setServiceDataReady(err error) {
	if !r.serviceDataReady.Ready() {
		r.serviceDataReady.Set(struct{}{}, err)
	}
}

func (r *registry) refreshServices(ctx context.Context) error {

	// currentPageToken is the most recently returned page token from ListNexusServices.
	// It is used in combination with r.curTableVersion to determine whether to do a long poll or a simple get.
	var currentPageToken []byte

	fetchFn := func(ctx context.Context) error {
		callCtx, cancel := context.WithTimeout(ctx, r.config.ListServicesLongPollTimeout())
		defer cancel()

		shouldWait := currentPageToken == nil && r.curTableVersion != 0

		resp, err := r.client.ListNexusIncomingServices(callCtx, &matchingservice.ListNexusServicesRequest{
			LastKnownTableVersion: r.curTableVersion,
			NextPageToken:         currentPageToken,
			PageSize:              int32(r.config.ListServicesPageSize()),
			Wait:                  shouldWait,
		})

		if err != nil {
			if errors.Is(err, p.ErrNexusTableVersionConflict) {
				r.invalidateServiceData()
				currentPageToken = nil
			} else {
				// TODO: error handling, esp if table ownership changes or owner is unavailable
				r.setServiceDataReady(err) // TODO: validate this is correct
				return err
			}
		}

		// If we got no error and no response, then this is the owning partition and our in-memory
		// view should be correct, so no need to update.
		// TODO: need to be absolutely sure of this logic
		if resp != nil {
			if currentPageToken == nil {
				// Got first page, need to replace in-memory service data
				r.setServiceData(resp.TableVersion, resp.Services)
			} else {
				// Got non-first page, add services to in-memory service data. Table version should not have changed.
				r.appendServices(resp.Services)
			}
			currentPageToken = resp.NextPageToken
		}
		r.setServiceDataReady(nil)
		return nil
	}

	minWaitTime := r.config.ListServicesMinWaitTime()

	for ctx.Err() == nil {
		start := time.Now()
		_ = backoff.ThrottleRetryContext(ctx, fetchFn, r.config.ListServicesRetryPolicy, nil)
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
