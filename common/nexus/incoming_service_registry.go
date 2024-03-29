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

	"go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/internal/goro"
)

type (
	IncomingServiceRegistryConfig struct {
		refreshLongPollTimeout dynamicconfig.DurationPropertyFn
		refreshPageSize        dynamicconfig.IntPropertyFn
		refreshMinWait         dynamicconfig.DurationPropertyFn
		refreshRetryPolicy     backoff.RetryPolicy
	}

	// IncomingServiceRegistry manages a cached view of Nexus incoming services.
	// Services are lazily-loaded into memory on the first read. Thereafter, service data is kept up to date by
	// background long polling on matching service ListNexusIncomingServices.
	IncomingServiceRegistry struct {
		config *IncomingServiceRegistryConfig

		status           int32
		serviceDataReady *future.FutureImpl[struct{}]

		dataLock     sync.RWMutex // protects tableVersion and services
		tableVersion int64
		services     map[string]*nexus.IncomingService // mapping of service ID -> service

		refreshPoller *goro.Handle

		matchingClient matchingservice.MatchingServiceClient
		persistence    p.NexusIncomingServiceManager
		logger         log.Logger
	}
)

func NewIncomingServiceRegistryConfig(dc *dynamicconfig.Collection) *IncomingServiceRegistryConfig {
	config := &IncomingServiceRegistryConfig{
		refreshLongPollTimeout: dc.GetDurationProperty(dynamicconfig.FrontendRefreshNexusIncomingServicesLongPollTimeout, 5*time.Minute),
		refreshPageSize:        dc.GetIntProperty(dynamicconfig.NexusIncomingServiceListDefaultPageSize, 1000),
		refreshMinWait:         dc.GetDurationProperty(dynamicconfig.FrontendRefreshNexusIncomingServicesLongPollTimeout, 1*time.Second),
	}
	config.refreshRetryPolicy = backoff.NewExponentialRetryPolicy(config.refreshMinWait()).WithMaximumInterval(config.refreshLongPollTimeout())
	return config
}

func NewIncomingServiceRegistry(
	config *IncomingServiceRegistryConfig,
	matchingClient matchingservice.MatchingServiceClient,
	persistence p.NexusIncomingServiceManager,
	logger log.Logger,
) *IncomingServiceRegistry {
	return &IncomingServiceRegistry{
		status:           common.DaemonStatusInitialized,
		config:           config,
		serviceDataReady: future.NewFuture[struct{}](),
		services:         make(map[string]*nexus.IncomingService),
		matchingClient:   matchingClient,
		persistence:      persistence,
		logger:           logger,
	}
}

func (r *IncomingServiceRegistry) Start() {
	if !atomic.CompareAndSwapInt32(&r.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	refreshCtx := headers.SetCallerInfo(
		context.Background(),
		headers.SystemBackgroundCallerInfo,
	)
	r.refreshPoller = goro.NewHandle(refreshCtx).Go(r.refreshServicesLoop)
}

func (r *IncomingServiceRegistry) Stop() {
	if !atomic.CompareAndSwapInt32(&r.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	r.refreshPoller.Cancel()
	<-r.refreshPoller.Done()
}

func (r *IncomingServiceRegistry) Get(ctx context.Context, id string) (*nexus.IncomingService, error) {
	if !r.serviceDataReady.Ready() {
		if err := r.initializeServices(ctx); err != nil {
			r.logger.Error("error initializing Nexus incoming service cache.", tag.Error(err))
			return nil, serviceerror.NewInternal("error loading Nexus incoming service data.")
		}
	}

	r.dataLock.RLock()
	defer r.dataLock.RUnlock()

	service, ok := r.services[id]
	if !ok {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("could not find Nexus incoming service with ID: %v", id))
	}

	return service, nil
}

func (r *IncomingServiceRegistry) refreshServicesLoop(ctx context.Context) error {
	minWaitTime := r.config.refreshMinWait()

	for ctx.Err() == nil {
		start := time.Now()
		_ = backoff.ThrottleRetryContext(ctx, r.refreshServices, r.config.refreshRetryPolicy, nil)
		elapsed := time.Since(start)

		// In general, we want to start a new call immediately on completion of the previous
		// one. But if the remote is broken and returns success immediately, we might end up
		// spinning. So enforce a minimum wait time that increases as long as we keep getting
		// very fast replies.
		if elapsed < minWaitTime {
			common.InterruptibleSleep(ctx, minWaitTime-elapsed)
			// Don't let this get near our call timeout, otherwise we can't tell the difference
			// between a fast reply and a timeout.
			minWaitTime = min(minWaitTime*2, r.config.refreshLongPollTimeout()/2)
		} else {
			minWaitTime = r.config.refreshMinWait()
		}
	}

	return ctx.Err()
}

// refreshServices sends long-poll requests to matching to check for any updates to service data.
// It waits to send any requests until service data has been initialized.
func (r *IncomingServiceRegistry) refreshServices(ctx context.Context) error {
	// Wait for service data to be initialized before long polling.
	if _, err := r.serviceDataReady.Get(ctx); err != nil {
		return err
	}

	r.dataLock.RLock()
	currentTableVersion := r.tableVersion
	r.dataLock.RUnlock()

	resp, err := r.matchingClient.ListNexusIncomingServices(ctx, &matchingservice.ListNexusIncomingServicesRequest{
		NextPageToken:         nil,
		PageSize:              int32(r.config.refreshPageSize()),
		LastKnownTableVersion: currentTableVersion,
		Wait:                  true,
	})
	if err != nil {
		return err
	}

	if resp.TableVersion == currentTableVersion {
		// Long poll returned with no changes
		return nil
	}

	// changes were returned by long poll so acquire write lock to prevent reads while updating
	r.dataLock.Lock()
	defer r.dataLock.Unlock()

	if r.tableVersion >= resp.TableVersion {
		// service data was updated while waiting for the write lock
		return nil
	}

	currentTableVersion = resp.TableVersion
	services := make(map[string]*nexus.IncomingService)
	for _, service := range resp.Services {
		services[service.Id] = service
	}

	currentPageToken := resp.NextPageToken
	for currentPageToken != nil {
		resp, err = r.matchingClient.ListNexusIncomingServices(ctx, &matchingservice.ListNexusIncomingServicesRequest{
			NextPageToken:         currentPageToken,
			PageSize:              int32(r.config.refreshPageSize()),
			LastKnownTableVersion: currentTableVersion,
			Wait:                  false,
		})

		if err != nil {
			if errors.Is(err, p.ErrNexusTableVersionConflict) {
				// Indicates table was updated during paging, so reset and start from the beginning
				currentTableVersion, services, err = r.getAllServicesMatching(ctx)
				if err != nil {
					return err
				}
				break
			}
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		for _, service := range resp.Services {
			services[service.Id] = service
		}

		currentPageToken = resp.NextPageToken
	}

	r.tableVersion = currentTableVersion
	r.services = services
	return nil
}

// initializeServices initializes the in-memory view of service data.
// It first tries to load from matching service and falls back to querying persistence directly if matching is unavailable.
func (r *IncomingServiceRegistry) initializeServices(ctx context.Context) error {
	r.dataLock.Lock()
	defer r.dataLock.Unlock()

	if r.serviceDataReady.Ready() {
		// Indicates service data was loaded by another thread while waiting for write lock.
		return nil
	}

	tableVersion, services, err := r.getAllServicesMatching(ctx)
	if err != nil {
		// Fallback to persistence on matching error during initial load.
		tableVersion, services, err = r.getAllServicesPersistence(ctx)
		if err != nil {
			return err
		}
	}

	r.tableVersion = tableVersion
	r.services = services
	if !r.serviceDataReady.Ready() {
		r.serviceDataReady.Set(struct{}{}, nil)
	}

	return nil
}

// getAllServicesMatching paginates over all services returned by matching. It always does a simple get.
func (r *IncomingServiceRegistry) getAllServicesMatching(ctx context.Context) (int64, map[string]*nexus.IncomingService, error) {
	var currentPageToken []byte

	currentTableVersion := int64(0)
	services := make(map[string]*nexus.IncomingService)

	for ctx.Err() == nil {
		resp, err := r.matchingClient.ListNexusIncomingServices(ctx, &matchingservice.ListNexusIncomingServicesRequest{
			NextPageToken:         currentPageToken,
			PageSize:              int32(r.config.refreshPageSize()),
			LastKnownTableVersion: currentTableVersion,
			Wait:                  false,
		})
		if err != nil {
			if errors.Is(err, p.ErrNexusTableVersionConflict) {
				// indicates table was updated during paging, so reset and start from the beginning
				currentPageToken = nil
				currentTableVersion = 0
				services = make(map[string]*nexus.IncomingService)
				continue
			}
			return 0, nil, err
		}

		currentTableVersion = resp.TableVersion
		for _, service := range resp.Services {
			services[service.Id] = service
		}

		if resp.NextPageToken == nil {
			return currentTableVersion, services, nil
		}

		currentPageToken = resp.NextPageToken
	}

	return 0, nil, ctx.Err()
}

// getAllServicesPersistence paginates over all services returned by persistence.
// Should only be used as a fall-back if matching service is unavailable during initial load.
func (r *IncomingServiceRegistry) getAllServicesPersistence(ctx context.Context) (int64, map[string]*nexus.IncomingService, error) {
	var currentPageToken []byte

	currentTableVersion := int64(0)
	services := make(map[string]*nexus.IncomingService)

	for ctx.Err() == nil {
		resp, err := r.persistence.ListNexusIncomingServices(ctx, &p.ListNexusIncomingServicesRequest{
			LastKnownTableVersion: currentTableVersion,
			NextPageToken:         currentPageToken,
			PageSize:              r.config.refreshPageSize(),
		})
		if err != nil {
			if errors.Is(err, p.ErrNexusTableVersionConflict) {
				// indicates table was updated during paging, so reset and start from the beginning
				currentPageToken = nil
				currentTableVersion = 0
				services = make(map[string]*nexus.IncomingService)
				continue
			}
			return 0, nil, err
		}

		currentTableVersion = resp.TableVersion
		for _, entry := range resp.Entries {
			services[entry.Id] = IncomingServicePersistedEntryToExternalAPI(entry)
		}

		if resp.NextPageToken == nil {
			return currentTableVersion, services, nil
		}

		currentPageToken = resp.NextPageToken
	}

	return 0, nil, ctx.Err()
}
