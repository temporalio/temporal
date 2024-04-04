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
	"time"

	"go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/internal/goro"
)

var errNexusAPIsDisabled = serviceerror.NewNotFound("error Nexus APIs are disabled.")

type (
	IncomingServiceRegistryConfig struct {
		nexusAPIsEnabled       dynamicconfig.BoolPropertyFn
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

		serviceDataReady chan struct{}

		dataLock     sync.RWMutex // Protects tableVersion and services.
		tableVersion int64
		services     map[string]*nexus.IncomingService // Mapping of service ID -> service.

		refreshPoller *goro.Handle

		matchingClient matchingservice.MatchingServiceClient
		persistence    p.NexusIncomingServiceManager
		logger         log.Logger
	}
)

func NewIncomingServiceRegistryConfig(dc *dynamicconfig.Collection) *IncomingServiceRegistryConfig {
	config := &IncomingServiceRegistryConfig{
		nexusAPIsEnabled:       dc.GetBoolProperty(dynamicconfig.FrontendEnableNexusAPIs, false),
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
		config:           config,
		serviceDataReady: make(chan struct{}),
		services:         make(map[string]*nexus.IncomingService),
		matchingClient:   matchingClient,
		persistence:      persistence,
		logger:           logger,
	}
}

// StartLifecycle starts this component. It should only be invoked by an fx lifecycle hook.
// Should not be called multiple times or concurrently with StopLifecycle()
func (r *IncomingServiceRegistry) StartLifecycle() {
	backgroundCtx := headers.SetCallerInfo(
		context.Background(),
		headers.SystemBackgroundCallerInfo,
	)
	r.refreshPoller = goro.NewHandle(backgroundCtx).Go(r.refreshServicesLoop)
}

// StopLifecycle stops this component. It should only be invoked by an fx lifecycle hook.
// Should not be called multiple times or concurrently with StartLifecycle()
func (r *IncomingServiceRegistry) StopLifecycle() {
	if r.refreshPoller != nil {
		r.refreshPoller.Cancel()
		<-r.refreshPoller.Done()
	}
}

func (r *IncomingServiceRegistry) Get(ctx context.Context, id string) (*nexus.IncomingService, error) {
	if !r.config.nexusAPIsEnabled() {
		return nil, errNexusAPIsDisabled
	}

	if err := r.waitUntilInitialized(ctx); err != nil {
		return nil, err
	}

	r.dataLock.RLock()
	defer r.dataLock.RUnlock()

	service, ok := r.services[id]
	if !ok {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("could not find Nexus incoming service with ID: %v", id))
	}

	return service, nil
}

func (r *IncomingServiceRegistry) waitUntilInitialized(ctx context.Context) error {
	select {
	case <-r.serviceDataReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *IncomingServiceRegistry) refreshServicesLoop(ctx context.Context) error {
	hasLoadedServiceData := false
	minWaitTime := r.config.refreshMinWait()

	for ctx.Err() == nil {
		// Wait for Nexus APIs to be enabled.
		if !r.config.nexusAPIsEnabled() {
			if hasLoadedServiceData {
				// Nexus APIs were previously enabled and service data loaded, so make future requests wait for
				// reload once the FF is re-enabled.
				r.serviceDataReady = make(chan struct{})
			}
			hasLoadedServiceData = false
			common.InterruptibleSleep(ctx, r.config.refreshMinWait())
			continue
		}

		start := time.Now()
		if !hasLoadedServiceData {
			// Loading services for the first time after being (re)enabled, so load with fallback to persistence
			// and unblock any threads waiting on r.serviceDataReady if successful.
			err := backoff.ThrottleRetryContext(ctx, r.loadServices, r.config.refreshRetryPolicy, nil)
			if err == nil {
				hasLoadedServiceData = true
				close(r.serviceDataReady)
			}
		} else {
			// Services have previously been loaded, so just keep them up to date by with long poll requests
			// to matching, without fallback to persistence. Ignoring long poll errors since we will just retry
			// on next loop iteration.
			_ = backoff.ThrottleRetryContext(ctx, r.refreshServices, r.config.refreshRetryPolicy, nil)
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
			minWaitTime = min(minWaitTime*2, r.config.refreshLongPollTimeout()/2)
		} else {
			minWaitTime = r.config.refreshMinWait()
		}
	}

	return ctx.Err()
}

// loadServices initializes the in-memory view of service data.
// It first tries to load from matching service and falls back to querying persistence directly if matching is unavailable.
func (r *IncomingServiceRegistry) loadServices(ctx context.Context) error {
	tableVersion, services, err := r.getAllServicesMatching(ctx)
	if err != nil {
		// Fallback to persistence on matching error during initial load.
		r.logger.Error("error from matching when initializing Nexus incoming service cache", tag.Error(err))
		tableVersion, services, err = r.getAllServicesPersistence(ctx)
		if err != nil {
			return err
		}
	}

	r.tableVersion = tableVersion
	r.services = services
	return nil
}

// refreshServices sends long-poll requests to matching to check for any updates to service data.
func (r *IncomingServiceRegistry) refreshServices(ctx context.Context) error {
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
		r.logger.Error("long poll to refresh Nexus incoming services returned error", tag.Error(err))
		return err
	}

	if resp.TableVersion == currentTableVersion {
		// Long poll returned with no changes.
		return nil
	}

	currentTableVersion = resp.TableVersion
	services := make(map[string]*nexus.IncomingService, len(resp.Services))
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
				// Indicates table was updated during paging, so reset and start from the beginning.
				currentTableVersion, services, err = r.getAllServicesMatching(ctx)
				if err != nil {
					r.logger.Error("error during background refresh of Nexus incoming services", tag.Error(err))
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

	r.dataLock.Lock()
	defer r.dataLock.Unlock()

	r.tableVersion = currentTableVersion
	r.services = services

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
				// indicates table was updated during paging, so reset and start from the beginning.
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
				// indicates table was updated during paging, so reset and start from the beginning.
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
