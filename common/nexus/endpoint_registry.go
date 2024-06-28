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

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/internal/goro"
)

type (
	EndpointRegistryConfig struct {
		refreshEnabled         dynamicconfig.BoolPropertyFn
		refreshLongPollTimeout dynamicconfig.DurationPropertyFn
		refreshPageSize        dynamicconfig.IntPropertyFn
		refreshMinWait         dynamicconfig.DurationPropertyFn
		refreshRetryPolicy     backoff.RetryPolicy
	}

	EndpointRegistry interface {
		// GetByName returns an endpoint entry for the endpoint name for a caller from the given namespace ID.
		// Note that the default implementation is global to the cluster and can ignore the namespace ID param.
		GetByName(ctx context.Context, namespaceID namespace.ID, endpointName string) (*persistencespb.NexusEndpointEntry, error)
		GetByID(ctx context.Context, endpointID string) (*persistencespb.NexusEndpointEntry, error)
		StartLifecycle()
		StopLifecycle()
	}

	// EndpointRegistryImpl manages a cached view of Nexus endpoints.
	// Endpoints are lazily-loaded into memory on the first read. Thereafter, endpoint data is kept up to date by
	// background long polling on matching service ListNexusEndpoints.
	EndpointRegistryImpl struct {
		config *EndpointRegistryConfig

		dataReady chan struct{}

		dataLock        sync.RWMutex // Protects tableVersion and endpoints.
		tableVersion    int64
		endpointsByID   map[string]*persistencespb.NexusEndpointEntry // Mapping of endpoint ID -> endpoint.
		endpointsByName map[string]*persistencespb.NexusEndpointEntry // Mapping of endpoint name -> endpoint.

		refreshPoller *goro.Handle

		matchingClient matchingservice.MatchingServiceClient
		persistence    p.NexusEndpointManager
		logger         log.Logger
	}
)

func NewEndpointRegistryConfig(dc *dynamicconfig.Collection) *EndpointRegistryConfig {
	config := &EndpointRegistryConfig{
		refreshEnabled:         dynamicconfig.EnableNexus.Get(dc),
		refreshLongPollTimeout: dynamicconfig.RefreshNexusEndpointsLongPollTimeout.Get(dc),
		refreshPageSize:        dynamicconfig.NexusEndpointListDefaultPageSize.Get(dc),
		refreshMinWait:         dynamicconfig.RefreshNexusEndpointsMinWait.Get(dc),
	}
	config.refreshRetryPolicy = backoff.NewExponentialRetryPolicy(config.refreshMinWait()).WithMaximumInterval(config.refreshLongPollTimeout())
	return config
}

func NewEndpointRegistry(
	config *EndpointRegistryConfig,
	matchingClient matchingservice.MatchingServiceClient,
	persistence p.NexusEndpointManager,
	logger log.Logger,
) *EndpointRegistryImpl {
	return &EndpointRegistryImpl{
		config:          config,
		dataReady:       make(chan struct{}),
		endpointsByID:   make(map[string]*persistencespb.NexusEndpointEntry),
		endpointsByName: make(map[string]*persistencespb.NexusEndpointEntry),
		matchingClient:  matchingClient,
		persistence:     persistence,
		logger:          logger,
	}
}

// StartLifecycle starts this component. It should only be invoked by an fx lifecycle hook.
// Should not be called multiple times or concurrently with StopLifecycle()
func (r *EndpointRegistryImpl) StartLifecycle() {
	backgroundCtx := headers.SetCallerInfo(
		context.Background(),
		headers.SystemBackgroundCallerInfo,
	)
	r.refreshPoller = goro.NewHandle(backgroundCtx).Go(r.refreshEndpointsLoop)
}

// StopLifecycle stops this component. It should only be invoked by an fx lifecycle hook.
// Should not be called multiple times or concurrently with StartLifecycle()
func (r *EndpointRegistryImpl) StopLifecycle() {
	if r.refreshPoller != nil {
		r.refreshPoller.Cancel()
		<-r.refreshPoller.Done()
	}
}

func (r *EndpointRegistryImpl) GetByName(ctx context.Context, _ namespace.ID, endpointName string) (*persistencespb.NexusEndpointEntry, error) {
	if err := r.waitUntilInitialized(ctx); err != nil {
		return nil, err
	}
	r.dataLock.RLock()
	defer r.dataLock.RUnlock()

	endpoint, ok := r.endpointsByName[endpointName]
	if !ok {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("could not find Nexus endpoint by name: %v", endpointName))
	}
	return endpoint, nil
}

func (r *EndpointRegistryImpl) GetByID(ctx context.Context, id string) (*persistencespb.NexusEndpointEntry, error) {
	if err := r.waitUntilInitialized(ctx); err != nil {
		return nil, err
	}

	r.dataLock.RLock()
	defer r.dataLock.RUnlock()

	endpoint, ok := r.endpointsByID[id]
	if !ok {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("could not find Nexus endpoint with ID: %v", id))
	}

	return endpoint, nil
}

func (r *EndpointRegistryImpl) waitUntilInitialized(ctx context.Context) error {
	select {
	case <-r.dataReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *EndpointRegistryImpl) refreshEndpointsLoop(ctx context.Context) error {
	hasLoadedEndpointData := false
	minWaitTime := r.config.refreshMinWait()

	for ctx.Err() == nil {
		if !r.config.refreshEnabled() {
			if hasLoadedEndpointData {
				// Nexus APIs were previously enabled and endpoint data loaded, so make future requests wait for
				// reload once the FF is re-enabled.
				r.dataReady = make(chan struct{})
			}
			hasLoadedEndpointData = false
			util.InterruptibleSleep(ctx, r.config.refreshMinWait())
			continue
		}

		start := time.Now()
		if !hasLoadedEndpointData {
			// Loading endpoints for the first time after being (re)enabled, so load with fallback to persistence
			// and unblock any threads waiting on r.dataReady if successful.
			err := backoff.ThrottleRetryContext(ctx, r.loadEndpoints, r.config.refreshRetryPolicy, nil)
			if err == nil {
				hasLoadedEndpointData = true
				close(r.dataReady)
			}
		} else {
			// Endpoints have previously been loaded, so just keep them up to date with long poll requests to
			// matching, without fallback to persistence. Ignoring long poll errors since we will just retry
			// on next loop iteration.
			_ = backoff.ThrottleRetryContext(ctx, r.refreshEndpoints, r.config.refreshRetryPolicy, nil)
		}
		elapsed := time.Since(start)

		// In general, we want to start a new call immediately on completion of the previous
		// one. But if the remote is broken and returns success immediately, we might end up
		// spinning. So enforce a minimum wait time that increases as long as we keep getting
		// very fast replies.
		if elapsed < minWaitTime {
			util.InterruptibleSleep(ctx, minWaitTime-elapsed)
			// Don't let this get near our call timeout, otherwise we can't tell the difference
			// between a fast reply and a timeout.
			minWaitTime = min(minWaitTime*2, r.config.refreshLongPollTimeout()/2)
		} else {
			minWaitTime = r.config.refreshMinWait()
		}
	}

	return ctx.Err()
}

// loadEndpoints initializes the in-memory view of endpoints data.
// It first tries to load from matching service and falls back to querying persistence directly if matching is unavailable.
func (r *EndpointRegistryImpl) loadEndpoints(ctx context.Context) error {
	tableVersion, endpoints, err := r.getAllEndpointsMatching(ctx)
	if err != nil {
		// Fallback to persistence on matching error during initial load.
		r.logger.Error("error from matching when initializing Nexus endpoint cache", tag.Error(err))
		tableVersion, endpoints, err = r.getAllEndpointsPersistence(ctx)
		if err != nil {
			return err
		}
	}
	endpointsByID := make(map[string]*persistencespb.NexusEndpointEntry, len(endpoints))
	endpointsByName := make(map[string]*persistencespb.NexusEndpointEntry, len(endpoints))
	for _, endpoint := range endpoints {
		endpointsByID[endpoint.Id] = endpoint
		endpointsByName[endpoint.Endpoint.Spec.Name] = endpoint
	}

	r.tableVersion = tableVersion
	r.endpointsByID = endpointsByID
	r.endpointsByName = endpointsByName
	return nil
}

// refreshEndpoints sends long-poll requests to matching to check for any updates to endpoint data.
func (r *EndpointRegistryImpl) refreshEndpoints(ctx context.Context) error {
	r.dataLock.RLock()
	currentTableVersion := r.tableVersion
	r.dataLock.RUnlock()

	resp, err := r.matchingClient.ListNexusEndpoints(ctx, &matchingservice.ListNexusEndpointsRequest{
		NextPageToken:         nil,
		PageSize:              int32(r.config.refreshPageSize()),
		LastKnownTableVersion: currentTableVersion,
		Wait:                  true,
	})
	if err != nil {
		if ctx.Err() == nil {
			r.logger.Error("long poll to refresh Nexus endpoints returned error", tag.Error(err))
		}
		return err
	}

	if resp.TableVersion == currentTableVersion {
		// Long poll returned with no changes.
		return nil
	}

	currentTableVersion = resp.TableVersion
	entries := resp.Entries

	currentPageToken := resp.NextPageToken
	for len(currentPageToken) != 0 {
		resp, err = r.matchingClient.ListNexusEndpoints(ctx, &matchingservice.ListNexusEndpointsRequest{
			NextPageToken:         currentPageToken,
			PageSize:              int32(r.config.refreshPageSize()),
			LastKnownTableVersion: currentTableVersion,
			Wait:                  false,
		})

		if err != nil {
			var fpe *serviceerror.FailedPrecondition
			if errors.As(err, &fpe) && fpe.Message == p.ErrNexusTableVersionConflict.Error() {
				// Indicates table was updated during paging, so reset and start from the beginning.
				currentTableVersion, entries, err = r.getAllEndpointsMatching(ctx)
				if err != nil {
					r.logger.Error("error during background refresh of Nexus endpoints", tag.Error(err))
					return err
				}
				break
			}
			return err
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		currentPageToken = resp.NextPageToken
		entries = append(entries, resp.Entries...)
	}

	endpointsByID := make(map[string]*persistencespb.NexusEndpointEntry, len(entries))
	endpointsByName := make(map[string]*persistencespb.NexusEndpointEntry, len(entries))
	for _, entry := range entries {
		endpointsByID[entry.Id] = entry
		endpointsByName[entry.Endpoint.Spec.Name] = entry
	}

	r.dataLock.Lock()
	defer r.dataLock.Unlock()

	r.tableVersion = currentTableVersion
	r.endpointsByID = endpointsByID
	r.endpointsByName = endpointsByName

	return nil
}

// getAllEndpointsMatching paginates over all endpoints returned by matching. It always does a simple get.
func (r *EndpointRegistryImpl) getAllEndpointsMatching(ctx context.Context) (int64, []*persistencespb.NexusEndpointEntry, error) {
	var currentPageToken []byte
	currentTableVersion := int64(0)
	entries := make([]*persistencespb.NexusEndpointEntry, 0)

	for ctx.Err() == nil {
		resp, err := r.matchingClient.ListNexusEndpoints(ctx, &matchingservice.ListNexusEndpointsRequest{
			NextPageToken:         currentPageToken,
			PageSize:              int32(r.config.refreshPageSize()),
			LastKnownTableVersion: currentTableVersion,
			Wait:                  false,
		})
		if err != nil {
			var fpe *serviceerror.FailedPrecondition
			if errors.As(err, &fpe) && fpe.Message == p.ErrNexusTableVersionConflict.Error() {
				// indicates table was updated during paging, so reset and start from the beginning.
				currentPageToken = nil
				currentTableVersion = 0
				entries = make([]*persistencespb.NexusEndpointEntry, 0, len(entries))
				continue
			}
			return 0, nil, err
		}

		currentTableVersion = resp.TableVersion
		entries = append(entries, resp.GetEntries()...)

		if len(resp.NextPageToken) == 0 {
			return currentTableVersion, entries, nil
		}

		currentPageToken = resp.NextPageToken
	}

	return 0, nil, ctx.Err()
}

// getAllEndpointsPersistence paginates over all endpoints returned by persistence.
// Should only be used as a fall-back if matching service is unavailable during initial load.
func (r *EndpointRegistryImpl) getAllEndpointsPersistence(ctx context.Context) (int64, []*persistencespb.NexusEndpointEntry, error) {
	var currentPageToken []byte

	currentTableVersion := int64(0)
	entries := make([]*persistencespb.NexusEndpointEntry, 0)

	for ctx.Err() == nil {
		resp, err := r.persistence.ListNexusEndpoints(ctx, &p.ListNexusEndpointsRequest{
			LastKnownTableVersion: currentTableVersion,
			NextPageToken:         currentPageToken,
			PageSize:              r.config.refreshPageSize(),
		})
		if err != nil {
			var fpe *serviceerror.FailedPrecondition
			if errors.As(err, &fpe) && fpe.Message == p.ErrNexusTableVersionConflict.Error() {
				// indicates table was updated during paging, so reset and start from the beginning.
				currentPageToken = nil
				currentTableVersion = 0
				entries = make([]*persistencespb.NexusEndpointEntry, 0, len(entries))
				continue
			}
			return 0, nil, err
		}

		currentTableVersion = resp.TableVersion
		entries = append(entries, resp.Entries...)

		if len(resp.NextPageToken) == 0 {
			return currentTableVersion, entries, nil
		}

		currentPageToken = resp.NextPageToken
	}

	return 0, nil, ctx.Err()
}
