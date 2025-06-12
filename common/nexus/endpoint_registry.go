package nexus

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/util"
)

type (
	EndpointRegistryConfig struct {
		refreshEnabled         dynamicconfig.TypedSubscribable[bool]
		refreshLongPollTimeout dynamicconfig.DurationPropertyFn
		refreshPageSize        dynamicconfig.IntPropertyFn
		refreshMinWait         dynamicconfig.DurationPropertyFn
		refreshRetryPolicy     backoff.RetryPolicy
		readThroughCacheSize   dynamicconfig.IntPropertyFn
		readThroughCacheTTL    dynamicconfig.DurationPropertyFn
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

		dataReady atomic.Pointer[dataReady]

		dataLock        sync.RWMutex // Protects tableVersion and endpoints.
		tableVersion    int64
		endpointsByID   map[string]*persistencespb.NexusEndpointEntry // Mapping of endpoint ID -> endpoint.
		endpointsByName map[string]*persistencespb.NexusEndpointEntry // Mapping of endpoint name -> endpoint.

		cancelDcSub func()

		matchingClient matchingservice.MatchingServiceClient
		persistence    p.NexusEndpointManager
		logger         log.Logger

		readThroughCacheByID cache.Cache
	}

	dataReady struct {
		refresh *goro.Handle  // handle to refresh goroutine
		ready   chan struct{} // channel that clients can wait on for state changes
	}
)

var ErrNexusDisabled = serviceerror.NewFailedPrecondition("nexus is disabled")

func NewEndpointRegistryConfig(dc *dynamicconfig.Collection) *EndpointRegistryConfig {
	config := &EndpointRegistryConfig{
		refreshEnabled:         dynamicconfig.EnableNexus.Subscribe(dc),
		refreshLongPollTimeout: dynamicconfig.RefreshNexusEndpointsLongPollTimeout.Get(dc),
		refreshPageSize:        dynamicconfig.NexusEndpointListDefaultPageSize.Get(dc),
		refreshMinWait:         dynamicconfig.RefreshNexusEndpointsMinWait.Get(dc),
		readThroughCacheSize:   dynamicconfig.NexusReadThroughCacheSize.Get(dc),
		readThroughCacheTTL:    dynamicconfig.NexusReadThroughCacheTTL.Get(dc),
	}
	config.refreshRetryPolicy = backoff.NewExponentialRetryPolicy(config.refreshMinWait()).WithMaximumInterval(config.refreshLongPollTimeout())
	return config
}

func NewEndpointRegistry(
	config *EndpointRegistryConfig,
	matchingClient matchingservice.MatchingServiceClient,
	persistence p.NexusEndpointManager,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *EndpointRegistryImpl {
	return &EndpointRegistryImpl{
		config:          config,
		endpointsByID:   make(map[string]*persistencespb.NexusEndpointEntry),
		endpointsByName: make(map[string]*persistencespb.NexusEndpointEntry),
		matchingClient:  matchingClient,
		persistence:     persistence,
		logger:          logger,
		readThroughCacheByID: cache.NewWithMetrics(config.readThroughCacheSize(), &cache.Options{
			TTL: config.readThroughCacheTTL(),
		}, metricsHandler.WithTags(metrics.CacheTypeTag(metrics.NexusEndpointRegistryReadThroughCacheTypeTagValue))),
	}
}

// StartLifecycle starts this component. It should only be invoked by an fx lifecycle hook.
// Should not be called multiple times or concurrently with StopLifecycle()
func (r *EndpointRegistryImpl) StartLifecycle() {
	initial, cancel := r.config.refreshEnabled(r.setEnabled)
	r.cancelDcSub = cancel
	r.setEnabled(initial)
}

// StopLifecycle stops this component. It should only be invoked by an fx lifecycle hook.
// Should not be called multiple times or concurrently with StartLifecycle()
func (r *EndpointRegistryImpl) StopLifecycle() {
	r.cancelDcSub()
	r.setEnabled(false)
}

func (r *EndpointRegistryImpl) setEnabled(enabled bool) {
	oldReady := r.dataReady.Load()
	if oldReady == nil && enabled {
		backgroundCtx := headers.SetCallerInfo(
			context.Background(),
			headers.SystemBackgroundHighCallerInfo,
		)
		newReady := &dataReady{
			refresh: goro.NewHandle(backgroundCtx),
			ready:   make(chan struct{}),
		}
		if r.dataReady.CompareAndSwap(oldReady, newReady) {
			newReady.refresh.Go(func(ctx context.Context) error {
				return r.refreshEndpointsLoop(ctx, newReady)
			})
		}
	} else if oldReady != nil && !enabled {
		if r.dataReady.CompareAndSwap(oldReady, nil) {
			oldReady.refresh.Cancel()
			<-oldReady.refresh.Done()
			// If oldReady.ready was not already closed here, callers blocked in waitUntilInitialized
			// will block indefinitely (until context timeout). If we wanted to wake them up, we
			// could close ready here, but we would need to use a sync.Once to avoid closing it
			// twice. Then waitUntilInitialized would need to reload r.dataReady to check that the
			// wakeup was due to data being ready rather than this close.
		}
	}
}

func (r *EndpointRegistryImpl) GetByName(ctx context.Context, _ namespace.ID, endpointName string) (*persistencespb.NexusEndpointEntry, error) {
	if err := r.waitUntilInitialized(ctx); err != nil {
		return nil, err
	}
	r.dataLock.RLock()
	endpoint, ok := r.endpointsByName[endpointName]
	r.dataLock.RUnlock()

	if !ok {
		return nil, serviceerror.NewNotFoundf("could not find Nexus endpoint by name: %v", endpointName)
	}
	return endpoint, nil
}

func (r *EndpointRegistryImpl) GetByID(ctx context.Context, id string) (*persistencespb.NexusEndpointEntry, error) {
	if err := r.waitUntilInitialized(ctx); err != nil {
		return nil, err
	}

	r.dataLock.RLock()
	endpoint, ok := r.endpointsByID[id]
	r.dataLock.RUnlock()

	if !ok {
		// Entry not found, attempt read-through to persistence.
		fut := future.NewFuture[*persistencespb.NexusEndpointEntry]()
		cachedFut, err := r.readThroughCacheByID.PutIfNotExist(id, fut)
		if err != nil {
			return nil, err
		}
		// The future was already in the cache, reuse it.
		if cachedFut != fut {
			return cachedFut.(future.Future[*persistencespb.NexusEndpointEntry]).Get(ctx)
		}
		endpoint, err = r.persistence.GetNexusEndpoint(ctx, &p.GetNexusEndpointRequest{
			ID: id,
		})
		fut.Set(endpoint, err)
		return endpoint, err
	}

	return endpoint, nil
}

func (r *EndpointRegistryImpl) waitUntilInitialized(ctx context.Context) error {
	dataReady := r.dataReady.Load()
	if dataReady == nil {
		return ErrNexusDisabled
	}
	select {
	case <-dataReady.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *EndpointRegistryImpl) refreshEndpointsLoop(ctx context.Context, dataReady *dataReady) error {
	hasLoadedEndpointData := false

	for ctx.Err() == nil {
		start := time.Now()
		enforceMinWait := true
		if !hasLoadedEndpointData {
			// Loading endpoints for the first time after being (re)enabled, so load with fallback to persistence
			// and unblock any threads waiting on r.dataReady if successful.
			err := backoff.ThrottleRetryContext(ctx, r.loadEndpoints, r.config.refreshRetryPolicy, nil)
			if err == nil {
				hasLoadedEndpointData = true
				enforceMinWait = false
				// Note: do not reload r.dataReady here, use value from argument to ensure that
				// each channel is closed no more than once.
				close(dataReady.ready)
			}
		} else {
			r.dataLock.Lock()
			prevTableVersion := r.tableVersion
			r.dataLock.Unlock()

			// Endpoints have previously been loaded, so just keep them up to date with long poll requests to
			// matching, without fallback to persistence. Ignoring long poll errors since we will just retry
			// on next loop iteration.
			_ = backoff.ThrottleRetryContext(ctx, r.refreshEndpoints, r.config.refreshRetryPolicy, nil)

			r.dataLock.Lock()
			enforceMinWait = prevTableVersion == r.tableVersion
			r.dataLock.Unlock()
		}
		elapsed := time.Since(start)

		minWaitTime := r.config.refreshMinWait()
		// In general, we want to start a new call immediately on completion of the previous one. But if the remote is
		// broken and returns success immediately, we might end up spinning. So enforce a minimum wait time that
		// increases as long as we keep getting very fast replies. Only enforce the min wait if the remote does not
		// return new data.
		if enforceMinWait && elapsed < minWaitTime {
			util.InterruptibleSleep(ctx, minWaitTime-elapsed)
		}
	}

	return ctx.Err()
}

// loadEndpoints initializes the in-memory view of endpoints data.
// It first tries to load from matching service and falls back to querying persistence directly if matching is unavailable.
func (r *EndpointRegistryImpl) loadEndpoints(ctx context.Context) error {
	tableVersion, endpoints, err := r.getAllEndpointsMatchingWithPersistenceFallback(ctx)
	if err != nil {
		return err
	}
	endpointsByID := make(map[string]*persistencespb.NexusEndpointEntry, len(endpoints))
	endpointsByName := make(map[string]*persistencespb.NexusEndpointEntry, len(endpoints))
	for _, endpoint := range endpoints {
		endpointsByID[endpoint.Id] = endpoint
		endpointsByName[endpoint.Endpoint.Spec.Name] = endpoint
	}

	r.dataLock.Lock()
	defer r.dataLock.Unlock()

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

func (r *EndpointRegistryImpl) getAllEndpointsMatchingWithPersistenceFallback(ctx context.Context) (int64, []*persistencespb.NexusEndpointEntry, error) {
	tableVersion, endpoints, err := r.getAllEndpointsMatching(ctx)
	if err != nil {
		// Fallback to persistence on matching error during initial load.
		r.logger.Error("error from matching when initializing Nexus endpoint cache", tag.Error(err))
		tableVersion, endpoints, err = r.getAllEndpointsPersistence(ctx)
	}
	return tableVersion, endpoints, err
}

// getAllEndpointsMatching paginates over all endpoints returned by matching. It always does a simple get.
func (r *EndpointRegistryImpl) getAllEndpointsMatching(ctx context.Context) (int64, []*persistencespb.NexusEndpointEntry, error) {
	return r.getAllEndpoints(ctx, func(currentTableVersion int64, currentPageToken []byte) (int64, []byte, []*persistencespb.NexusEndpointEntry, error) {
		resp, err := r.matchingClient.ListNexusEndpoints(ctx, &matchingservice.ListNexusEndpointsRequest{
			NextPageToken:         currentPageToken,
			PageSize:              int32(r.config.refreshPageSize()),
			LastKnownTableVersion: currentTableVersion,
			Wait:                  false,
		})
		if err != nil {
			return 0, nil, nil, err
		}
		return resp.TableVersion, resp.NextPageToken, resp.Entries, nil
	})
}

// getAllEndpointsPersistence paginates over all endpoints returned by persistence.
// Should only be used as a fall-back if matching service is unavailable during initial load.
func (r *EndpointRegistryImpl) getAllEndpointsPersistence(ctx context.Context) (int64, []*persistencespb.NexusEndpointEntry, error) {
	return r.getAllEndpoints(ctx, func(currentTableVersion int64, currentPageToken []byte) (int64, []byte, []*persistencespb.NexusEndpointEntry, error) {
		resp, err := r.persistence.ListNexusEndpoints(ctx, &p.ListNexusEndpointsRequest{
			LastKnownTableVersion: currentTableVersion,
			NextPageToken:         currentPageToken,
			PageSize:              r.config.refreshPageSize(),
		})
		if err != nil {
			return 0, nil, nil, err
		}
		return resp.TableVersion, resp.NextPageToken, resp.Entries, nil
	})
}

// getAllEndpointsPersistence paginates over all endpoints returned by persistence.
// Should only be used as a fall-back if matching service is unavailable during initial load.
func (r *EndpointRegistryImpl) getAllEndpoints(ctx context.Context, getter func(int64, []byte) (int64, []byte, []*persistencespb.NexusEndpointEntry, error)) (int64, []*persistencespb.NexusEndpointEntry, error) {
	var currentPageToken []byte

	currentTableVersion := int64(0)
	entries := make([]*persistencespb.NexusEndpointEntry, 0)

	for ctx.Err() == nil {
		respTableVersion, respNextPageToken, respEntries, err := getter(currentTableVersion, currentPageToken)
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

		currentTableVersion = respTableVersion
		entries = append(entries, respEntries...)

		if len(respNextPageToken) == 0 {
			return currentTableVersion, entries, nil
		}

		currentPageToken = respNextPageToken
	}

	return 0, nil, ctx.Err()
}
