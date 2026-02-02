//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination registry_mock.go

// Package nsregistry provides a cached namespace registry that reduces load on persistence
// by maintaining an in-memory cache of namespace information.
//
// The registry supports two modes of cache synchronization:
//
//  1. Watch-based: If the persistence layer supports watching namespace changes
//     (via WatchNamespaces), the registry subscribes to namespace events. This provides
//     timely cache updates when namespaces are created, updated, or deleted without needing to
//     poll the database.
//
//  2. Polling-based: If watches are not supported (persistence returns ErrWatchNotSupported when
//     trying to start a watch), the registry falls back to periodic polling at a configurable
//     interval set via dynamic config.
//
// The registry also supports read-through caching: when a namespace is requested but not
// found in the cache, it queries persistence directly and caches the result. A separate
// not-found cache prevents repeated lookups for non-existent namespaces.
//
// State change callbacks can be registered to receive notifications when namespace state changes. If watches
// are in use, callbacks are called immediately after a watch event is received. If polling is in use, callbacks
// are called each time the cache is refreshed from the database.
package nsregistry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/pingable"
	expmaps "golang.org/x/exp/maps"
)

const (
	readthroughCacheSize = 64 * 1024
	// CacheRefreshFailureRetryInterval is the wait time
	// if refreshment encounters error
	CacheRefreshFailureRetryInterval = 1 * time.Second
	CacheRefreshPageSize             = 1000
	readthroughCacheTTL              = 1 * time.Second // represents minimum time to wait before trying to readthrough again
	readthroughTimeout               = 3 * time.Second
	// startWatchMaxAttempts limits initial watch setup retries to avoid blocking
	// server startup indefinitely if persistence is temporarily unavailable.
	startWatchMaxAttempts = 10
	// Metrics and logs are emitted for callbacks that take longer than slowCallbackDuration
	slowCallbackDuration = 250 * time.Millisecond
)

var (
	readthroughNotFoundCacheOpts = cache.Options{
		TTL: readthroughCacheTTL,
	}
)

type (
	// Clock provides timestamping to Registry objects
	Clock interface {
		// Now returns the current time.
		Now() time.Time
	}

	// Persistence describes the durable storage requirements for a Registry
	// instance.
	Persistence interface {
		// GetNamespace reads the state for a single namespace by name or ID
		// from persistent storage, returning an instance of
		// serviceerror.NamespaceNotFound if there is no matching Namespace.
		GetNamespace(
			context.Context,
			*persistence.GetNamespaceRequest,
		) (*persistence.GetNamespaceResponse, error)

		// ListNamespaces fetches a paged set of namespace persistent state
		// instances.
		ListNamespaces(
			context.Context,
			*persistence.ListNamespacesRequest,
		) (*persistence.ListNamespacesResponse, error)

		// GetMetadata fetches the notification version for Temporal namespaces.
		GetMetadata(context.Context) (*persistence.GetMetadataResponse, error)

		// WatchNamespaces returns a channel that receives events when namespaces are created, updated, or deleted. It returns
		// an error if the watch cannot be started or ErrWatchNotSupported if the persistence implementation does not support
		// watches.
		WatchNamespaces(ctx context.Context) (<-chan *persistence.NamespaceWatchEvent, error)
	}

	registry struct {
		refresher               *goro.Handle
		persistence             Persistence
		globalNamespacesEnabled bool
		clock                   Clock
		metricsHandler          metrics.Handler
		logger                  log.Logger
		refreshInterval         dynamicconfig.DurationPropertyFn

		// nsMapsLock protects nameToID, idToNamespace, and stateChangedDuringReadthrough
		nsMapsLock    sync.RWMutex
		nameToID      map[namespace.Name]namespace.ID
		idToNamespace map[namespace.ID]*namespace.Namespace

		// stateChangeCallbacks is a sync.Map so that it can be used without contending for the lock protecting the cache maps; we don't
		// need to block namespace operations while running or updating callbacks.
		stateChangeCallbacks          sync.Map // map[any]StateChangeCallbackFn
		stateChangedDuringReadthrough []*namespace.Namespace

		// readthroughLock protects readthroughNotFoundCache and requests to persistence
		// it should be acquired before checking readthroughNotFoundCache, making a request
		// to persistence, or updating readthroughNotFoundCache
		// It should be acquired before nsMapsLock (above) if both are required
		readthroughLock sync.Mutex
		// readthroughNotFoundCache stores namespaces that missed the above caches
		// AND was not found when reading through to the persistence layer
		readthroughNotFoundCache cache.Cache

		// Temporary solution to force read search attributes from persistence
		forceSearchAttributesCacheRefreshOnRead dynamicconfig.BoolPropertyFn
		replicationResolverFactory              namespace.ReplicationResolverFactory
	}

	// watchStartResult holds the result of successfully starting a namespace watch.
	watchStartResult struct {
		eventCh     <-chan *persistence.NamespaceWatchEvent
		watchCtx    context.Context
		watchCancel context.CancelFunc
	}
)

var _ namespace.Registry = (*registry)(nil)

// NewRegistry creates a new instance of Registry for accessing and caching
// namespace information to reduce the load on persistence.
func NewRegistry(
	aPersistence Persistence,
	enableGlobalNamespaces bool,
	refreshInterval dynamicconfig.DurationPropertyFn,
	forceSearchAttributesCacheRefreshOnRead dynamicconfig.BoolPropertyFn,
	metricsHandler metrics.Handler,
	logger log.Logger,
	replicationResolverFactory namespace.ReplicationResolverFactory,
) *registry {
	reg := &registry{
		persistence:              aPersistence,
		globalNamespacesEnabled:  enableGlobalNamespaces,
		clock:                    clock.NewRealTimeSource(),
		metricsHandler:           metricsHandler.WithTags(metrics.OperationTag(metrics.NamespaceCacheScope)),
		logger:                   logger,
		nameToID:                 make(map[namespace.Name]namespace.ID),
		idToNamespace:            make(map[namespace.ID]*namespace.Namespace),
		refreshInterval:          refreshInterval,
		readthroughNotFoundCache: cache.New(readthroughCacheSize, &readthroughNotFoundCacheOpts),

		forceSearchAttributesCacheRefreshOnRead: forceSearchAttributesCacheRefreshOnRead,
		replicationResolverFactory:              replicationResolverFactory,
	}
	return reg
}

// GetRegistrySize observes the size of the by-name and by-ID maps.
func (r *registry) GetRegistrySize() (idToNamespaceSize int64, nameToIDSize int64) {
	r.nsMapsLock.RLock()
	defer r.nsMapsLock.RUnlock()
	return int64(len(r.idToNamespace)), int64(len(r.nameToID))
}

func (r *registry) RefreshNamespaceById(id namespace.ID) (*namespace.Namespace, error) {
	r.readthroughLock.Lock()
	defer r.readthroughLock.Unlock()
	ns, err := r.getNamespaceByIDPersistence(id)
	if err != nil {
		return nil, err
	}
	r.updateSingleNamespace(ns, false)
	return ns, nil
}

// Start initializes the namespace registry and begins background refresh. Should only be invoked by fx lifecycle hook.
// Should not be called multiple times or concurrently with Stop().
//
// It first attempts to establish a watch on namespace changes. If watches are supported, events are processed as they
// arrive. If not supported, falls back to periodic polling. Start blocks until the initial namespace refresh completes.
// The initial refresh must succeed or the function will fatal.
func (r *registry) Start() {
	ctx := headers.SetCallerInfo(
		context.Background(),
		headers.SystemBackgroundHighCallerInfo,
	)

	watchStarted := false
	r.refresher, watchStarted = r.runWatchLoop(ctx)
	if watchStarted {
		// Watch started successfully
		return
	}

	if err := r.refresher.Err(); !errors.Is(err, persistence.ErrWatchNotSupported) {
		// Watch failed to start for a reason other than ErrWatchNotSupported
		metrics.NamespaceRegistryWatchStartFailures.With(r.metricsHandler).Record(1)
		r.logger.Warn("Unable to start namespace watch - falling back to polling", tag.Error(err))
	} else {
		r.logger.Info("Watch not supported by persistence, namespace registry will use polling")
	}

	// Fall back to polling
	if err := r.refreshNamespaces(ctx); err != nil {
		r.logger.Fatal("Unable to initialize namespace registry", tag.Error(err))
	}
	r.refresher = goro.NewHandle(ctx).Go(r.runPollingLoop)
}

// Stop ends background refresh. Should only be invoked by fx lifecycle hook.
// Should not be called multiple times or concurrently with Start().
func (r *registry) Stop() {
	// refresher may be nil if watch failed to start and we're shutting down.
	if r.refresher != nil {
		r.refresher.Cancel()
		<-r.refresher.Done()
	}
}

func (r *registry) GetPingChecks() []pingable.Check {
	return []pingable.Check{
		{
			Name: "namespace registry lock",
			// we don't do any persistence ops, this shouldn't be blocked
			Timeout: 10 * time.Second,
			Ping: func() []pingable.Pingable {
				// just checking if we can acquire the lock
				r.nsMapsLock.Lock()
				// nolint:staticcheck
				r.nsMapsLock.Unlock()
				return nil
			},
			MetricsName: metrics.DDNamespaceRegistryLockLatency.Name(),
		},
	}
}

func (r *registry) getAllNamespace() []*namespace.Namespace {
	r.nsMapsLock.RLock()
	defer r.nsMapsLock.RUnlock()
	return expmaps.Values(r.idToNamespace)
}

func (r *registry) RegisterStateChangeCallback(key any, cb namespace.StateChangeCallbackFn) {
	// Store callback first to avoid race where watch events arrive between reading the namespace snapshot and storing the
	// callback. This ensures no events are missed, but introduces a different trade-off: The callback may receive duplicate
	// calls for the same namespace if a watch event arrives while we're iterating through the catch-up loop below. For
	// example:
	//   1. Callback is stored in stateChangeCallbacks
	//   2. Watch event arrives for namespace X, callback is invoked
	//   3. Catch-up loop reaches namespace X, callback is invoked again
	//
	// This is acceptable because callbacks are rarely added (so unlikely to trigger this) and callbacks should be idempotent anyway.
	callbackWithTiming := func(ns *namespace.Namespace, deletedFromDb bool) {
		// Track callback duration so we can identify slow callbacks
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			if duration > slowCallbackDuration {
				metrics.NamespaceRegistrySlowCallbacks.With(r.metricsHandler).Record(1)
				r.logger.Warn(
					"Namespace registry callback slow",
					tag.Key(fmt.Sprintf("%v", key)),
					tag.Duration("duration", duration),
				)
			}
		}()

		cb(ns, deletedFromDb)
	}

	r.stateChangeCallbacks.Store(key, namespace.StateChangeCallbackFn(callbackWithTiming))

	r.nsMapsLock.RLock()
	allNamespaces := expmaps.Values(r.idToNamespace)
	r.nsMapsLock.RUnlock()

	// call once for each namespace already in the registry
	for _, ns := range allNamespaces {
		callbackWithTiming(ns, false)
	}
}

func (r *registry) UnregisterStateChangeCallback(key any) {
	r.stateChangeCallbacks.Delete(key)
}

// GetNamespace retrieves the information from the internal maps if it exists, otherwise retrieves the information from metadata
// store and update internal entries with an expiry before returning back
func (r *registry) GetNamespace(name namespace.Name) (*namespace.Namespace, error) {
	if name == "" {
		return nil, serviceerror.NewInvalidArgument("Namespace is empty.")
	}
	return r.getOrReadthroughNamespace(name)
}

// GetNamespaceWithOptions retrieves a namespace entry by name, with behavior controlled by options.
func (r *registry) GetNamespaceWithOptions(name namespace.Name, opts namespace.GetNamespaceOptions) (*namespace.Namespace, error) {
	if name == "" {
		return nil, serviceerror.NewInvalidArgument("Namespace is empty.")
	}
	if opts.DisableReadthrough {
		return r.getNamespace(name)
	}
	return r.getOrReadthroughNamespace(name)
}

// GetNamespaceByID retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (r *registry) GetNamespaceByID(id namespace.ID) (*namespace.Namespace, error) {
	if id == "" {
		return nil, serviceerror.NewInvalidArgument("NamespaceID is empty.")
	}
	return r.getOrReadthroughNamespaceByID(id)
}

// GetNamespaceByIDWithOptions retrieves a namespace entry by id, with behavior controlled by options.
func (r *registry) GetNamespaceByIDWithOptions(id namespace.ID, opts namespace.GetNamespaceOptions) (*namespace.Namespace, error) {
	if id == "" {
		return nil, serviceerror.NewInvalidArgument("NamespaceID is empty.")
	}
	if opts.DisableReadthrough {
		return r.getNamespaceByID(id)
	}
	return r.getOrReadthroughNamespaceByID(id)
}

// GetNamespaceID retrieves namespaceID by using GetNamespace
func (r *registry) GetNamespaceID(
	name namespace.Name,
) (namespace.ID, error) {

	ns, err := r.GetNamespace(name)
	if err != nil {
		return "", err
	}
	return ns.ID(), nil
}

// GetNamespaceName returns namespace name given the namespace id
func (r *registry) GetNamespaceName(
	id namespace.ID,
) (namespace.Name, error) {

	ns, err := r.getOrReadthroughNamespaceByID(id)
	if err != nil {
		return "", err
	}
	return ns.Name(), nil
}

// GetCustomSearchAttributesMapper is a temporary solution to be able to get search attributes
// with from persistence if forceSearchAttributesCacheRefreshOnRead is true.
func (r *registry) GetCustomSearchAttributesMapper(name namespace.Name) (namespace.CustomSearchAttributesMapper, error) {
	var ns *namespace.Namespace
	var err error
	if r.forceSearchAttributesCacheRefreshOnRead() {
		r.readthroughLock.Lock()
		defer r.readthroughLock.Unlock()
		ns, err = r.getNamespaceByNamePersistence(name)
	} else {
		ns, err = r.GetNamespace(name)
	}
	if err != nil {
		return namespace.CustomSearchAttributesMapper{}, err
	}
	return ns.CustomSearchAttributesMapper(), nil
}

// watchLoop processes namespace watch events and handles restarts on failure.
// It returns when the context is cancelled or the watch channel is closed.
func (r *registry) watchLoop(ctx context.Context, watchCh <-chan *persistence.NamespaceWatchEvent) {
	r.logger.Info("Starting namespace registry loop")
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-watchCh:
			var err error
			if ok {
				if event.Err == nil {
					err = r.processWatchEvent(event)
				} else {
					err = event.Err
				}
			}

			if !ok || err != nil {
				r.logger.Error("Namespace watch failed, restarting", tag.Error(err), tag.Bool("closed", ok))
				metrics.NamespaceRegistryWatchReconnections.With(r.metricsHandler).Record(1)
				return
			}
		}
	}
}

// watchStartRetryPolicy returns a retry policy for watch startup attempts.
// On initial startup (initialWatch=true), retries are limited to avoid blocking server startup indefinitely.
// On reconnection after a previous success (initialWatch=false), retries continue indefinitely.
func watchStartRetryPolicy(initialWatch bool) backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(CacheRefreshFailureRetryInterval)
	if initialWatch {
		return policy.WithMaximumAttempts(startWatchMaxAttempts)
	}
	return policy.WithExpirationInterval(backoff.NoInterval)
}

// runWatchLoop starts a goroutine that establishes and maintains a namespace watch. Returns the goroutine handle and a
// boolean indicating whether the watch started successfully. This method blocks until the watch is established and the first refresh
// is successful.
//
// Uses ShutdownOnce to track whether the watch has ever started successfully, which affects retry behavior: limited
// retries on initial startup, unlimited on reconnection.
func (r *registry) runWatchLoop(ctx context.Context) (*goro.Handle, bool) {
	// watchStartedOnce tracks whether the watch has ever started successfully.
	// Used to determine retry policy and signal to the caller when watch is ready.
	watchStartedOnce := channel.NewShutdownOnce()

	handle := goro.NewHandle(ctx).Go(
		func(ctx context.Context) error {
			// Outer loop handles watch restarts after connection failures.
			for {
				select {
				case <-ctx.Done():
					return nil
				default:
				}

				result, err := r.startWatch(ctx, !watchStartedOnce.IsShutdown())
				if err != nil {
					return err
				}

				watchStartedOnce.Shutdown()
				r.watchLoop(result.watchCtx, result.eventCh)
				result.watchCancel()
			}
		},
	)

	// Wait for either the watch to start successfully, or the goroutine to exit (due to error
	// or because watch is not supported). Return true only if watch started successfully.
	select {
	case <-watchStartedOnce.Channel():
		return handle, true
	case <-handle.Done():
		return handle, false
	}
}

// startWatch attempts to establish a namespace watch with retries.
// Returns the watch channel and context on success.
func (r *registry) startWatch(ctx context.Context, initialWatch bool) (watchStartResult, error) {
	return backoff.ThrottleRetryContextWithReturn(
		ctx,
		func(ctx context.Context) (startResult watchStartResult, err error) {
			// Create fresh watch context for this attempt
			watchCtx, watchCancel := context.WithCancel(ctx)
			defer func() {
				if err != nil {
					// Cancel attempt's watch context to clean up any partial watch state
					watchCancel()
				}
			}()

			startResult.watchCtx = watchCtx
			startResult.watchCancel = watchCancel

			if startResult.eventCh, err = r.persistence.WatchNamespaces(watchCtx); err != nil {
				if !errors.Is(err, persistence.ErrWatchNotSupported) {
					r.logger.Error("Error starting namespace watch", tag.Error(err))
				}
				return
			}

			// A full refresh must be done *after* (not before) the watch is established to ensure no updates are missed. If the
			// refresh were done before establishing the watch, updates that occur between the two could be missed.
			r.logger.Info("Namespace watch started")
			if err = r.refreshNamespaces(ctx); err != nil {
				r.logger.Error("Error refreshing namespaces after watch start", tag.Error(err))
				return
			}
			r.logger.Info("Initial refresh completed")

			return
		},
		watchStartRetryPolicy(initialWatch),
		func(err error) bool {
			return !errors.Is(err, persistence.ErrWatchNotSupported)
		},
	)
}

// runPollingLoop periodically refreshes the namespace cache.
// Used as fallback when namespace watches are not supported.
func (r *registry) runPollingLoop(ctx context.Context) error {
	timer := time.NewTimer(r.refreshInterval())

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-timer.C:
			err := r.refreshNamespaces(ctx)
			for err != nil {
				r.logger.Error("Error refreshing namespace cache", tag.Error(err))
				timerFailureRetry := time.NewTimer(CacheRefreshFailureRetryInterval)
				select {
				case <-ctx.Done():
					return nil
				case <-timerFailureRetry.C:
				}
				err = r.refreshNamespaces(ctx)
			}
		}
		timer.Reset(r.refreshInterval())
	}
}

func (r *registry) refreshNamespaces(ctx context.Context) (err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			metrics.NamespaceRegistryRefreshFailures.With(r.metricsHandler).Record(1)
		}
		metrics.NamespaceRegistryRefreshLatency.With(r.metricsHandler).Record(time.Since(start))
	}()

	request := &persistence.ListNamespacesRequest{
		PageSize:       CacheRefreshPageSize,
		IncludeDeleted: true,
	}
	var namespacesDb namespace.Namespaces
	namespaceIDsDb := make(map[namespace.ID]struct{})

	for {
		// TODO: consider adding a timeout and/or retries here - long ListNamespaces
		// calls could delay watch reconnection or block shutdown
		response, err := r.persistence.ListNamespaces(ctx, request)
		if err != nil {
			return err
		}
		for _, namespaceDb := range response.Namespaces {
			ns, err := namespace.FromPersistentState(
				namespaceDb.Namespace,
				r.replicationResolverFactory(namespaceDb.Namespace),
				namespace.WithGlobalFlag(namespaceDb.IsGlobalNamespace),
				namespace.WithNotificationVersion(namespaceDb.NotificationVersion),
			)
			if err != nil {
				return err
			}
			namespacesDb = append(namespacesDb, ns)
			namespaceIDsDb[namespace.ID(namespaceDb.Namespace.Info.Id)] = struct{}{}
		}
		if len(response.NextPageToken) == 0 {
			break
		}
		request.NextPageToken = response.NextPageToken
	}

	// Make a copy of the existing namespace maps (excluding deleted), so we can calculate diff and do atomic swap.
	newNameToID := make(map[namespace.Name]namespace.ID)
	newIDToNamespace := make(map[namespace.ID]*namespace.Namespace)

	var deletedEntries []*namespace.Namespace
	for _, ns := range r.getAllNamespace() {
		if _, namespaceExistsDb := namespaceIDsDb[ns.ID()]; !namespaceExistsDb {
			deletedEntries = append(deletedEntries, ns)
			continue
		}
		newNameToID[ns.Name()] = ns.ID()
		newIDToNamespace[ns.ID()] = ns
	}

	var stateChanged []*namespace.Namespace
	for _, aNamespace := range namespacesDb {
		oldNS := r.updateIDToNamespace(newIDToNamespace, aNamespace.ID(), aNamespace)
		// If namespace was renamed, remove entry for the old name
		if oldNS != nil && oldNS.Name() != aNamespace.Name() {
			delete(newNameToID, oldNS.Name())
		}
		newNameToID[aNamespace.Name()] = aNamespace.ID()

		if namespaceStateChanged(oldNS, aNamespace) {
			stateChanged = append(stateChanged, aNamespace)
		}
	}

	r.nsMapsLock.Lock()
	r.idToNamespace = newIDToNamespace
	r.nameToID = newNameToID
	stateChanged = append(stateChanged, r.stateChangedDuringReadthrough...)
	r.stateChangedDuringReadthrough = nil
	r.nsMapsLock.Unlock()

	r.stateChangeCallbacks.Range(
		func(_, value any) bool {
			//revive:disable-next-line:unchecked-type-assertion
			cb := value.(namespace.StateChangeCallbackFn)

			for _, ns := range deletedEntries {
				cb(ns, true)
			}
			for _, ns := range stateChanged {
				cb(ns, false)
			}

			return true
		})

	return nil
}

// processWatchEvent handles a single namespace watch event by updating the cache
// and invoking state change callbacks.
func (r *registry) processWatchEvent(event *persistence.NamespaceWatchEvent) error {
	var executeCallbacks bool
	var ns *namespace.Namespace

	switch event.Type {
	case persistence.NamespaceWatchEventTypeCreate, persistence.NamespaceWatchEventTypeUpdate:
		// event.Response is assumed non-nil here; persistence implementations must ensure this.
		var err error
		ns, err = namespace.FromPersistentState(
			event.Response.Namespace,
			r.replicationResolverFactory(event.Response.Namespace),
			namespace.WithGlobalFlag(event.Response.IsGlobalNamespace),
			namespace.WithNotificationVersion(event.Response.NotificationVersion),
		)
		if err != nil {
			return err
		}
		executeCallbacks = r.updateSingleNamespace(ns, true)
	case persistence.NamespaceWatchEventTypeDelete:
		ns = r.deleteNamespace(event.NamespaceID)
		executeCallbacks = ns != nil
	default:
		r.logger.Warn("Unknown namespace watch event type", tag.Int("eventType", int(event.Type)))
	}

	if executeCallbacks {
		isDelete := event.Type == persistence.NamespaceWatchEventTypeDelete

		r.stateChangeCallbacks.Range(
			func(key, value any) bool {
				//revive:disable-next-line:unchecked-type-assertion
				cb := value.(namespace.StateChangeCallbackFn)
				cb(ns, isDelete)
				return true
			})
	}

	return nil
}

// deleteNamespace removes a namespace from the cache and returns the deleted namespace if it existed
func (r *registry) deleteNamespace(id namespace.ID) *namespace.Namespace {
	r.nsMapsLock.Lock()
	defer r.nsMapsLock.Unlock()
	ns, exists := r.idToNamespace[id]
	if !exists {
		return nil
	}
	delete(r.idToNamespace, id)
	delete(r.nameToID, ns.Name())
	return ns
}

func (r *registry) updateIDToNamespace(
	iDToNamespace map[namespace.ID]*namespace.Namespace,
	id namespace.ID,
	newNS *namespace.Namespace,
) *namespace.Namespace {
	oldNS, _ := iDToNamespace[id]
	iDToNamespace[id] = newNS
	return oldNS
}

// getNamespace retrieves the information from the cache if it exists
func (r *registry) getNamespace(name namespace.Name) (*namespace.Namespace, error) {
	r.nsMapsLock.RLock()
	defer r.nsMapsLock.RUnlock()
	if id, ok := r.nameToID[name]; ok {
		return r.getNamespaceByIDLocked(id)
	}
	return nil, serviceerror.NewNamespaceNotFound(name.String())
}

// getNamespaceByID retrieves the information from the cache if it exists.
func (r *registry) getNamespaceByID(id namespace.ID) (*namespace.Namespace, error) {
	r.nsMapsLock.RLock()
	defer r.nsMapsLock.RUnlock()
	return r.getNamespaceByIDLocked(id)
}

func (r *registry) getNamespaceByIDLocked(id namespace.ID) (*namespace.Namespace, error) {
	if ns, ok := r.idToNamespace[id]; ok {
		return ns, nil
	}
	return nil, serviceerror.NewNamespaceNotFound(id.String())
}

// getOrReadthroughNamespace returns namespace information if it exists or reads through
// to the persistence layer and updates internal entry if it doesn't
func (r *registry) getOrReadthroughNamespace(name namespace.Name) (*namespace.Namespace, error) {
	// check main caches
	ns, err := r.getNamespace(name)
	if err == nil {
		return ns, nil
	}

	r.readthroughLock.Lock()
	defer r.readthroughLock.Unlock()

	// check again in case there was an update while waiting
	ns, err = r.getNamespace(name)
	if err == nil {
		return ns, nil
	}

	// check readthrough cache
	if r.readthroughNotFoundCache.Get(name.String()) != nil {
		return nil, serviceerror.NewNamespaceNotFound(name.String())
	}

	// readthrough to persistence layer and update readthrough cache if not found
	ns, err = r.getNamespaceByNamePersistence(name)
	if err != nil {
		return nil, err
	}

	// update main entry if found
	r.updateSingleNamespace(ns, false)

	return ns, nil
}

// getOrReadthroughNamespaceByID retrieves the namespace information if it exists or reads through
// to the persistence layer and updates internal entry if it doesn't
func (r *registry) getOrReadthroughNamespaceByID(id namespace.ID) (*namespace.Namespace, error) {
	// check main caches
	ns, err := r.getNamespaceByID(id)
	if err == nil {
		return ns, nil
	}

	r.readthroughLock.Lock()
	defer r.readthroughLock.Unlock()

	// check again in case there was an update while waiting
	ns, err = r.getNamespaceByID(id)
	if err == nil {
		return ns, nil
	}

	// check readthrough cache
	if r.readthroughNotFoundCache.Get(id.String()) != nil {
		return nil, serviceerror.NewNamespaceNotFound(id.String())
	}

	// readthrough to persistence layer and update readthrough cache if not found
	ns, err = r.getNamespaceByIDPersistence(id)
	if err != nil {
		return nil, err
	}

	// update main entry if found
	r.updateSingleNamespace(ns, false)

	return ns, nil
}

// updateSingleNamespace updates the cache with a namespace if it's newer than what we have.
// Returns true if the namespace state changed.
// When updatedViaWatch is true, we skip adding to stateChangedDuringReadthrough since watch events
// trigger callbacks immediately and don't need to be queued for later delivery.
func (r *registry) updateSingleNamespace(ns *namespace.Namespace, updatedViaWatch bool) bool {
	r.nsMapsLock.Lock()
	defer r.nsMapsLock.Unlock()

	if curEntry, ok := r.idToNamespace[ns.ID()]; ok {
		if curEntry.NotificationVersion() >= ns.NotificationVersion() {
			// More up-to-date version already stored
			return false
		}
	}

	oldNS := r.updateIDToNamespace(r.idToNamespace, ns.ID(), ns)
	// If namespace was renamed, remove entry for the old name
	if oldNS != nil && oldNS.Name() != ns.Name() {
		delete(r.nameToID, oldNS.Name())
	}
	r.nameToID[ns.Name()] = ns.ID()

	changed := namespaceStateChanged(oldNS, ns)
	if changed && !updatedViaWatch {
		r.stateChangedDuringReadthrough = append(r.stateChangedDuringReadthrough, ns)
	}

	return changed
}

func (r *registry) getNamespaceByNamePersistence(name namespace.Name) (*namespace.Namespace, error) {
	request := &persistence.GetNamespaceRequest{
		Name: name.String(),
	}

	ns, err := r.getNamespacePersistence(request)
	if err != nil {
		if _, ok := err.(*serviceerror.NamespaceNotFound); ok {
			r.readthroughNotFoundCache.Put(name.String(), struct{}{})
		}
		// TODO: we should return the actual error we got (e.g. timeout)
		return nil, serviceerror.NewNamespaceNotFound(name.String())
	}
	return ns, nil
}

func (r *registry) getNamespaceByIDPersistence(id namespace.ID) (*namespace.Namespace, error) {
	request := &persistence.GetNamespaceRequest{
		ID: id.String(),
	}

	ns, err := r.getNamespacePersistence(request)
	if err != nil {
		if _, ok := err.(*serviceerror.NamespaceNotFound); ok {
			r.readthroughNotFoundCache.Put(id.String(), struct{}{})
		}
		// TODO: we should return the actual error we got (e.g. timeout)
		return nil, serviceerror.NewNamespaceNotFound(id.String())
	}
	return ns, nil
}

func (r *registry) getNamespacePersistence(request *persistence.GetNamespaceRequest) (*namespace.Namespace, error) {
	ctx, cancel := context.WithTimeout(context.Background(), readthroughTimeout)
	defer cancel()
	ctx = headers.SetCallerType(ctx, headers.CallerTypeAPI)
	ctx = headers.SetCallerName(ctx, headers.CallerNameSystem)

	response, err := r.persistence.GetNamespace(ctx, request)
	if err != nil {
		return nil, err
	}
	return namespace.FromPersistentState(
		response.Namespace,
		r.replicationResolverFactory(response.Namespace),
		namespace.WithGlobalFlag(response.IsGlobalNamespace),
		namespace.WithNotificationVersion(response.NotificationVersion),
	)
}

// this test should include anything that might affect whether a namespace is active on
// this cluster.
// returns true if the state was changed or false if not
func namespaceStateChanged(old *namespace.Namespace, new *namespace.Namespace) bool {
	return old == nil ||
		old.State() != new.State() ||
		old.Name() != new.Name() ||
		old.IsGlobalNamespace() != new.IsGlobalNamespace() ||
		// TODO: Refactor to use ns.ActiveInCluster() api
		old.ActiveClusterName(namespace.EmptyBusinessID) != new.ActiveClusterName(namespace.EmptyBusinessID) ||
		old.ReplicationState() != new.ReplicationState()
}
