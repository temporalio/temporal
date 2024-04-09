// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination registry_mock.go

package namespace

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/exp/maps"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/internal/goro"
)

// ReplicationPolicy is the namespace's replication policy,
// derived from namespace's replication config
type ReplicationPolicy int

const (
	// ReplicationPolicyOneCluster indicate that workflows does not need to be replicated
	// applicable to local namespace & global namespace with one cluster
	ReplicationPolicyOneCluster ReplicationPolicy = 0
	// ReplicationPolicyMultiCluster indicate that workflows need to be replicated
	ReplicationPolicyMultiCluster ReplicationPolicy = 1
)

const (
	cacheMaxSize = 64 * 1024
	cacheTTL     = 0 // 0 means infinity
	// CacheRefreshFailureRetryInterval is the wait time
	// if refreshment encounters error
	CacheRefreshFailureRetryInterval = 1 * time.Second
	CacheRefreshPageSize             = 1000
	readthroughCacheTTL              = 1 * time.Second // represents minimum time to wait before trying to readthrough again
	readthroughTimeout               = 3 * time.Second
)

const (
	stopped int32 = iota
	starting
	running
	stopping
)

var (
	cacheOpts = cache.Options{
		TTL: cacheTTL,
	}
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
	}

	// PrepareCallbackFn is function to be called before CallbackFn is called,
	// it is guaranteed that PrepareCallbackFn and CallbackFn pair will be both called or non will be called
	PrepareCallbackFn func()

	// CallbackFn is function to be called when the namespace cache entries are changed
	// it is guaranteed that PrepareCallbackFn and CallbackFn pair will be both called or non will be called
	CallbackFn func(oldNamespaces []*Namespace, newNamespaces []*Namespace)

	// StateChangeCallbackFn can be registered to be called on any namespace state change or
	// addition/removal from database, plus once for all namespaces after registration. There
	// is no guarantee about when these are called.
	StateChangeCallbackFn func(ns *Namespace, deletedFromDb bool)

	// Registry provides access to Namespace objects by name or by ID.
	Registry interface {
		common.Pingable
		GetNamespace(name Name) (*Namespace, error)
		GetNamespaceWithOptions(name Name, opts GetNamespaceOptions) (*Namespace, error)
		GetNamespaceByID(id ID) (*Namespace, error)
		GetNamespaceByIDWithOptions(id ID, opts GetNamespaceOptions) (*Namespace, error)
		GetNamespaceID(name Name) (ID, error)
		GetNamespaceName(id ID) (Name, error)
		GetCacheSize() (sizeOfCacheByName int64, sizeOfCacheByID int64)
		// Registers callback for namespace state changes.
		// StateChangeCallbackFn will be invoked for a new/deleted namespace or namespace that has
		// State, ReplicationState, ActiveCluster, or isGlobalNamespace config changed.
		RegisterStateChangeCallback(key any, cb StateChangeCallbackFn)
		UnregisterStateChangeCallback(key any)
		// GetCustomSearchAttributesMapper is a temporary solution to be able to get search attributes
		// with from persistence if forceSearchAttributesCacheRefreshOnRead is true.
		GetCustomSearchAttributesMapper(name Name) (CustomSearchAttributesMapper, error)
		Start()
		Stop()
	}

	GetNamespaceOptions struct {
		// Setting this disables the readthrough logic, i.e. only looks at the current in-memory
		// registry. This is useful if you want to avoid latency or avoid polluting the negative
		// lookup cache. Note that you may get false negatives (namespace not found) if the
		// namespace was created very recently.
		DisableReadthrough bool
	}

	registry struct {
		status                  int32
		refresher               *goro.Handle
		triggerRefreshCh        chan chan struct{}
		persistence             Persistence
		globalNamespacesEnabled bool
		clock                   Clock
		metricsHandler          metrics.Handler
		logger                  log.Logger
		refreshInterval         dynamicconfig.DurationPropertyFn

		// cacheLock protects cachNameToID, cacheByID and stateChangeCallbacks.
		cacheLock                     sync.RWMutex
		cacheNameToID                 cache.Cache
		cacheByID                     cache.Cache
		stateChangeCallbacks          map[any]StateChangeCallbackFn
		stateChangedDuringReadthrough []*Namespace

		// readthroughLock protects readthroughNotFoundCache and requests to persistence
		// it should be acquired before checking readthroughNotFoundCache, making a request
		// to persistence, or updating readthroughNotFoundCache
		// It should be acquired before cacheLock (above) if both are required
		readthroughLock sync.Mutex
		// readthroughNotFoundCache stores namespaces that missed the above caches
		// AND was not found when reading through to the persistence layer
		readthroughNotFoundCache cache.Cache

		// Temporary solution to force read search attributes from persistence
		forceSearchAttributesCacheRefreshOnRead dynamicconfig.BoolPropertyFn
	}
)

// NewRegistry creates a new instance of Registry for accessing and caching
// namespace information to reduce the load on persistence.
func NewRegistry(
	persistence Persistence,
	enableGlobalNamespaces bool,
	refreshInterval dynamicconfig.DurationPropertyFn,
	forceSearchAttributesCacheRefreshOnRead dynamicconfig.BoolPropertyFn,
	metricsHandler metrics.Handler,
	logger log.Logger,
) Registry {
	reg := &registry{
		triggerRefreshCh:         make(chan chan struct{}, 1),
		persistence:              persistence,
		globalNamespacesEnabled:  enableGlobalNamespaces,
		clock:                    clock.NewRealTimeSource(),
		metricsHandler:           metricsHandler.WithTags(metrics.OperationTag(metrics.NamespaceCacheScope)),
		logger:                   logger,
		cacheNameToID:            cache.New(cacheMaxSize, &cacheOpts, metrics.NoopMetricsHandler),
		cacheByID:                cache.New(cacheMaxSize, &cacheOpts, metrics.NoopMetricsHandler),
		refreshInterval:          refreshInterval,
		stateChangeCallbacks:     make(map[any]StateChangeCallbackFn),
		readthroughNotFoundCache: cache.New(cacheMaxSize, &readthroughNotFoundCacheOpts, metrics.NoopMetricsHandler),

		forceSearchAttributesCacheRefreshOnRead: forceSearchAttributesCacheRefreshOnRead,
	}
	return reg
}

// GetCacheSize observes the size of the by-name and by-ID caches in number of
// entries.
func (r *registry) GetCacheSize() (sizeOfCacheByName int64, sizeOfCacheByID int64) {
	r.cacheLock.RLock()
	defer r.cacheLock.RUnlock()
	return int64(r.cacheByID.Size()), int64(r.cacheNameToID.Size())
}

// Start the background refresh of Namespace data.
func (r *registry) Start() {
	if !atomic.CompareAndSwapInt32(&r.status, stopped, starting) {
		return
	}
	defer atomic.StoreInt32(&r.status, running)

	// initialize the cache by initial scan
	ctx := headers.SetCallerInfo(
		context.Background(),
		headers.SystemBackgroundCallerInfo,
	)

	err := r.refreshNamespaces(ctx)
	if err != nil {
		r.logger.Fatal("Unable to initialize namespace cache", tag.Error(err))
	}
	r.refresher = goro.NewHandle(ctx).Go(r.refreshLoop)
}

// Stop the background refresh of Namespace data
func (r *registry) Stop() {
	if !atomic.CompareAndSwapInt32(&r.status, running, stopping) {
		return
	}
	defer atomic.StoreInt32(&r.status, stopped)
	r.refresher.Cancel()
	<-r.refresher.Done()
}

func (r *registry) GetPingChecks() []common.PingCheck {
	return []common.PingCheck{
		{
			Name: "namespace registry lock",
			// we don't do any persistence ops, this shouldn't be blocked
			Timeout: 10 * time.Second,
			Ping: func() []common.Pingable {
				r.cacheLock.Lock()
				//lint:ignore SA2001 just checking if we can acquire the lock
				r.cacheLock.Unlock()
				return nil
			},
			MetricsName: metrics.DDNamespaceRegistryLockLatency.Name(),
		},
	}
}

func (r *registry) getAllNamespace() map[ID]*Namespace {
	r.cacheLock.RLock()
	defer r.cacheLock.RUnlock()
	return r.getAllNamespaceLocked()
}

func (r *registry) getAllNamespaceLocked() map[ID]*Namespace {
	result := make(map[ID]*Namespace)

	ite := r.cacheByID.Iterator()
	defer ite.Close()

	for ite.HasNext() {
		entry := ite.Next()
		id := entry.Key().(ID)
		result[id] = entry.Value().(*Namespace)
	}
	return result
}

func (r *registry) RegisterStateChangeCallback(key any, cb StateChangeCallbackFn) {
	r.cacheLock.Lock()
	r.stateChangeCallbacks[key] = cb
	allNamespaces := r.getAllNamespaceLocked()
	r.cacheLock.Unlock()

	// call once for each namespace already in the registry
	for _, ns := range allNamespaces {
		cb(ns, false)
	}
}

func (r *registry) UnregisterStateChangeCallback(key any) {
	r.cacheLock.Lock()
	defer r.cacheLock.Unlock()
	delete(r.stateChangeCallbacks, key)
}

// GetNamespace retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (r *registry) GetNamespace(name Name) (*Namespace, error) {
	if name == "" {
		return nil, serviceerror.NewInvalidArgument("Namespace is empty.")
	}
	return r.getOrReadthroughNamespace(name)
}

// GetNamespaceWithOptions retrieves a namespace entry by name, with behavior controlled by options.
func (r *registry) GetNamespaceWithOptions(name Name, opts GetNamespaceOptions) (*Namespace, error) {
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
func (r *registry) GetNamespaceByID(id ID) (*Namespace, error) {
	if id == "" {
		return nil, serviceerror.NewInvalidArgument("NamespaceID is empty.")
	}
	return r.getOrReadthroughNamespaceByID(id)
}

// GetNamespaceByIDWithOptions retrieves a namespace entry by id, with behavior controlled by options.
func (r *registry) GetNamespaceByIDWithOptions(id ID, opts GetNamespaceOptions) (*Namespace, error) {
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
	name Name,
) (ID, error) {

	ns, err := r.GetNamespace(name)
	if err != nil {
		return "", err
	}
	return ns.ID(), nil
}

// GetNamespaceName returns namespace name given the namespace id
func (r *registry) GetNamespaceName(
	id ID,
) (Name, error) {

	ns, err := r.getOrReadthroughNamespaceByID(id)
	if err != nil {
		return "", err
	}
	return ns.Name(), nil
}

// GetCustomSearchAttributesMapper is a temporary solution to be able to get search attributes
// with from persistence if forceSearchAttributesCacheRefreshOnRead is true.
func (r *registry) GetCustomSearchAttributesMapper(name Name) (CustomSearchAttributesMapper, error) {
	var ns *Namespace
	var err error
	if r.forceSearchAttributesCacheRefreshOnRead() {
		r.readthroughLock.Lock()
		defer r.readthroughLock.Unlock()
		ns, err = r.getNamespaceByNamePersistence(name)
	} else {
		ns, err = r.GetNamespace(name)
	}
	if err != nil {
		return CustomSearchAttributesMapper{}, err
	}
	return ns.CustomSearchAttributesMapper(), nil
}

func (r *registry) refreshLoop(ctx context.Context) error {
	// Put timer events on our channel so we can select on just one below.
	go func() {
		timer := time.NewTicker(r.refreshInterval())

		for {
			select {
			case <-timer.C:
				select {
				case r.triggerRefreshCh <- nil:
				default:
				}
				timer.Reset(r.refreshInterval())
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case replyCh := <-r.triggerRefreshCh:
			for err := r.refreshNamespaces(ctx); err != nil; err = r.refreshNamespaces(ctx) {
				select {
				case <-ctx.Done():
					return nil
				default:
					r.logger.Error("Error refreshing namespace cache", tag.Error(err))
					timer := time.NewTimer(CacheRefreshFailureRetryInterval)
					select {
					case <-timer.C:
					case <-ctx.Done():
						timer.Stop()
						return nil
					}
				}
			}
			if replyCh != nil {
				replyCh <- struct{}{} // TODO: close replyCh?
			}
		}
	}
}

func (r *registry) refreshNamespaces(ctx context.Context) error {
	request := &persistence.ListNamespacesRequest{
		PageSize:       CacheRefreshPageSize,
		IncludeDeleted: true,
	}
	var namespacesDb Namespaces
	namespaceIDsDb := make(map[ID]struct{})

	for {
		response, err := r.persistence.ListNamespaces(ctx, request)
		if err != nil {
			return err
		}
		for _, namespaceDb := range response.Namespaces {
			namespacesDb = append(namespacesDb, FromPersistentState(namespaceDb))
			namespaceIDsDb[ID(namespaceDb.Namespace.Info.Id)] = struct{}{}
		}
		if len(response.NextPageToken) == 0 {
			break
		}
		request.NextPageToken = response.NextPageToken
	}

	// Make a copy of the existing namespace cache (excluding deleted), so we can calculate diff and do "compare and swap".
	newCacheNameToID := cache.New(cacheMaxSize, &cacheOpts, metrics.NoopMetricsHandler)
	newCacheByID := cache.New(cacheMaxSize, &cacheOpts, metrics.NoopMetricsHandler)
	var deletedEntries []*Namespace
	for _, namespace := range r.getAllNamespace() {
		if _, namespaceExistsDb := namespaceIDsDb[namespace.ID()]; !namespaceExistsDb {
			deletedEntries = append(deletedEntries, namespace)
			continue
		}
		newCacheNameToID.Put(Name(namespace.info.Name), ID(namespace.info.Id))
		newCacheByID.Put(ID(namespace.info.Id), namespace)
	}

	var stateChanged []*Namespace
	for _, namespace := range namespacesDb {
		oldNS := r.updateIDToNamespaceCache(newCacheByID, namespace.ID(), namespace)
		newCacheNameToID.Put(namespace.Name(), namespace.ID())

		if namespaceStateChanged(oldNS, namespace) {
			stateChanged = append(stateChanged, namespace)
		}
	}

	var stateChangeCallbacks []StateChangeCallbackFn

	r.cacheLock.Lock()
	r.cacheByID = newCacheByID
	r.cacheNameToID = newCacheNameToID
	stateChanged = append(stateChanged, r.stateChangedDuringReadthrough...)
	r.stateChangedDuringReadthrough = nil
	stateChangeCallbacks = maps.Values(r.stateChangeCallbacks)
	r.cacheLock.Unlock()

	// call state change callbacks
	for _, cb := range stateChangeCallbacks {
		for _, ns := range deletedEntries {
			cb(ns, true)
		}
		for _, ns := range stateChanged {
			cb(ns, false)
		}
	}

	return nil
}

func (r *registry) updateIDToNamespaceCache(
	cacheByID cache.Cache,
	id ID,
	newNS *Namespace,
) (oldNS *Namespace) {
	oldCacheRec := cacheByID.Put(id, newNS)
	if oldNS, ok := oldCacheRec.(*Namespace); ok {
		return oldNS
	}
	return nil
}

// getNamespace retrieves the information from the cache if it exists
func (r *registry) getNamespace(name Name) (*Namespace, error) {
	r.cacheLock.RLock()
	defer r.cacheLock.RUnlock()
	if id, ok := r.cacheNameToID.Get(name).(ID); ok {
		return r.getNamespaceByIDLocked(id)
	}
	return nil, serviceerror.NewNamespaceNotFound(name.String())
}

// getNamespaceByID retrieves the information from the cache if it exists.
func (r *registry) getNamespaceByID(id ID) (*Namespace, error) {
	r.cacheLock.RLock()
	defer r.cacheLock.RUnlock()
	return r.getNamespaceByIDLocked(id)
}

func (r *registry) getNamespaceByIDLocked(id ID) (*Namespace, error) {
	if ns, ok := r.cacheByID.Get(id).(*Namespace); ok {
		return ns, nil
	}
	return nil, serviceerror.NewNamespaceNotFound(id.String())
}

// getOrReadthroughNamespace retrieves the information from the cache if it exists or reads through
// to the persistence layer and updates caches if it doesn't
func (r *registry) getOrReadthroughNamespace(name Name) (*Namespace, error) {
	// check main caches
	cacheHit, cacheErr := r.getNamespace(name)
	if cacheErr == nil {
		return cacheHit, nil
	}

	r.readthroughLock.Lock()
	defer r.readthroughLock.Unlock()

	// check caches again in case there was an update while waiting
	cacheHit, cacheErr = r.getNamespace(name)
	if cacheErr == nil {
		return cacheHit, nil
	}

	// check readthrough cache
	if r.readthroughNotFoundCache.Get(name.String()) != nil {
		return nil, serviceerror.NewNamespaceNotFound(name.String())
	}

	// readthrough to persistence layer and update readthrough cache if not found
	ns, err := r.getNamespaceByNamePersistence(name)
	if err != nil {
		return nil, err
	}

	// update main caches if found
	r.updateCachesSingleNamespace(ns)

	return ns, nil
}

// getOrReadthroughNamespaceByID retrieves the information from the cache if it exists or reads through
// to the persistence layer and updates caches if it doesn't
func (r *registry) getOrReadthroughNamespaceByID(id ID) (*Namespace, error) {
	// check main caches
	cacheHit, cacheErr := r.getNamespaceByID(id)
	if cacheErr == nil {
		return cacheHit, nil
	}

	r.readthroughLock.Lock()
	defer r.readthroughLock.Unlock()

	// check caches again in case there was an update while waiting
	cacheHit, cacheErr = r.getNamespaceByID(id)
	if cacheErr == nil {
		return cacheHit, nil
	}

	// check readthrough cache
	if r.readthroughNotFoundCache.Get(id.String()) != nil {
		return nil, serviceerror.NewNamespaceNotFound(id.String())
	}

	// readthrough to persistence layer and update readthrough cache if not found
	ns, err := r.getNamespaceByIDPersistence(id)
	if err != nil {
		return nil, err
	}

	// update main caches if found
	r.updateCachesSingleNamespace(ns)

	return ns, nil
}

func (r *registry) updateCachesSingleNamespace(ns *Namespace) {
	r.cacheLock.Lock()
	defer r.cacheLock.Unlock()

	if curEntry, ok := r.cacheByID.Get(ns.ID()).(*Namespace); ok {
		if curEntry.NotificationVersion() >= ns.NotificationVersion() {
			// More up to date version already put in cache by refresh
			return
		}
	}

	oldNS := r.updateIDToNamespaceCache(r.cacheByID, ns.ID(), ns)
	r.cacheNameToID.Put(ns.Name(), ns.ID())
	if namespaceStateChanged(oldNS, ns) {
		r.stateChangedDuringReadthrough = append(r.stateChangedDuringReadthrough, ns)
	}
}

func (r *registry) getNamespaceByNamePersistence(name Name) (*Namespace, error) {
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

func (r *registry) getNamespaceByIDPersistence(id ID) (*Namespace, error) {
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

func (r *registry) getNamespacePersistence(request *persistence.GetNamespaceRequest) (*Namespace, error) {
	ctx, cancel := context.WithTimeout(context.Background(), readthroughTimeout)
	defer cancel()
	ctx = headers.SetCallerType(ctx, headers.CallerTypeAPI)
	ctx = headers.SetCallerName(ctx, headers.CallerNameSystem)

	response, err := r.persistence.GetNamespace(ctx, request)
	if err != nil {
		return nil, err
	}

	return FromPersistentState(response), nil
}

// this test should include anything that might affect whether a namespace is active on
// this cluster.
// returns true if the state was changed or false if not
func namespaceStateChanged(old *Namespace, new *Namespace) bool {
	return old == nil ||
		old.State() != new.State() ||
		old.IsGlobalNamespace() != new.IsGlobalNamespace() ||
		old.ActiveClusterName() != new.ActiveClusterName() ||
		old.ReplicationState() != new.ReplicationState()
}
