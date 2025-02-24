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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination registry_mock.go

package nsregistry

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/pingable"
	"go.temporal.io/server/internal/goro"
	expmaps "golang.org/x/exp/maps"
)

const (
	cacheMaxSize = 64 * 1024
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
	registry struct {
		status                  int32
		refresher               *goro.Handle
		persistence             Persistence
		globalNamespacesEnabled bool
		clock                   Clock
		metricsHandler          metrics.Handler
		logger                  log.Logger
		refreshInterval         dynamicconfig.DurationPropertyFn

		// cacheLock protects cachNameToID, cacheByID and stateChangeCallbacks.

		registryLock  sync.RWMutex
		nameToID      map[namespace.Name]namespace.ID
		idToNamespace map[namespace.ID]*namespace.Namespace

		stateChangeCallbacks          map[any]namespace.StateChangeCallbackFn
		stateChangedDuringReadthrough []*namespace.Namespace

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
		stateChangeCallbacks:     make(map[any]namespace.StateChangeCallbackFn),
		readthroughNotFoundCache: cache.New(cacheMaxSize, &readthroughNotFoundCacheOpts),

		forceSearchAttributesCacheRefreshOnRead: forceSearchAttributesCacheRefreshOnRead,
	}
	return reg
}

// GetCacheSize observes the size of the by-name and by-ID caches in number of
// entries.
func (r *registry) GetCacheSize() (sizeOfCacheByName int64, sizeOfCacheByID int64) {
	r.registryLock.RLock()
	defer r.registryLock.RUnlock()
	return int64(len(r.idToNamespace)), int64(len(r.nameToID))
}

func (r *registry) RefreshNamespaceById(id namespace.ID) (*namespace.Namespace, error) {
	r.readthroughLock.Lock()
	defer r.readthroughLock.Unlock()
	ns, err := r.getNamespaceByIDPersistence(id)
	if err != nil {
		return nil, err
	}
	r.updateCachesSingleNamespace(ns)
	return ns, nil
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

func (r *registry) GetPingChecks() []pingable.Check {
	return []pingable.Check{
		{
			Name: "namespace registry lock",
			// we don't do any persistence ops, this shouldn't be blocked
			Timeout: 10 * time.Second,
			Ping: func() []pingable.Pingable {
				// just checking if we can acquire the lock
				r.registryLock.Lock()
				// nolint:staticcheck
				r.registryLock.Unlock()
				return nil
			},
			MetricsName: metrics.DDNamespaceRegistryLockLatency.Name(),
		},
	}
}

func (r *registry) getAllNamespace() []*namespace.Namespace {
	r.registryLock.RLock()
	defer r.registryLock.RUnlock()
	return r.getAllNamespaceLocked()
}

func (r *registry) getAllNamespaceLocked() []*namespace.Namespace {
	result := make([]*namespace.Namespace, len(r.idToNamespace))
	i := 0
	for _, value := range r.idToNamespace {
		result[i] = value
		i++
	}
	return result
}

func (r *registry) RegisterStateChangeCallback(key any, cb namespace.StateChangeCallbackFn) {
	r.registryLock.Lock()
	r.stateChangeCallbacks[key] = cb
	allNamespaces := r.getAllNamespaceLocked()
	r.registryLock.Unlock()

	// call once for each namespace already in the registry
	for _, ns := range allNamespaces {
		cb(ns, false)
	}
}

func (r *registry) UnregisterStateChangeCallback(key any) {
	r.registryLock.Lock()
	delete(r.stateChangeCallbacks, key)
	r.registryLock.Unlock()
}

// GetNamespace retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
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

func (r *registry) refreshLoop(ctx context.Context) error {
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

func (r *registry) refreshNamespaces(ctx context.Context) error {
	request := &persistence.ListNamespacesRequest{
		PageSize:       CacheRefreshPageSize,
		IncludeDeleted: true,
	}
	var namespacesDb namespace.Namespaces
	namespaceIDsDb := make(map[namespace.ID]struct{})

	for {
		response, err := r.persistence.ListNamespaces(ctx, request)
		if err != nil {
			return err
		}
		for _, namespaceDb := range response.Namespaces {
			ns := namespace.FromPersistentState(
				namespaceDb.Namespace,
				namespace.WithGlobalFlag(namespaceDb.IsGlobalNamespace),
				namespace.WithNotificationVersion(namespaceDb.NotificationVersion))
			namespacesDb = append(namespacesDb, ns)
			namespaceIDsDb[namespace.ID(namespaceDb.Namespace.Info.Id)] = struct{}{}
		}
		if len(response.NextPageToken) == 0 {
			break
		}
		request.NextPageToken = response.NextPageToken
	}

	// Make a copy of the existing namespace cache (excluding deleted), so we can calculate diff and do "compare and swap".
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
		oldNS := r.updateIDToNamespaceCache(newIDToNamespace, aNamespace.ID(), aNamespace)
		newNameToID[aNamespace.Name()] = aNamespace.ID()

		if namespaceStateChanged(oldNS, aNamespace) {
			stateChanged = append(stateChanged, aNamespace)
		}
	}

	var stateChangeCallbacks []namespace.StateChangeCallbackFn

	r.registryLock.Lock()
	r.idToNamespace = newIDToNamespace
	r.nameToID = newNameToID
	stateChanged = append(stateChanged, r.stateChangedDuringReadthrough...)
	r.stateChangedDuringReadthrough = nil
	stateChangeCallbacks = expmaps.Values(r.stateChangeCallbacks)
	r.registryLock.Unlock()

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
	r.registryLock.RLock()
	defer r.registryLock.RUnlock()
	if id, ok := r.nameToID[name]; ok {
		return r.getNamespaceByIDLocked(id)
	}
	return nil, serviceerror.NewNamespaceNotFound(name.String())
}

// getNamespaceByID retrieves the information from the cache if it exists.
func (r *registry) getNamespaceByID(id namespace.ID) (*namespace.Namespace, error) {
	r.registryLock.RLock()
	defer r.registryLock.RUnlock()
	return r.getNamespaceByIDLocked(id)
}

func (r *registry) getNamespaceByIDLocked(id namespace.ID) (*namespace.Namespace, error) {
	if ns, ok := r.idToNamespace[id]; ok {
		return ns, nil
	}
	return nil, serviceerror.NewNamespaceNotFound(id.String())
}

// getOrReadthroughNamespace retrieves the information from the cache if it exists or reads through
// to the persistence layer and updates caches if it doesn't
func (r *registry) getOrReadthroughNamespace(name namespace.Name) (*namespace.Namespace, error) {
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
func (r *registry) getOrReadthroughNamespaceByID(id namespace.ID) (*namespace.Namespace, error) {
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

func (r *registry) updateCachesSingleNamespace(ns *namespace.Namespace) {
	r.registryLock.Lock()
	defer r.registryLock.Unlock()

	if curEntry, ok := r.idToNamespace[ns.ID()]; ok {
		if curEntry.NotificationVersion() >= ns.NotificationVersion() {
			// More up-to-date version already put in cache by refresh
			return
		}
	}

	oldNS := r.updateIDToNamespaceCache(r.idToNamespace, ns.ID(), ns)
	r.nameToID[ns.Name()] = ns.ID()
	if namespaceStateChanged(oldNS, ns) {
		r.stateChangedDuringReadthrough = append(r.stateChangedDuringReadthrough, ns)
	}
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
		namespace.WithGlobalFlag(response.IsGlobalNamespace),
		namespace.WithNotificationVersion(response.NotificationVersion)), nil
}

// this test should include anything that might affect whether a namespace is active on
// this cluster.
// returns true if the state was changed or false if not
func namespaceStateChanged(old *namespace.Namespace, new *namespace.Namespace) bool {
	return old == nil ||
		old.State() != new.State() ||
		old.IsGlobalNamespace() != new.IsGlobalNamespace() ||
		old.ActiveClusterName() != new.ActiveClusterName() ||
		old.ReplicationState() != new.ReplicationState()
}
