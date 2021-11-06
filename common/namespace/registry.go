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
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
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
	cacheInitialSize = 10 * 1024
	cacheMaxSize     = 64 * 1024
	cacheTTL         = 0 // 0 means infinity
	// CacheRefreshInterval namespace cache refresh interval
	CacheRefreshInterval = 10 * time.Second
	// CacheRefreshFailureRetryInterval is the wait time
	// if refreshment encounters error
	CacheRefreshFailureRetryInterval = 1 * time.Second
	CacheRefreshPageSize             = 200

	cacheInitialized int32 = 0
	cacheStarted     int32 = 1
	cacheStopped     int32 = 2
)

const (
	stopped int32 = iota
	starting
	running
	stopping
)

var (
	cacheOpts = cache.Options{
		InitialCapacity: cacheInitialSize,
		TTL:             cacheTTL,
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
		// serviceerror.NotFound if there is no matching Namespace.
		GetNamespace(
			request *persistence.GetNamespaceRequest,
		) (*persistence.GetNamespaceResponse, error)

		// ListNamespaces fetches a paged set of namespace persistent state
		// instances.
		ListNamespaces(
			*persistence.ListNamespacesRequest,
		) (*persistence.ListNamespacesResponse, error)

		// GetMetadata fetches the notification version for Temporal namespaces.
		GetMetadata() (*persistence.GetMetadataResponse, error)
	}

	// PrepareCallbackFn is function to be called before CallbackFn is called,
	// it is guaranteed that PrepareCallbackFn and CallbackFn pair will be both called or non will be called
	PrepareCallbackFn func()

	// CallbackFn is function to be called when the namespace cache entries are changed
	// it is guaranteed that PrepareCallbackFn and CallbackFn pair will be both called or non will be called
	CallbackFn func(oldNamespaces []*Namespace, newNamespaces []*Namespace)

	// Registry provides access to Namespace objects by name or by ID.
	Registry interface {
		common.Daemon
		RegisterNamespaceChangeCallback(shard int32, initialNotificationVersion int64, prepareCallback PrepareCallbackFn, callback CallbackFn)
		UnregisterNamespaceChangeCallback(shard int32)
		GetNamespace(name Name) (*Namespace, error)
		GetNamespaceByID(id ID) (*Namespace, error)
		GetNamespaceID(name Name) (ID, error)
		GetNamespaceName(id ID) (Name, error)
		GetCacheSize() (sizeOfCacheByName int64, sizeOfCacheByID int64)
		// Refresh forces an immediate refresh of the namespace cache and blocks until it's complete.
		Refresh()
	}

	registry struct {
		status                  int32
		refresher               *goro.Handle
		triggerRefreshCh        chan chan struct{}
		persistence             Persistence
		globalNamespacesEnabled bool
		clock                   Clock
		metricsClient           metrics.Client
		logger                  log.Logger
		lastRefreshTime         atomic.Value

		// cacheLock protects cachNameToID and cacheByID. If the exclusive side
		// is to be held at the same time as the callbackLock (below), this lock
		// MUST be acquired *first*.
		cacheLock     sync.RWMutex
		cacheNameToID cache.Cache
		cacheByID     cache.Cache

		// callbackLock protects prepareCallbacks and callbacks. Do not call
		// cacheLock.Lock() (the other lock in this struct, above) while holding
		// this lock or you risk a deadlock.
		callbackLock     sync.Mutex
		prepareCallbacks map[int32]PrepareCallbackFn
		callbacks        map[int32]CallbackFn
	}
)

// NewRegistry creates a new instance of Registry for accessing and caching
// namespace information to reduce the load on persistence.
func NewRegistry(
	persistence Persistence,
	enableGlobalNamespaces bool,
	metricsClient metrics.Client,
	logger log.Logger,
) Registry {
	reg := &registry{
		triggerRefreshCh:        make(chan chan struct{}, 1),
		persistence:             persistence,
		globalNamespacesEnabled: enableGlobalNamespaces,
		clock:                   clock.NewRealTimeSource(),
		metricsClient:           metricsClient,
		logger:                  logger,
		cacheNameToID:           cache.New(cacheMaxSize, &cacheOpts),
		cacheByID:               cache.New(cacheMaxSize, &cacheOpts),
		prepareCallbacks:        make(map[int32]PrepareCallbackFn),
		callbacks:               make(map[int32]CallbackFn),
	}
	reg.lastRefreshTime.Store(time.Time{})
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
	err := r.refreshNamespaces(context.Background())
	if err != nil {
		r.logger.Fatal("Unable to initialize namespace cache", tag.Error(err))
	}
	r.refresher = goro.Go(context.Background(), r.refreshLoop)
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

func (r *registry) getAllNamespace() map[ID]*Namespace {
	result := make(map[ID]*Namespace)
	r.cacheLock.RLock()
	defer r.cacheLock.RUnlock()

	ite := r.cacheByID.Iterator()
	defer ite.Close()

	for ite.HasNext() {
		entry := ite.Next()
		id := entry.Key().(ID)
		result[id] = entry.Value().(*Namespace)
	}
	return result
}

// RegisterNamespaceChangeCallback set a namespace change callback WARN:
// callback functions MUST NOT call back into this registry instance, either to
// unregister themselves or to look up Namespaces.
func (r *registry) RegisterNamespaceChangeCallback(
	shard int32,
	initialNotificationVersion int64,
	prepareCallback PrepareCallbackFn,
	callback CallbackFn,
) {

	r.callbackLock.Lock()
	r.prepareCallbacks[shard] = prepareCallback
	r.callbacks[shard] = callback
	r.callbackLock.Unlock()

	// this section is trying to make the shard catch up with namespace changes
	namespaces := Namespaces{}
	for _, namespace := range r.getAllNamespace() {
		namespaces = append(namespaces, namespace)
	}
	// we mush notify the change in a ordered fashion
	// since history shard have to update the shard info
	// with namespace change version.
	sort.Sort(namespaces)

	var oldEntries []*Namespace
	var newEntries []*Namespace
	for _, namespace := range namespaces {
		if namespace.notificationVersion >= initialNotificationVersion {
			oldEntries = append(oldEntries, nil)
			newEntries = append(newEntries, namespace)
		}
	}
	if len(oldEntries) > 0 {
		prepareCallback()
		callback(oldEntries, newEntries)
	}
}

// UnregisterNamespaceChangeCallback delete a namespace failover callback
func (r *registry) UnregisterNamespaceChangeCallback(
	shard int32,
) {
	r.callbackLock.Lock()
	defer r.callbackLock.Unlock()

	delete(r.prepareCallbacks, shard)
	delete(r.callbacks, shard)
}

// GetNamespace retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (r *registry) GetNamespace(name Name) (*Namespace, error) {
	if name == "" {
		return nil, serviceerror.NewInvalidArgument("Namespace is empty.")
	}
	return r.getNamespace(name)
}

// GetNamespaceByID retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (r *registry) GetNamespaceByID(id ID) (*Namespace, error) {
	if id == "" {
		return nil, serviceerror.NewInvalidArgument("NamespaceID is empty.")
	}
	return r.getNamespaceByID(id)
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

	ns, err := r.getNamespaceByID(id)
	if err != nil {
		return "", err
	}
	return ns.Name(), nil
}

func (r *registry) Refresh() {
	replyCh := make(chan struct{})
	r.triggerRefreshCh <- replyCh
	<-replyCh
}

func (r *registry) refreshLoop(ctx context.Context) error {
	timer := time.NewTicker(CacheRefreshInterval)
	defer timer.Stop()

	// Put timer events on our channel so we can select on just one below.
	go func() {
		for range timer.C {
			select {
			case r.triggerRefreshCh <- nil:
			default:
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
					select {
					case <-time.After(CacheRefreshFailureRetryInterval):
					case <-ctx.Done():
						return nil
					}
				}
			}
			if replyCh != nil {
				replyCh <- struct{}{}
			}
		}
	}
}

func (r *registry) refreshNamespaces(ctx context.Context) error {
	// first load the metadata record, then load namespaces
	// this can guarantee that namespaces in the cache are not updated more than metadata record
	metadata, err := r.persistence.GetMetadata()
	if err != nil {
		return err
	}
	namespaceNotificationVersion := metadata.NotificationVersion

	var token []byte
	request := &persistence.ListNamespacesRequest{PageSize: CacheRefreshPageSize}
	var namespaces Namespaces
	continuePage := true

	for continuePage {
		request.NextPageToken = token
		response, err := r.persistence.ListNamespaces(request)
		if err != nil {
			return err
		}
		token = response.NextPageToken
		for _, namespace := range response.Namespaces {
			namespaces = append(namespaces, FromPersistentState(namespace))
		}
		continuePage = len(token) != 0
	}

	// we mush apply the namespace change by order
	// since history shard have to update the shard info
	// with namespace change version.
	sort.Sort(namespaces)

	var oldEntries []*Namespace
	var newEntries []*Namespace

	// make a copy of the existing namespace cache, so we can calculate diff and do compare and swap
	newCacheNameToID := cache.New(cacheMaxSize, &cacheOpts)
	newCacheByID := cache.New(cacheMaxSize, &cacheOpts)
	for _, namespace := range r.getAllNamespace() {
		newCacheNameToID.Put(Name(namespace.info.Name), ID(namespace.info.Id))
		newCacheByID.Put(ID(namespace.info.Id), namespace)
	}

UpdateLoop:
	for _, namespace := range namespaces {
		if namespace.notificationVersion >= namespaceNotificationVersion {
			// this guarantee that namespace change events before the
			// namespaceNotificationVersion is loaded into the cache.

			// the namespace change events after the namespaceNotificationVersion
			// will be loaded into cache in the next refresh
			break UpdateLoop
		}
		oldNS := r.updateIDToNamespaceCache(newCacheByID, namespace.ID(), namespace)
		r.updateNameToIDCache(newCacheNameToID, namespace.Name(), namespace.ID())

		if oldNS != nil {
			oldEntries = append(oldEntries, oldNS)
			newEntries = append(newEntries, namespace)
		}
	}

	// NOTE: READ REF BEFORE MODIFICATION
	// ref: historyEngine.go registerNamespaceFailoverCallback function
	r.publishCacheUpdate(func() (Namespaces, Namespaces) {
		r.cacheLock.Lock()
		defer r.cacheLock.Unlock()
		r.cacheByID = newCacheByID
		r.cacheNameToID = newCacheNameToID
		return oldEntries, newEntries
	})
	return nil
}

func (r *registry) updateNameToIDCache(c cache.Cache, name Name, id ID) {
	c.Put(name, id)
}

func (r *registry) updateIDToNamespaceCache(
	cacheByID cache.Cache,
	id ID,
	newNS *Namespace,
) *Namespace {
	oldCacheRec := cacheByID.Put(id, newNS)
	if oldNS, ok := oldCacheRec.(*Namespace); ok &&
		newNS.notificationVersion > oldNS.notificationVersion &&
		r.globalNamespacesEnabled {
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
	return nil, serviceerror.NewNotFound(
		fmt.Sprintf("Namespace name %q not found", name))
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
	return nil, serviceerror.NewNotFound(
		fmt.Sprintf("Namespace id %q not found", id))
}

func (r *registry) publishCacheUpdate(
	updateCache func() (Namespaces, Namespaces),
) {
	now := r.clock.Now()
	r.callbackLock.Lock()
	defer r.callbackLock.Unlock()
	r.triggerNamespaceChangePrepareCallbackLocked()
	oldEntries, newEntries := updateCache()
	r.triggerNamespaceChangeCallbackLocked(oldEntries, newEntries)
	r.lastRefreshTime.Store(now)
}

func (r *registry) triggerNamespaceChangePrepareCallbackLocked() {
	sw := r.metricsClient.StartTimer(
		metrics.NamespaceCacheScope, metrics.NamespaceCachePrepareCallbacksLatency)
	defer sw.Stop()

	for _, prepareCallback := range r.prepareCallbacks {
		prepareCallback()
	}
}

func (r *registry) triggerNamespaceChangeCallbackLocked(
	oldNamespaces []*Namespace,
	newNamespaces []*Namespace,
) {

	sw := r.metricsClient.StartTimer(
		metrics.NamespaceCacheScope, metrics.NamespaceCacheCallbacksLatency)
	defer sw.Stop()

	for _, callback := range r.callbacks {
		callback(oldNamespaces, newNamespaces)
	}
}

func byName(name Name) *persistence.GetNamespaceRequest {
	return &persistence.GetNamespaceRequest{Name: name.String()}
}

func byID(id ID) *persistence.GetNamespaceRequest {
	return &persistence.GetNamespaceRequest{ID: id.String()}
}
