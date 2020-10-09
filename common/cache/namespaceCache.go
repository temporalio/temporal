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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination namespaceCache_mock.go -self_package go.temporal.io/server/common/cache

package cache

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
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
	namespaceCacheInitialSize = 10 * 1024
	namespaceCacheMaxSize     = 64 * 1024
	namespaceCacheTTL         = 0 // 0 means infinity
	// NamespaceCacheMinRefreshInterval namespace cache refresh interval
	NamespaceCacheMinRefreshInterval = 2 * time.Second
	// NamespaceCacheRefreshInterval namespace cache refresh interval
	NamespaceCacheRefreshInterval = 10 * time.Second
	// NamespaceCacheRefreshFailureRetryInterval is the wait time
	// if refreshment encounters error
	NamespaceCacheRefreshFailureRetryInterval = 1 * time.Second
	namespaceCacheRefreshPageSize             = 200

	namespaceCacheInitialized int32 = 0
	namespaceCacheStarted     int32 = 1
	namespaceCacheStopped     int32 = 2
)

type (
	// PrepareCallbackFn is function to be called before CallbackFn is called,
	// it is guaranteed that PrepareCallbackFn and CallbackFn pair will be both called or non will be called
	PrepareCallbackFn func()

	// CallbackFn is function to be called when the namespace cache entries are changed
	// it is guaranteed that PrepareCallbackFn and CallbackFn pair will be both called or non will be called
	CallbackFn func(prevNamespaces []*NamespaceCacheEntry, nextNamespaces []*NamespaceCacheEntry)

	// NamespaceCache is used the cache namespace information and configuration to avoid making too many calls to cassandra.
	// This cache is mainly used by frontend for resolving namespace names to namespace uuids which are used throughout the
	// system.  Each namespace entry is kept in the cache for one hour but also has an expiry of 10 seconds.  This results
	// in updating the namespace entry every 10 seconds but in the case of a cassandra failure we can still keep on serving
	// requests using the stale entry from cache upto an hour
	NamespaceCache interface {
		common.Daemon
		RegisterNamespaceChangeCallback(shard int32, initialNotificationVersion int64, prepareCallback PrepareCallbackFn, callback CallbackFn)
		UnregisterNamespaceChangeCallback(shard int32)
		GetNamespace(name string) (*NamespaceCacheEntry, error)
		GetNamespaceByID(id string) (*NamespaceCacheEntry, error)
		GetNamespaceID(name string) (string, error)
		GetNamespaceName(id string) (string, error)
		GetAllNamespace() map[string]*NamespaceCacheEntry
		GetCacheSize() (sizeOfCacheByName int64, sizeOfCacheByID int64)
	}

	namespaceCache struct {
		status          int32
		shutdownChan    chan struct{}
		cacheNameToID   *atomic.Value
		cacheByID       *atomic.Value
		metadataMgr     persistence.MetadataManager
		clusterMetadata cluster.Metadata
		timeSource      clock.TimeSource
		metricsClient   metrics.Client
		logger          log.Logger

		// refresh lock is used to guarantee at most one
		// coroutine is doing namespace refreshment
		refreshLock     sync.Mutex
		lastRefreshTime atomic.Value
		checkLock       sync.Mutex
		lastCheckTime   time.Time

		callbackLock     sync.Mutex
		prepareCallbacks map[int32]PrepareCallbackFn
		callbacks        map[int32]CallbackFn
	}

	// NamespaceCacheEntries is NamespaceCacheEntry slice
	NamespaceCacheEntries []*NamespaceCacheEntry

	// NamespaceCacheEntry contains the info and config for a namespace
	NamespaceCacheEntry struct {
		clusterMetadata cluster.Metadata
		sync.RWMutex
		info                        *persistenceblobs.NamespaceInfo
		config                      *persistenceblobs.NamespaceConfig
		replicationConfig           *persistenceblobs.NamespaceReplicationConfig
		configVersion               int64
		failoverVersion             int64
		isGlobalNamespace           bool
		failoverNotificationVersion int64
		notificationVersion         int64
		initialized                 bool
	}
)

// NewNamespaceCache creates a new instance of cache for holding onto namespace information to reduce the load on persistence
func NewNamespaceCache(
	metadataMgr persistence.MetadataManager,
	clusterMetadata cluster.Metadata,
	metricsClient metrics.Client,
	logger log.Logger,
) NamespaceCache {

	cache := &namespaceCache{
		status:           namespaceCacheInitialized,
		shutdownChan:     make(chan struct{}),
		cacheNameToID:    &atomic.Value{},
		cacheByID:        &atomic.Value{},
		metadataMgr:      metadataMgr,
		clusterMetadata:  clusterMetadata,
		timeSource:       clock.NewRealTimeSource(),
		metricsClient:    metricsClient,
		logger:           logger,
		prepareCallbacks: make(map[int32]PrepareCallbackFn),
		callbacks:        make(map[int32]CallbackFn),
	}
	cache.cacheNameToID.Store(newNamespaceCache())
	cache.cacheByID.Store(newNamespaceCache())
	cache.lastRefreshTime.Store(time.Time{})

	return cache
}

func newNamespaceCache() Cache {
	opts := &Options{}
	opts.InitialCapacity = namespaceCacheInitialSize
	opts.TTL = namespaceCacheTTL
	return New(namespaceCacheMaxSize, opts)
}

func newNamespaceCacheEntry(
	clusterMetadata cluster.Metadata,
) *NamespaceCacheEntry {

	return &NamespaceCacheEntry{
		clusterMetadata: clusterMetadata,
		initialized:     false,
	}
}

// NewGlobalNamespaceCacheEntryForTest returns an entry with test data
func NewGlobalNamespaceCacheEntryForTest(
	info *persistenceblobs.NamespaceInfo,
	config *persistenceblobs.NamespaceConfig,
	repConfig *persistenceblobs.NamespaceReplicationConfig,
	failoverVersion int64,
	clusterMetadata cluster.Metadata,
) *NamespaceCacheEntry {

	return &NamespaceCacheEntry{
		info:              info,
		config:            config,
		isGlobalNamespace: true,
		replicationConfig: repConfig,
		failoverVersion:   failoverVersion,
		clusterMetadata:   clusterMetadata,
	}
}

// NewLocalNamespaceCacheEntryForTest returns an entry with test data
func NewLocalNamespaceCacheEntryForTest(
	info *persistenceblobs.NamespaceInfo,
	config *persistenceblobs.NamespaceConfig,
	targetCluster string,
	clusterMetadata cluster.Metadata,
) *NamespaceCacheEntry {

	return &NamespaceCacheEntry{
		info:              info,
		config:            config,
		isGlobalNamespace: false,
		replicationConfig: &persistenceblobs.NamespaceReplicationConfig{
			ActiveClusterName: targetCluster,
			Clusters:          []string{targetCluster},
		},
		failoverVersion: common.EmptyVersion,
		clusterMetadata: clusterMetadata,
	}
}

// NewNamespaceCacheEntryForTest returns an entry with test data
func NewNamespaceCacheEntryForTest(
	info *persistenceblobs.NamespaceInfo,
	config *persistenceblobs.NamespaceConfig,
	isGlobalNamespace bool,
	repConfig *persistenceblobs.NamespaceReplicationConfig,
	failoverVersion int64,
	clusterMetadata cluster.Metadata,
) *NamespaceCacheEntry {

	return &NamespaceCacheEntry{
		info:              info,
		config:            config,
		isGlobalNamespace: isGlobalNamespace,
		replicationConfig: repConfig,
		failoverVersion:   failoverVersion,
		clusterMetadata:   clusterMetadata,
	}
}

func (c *namespaceCache) GetCacheSize() (sizeOfCacheByName int64, sizeOfCacheByID int64) {
	return int64(c.cacheByID.Load().(Cache).Size()), int64(c.cacheNameToID.Load().(Cache).Size())
}

// Start start the background refresh of namespace
func (c *namespaceCache) Start() {
	if !atomic.CompareAndSwapInt32(&c.status, namespaceCacheInitialized, namespaceCacheStarted) {
		return
	}

	// initialize the cache by initial scan
	err := c.refreshNamespaces()
	if err != nil {
		c.logger.Fatal("Unable to initialize namespace cache", tag.Error(err))
	}
	go c.refreshLoop()
}

// Start start the background refresh of namespace
func (c *namespaceCache) Stop() {

	if !atomic.CompareAndSwapInt32(&c.status, namespaceCacheStarted, namespaceCacheStopped) {
		return
	}
	close(c.shutdownChan)
}

func (c *namespaceCache) GetAllNamespace() map[string]*NamespaceCacheEntry {
	result := make(map[string]*NamespaceCacheEntry)
	ite := c.cacheByID.Load().(Cache).Iterator()
	defer ite.Close()

	for ite.HasNext() {
		entry := ite.Next()
		id := entry.Key().(string)
		namespaceCacheEntry := entry.Value().(*NamespaceCacheEntry)
		namespaceCacheEntry.RLock()
		dup := namespaceCacheEntry.duplicate()
		namespaceCacheEntry.RUnlock()
		result[id] = dup
	}
	return result
}

// RegisterNamespaceChangeCallback set a namespace change callback
// WARN: the beforeCallback function will be triggered by namespace cache when holding the namespace cache lock,
// make sure the callback function will not call namespace cache again in case of dead lock
// afterCallback will be invoked when NOT holding the namespace cache lock.
func (c *namespaceCache) RegisterNamespaceChangeCallback(
	shard int32,
	initialNotificationVersion int64,
	prepareCallback PrepareCallbackFn,
	callback CallbackFn,
) {

	c.callbackLock.Lock()
	c.prepareCallbacks[shard] = prepareCallback
	c.callbacks[shard] = callback
	c.callbackLock.Unlock()

	// this section is trying to make the shard catch up with namespace changes
	namespaces := NamespaceCacheEntries{}
	for _, namespace := range c.GetAllNamespace() {
		namespaces = append(namespaces, namespace)
	}
	// we mush notify the change in a ordered fashion
	// since history shard have to update the shard info
	// with namespace change version.
	sort.Sort(namespaces)

	var prevEntries []*NamespaceCacheEntry
	var nextEntries []*NamespaceCacheEntry
	for _, namespace := range namespaces {
		if namespace.notificationVersion >= initialNotificationVersion {
			prevEntries = append(prevEntries, nil)
			nextEntries = append(nextEntries, namespace)
		}
	}
	if len(prevEntries) > 0 {
		prepareCallback()
		callback(prevEntries, nextEntries)
	}
}

// UnregisterNamespaceChangeCallback delete a namespace failover callback
func (c *namespaceCache) UnregisterNamespaceChangeCallback(
	shard int32,
) {

	c.callbackLock.Lock()
	defer c.callbackLock.Unlock()

	delete(c.prepareCallbacks, shard)
	delete(c.callbacks, shard)
}

// GetNamespace retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *namespaceCache) GetNamespace(
	name string,
) (*NamespaceCacheEntry, error) {

	if name == "" {
		return nil, serviceerror.NewInvalidArgument("Namespace is empty.")
	}
	return c.getNamespace(name)
}

// GetNamespaceByID retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *namespaceCache) GetNamespaceByID(
	id string,
) (*NamespaceCacheEntry, error) {

	if id == "" {
		return nil, serviceerror.NewInvalidArgument("NamespaceID is empty.")
	}
	return c.getNamespaceByID(id, true)
}

// GetNamespaceID retrieves namespaceID by using GetNamespace
func (c *namespaceCache) GetNamespaceID(
	name string,
) (string, error) {

	entry, err := c.GetNamespace(name)
	if err != nil {
		return "", err
	}
	return entry.info.Id, nil
}

// GetNamespaceName returns namespace name given the namespace id
func (c *namespaceCache) GetNamespaceName(
	id string,
) (string, error) {

	entry, err := c.getNamespaceByID(id, false)
	if err != nil {
		return "", err
	}
	return entry.info.Name, nil
}

func (c *namespaceCache) refreshLoop() {
	timer := time.NewTicker(NamespaceCacheRefreshInterval)
	defer timer.Stop()

	for {
		select {
		case <-c.shutdownChan:
			return
		case <-timer.C:
			for err := c.refreshNamespaces(); err != nil; err = c.refreshNamespaces() {
				c.logger.Error("Error refreshing namespace cache", tag.Error(err))
				time.Sleep(NamespaceCacheRefreshFailureRetryInterval)

				if _, opened := <-c.shutdownChan; !opened {
					return
				}
			}
		}
	}
}

func (c *namespaceCache) refreshNamespaces() error {
	c.refreshLock.Lock()
	defer c.refreshLock.Unlock()
	return c.refreshNamespacesLocked()
}

// this function only refresh the namespaces in the v2 table
// the namespaces in the v1 table will be refreshed if cache is stale
func (c *namespaceCache) refreshNamespacesLocked() error {
	now := c.timeSource.Now()

	// first load the metadata record, then load namespaces
	// this can guarantee that namespaces in the cache are not updated more than metadata record
	metadata, err := c.metadataMgr.GetMetadata()
	if err != nil {
		return err
	}
	namespaceNotificationVersion := metadata.NotificationVersion

	var token []byte
	request := &persistence.ListNamespacesRequest{PageSize: namespaceCacheRefreshPageSize}
	var namespaces NamespaceCacheEntries
	continuePage := true

	for continuePage {
		request.NextPageToken = token
		response, err := c.metadataMgr.ListNamespaces(request)
		if err != nil {
			return err
		}
		token = response.NextPageToken
		for _, namespace := range response.Namespaces {
			namespaces = append(namespaces, c.buildEntryFromRecord(namespace))
		}
		continuePage = len(token) != 0
	}

	// we mush apply the namespace change by order
	// since history shard have to update the shard info
	// with namespace change version.
	sort.Sort(namespaces)

	var prevEntries []*NamespaceCacheEntry
	var nextEntries []*NamespaceCacheEntry

	// make a copy of the existing namespace cache, so we can calculate diff and do compare and swap
	newCacheNameToID := newNamespaceCache()
	newCacheByID := newNamespaceCache()
	for _, namespace := range c.GetAllNamespace() {
		newCacheNameToID.Put(namespace.info.Name, namespace.info.Id)
		newCacheByID.Put(namespace.info.Id, namespace)
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
		prevEntry, nextEntry, err := c.updateIDToNamespaceCache(newCacheByID, namespace.info.Id, namespace)
		if err != nil {
			return err
		}
		c.updateNameToIDCache(newCacheNameToID, nextEntry.info.Name, nextEntry.info.Id)

		if prevEntry != nil {
			prevEntries = append(prevEntries, prevEntry)
			nextEntries = append(nextEntries, nextEntry)
		}
	}

	// NOTE: READ REF BEFORE MODIFICATION
	// ref: historyEngine.go registerNamespaceFailoverCallback function
	c.callbackLock.Lock()
	defer c.callbackLock.Unlock()
	c.triggerNamespaceChangePrepareCallbackLocked()
	c.cacheByID.Store(newCacheByID)
	c.cacheNameToID.Store(newCacheNameToID)
	c.triggerNamespaceChangeCallbackLocked(prevEntries, nextEntries)

	// only update last refresh time when refresh succeeded
	c.lastRefreshTime.Store(now)
	return nil
}

func (c *namespaceCache) checkNamespaceExists(
	name string,
	id string,
) (bool, error) {
	now := c.timeSource.Now()
	if now.Sub(c.lastRefreshTime.Load().(time.Time)) < NamespaceCacheMinRefreshInterval {
		return false, nil
	}

	c.checkLock.Lock()
	defer c.checkLock.Unlock()

	now = c.timeSource.Now()
	if now.Sub(c.lastCheckTime) < NamespaceCacheMinRefreshInterval {
		return true, nil
	}
	c.lastCheckTime = now
	_, err := c.metadataMgr.GetNamespace(&persistence.GetNamespaceRequest{Name: name, ID: id})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (c *namespaceCache) updateNameToIDCache(
	cacheNameToID Cache,
	name string,
	id string,
) {

	cacheNameToID.Put(name, id)
}

func (c *namespaceCache) updateIDToNamespaceCache(
	cacheByID Cache,
	id string,
	record *NamespaceCacheEntry,
) (*NamespaceCacheEntry, *NamespaceCacheEntry, error) {

	elem, err := cacheByID.PutIfNotExist(id, newNamespaceCacheEntry(c.clusterMetadata))
	if err != nil {
		return nil, nil, err
	}
	entry := elem.(*NamespaceCacheEntry)

	entry.Lock()
	defer entry.Unlock()

	var prevNamespace *NamespaceCacheEntry
	triggerCallback := c.clusterMetadata.IsGlobalNamespaceEnabled() &&
		// initialized will be true when the entry contains valid data
		entry.initialized &&
		record.notificationVersion > entry.notificationVersion
	if triggerCallback {
		prevNamespace = entry.duplicate()
	}

	entry.info = record.info
	entry.config = record.config
	entry.replicationConfig = record.replicationConfig
	entry.configVersion = record.configVersion
	entry.failoverVersion = record.failoverVersion
	entry.isGlobalNamespace = record.isGlobalNamespace
	entry.failoverNotificationVersion = record.failoverNotificationVersion
	entry.notificationVersion = record.notificationVersion
	entry.initialized = record.initialized

	nextNamespace := entry.duplicate()

	return prevNamespace, nextNamespace, nil
}

// getNamespace retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *namespaceCache) getNamespace(
	name string,
) (*NamespaceCacheEntry, error) {

	id, cacheHit := c.cacheNameToID.Load().(Cache).Get(name).(string)
	if cacheHit {
		return c.getNamespaceByID(id, true)
	}

	doContinue, err := c.checkNamespaceExists(name, "")
	if err != nil {
		return nil, err
	}
	if !doContinue {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("namespace: %v not found", name))
	}

	c.refreshLock.Lock()
	defer c.refreshLock.Unlock()
	id, cacheHit = c.cacheNameToID.Load().(Cache).Get(name).(string)
	if cacheHit {
		return c.getNamespaceByID(id, true)
	}
	if err := c.refreshNamespacesLocked(); err != nil {
		return nil, err
	}
	id, cacheHit = c.cacheNameToID.Load().(Cache).Get(name).(string)
	if cacheHit {
		return c.getNamespaceByID(id, true)
	}
	// impossible case
	return nil, serviceerror.NewNotFound(fmt.Sprintf("namespace: %v not found", name))
}

// getNamespaceByID retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *namespaceCache) getNamespaceByID(
	id string,
	deepCopy bool,
) (*NamespaceCacheEntry, error) {

	var result *NamespaceCacheEntry
	entry, cacheHit := c.cacheByID.Load().(Cache).Get(id).(*NamespaceCacheEntry)
	if cacheHit {
		entry.RLock()
		result = entry
		if deepCopy {
			result = entry.duplicate()
		}
		entry.RUnlock()
		return result, nil
	}

	doContinue, err := c.checkNamespaceExists("", id)
	if err != nil {
		return nil, err
	}
	if !doContinue {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("namespace ID: %v not found", id))
	}

	c.refreshLock.Lock()
	defer c.refreshLock.Unlock()
	entry, cacheHit = c.cacheByID.Load().(Cache).Get(id).(*NamespaceCacheEntry)
	if cacheHit {
		entry.RLock()
		result = entry
		if deepCopy {
			result = entry.duplicate()
		}
		entry.RUnlock()
		return result, nil
	}
	if err := c.refreshNamespacesLocked(); err != nil {
		return nil, err
	}
	entry, cacheHit = c.cacheByID.Load().(Cache).Get(id).(*NamespaceCacheEntry)
	if cacheHit {
		entry.RLock()
		result = entry
		if deepCopy {
			result = entry.duplicate()
		}
		entry.RUnlock()
		return result, nil
	}
	return nil, serviceerror.NewNotFound(fmt.Sprintf("namespace ID: %v not found", id))
}

func (c *namespaceCache) triggerNamespaceChangePrepareCallbackLocked() {
	sw := c.metricsClient.StartTimer(metrics.NamespaceCacheScope, metrics.NamespaceCachePrepareCallbacksLatency)
	defer sw.Stop()

	for _, prepareCallback := range c.prepareCallbacks {
		prepareCallback()
	}
}

func (c *namespaceCache) triggerNamespaceChangeCallbackLocked(
	prevNamespaces []*NamespaceCacheEntry,
	nextNamespaces []*NamespaceCacheEntry,
) {

	sw := c.metricsClient.StartTimer(metrics.NamespaceCacheScope, metrics.NamespaceCacheCallbacksLatency)
	defer sw.Stop()

	for _, callback := range c.callbacks {
		callback(prevNamespaces, nextNamespaces)
	}
}

func (c *namespaceCache) buildEntryFromRecord(
	record *persistence.GetNamespaceResponse,
) *NamespaceCacheEntry {

	// this is a shallow copy, but since the record is generated by persistence
	// and only accessible here, it would be fine
	newEntry := newNamespaceCacheEntry(c.clusterMetadata)
	newEntry.info = record.Namespace.Info
	newEntry.config = record.Namespace.Config
	newEntry.replicationConfig = record.Namespace.ReplicationConfig
	newEntry.configVersion = record.Namespace.ConfigVersion
	newEntry.failoverVersion = record.Namespace.FailoverVersion
	newEntry.isGlobalNamespace = record.IsGlobalNamespace
	newEntry.failoverNotificationVersion = record.Namespace.FailoverNotificationVersion
	newEntry.notificationVersion = record.NotificationVersion
	newEntry.initialized = true
	return newEntry
}

func (entry *NamespaceCacheEntry) duplicate() *NamespaceCacheEntry {
	// this is a deep copy
	result := newNamespaceCacheEntry(entry.clusterMetadata)
	result.info = proto.Clone(entry.info).(*persistenceblobs.NamespaceInfo)
	if result.info.Data == nil {
		result.info.Data = make(map[string]string, 0)
	}

	result.config = proto.Clone(entry.config).(*persistenceblobs.NamespaceConfig)
	if result.config.BadBinaries == nil || result.config.BadBinaries.Binaries == nil {
		result.config.BadBinaries.Binaries = make(map[string]*namespacepb.BadBinaryInfo, 0)
	}

	result.replicationConfig = proto.Clone(entry.replicationConfig).(*persistenceblobs.NamespaceReplicationConfig)
	result.configVersion = entry.configVersion
	result.failoverVersion = entry.failoverVersion
	result.isGlobalNamespace = entry.isGlobalNamespace
	result.failoverNotificationVersion = entry.failoverNotificationVersion
	result.notificationVersion = entry.notificationVersion
	result.initialized = entry.initialized
	return result
}

// GetInfo return the namespace info
func (entry *NamespaceCacheEntry) GetInfo() *persistenceblobs.NamespaceInfo {
	return entry.info
}

// GetConfig return the namespace config
func (entry *NamespaceCacheEntry) GetConfig() *persistenceblobs.NamespaceConfig {
	return entry.config
}

// GetReplicationConfig return the namespace replication config
func (entry *NamespaceCacheEntry) GetReplicationConfig() *persistenceblobs.NamespaceReplicationConfig {
	return entry.replicationConfig
}

// GetConfigVersion return the namespace config version
func (entry *NamespaceCacheEntry) GetConfigVersion() int64 {
	return entry.configVersion
}

// GetFailoverVersion return the namespace failover version
func (entry *NamespaceCacheEntry) GetFailoverVersion() int64 {
	return entry.failoverVersion
}

// IsGlobalNamespace return whether the namespace is a global namespace
func (entry *NamespaceCacheEntry) IsGlobalNamespace() bool {
	return entry.isGlobalNamespace
}

// GetFailoverNotificationVersion return the global notification version of when failover happened
func (entry *NamespaceCacheEntry) GetFailoverNotificationVersion() int64 {
	return entry.failoverNotificationVersion
}

// GetNotificationVersion return the global notification version of when namespace changed
func (entry *NamespaceCacheEntry) GetNotificationVersion() int64 {
	return entry.notificationVersion
}

// IsNamespaceActive return whether the namespace is active, i.e. non global namespace or global namespace which active cluster is the current cluster
func (entry *NamespaceCacheEntry) IsNamespaceActive() bool {
	if !entry.isGlobalNamespace {
		// namespace is not a global namespace, meaning namespace is always "active" within each cluster
		return true
	}
	return entry.clusterMetadata.GetCurrentClusterName() == entry.replicationConfig.ActiveClusterName
}

// GetReplicationPolicy return the derived workflow replication policy
func (entry *NamespaceCacheEntry) GetReplicationPolicy() ReplicationPolicy {
	// frontend guarantee that the clusters always contains the active namespace, so if the # of clusters is 1
	// then we do not need to send out any events for replication
	if entry.isGlobalNamespace && len(entry.replicationConfig.Clusters) > 1 {
		return ReplicationPolicyMultiCluster
	}
	return ReplicationPolicyOneCluster
}

// GetNamespaceNotActiveErr return err if namespace is not active, nil otherwise
func (entry *NamespaceCacheEntry) GetNamespaceNotActiveErr() error {
	if entry.IsNamespaceActive() {
		// namespace is consider active
		return nil
	}
	return serviceerror.NewNamespaceNotActive(
		entry.info.Name,
		entry.clusterMetadata.GetCurrentClusterName(),
		entry.replicationConfig.ActiveClusterName,
	)
}

// Len return length
func (t NamespaceCacheEntries) Len() int {
	return len(t)
}

// Swap implements sort.Interface.
func (t NamespaceCacheEntries) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Less implements sort.Interface
func (t NamespaceCacheEntries) Less(i, j int) bool {
	return t[i].notificationVersion < t[j].notificationVersion
}

// CreateNamespaceCacheEntry create a cache entry with namespace
func CreateNamespaceCacheEntry(
	namespace string,
) *NamespaceCacheEntry {

	return &NamespaceCacheEntry{info: &persistenceblobs.NamespaceInfo{Name: namespace}}
}

// SampleRetentionKey is key to specify sample retention
var SampleRetentionKey = "sample_retention_days"

// SampleRateKey is key to specify sample rate
var SampleRateKey = "sample_retention_rate"

// GetRetentionDays returns retention in days for given workflow
func (entry *NamespaceCacheEntry) GetRetentionDays(
	workflowID string,
) int32 {

	if entry.IsSampledForLongerRetention(workflowID) {
		if sampledRetentionValue, ok := entry.info.Data[SampleRetentionKey]; ok {
			sampledRetentionDays, err := strconv.Atoi(sampledRetentionValue)
			if err != nil || sampledRetentionDays < timestamp.DaysFromDuration(entry.config.Retention) {
				return timestamp.DaysInt32FromDuration(entry.config.Retention)
			}
			return int32(sampledRetentionDays)
		}
	}

	if entry.config.Retention == nil {
		return 0
	}

	return timestamp.DaysInt32FromDuration(entry.config.Retention)
}

// IsSampledForLongerRetentionEnabled return whether sample for longer retention is enabled or not
func (entry *NamespaceCacheEntry) IsSampledForLongerRetentionEnabled(
	workflowID string,
) bool {

	_, ok := entry.info.Data[SampleRateKey]
	return ok
}

// IsSampledForLongerRetention return should given workflow been sampled or not
func (entry *NamespaceCacheEntry) IsSampledForLongerRetention(
	workflowID string,
) bool {

	if sampledRateValue, ok := entry.info.Data[SampleRateKey]; ok {
		sampledRate, err := strconv.ParseFloat(sampledRateValue, 64)
		if err != nil {
			return false
		}

		h := fnv.New32a()
		_, err = h.Write([]byte(workflowID))
		if err != nil {
			return false
		}
		hash := h.Sum32()

		r := float64(hash%1000) / float64(1000) // use 1000 so we support one decimal rate like 1.5%.
		if r < sampledRate {                    // sampled
			return true
		}
	}
	return false
}
