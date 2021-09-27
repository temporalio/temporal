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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination cache_mock.go

package namespace

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/cache"

	"github.com/gogo/protobuf/proto"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
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
	// CacheRefreshInterval namespace cache refresh interval
	CacheRefreshInterval = 10 * time.Second
	// CacheRefreshFailureRetryInterval is the wait time
	// if refreshment encounters error
	CacheRefreshFailureRetryInterval = 1 * time.Second
	namespaceCacheRefreshPageSize    = 200

	namespaceCacheInitialized int32 = 0
	namespaceCacheStarted     int32 = 1
	namespaceCacheStopped     int32 = 2
)

type (
	// EntryMutation changes a CacheEntry "in-flight" during a Clone operation.
	EntryMutation interface {
		apply(*persistence.GetNamespaceResponse)
	}

	// BadBinaryError is an error type carrying additional information about
	// when/why/who configured a given checksum as being bad.
	BadBinaryError struct {
		cksum string
		info  *namespacepb.BadBinaryInfo
	}

	ClusterInfo interface {
		IsGlobalNamespaceEnabled() bool
		GetCurrentClusterName() string
	}

	Persistence interface {
		ListNamespaces(
			*persistence.ListNamespacesRequest,
		) (*persistence.ListNamespacesResponse, error)
		GetMetadata() (*persistence.GetMetadataResponse, error)
	}

	// PrepareCallbackFn is function to be called before CallbackFn is called,
	// it is guaranteed that PrepareCallbackFn and CallbackFn pair will be both called or non will be called
	PrepareCallbackFn func()

	// CallbackFn is function to be called when the namespace cache entries are changed
	// it is guaranteed that PrepareCallbackFn and CallbackFn pair will be both called or non will be called
	CallbackFn func(prevNamespaces []*CacheEntry, nextNamespaces []*CacheEntry)

	// Cache is used the cache namespace information and configuration to avoid making too many calls to cassandra.
	// This cache is mainly used by frontend for resolving namespace names to namespace uuids which are used throughout the
	// system.  Each namespace entry is kept in the cache for one hour but also has an expiry of 10 seconds.  This results
	// in updating the namespace entry every 10 seconds but in the case of a cassandra failure we can still keep on serving
	// requests using the stale entry from cache upto an hour
	Cache interface {
		common.Daemon
		RegisterNamespaceChangeCallback(shard int32, initialNotificationVersion int64, prepareCallback PrepareCallbackFn, callback CallbackFn)
		UnregisterNamespaceChangeCallback(shard int32)
		GetNamespace(name string) (*CacheEntry, error)
		GetNamespaceByID(id string) (*CacheEntry, error)
		GetNamespaceID(name string) (string, error)
		GetNamespaceName(id string) (string, error)
		GetAllNamespace() map[string]*CacheEntry
		GetCacheSize() (sizeOfCacheByName int64, sizeOfCacheByID int64)
		// Refresh forces an immediate refresh of the namespace cache and blocks until it's complete.
		Refresh()
	}

	namespaceCache struct {
		status        int32
		shutdownChan  chan struct{}
		cacheNameToID *atomic.Value
		cacheByID     *atomic.Value
		persistence   Persistence
		thisCluster   ClusterInfo
		timeSource    clock.TimeSource
		metricsClient metrics.Client
		logger        log.Logger

		// refresh lock is used to guarantee at most one
		// coroutine is doing namespace refreshment
		refreshLock     sync.Mutex
		lastRefreshTime atomic.Value
		checkLock       sync.Mutex
		lastCheckTime   time.Time

		callbackLock     sync.Mutex
		prepareCallbacks map[int32]PrepareCallbackFn
		callbacks        map[int32]CallbackFn
		triggerRefreshCh chan chan struct{}
	}

	// cacheEntries CacheEntry slice
	cacheEntries []*CacheEntry

	// CacheEntry contains the info and config for a namespace
	CacheEntry struct {
		thisCluster ClusterInfo
		sync.RWMutex
		info                        *persistencespb.NamespaceInfo
		config                      *persistencespb.NamespaceConfig
		replicationConfig           *persistencespb.NamespaceReplicationConfig
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
	persistence Persistence,
	thisCluster ClusterInfo,
	metricsClient metrics.Client,
	logger log.Logger,
) Cache {

	nscache := &namespaceCache{
		status:           namespaceCacheInitialized,
		shutdownChan:     make(chan struct{}),
		cacheNameToID:    &atomic.Value{},
		cacheByID:        &atomic.Value{},
		persistence:      persistence,
		thisCluster:      thisCluster,
		timeSource:       clock.NewRealTimeSource(),
		metricsClient:    metricsClient,
		logger:           logger,
		prepareCallbacks: make(map[int32]PrepareCallbackFn),
		callbacks:        make(map[int32]CallbackFn),
		triggerRefreshCh: make(chan chan struct{}, 1),
	}
	nscache.cacheNameToID.Store(newCache())
	nscache.cacheByID.Store(newCache())
	nscache.lastRefreshTime.Store(time.Time{})

	return nscache
}

func newCache() cache.Cache {
	opts := &cache.Options{}
	opts.InitialCapacity = namespaceCacheInitialSize
	opts.TTL = namespaceCacheTTL
	return cache.New(namespaceCacheMaxSize, opts)
}

func (c *namespaceCache) GetCacheSize() (sizeOfCacheByName int64, sizeOfCacheByID int64) {
	return int64(c.cacheByID.Load().(cache.Cache).Size()), int64(c.cacheNameToID.Load().(cache.Cache).Size())
}

// Start the background refresh of namespace
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

// Stop the background refresh of namespace
func (c *namespaceCache) Stop() {

	if !atomic.CompareAndSwapInt32(&c.status, namespaceCacheStarted, namespaceCacheStopped) {
		return
	}
	close(c.shutdownChan)
}

func (c *namespaceCache) GetAllNamespace() map[string]*CacheEntry {
	result := make(map[string]*CacheEntry)
	ite := c.cacheByID.Load().(cache.Cache).Iterator()
	defer ite.Close()

	for ite.HasNext() {
		entry := ite.Next()
		id := entry.Key().(string)
		namespaceCacheEntry := entry.Value().(*CacheEntry)
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
	namespaces := cacheEntries{}
	for _, namespace := range c.GetAllNamespace() {
		namespaces = append(namespaces, namespace)
	}
	// we mush notify the change in a ordered fashion
	// since history shard have to update the shard info
	// with namespace change version.
	sort.Sort(namespaces)

	var prevEntries []*CacheEntry
	var nextEntries []*CacheEntry
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
) (*CacheEntry, error) {

	if name == "" {
		return nil, serviceerror.NewInvalidArgument("Namespace is empty.")
	}
	return c.getNamespace(name)
}

// GetNamespaceByID retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *namespaceCache) GetNamespaceByID(
	id string,
) (*CacheEntry, error) {

	if id == "" {
		return nil, serviceerror.NewInvalidArgument("NamespaceID is empty.")
	}
	return c.getNamespaceByID(id)
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

	entry, err := c.getNamespaceByID(id)
	if err != nil {
		return "", err
	}
	return entry.info.Name, nil
}

func (c *namespaceCache) Refresh() {
	replyCh := make(chan struct{})
	c.triggerRefreshCh <- replyCh
	<-replyCh
}

func (c *namespaceCache) refreshLoop() {
	timer := time.NewTicker(CacheRefreshInterval)
	defer timer.Stop()

	// Put timer events on our channel so we can select on just one below.
	go func() {
		for range timer.C {
			select {
			case c.triggerRefreshCh <- nil:
			default:
			}

		}
	}()

	for {
		select {
		case <-c.shutdownChan:
			return
		case replyCh := <-c.triggerRefreshCh:
			for err := c.refreshNamespaces(); err != nil; err = c.refreshNamespaces() {
				select {
				case <-c.shutdownChan:
					return
				default:
					c.logger.Error("Error refreshing namespace cache", tag.Error(err))
					time.Sleep(CacheRefreshFailureRetryInterval)
				}
			}
			if replyCh != nil {
				replyCh <- struct{}{}
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
	metadata, err := c.persistence.GetMetadata()
	if err != nil {
		return err
	}
	namespaceNotificationVersion := metadata.NotificationVersion

	var token []byte
	request := &persistence.ListNamespacesRequest{PageSize: namespaceCacheRefreshPageSize}
	var namespaces cacheEntries
	continuePage := true

	for continuePage {
		request.NextPageToken = token
		response, err := c.persistence.ListNamespaces(request)
		if err != nil {
			return err
		}
		token = response.NextPageToken
		for _, namespace := range response.Namespaces {
			namespaces = append(namespaces, FromPersistentState(c.thisCluster, namespace))
		}
		continuePage = len(token) != 0
	}

	// we mush apply the namespace change by order
	// since history shard have to update the shard info
	// with namespace change version.
	sort.Sort(namespaces)

	var prevEntries []*CacheEntry
	var nextEntries []*CacheEntry

	// make a copy of the existing namespace cache, so we can calculate diff and do compare and swap
	newCacheNameToID := newCache()
	newCacheByID := newCache()
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

func (c *namespaceCache) updateNameToIDCache(
	cacheNameToID cache.Cache,
	name string,
	id string,
) {

	cacheNameToID.Put(name, id)
}

func (c *namespaceCache) updateIDToNamespaceCache(
	cacheByID cache.Cache,
	id string,
	record *CacheEntry,
) (*CacheEntry, *CacheEntry, error) {

	elem, err := cacheByID.PutIfNotExist(id, &CacheEntry{thisCluster: c.thisCluster})
	if err != nil {
		return nil, nil, err
	}
	entry := elem.(*CacheEntry)

	entry.Lock()
	defer entry.Unlock()

	var prevNamespace *CacheEntry
	triggerCallback := c.thisCluster.IsGlobalNamespaceEnabled() &&
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
) (*CacheEntry, error) {

	id, cacheHit := c.cacheNameToID.Load().(cache.Cache).Get(name).(string)
	if cacheHit {
		return c.getNamespaceByID(id)
	}
	return nil, serviceerror.NewNotFound(fmt.Sprintf("namespace: %v not found", name))
}

// getNamespaceByID retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *namespaceCache) getNamespaceByID(
	id string,
) (*CacheEntry, error) {

	var result *CacheEntry
	entry, cacheHit := c.cacheByID.Load().(cache.Cache).Get(id).(*CacheEntry)
	if cacheHit {
		entry.RLock()
		result = entry.duplicate()
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
	prevNamespaces []*CacheEntry,
	nextNamespaces []*CacheEntry,
) {

	sw := c.metricsClient.StartTimer(metrics.NamespaceCacheScope, metrics.NamespaceCacheCallbacksLatency)
	defer sw.Stop()

	for _, callback := range c.callbacks {
		callback(prevNamespaces, nextNamespaces)
	}
}

func FromPersistentState(
	thisCluster ClusterInfo,
	record *persistence.GetNamespaceResponse,
) *CacheEntry {
	return &CacheEntry{
		thisCluster:                 thisCluster,
		info:                        record.Namespace.Info,
		config:                      record.Namespace.Config,
		replicationConfig:           record.Namespace.ReplicationConfig,
		configVersion:               record.Namespace.ConfigVersion,
		failoverVersion:             record.Namespace.FailoverVersion,
		isGlobalNamespace:           record.IsGlobalNamespace,
		failoverNotificationVersion: record.Namespace.FailoverNotificationVersion,
		notificationVersion:         record.NotificationVersion,
		initialized:                 true,
	}
}

func (entry *CacheEntry) Clone(ms ...EntryMutation) *CacheEntry {
	newEntry := entry.duplicate()
	r := persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info:                        newEntry.info,
			Config:                      newEntry.config,
			ReplicationConfig:           newEntry.replicationConfig,
			ConfigVersion:               newEntry.configVersion,
			FailoverNotificationVersion: newEntry.failoverNotificationVersion,
			FailoverVersion:             newEntry.failoverVersion,
		},
		IsGlobalNamespace:   newEntry.isGlobalNamespace,
		NotificationVersion: newEntry.notificationVersion,
	}
	for _, m := range ms {
		m.apply(&r)
	}
	return FromPersistentState(entry.thisCluster, &r)
}

func (entry *CacheEntry) duplicate() *CacheEntry {
	// this is a deep copy
	result := &CacheEntry{thisCluster: entry.thisCluster}
	result.info = proto.Clone(entry.info).(*persistencespb.NamespaceInfo)
	if result.info.Data == nil {
		result.info.Data = make(map[string]string)
	}

	result.config = proto.Clone(entry.config).(*persistencespb.NamespaceConfig)
	if result.config.BadBinaries == nil {
		result.config.BadBinaries = &namespacepb.BadBinaries{
			Binaries: make(map[string]*namespacepb.BadBinaryInfo),
		}
	} else if result.config.BadBinaries.Binaries == nil {
		result.config.BadBinaries.Binaries = make(map[string]*namespacepb.BadBinaryInfo)
	}

	result.replicationConfig = proto.Clone(entry.replicationConfig).(*persistencespb.NamespaceReplicationConfig)
	result.configVersion = entry.configVersion
	result.failoverVersion = entry.failoverVersion

	result.isGlobalNamespace = entry.isGlobalNamespace
	result.failoverNotificationVersion = entry.failoverNotificationVersion
	result.notificationVersion = entry.notificationVersion
	result.initialized = entry.initialized
	return result
}

// VisibilityArchivalState observes the visibility archive configuration (state
// and URI) for this namespace.
func (entry *CacheEntry) VisibilityArchivalState() ArchivalState {
	return ArchivalState{
		State: entry.config.VisibilityArchivalState,
		URI:   entry.config.VisibilityArchivalUri,
	}
}

// HistoryArchivalState observes the history archive configuration (state and
// URI) for this namespace.
func (entry *CacheEntry) HistoryArchivalState() ArchivalState {
	return ArchivalState{
		State: entry.config.HistoryArchivalState,
		URI:   entry.config.HistoryArchivalUri,
	}
}

// VerifyBinaryChecksum returns an error if the provided checksum is one of this
// namespace's configured bad binary checksums. The returned error (if any) will
// be unwrappable as BadBinaryError.
func (entry *CacheEntry) VerifyBinaryChecksum(cksum string) error {
	if entry.config.BadBinaries == nil ||
		entry.config.BadBinaries.Binaries == nil {
		return nil
	}
	if info, ok := entry.config.BadBinaries.Binaries[cksum]; ok {
		return BadBinaryError{cksum: cksum, info: info}
	}
	return nil
}

// ID observes this namespace's permanent unique identifier in string form.
func (entry *CacheEntry) ID() string {
	return entry.info.Id
}

// Name observes this namespace's configured name.
func (entry *CacheEntry) Name() string {
	return entry.info.Name
}

// ActiveClusterName observes the name of the cluster that is currently active
// for this namspace.
func (entry *CacheEntry) ActiveClusterName() string {
	return entry.replicationConfig.ActiveClusterName
}

// ClusterNames observes the names of the clusters to which this namespace is
// replicated.
func (entry *CacheEntry) ClusterNames() []string {
	// copy slice to preserve immutability
	out := make([]string, len(entry.replicationConfig.Clusters))
	copy(out, entry.replicationConfig.Clusters)
	return out
}

// GetConfigVersion return the namespace config version
func (entry *CacheEntry) GetConfigVersion() int64 {
	return entry.configVersion
}

// GetFailoverVersion return the namespace failover version
func (entry *CacheEntry) GetFailoverVersion() int64 {
	return entry.failoverVersion
}

// IsGlobalNamespace return whether the namespace is a global namespace
func (entry *CacheEntry) IsGlobalNamespace() bool {
	return entry.isGlobalNamespace
}

// GetFailoverNotificationVersion return the global notification version of when failover happened
func (entry *CacheEntry) GetFailoverNotificationVersion() int64 {
	return entry.failoverNotificationVersion
}

// GetNotificationVersion return the global notification version of when namespace changed
func (entry *CacheEntry) GetNotificationVersion() int64 {
	return entry.notificationVersion
}

// IsNamespaceActive return whether the namespace is active, i.e. non global namespace or global namespace which active cluster is the current cluster
func (entry *CacheEntry) IsNamespaceActive() bool {
	if !entry.isGlobalNamespace {
		// namespace is not a global namespace, meaning namespace is always "active" within each cluster
		return true
	}
	return entry.thisCluster.GetCurrentClusterName() == entry.replicationConfig.ActiveClusterName
}

// GetReplicationPolicy return the derived workflow replication policy
func (entry *CacheEntry) GetReplicationPolicy() ReplicationPolicy {
	// frontend guarantee that the clusters always contains the active namespace, so if the # of clusters is 1
	// then we do not need to send out any events for replication
	if entry.isGlobalNamespace && len(entry.replicationConfig.Clusters) > 1 {
		return ReplicationPolicyMultiCluster
	}
	return ReplicationPolicyOneCluster
}

// GetNamespaceNotActiveErr return err if namespace is not active, nil otherwise
func (entry *CacheEntry) GetNamespaceNotActiveErr() error {
	if entry.IsNamespaceActive() {
		// namespace is consider active
		return nil
	}
	return serviceerror.NewNamespaceNotActive(
		entry.info.Name,
		entry.thisCluster.GetCurrentClusterName(),
		entry.replicationConfig.ActiveClusterName,
	)
}

// Len return length
func (t cacheEntries) Len() int {
	return len(t)
}

// Swap implements sort.Interface.
func (t cacheEntries) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Less implements sort.Interface
func (t cacheEntries) Less(i, j int) bool {
	return t[i].notificationVersion < t[j].notificationVersion
}

// CreateNamespaceCacheEntry create a cache entry with namespace
func CreateNamespaceCacheEntry(
	namespace string,
) *CacheEntry {

	return &CacheEntry{info: &persistencespb.NamespaceInfo{Name: namespace}}
}

// SampleRetentionKey is key to specify sample retention
var SampleRetentionKey = "sample_retention_days"

// SampleRateKey is key to specify sample rate
var SampleRateKey = "sample_retention_rate"

// GetRetention returns retention in days for given workflow
func (entry *CacheEntry) GetRetention(
	workflowID string,
) time.Duration {

	if entry.config.Retention == nil {
		return 0
	}

	if entry.IsSampledForLongerRetention(workflowID) {
		if sampledRetentionValue, ok := entry.info.Data[SampleRetentionKey]; ok {
			sampledRetentionDays, err := strconv.Atoi(sampledRetentionValue)
			sampledRetention := *timestamp.DurationFromDays(int32(sampledRetentionDays))
			if err != nil || sampledRetention < *entry.config.Retention {
				return *entry.config.Retention
			}
			return sampledRetention
		}
	}

	return *entry.config.Retention
}

// IsSampledForLongerRetentionEnabled return whether sample for longer retention is enabled or not
func (entry *CacheEntry) IsSampledForLongerRetentionEnabled(string) bool {

	_, ok := entry.info.Data[SampleRateKey]
	return ok
}

// IsSampledForLongerRetention return should given workflow been sampled or not
func (entry *CacheEntry) IsSampledForLongerRetention(
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

// Error returns the reason associated with this bad binary.
func (e BadBinaryError) Error() string {
	return e.info.Reason
}

// Reason returns the reason associated with this bad binary.
func (e BadBinaryError) Reason() string {
	return e.info.Reason
}

// Operator returns the operator associated with this bad binary.
func (e BadBinaryError) Operator() string {
	return e.info.Operator
}

// Created returns the time at which this bad binary was declared to be bad.
func (e BadBinaryError) Created() time.Time {
	return *e.info.CreateTime
}

// Checksum observes the binary checksum that caused this error.
func (e BadBinaryError) Checksum() string {
	return e.cksum
}
