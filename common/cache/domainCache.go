// Copyright (c) 2017 Uber Technologies, Inc.
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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination domainCache_mock.go -self_package github.com/uber/cadence/common/cache

package cache

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

// ReplicationPolicy is the domain's replication policy,
// derived from domain's replication config
type ReplicationPolicy int

const (
	// ReplicationPolicyOneCluster indicate that workflows does not need to be replicated
	// applicable to local domain & global domain with one cluster
	ReplicationPolicyOneCluster ReplicationPolicy = 0
	// ReplicationPolicyMultiCluster indicate that workflows need to be replicated
	ReplicationPolicyMultiCluster ReplicationPolicy = 1
)

const (
	domainCacheInitialSize = 10 * 1024
	domainCacheMaxSize     = 64 * 1024
	domainCacheTTL         = 0 // 0 means infinity
	// DomainCacheRefreshInterval domain cache refresh interval
	DomainCacheRefreshInterval = 10 * time.Second
	// DomainCacheRefreshFailureRetryInterval is the wait time
	// if refreshment encounters error
	DomainCacheRefreshFailureRetryInterval = 1 * time.Second
	domainCacheRefreshPageSize             = 200

	domainCacheInitialized int32 = 0
	domainCacheStarted     int32 = 1
	domainCacheStopped     int32 = 2
)

type (
	// PrepareCallbackFn is function to be called before CallbackFn is called,
	// it is guaranteed that PrepareCallbackFn and CallbackFn pair will be both called or non will be called
	PrepareCallbackFn func()

	// CallbackFn is function to be called when the domain cache entries are changed
	// it is guaranteed that PrepareCallbackFn and CallbackFn pair will be both called or non will be called
	CallbackFn func(prevDomains []*DomainCacheEntry, nextDomains []*DomainCacheEntry)

	// DomainCache is used the cache domain information and configuration to avoid making too many calls to cassandra.
	// This cache is mainly used by frontend for resolving domain names to domain uuids which are used throughout the
	// system.  Each domain entry is kept in the cache for one hour but also has an expiry of 10 seconds.  This results
	// in updating the domain entry every 10 seconds but in the case of a cassandra failure we can still keep on serving
	// requests using the stale entry from cache upto an hour
	DomainCache interface {
		common.Daemon
		RegisterDomainChangeCallback(shard int, initialNotificationVersion int64, prepareCallback PrepareCallbackFn, callback CallbackFn)
		UnregisterDomainChangeCallback(shard int)
		GetDomain(name string) (*DomainCacheEntry, error)
		GetDomainByID(id string) (*DomainCacheEntry, error)
		GetDomainID(name string) (string, error)
		GetDomainName(id string) (string, error)
		GetAllDomain() map[string]*DomainCacheEntry
		GetCacheSize() (sizeOfCacheByName int64, sizeOfCacheByID int64)
	}

	domainCache struct {
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
		// coroutine is doing domain refreshment
		refreshLock sync.Mutex

		callbackLock     sync.Mutex
		prepareCallbacks map[int]PrepareCallbackFn
		callbacks        map[int]CallbackFn
	}

	// DomainCacheEntries is DomainCacheEntry slice
	DomainCacheEntries []*DomainCacheEntry

	// DomainCacheEntry contains the info and config for a domain
	DomainCacheEntry struct {
		clusterMetadata cluster.Metadata
		sync.RWMutex
		info                        *persistence.DomainInfo
		config                      *persistence.DomainConfig
		replicationConfig           *persistence.DomainReplicationConfig
		configVersion               int64
		failoverVersion             int64
		isGlobalDomain              bool
		failoverNotificationVersion int64
		notificationVersion         int64
		initialized                 bool
	}
)

// NewDomainCache creates a new instance of cache for holding onto domain information to reduce the load on persistence
func NewDomainCache(
	metadataMgr persistence.MetadataManager,
	clusterMetadata cluster.Metadata,
	metricsClient metrics.Client,
	logger log.Logger,
) DomainCache {

	cache := &domainCache{
		status:           domainCacheInitialized,
		shutdownChan:     make(chan struct{}),
		cacheNameToID:    &atomic.Value{},
		cacheByID:        &atomic.Value{},
		metadataMgr:      metadataMgr,
		clusterMetadata:  clusterMetadata,
		timeSource:       clock.NewRealTimeSource(),
		metricsClient:    metricsClient,
		logger:           logger,
		prepareCallbacks: make(map[int]PrepareCallbackFn),
		callbacks:        make(map[int]CallbackFn),
	}
	cache.cacheNameToID.Store(newDomainCache())
	cache.cacheByID.Store(newDomainCache())

	return cache
}

func newDomainCache() Cache {
	opts := &Options{}
	opts.InitialCapacity = domainCacheInitialSize
	opts.TTL = domainCacheTTL
	return New(domainCacheMaxSize, opts)
}

func newDomainCacheEntry(
	clusterMetadata cluster.Metadata,
) *DomainCacheEntry {

	return &DomainCacheEntry{
		clusterMetadata: clusterMetadata,
		initialized:     false,
	}
}

// NewGlobalDomainCacheEntryForTest returns an entry with test data
func NewGlobalDomainCacheEntryForTest(
	info *persistence.DomainInfo,
	config *persistence.DomainConfig,
	repConfig *persistence.DomainReplicationConfig,
	failoverVersion int64,
	clusterMetadata cluster.Metadata,
) *DomainCacheEntry {

	return &DomainCacheEntry{
		info:              info,
		config:            config,
		isGlobalDomain:    true,
		replicationConfig: repConfig,
		failoverVersion:   failoverVersion,
		clusterMetadata:   clusterMetadata,
	}
}

// NewLocalDomainCacheEntryForTest returns an entry with test data
func NewLocalDomainCacheEntryForTest(
	info *persistence.DomainInfo,
	config *persistence.DomainConfig,
	targetCluster string,
	clusterMetadata cluster.Metadata,
) *DomainCacheEntry {

	return &DomainCacheEntry{
		info:           info,
		config:         config,
		isGlobalDomain: false,
		replicationConfig: &persistence.DomainReplicationConfig{
			ActiveClusterName: targetCluster,
			Clusters:          []*persistence.ClusterReplicationConfig{{ClusterName: targetCluster}},
		},
		failoverVersion: common.EmptyVersion,
		clusterMetadata: clusterMetadata,
	}
}

// NewDomainCacheEntryForTest returns an entry with test data
func NewDomainCacheEntryForTest(
	info *persistence.DomainInfo,
	config *persistence.DomainConfig,
	isGlobalDomain bool,
	repConfig *persistence.DomainReplicationConfig,
	failoverVersion int64,
	clusterMetadata cluster.Metadata,
) *DomainCacheEntry {

	return &DomainCacheEntry{
		info:              info,
		config:            config,
		isGlobalDomain:    isGlobalDomain,
		replicationConfig: repConfig,
		failoverVersion:   failoverVersion,
		clusterMetadata:   clusterMetadata,
	}
}

func (c *domainCache) GetCacheSize() (sizeOfCacheByName int64, sizeOfCacheByID int64) {
	return int64(c.cacheByID.Load().(Cache).Size()), int64(c.cacheNameToID.Load().(Cache).Size())
}

// Start start the background refresh of domain
func (c *domainCache) Start() {
	if !atomic.CompareAndSwapInt32(&c.status, domainCacheInitialized, domainCacheStarted) {
		return
	}

	// initialize the cache by initial scan
	err := c.refreshDomains()
	if err != nil {
		c.logger.Fatal("Unable to initialize domain cache", tag.Error(err))
	}
	go c.refreshLoop()
}

// Start start the background refresh of domain
func (c *domainCache) Stop() {
	if !atomic.CompareAndSwapInt32(&c.status, domainCacheStarted, domainCacheStopped) {
		return
	}
	close(c.shutdownChan)
}

func (c *domainCache) GetAllDomain() map[string]*DomainCacheEntry {
	result := make(map[string]*DomainCacheEntry)
	ite := c.cacheByID.Load().(Cache).Iterator()
	defer ite.Close()

	for ite.HasNext() {
		entry := ite.Next()
		id := entry.Key().(string)
		domainCacheEntry := entry.Value().(*DomainCacheEntry)
		domainCacheEntry.RLock()
		dup := domainCacheEntry.duplicate()
		domainCacheEntry.RUnlock()
		result[id] = dup
	}
	return result
}

// RegisterDomainChangeCallback set a domain change callback
// WARN: the beforeCallback function will be triggered by domain cache when holding the domain cache lock,
// make sure the callback function will not call domain cache again in case of dead lock
// afterCallback will be invoked when NOT holding the domain cache lock.
func (c *domainCache) RegisterDomainChangeCallback(
	shard int,
	initialNotificationVersion int64,
	prepareCallback PrepareCallbackFn,
	callback CallbackFn,
) {

	c.callbackLock.Lock()
	c.prepareCallbacks[shard] = prepareCallback
	c.callbacks[shard] = callback
	c.callbackLock.Unlock()

	// this section is trying to make the shard catch up with domain changes
	domains := DomainCacheEntries{}
	for _, domain := range c.GetAllDomain() {
		domains = append(domains, domain)
	}
	// we mush notify the change in a ordered fashion
	// since history shard have to update the shard info
	// with domain change version.
	sort.Sort(domains)

	prevEntries := []*DomainCacheEntry{}
	nextEntries := []*DomainCacheEntry{}
	for _, domain := range domains {
		if domain.notificationVersion >= initialNotificationVersion {
			prevEntries = append(prevEntries, nil)
			nextEntries = append(nextEntries, domain)
		}
	}
	if len(prevEntries) > 0 {
		prepareCallback()
		callback(prevEntries, nextEntries)
	}
}

// UnregisterDomainChangeCallback delete a domain failover callback
func (c *domainCache) UnregisterDomainChangeCallback(
	shard int,
) {

	c.callbackLock.Lock()
	defer c.callbackLock.Unlock()

	delete(c.prepareCallbacks, shard)
	delete(c.callbacks, shard)
}

// GetDomain retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *domainCache) GetDomain(
	name string,
) (*DomainCacheEntry, error) {

	if name == "" {
		return nil, &workflow.BadRequestError{Message: "Domain is empty."}
	}
	return c.getDomain(name)
}

// GetDomainByID retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *domainCache) GetDomainByID(
	id string,
) (*DomainCacheEntry, error) {

	if id == "" {
		return nil, &workflow.BadRequestError{Message: "DomainID is empty."}
	}
	return c.getDomainByID(id, true)
}

// GetDomainID retrieves domainID by using GetDomain
func (c *domainCache) GetDomainID(
	name string,
) (string, error) {

	entry, err := c.GetDomain(name)
	if err != nil {
		return "", err
	}
	return entry.info.ID, nil
}

// GetDomainName returns domain name given the domain id
func (c *domainCache) GetDomainName(
	id string,
) (string, error) {

	entry, err := c.getDomainByID(id, false)
	if err != nil {
		return "", err
	}
	return entry.info.Name, nil
}

func (c *domainCache) refreshLoop() {
	timer := time.NewTicker(DomainCacheRefreshInterval)
	defer timer.Stop()

	for {
		select {
		case <-c.shutdownChan:
			return
		case <-timer.C:
			for err := c.refreshDomains(); err != nil; err = c.refreshDomains() {
				select {
				case <-c.shutdownChan:
					return
				default:
					c.logger.Error("Error refreshing domain cache", tag.Error(err))
					time.Sleep(DomainCacheRefreshFailureRetryInterval)
				}
			}
		}
	}
}

func (c *domainCache) refreshDomains() error {
	c.refreshLock.Lock()
	defer c.refreshLock.Unlock()
	return c.refreshDomainsLocked()
}

// this function only refresh the domains in the v2 table
// the domains in the v1 table will be refreshed if cache is stale
func (c *domainCache) refreshDomainsLocked() error {
	// first load the metadata record, then load domains
	// this can guarantee that domains in the cache are not updated more than metadata record
	metadata, err := c.metadataMgr.GetMetadata()
	if err != nil {
		return err
	}
	domainNotificationVersion := metadata.NotificationVersion

	var token []byte
	request := &persistence.ListDomainsRequest{PageSize: domainCacheRefreshPageSize}
	var domains DomainCacheEntries
	continuePage := true

	for continuePage {
		request.NextPageToken = token
		response, err := c.metadataMgr.ListDomains(request)
		if err != nil {
			return err
		}
		token = response.NextPageToken
		for _, domain := range response.Domains {
			domains = append(domains, c.buildEntryFromRecord(domain))
		}
		continuePage = len(token) != 0
	}

	// we mush apply the domain change by order
	// since history shard have to update the shard info
	// with domain change version.
	sort.Sort(domains)

	prevEntries := []*DomainCacheEntry{}
	nextEntries := []*DomainCacheEntry{}

	// make a copy of the existing domain cache, so we can calculate diff and do compare and swap
	newCacheNameToID := newDomainCache()
	newCacheByID := newDomainCache()
	for _, domain := range c.GetAllDomain() {
		newCacheNameToID.Put(domain.info.Name, domain.info.ID)
		newCacheByID.Put(domain.info.ID, domain)
	}

UpdateLoop:
	for _, domain := range domains {
		if domain.notificationVersion >= domainNotificationVersion {
			// this guarantee that domain change events before the
			// domainNotificationVersion is loaded into the cache.

			// the domain change events after the domainNotificationVersion
			// will be loaded into cache in the next refresh
			break UpdateLoop
		}
		prevEntry, nextEntry, err := c.updateIDToDomainCache(newCacheByID, domain.info.ID, domain)
		if err != nil {
			return err
		}
		c.updateNameToIDCache(newCacheNameToID, nextEntry.info.Name, nextEntry.info.ID)

		if prevEntry != nil {
			prevEntries = append(prevEntries, prevEntry)
			nextEntries = append(nextEntries, nextEntry)
		}
	}

	// NOTE: READ REF BEFORE MODIFICATION
	// ref: historyEngine.go registerDomainFailoverCallback function
	c.callbackLock.Lock()
	defer c.callbackLock.Unlock()
	c.triggerDomainChangePrepareCallbackLocked()
	c.cacheByID.Store(newCacheByID)
	c.cacheNameToID.Store(newCacheNameToID)
	c.triggerDomainChangeCallbackLocked(prevEntries, nextEntries)
	return nil
}

func (c *domainCache) checkDomainExists(
	name string,
	id string,
) error {

	_, err := c.metadataMgr.GetDomain(&persistence.GetDomainRequest{Name: name, ID: id})
	return err
}

func (c *domainCache) updateNameToIDCache(
	cacheNameToID Cache,
	name string,
	id string,
) {

	cacheNameToID.Put(name, id)
}

func (c *domainCache) updateIDToDomainCache(
	cacheByID Cache,
	id string,
	record *DomainCacheEntry,
) (*DomainCacheEntry, *DomainCacheEntry, error) {

	elem, err := cacheByID.PutIfNotExist(id, newDomainCacheEntry(c.clusterMetadata))
	if err != nil {
		return nil, nil, err
	}
	entry := elem.(*DomainCacheEntry)

	entry.Lock()
	defer entry.Unlock()

	var prevDomain *DomainCacheEntry
	triggerCallback := c.clusterMetadata.IsGlobalDomainEnabled() &&
		// initialized will be true when the entry contains valid data
		entry.initialized &&
		record.notificationVersion > entry.notificationVersion
	if triggerCallback {
		prevDomain = entry.duplicate()
	}

	entry.info = record.info
	entry.config = record.config
	entry.replicationConfig = record.replicationConfig
	entry.configVersion = record.configVersion
	entry.failoverVersion = record.failoverVersion
	entry.isGlobalDomain = record.isGlobalDomain
	entry.failoverNotificationVersion = record.failoverNotificationVersion
	entry.notificationVersion = record.notificationVersion
	entry.initialized = record.initialized

	nextDomain := entry.duplicate()

	return prevDomain, nextDomain, nil
}

// getDomain retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *domainCache) getDomain(
	name string,
) (*DomainCacheEntry, error) {

	id, cacheHit := c.cacheNameToID.Load().(Cache).Get(name).(string)
	if cacheHit {
		return c.getDomainByID(id, true)
	}

	if err := c.checkDomainExists(name, ""); err != nil {
		return nil, err
	}

	c.refreshLock.Lock()
	defer c.refreshLock.Unlock()
	id, cacheHit = c.cacheNameToID.Load().(Cache).Get(name).(string)
	if cacheHit {
		return c.getDomainByID(id, true)
	}
	if err := c.refreshDomainsLocked(); err != nil {
		return nil, err
	}
	id, cacheHit = c.cacheNameToID.Load().(Cache).Get(name).(string)
	if cacheHit {
		return c.getDomainByID(id, true)
	}
	// impossible case
	return nil, &workflow.InternalServiceError{Message: "domainCache encounter case where domain exists but cannot be loaded"}
}

// getDomainByID retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *domainCache) getDomainByID(
	id string,
	deepCopy bool,
) (*DomainCacheEntry, error) {

	var result *DomainCacheEntry
	entry, cacheHit := c.cacheByID.Load().(Cache).Get(id).(*DomainCacheEntry)
	if cacheHit {
		entry.RLock()
		result = entry
		if deepCopy {
			result = entry.duplicate()
		}
		entry.RUnlock()
		return result, nil
	}

	if err := c.checkDomainExists("", id); err != nil {
		return nil, err
	}

	c.refreshLock.Lock()
	defer c.refreshLock.Unlock()
	entry, cacheHit = c.cacheByID.Load().(Cache).Get(id).(*DomainCacheEntry)
	if cacheHit {
		entry.RLock()
		result = entry
		if deepCopy {
			result = entry.duplicate()
		}
		entry.RUnlock()
		return result, nil
	}
	if err := c.refreshDomainsLocked(); err != nil {
		return nil, err
	}
	entry, cacheHit = c.cacheByID.Load().(Cache).Get(id).(*DomainCacheEntry)
	if cacheHit {
		entry.RLock()
		result = entry
		if deepCopy {
			result = entry.duplicate()
		}
		entry.RUnlock()
		return result, nil
	}
	// impossible case
	return nil, &workflow.InternalServiceError{Message: "domainCache encounter case where domain exists but cannot be loaded"}
}

func (c *domainCache) triggerDomainChangePrepareCallbackLocked() {
	sw := c.metricsClient.StartTimer(metrics.DomainCacheScope, metrics.DomainCachePrepareCallbacksLatency)
	defer sw.Stop()

	for _, prepareCallback := range c.prepareCallbacks {
		prepareCallback()
	}
}

func (c *domainCache) triggerDomainChangeCallbackLocked(
	prevDomains []*DomainCacheEntry,
	nextDomains []*DomainCacheEntry,
) {

	sw := c.metricsClient.StartTimer(metrics.DomainCacheScope, metrics.DomainCacheCallbacksLatency)
	defer sw.Stop()

	for _, callback := range c.callbacks {
		callback(prevDomains, nextDomains)
	}
}

func (c *domainCache) buildEntryFromRecord(
	record *persistence.GetDomainResponse,
) *DomainCacheEntry {

	// this is a shallow copy, but since the record is generated by persistence
	// and only accessible here, it would be fine
	newEntry := newDomainCacheEntry(c.clusterMetadata)
	newEntry.info = record.Info
	newEntry.config = record.Config
	newEntry.replicationConfig = record.ReplicationConfig
	newEntry.configVersion = record.ConfigVersion
	newEntry.failoverVersion = record.FailoverVersion
	newEntry.isGlobalDomain = record.IsGlobalDomain
	newEntry.failoverNotificationVersion = record.FailoverNotificationVersion
	newEntry.notificationVersion = record.NotificationVersion
	newEntry.initialized = true
	return newEntry
}

func copyResetBinary(bins workflow.BadBinaries) workflow.BadBinaries {
	newbins := make(map[string]*workflow.BadBinaryInfo, len(bins.Binaries))
	for k, v := range bins.Binaries {
		newbins[k] = v
	}
	return workflow.BadBinaries{
		Binaries: newbins,
	}
}

func (entry *DomainCacheEntry) duplicate() *DomainCacheEntry {
	// this is a deep copy
	result := newDomainCacheEntry(entry.clusterMetadata)
	result.info = &persistence.DomainInfo{
		ID:          entry.info.ID,
		Name:        entry.info.Name,
		Status:      entry.info.Status,
		Description: entry.info.Description,
		OwnerEmail:  entry.info.OwnerEmail,
	}
	result.info.Data = map[string]string{}
	for k, v := range entry.info.Data {
		result.info.Data[k] = v
	}
	result.config = &persistence.DomainConfig{
		Retention:                entry.config.Retention,
		EmitMetric:               entry.config.EmitMetric,
		HistoryArchivalStatus:    entry.config.HistoryArchivalStatus,
		HistoryArchivalURI:       entry.config.HistoryArchivalURI,
		VisibilityArchivalStatus: entry.config.VisibilityArchivalStatus,
		VisibilityArchivalURI:    entry.config.VisibilityArchivalURI,
		BadBinaries:              copyResetBinary(entry.config.BadBinaries),
	}
	result.replicationConfig = &persistence.DomainReplicationConfig{
		ActiveClusterName: entry.replicationConfig.ActiveClusterName,
	}
	for _, clusterName := range entry.replicationConfig.Clusters {
		result.replicationConfig.Clusters = append(result.replicationConfig.Clusters, &*clusterName)
	}
	result.configVersion = entry.configVersion
	result.failoverVersion = entry.failoverVersion
	result.isGlobalDomain = entry.isGlobalDomain
	result.failoverNotificationVersion = entry.failoverNotificationVersion
	result.notificationVersion = entry.notificationVersion
	result.initialized = entry.initialized
	return result
}

// GetInfo return the domain info
func (entry *DomainCacheEntry) GetInfo() *persistence.DomainInfo {
	return entry.info
}

// GetConfig return the domain config
func (entry *DomainCacheEntry) GetConfig() *persistence.DomainConfig {
	return entry.config
}

// GetReplicationConfig return the domain replication config
func (entry *DomainCacheEntry) GetReplicationConfig() *persistence.DomainReplicationConfig {
	return entry.replicationConfig
}

// GetConfigVersion return the domain config version
func (entry *DomainCacheEntry) GetConfigVersion() int64 {
	return entry.configVersion
}

// GetFailoverVersion return the domain failover version
func (entry *DomainCacheEntry) GetFailoverVersion() int64 {
	return entry.failoverVersion
}

// IsGlobalDomain return whether the domain is a global domain
func (entry *DomainCacheEntry) IsGlobalDomain() bool {
	return entry.isGlobalDomain
}

// GetFailoverNotificationVersion return the global notification version of when failover happened
func (entry *DomainCacheEntry) GetFailoverNotificationVersion() int64 {
	return entry.failoverNotificationVersion
}

// GetNotificationVersion return the global notification version of when domain changed
func (entry *DomainCacheEntry) GetNotificationVersion() int64 {
	return entry.notificationVersion
}

// IsDomainActive return whether the domain is active, i.e. non global domain or global domain which active cluster is the current cluster
func (entry *DomainCacheEntry) IsDomainActive() bool {
	if !entry.isGlobalDomain {
		// domain is not a global domain, meaning domain is always "active" within each cluster
		return true
	}
	return entry.clusterMetadata.GetCurrentClusterName() == entry.replicationConfig.ActiveClusterName
}

// GetReplicationPolicy return the derived workflow replication policy
func (entry *DomainCacheEntry) GetReplicationPolicy() ReplicationPolicy {
	// frontend guarantee that the clusters always contains the active domain, so if the # of clusters is 1
	// then we do not need to send out any events for replication
	if entry.isGlobalDomain && len(entry.replicationConfig.Clusters) > 1 {
		return ReplicationPolicyMultiCluster
	}
	return ReplicationPolicyOneCluster
}

// GetDomainNotActiveErr return err if domain is not active, nil otherwise
func (entry *DomainCacheEntry) GetDomainNotActiveErr() error {
	if entry.IsDomainActive() {
		// domain is consider active
		return nil
	}
	return errors.NewDomainNotActiveError(
		entry.info.Name,
		entry.clusterMetadata.GetCurrentClusterName(),
		entry.replicationConfig.ActiveClusterName,
	)
}

// Len return length
func (t DomainCacheEntries) Len() int {
	return len(t)
}

// Swap implements sort.Interface.
func (t DomainCacheEntries) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

// Less implements sort.Interface
func (t DomainCacheEntries) Less(i, j int) bool {
	return t[i].notificationVersion < t[j].notificationVersion
}

// CreateDomainCacheEntry create a cache entry with domainName
func CreateDomainCacheEntry(
	domainName string,
) *DomainCacheEntry {

	return &DomainCacheEntry{info: &persistence.DomainInfo{Name: domainName}}
}

// SampleRetentionKey is key to specify sample retention
var SampleRetentionKey = "sample_retention_days"

// SampleRateKey is key to specify sample rate
var SampleRateKey = "sample_retention_rate"

// GetRetentionDays returns retention in days for given workflow
func (entry *DomainCacheEntry) GetRetentionDays(
	workflowID string,
) int32 {

	if entry.IsSampledForLongerRetention(workflowID) {
		if sampledRetentionValue, ok := entry.info.Data[SampleRetentionKey]; ok {
			sampledRetentionDays, err := strconv.Atoi(sampledRetentionValue)
			if err != nil || sampledRetentionDays < int(entry.config.Retention) {
				return entry.config.Retention
			}
			return int32(sampledRetentionDays)
		}
	}
	return entry.config.Retention
}

// IsSampledForLongerRetentionEnabled return whether sample for longer retention is enabled or not
func (entry *DomainCacheEntry) IsSampledForLongerRetentionEnabled(
	workflowID string,
) bool {

	_, ok := entry.info.Data[SampleRateKey]
	return ok
}

// IsSampledForLongerRetention return should given workflow been sampled or not
func (entry *DomainCacheEntry) IsSampledForLongerRetention(
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
