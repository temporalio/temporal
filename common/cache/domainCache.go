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

package cache

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

const (
	domainCacheInitialSize = 10 * 1024
	domainCacheMaxSize     = 64 * 1024
	domainCacheTTL         = 0 // 0 means infinity
	domainCacheEntryTTL    = 300 * time.Second
	// DomainCacheRefreshInterval domain cache refresh interval
	DomainCacheRefreshInterval = 10 * time.Second
	domainCacheRefreshPageSize = 100

	domainCacheLocked   int32 = 0
	domainCacheReleased int32 = 1

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
		timeSource      common.TimeSource
		metricsClient   metrics.Client
		logger          bark.Logger

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
		expiry                      time.Time
	}
)

// NewDomainCache creates a new instance of cache for holding onto domain information to reduce the load on persistence
func NewDomainCache(metadataMgr persistence.MetadataManager, clusterMetadata cluster.Metadata, metricsClient metrics.Client, logger bark.Logger) DomainCache {
	cache := &domainCache{
		status:           domainCacheInitialized,
		shutdownChan:     make(chan struct{}),
		cacheNameToID:    &atomic.Value{},
		cacheByID:        &atomic.Value{},
		metadataMgr:      metadataMgr,
		clusterMetadata:  clusterMetadata,
		timeSource:       common.NewRealTimeSource(),
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

func newDomainCacheEntry(clusterMetadata cluster.Metadata) *DomainCacheEntry {
	return &DomainCacheEntry{clusterMetadata: clusterMetadata}
}

// NewDomainCacheEntryWithInfo returns an entry with domainInfo
func NewDomainCacheEntryWithInfo(info *persistence.DomainInfo) *DomainCacheEntry {
	return &DomainCacheEntry{
		info: info,
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
	c.refreshDomains()
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
func (c *domainCache) RegisterDomainChangeCallback(shard int, initialNotificationVersion int64,
	prepareCallback PrepareCallbackFn, callback CallbackFn) {
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
func (c *domainCache) UnregisterDomainChangeCallback(shard int) {
	c.callbackLock.Lock()
	defer c.callbackLock.Unlock()

	delete(c.prepareCallbacks, shard)
	delete(c.callbacks, shard)
}

// GetDomain retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *domainCache) GetDomain(name string) (*DomainCacheEntry, error) {
	if name == "" {
		return nil, &workflow.BadRequestError{Message: "Domain is empty."}
	}
	return c.getDomain(name)
}

// GetDomainByID retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *domainCache) GetDomainByID(id string) (*DomainCacheEntry, error) {
	if id == "" {
		return nil, &workflow.BadRequestError{Message: "DomainID is empty."}
	}
	return c.getDomainByID(id)
}

// GetDomainID retrieves domainID by using GetDomain
func (c *domainCache) GetDomainID(name string) (string, error) {
	entry, err := c.GetDomain(name)
	if err != nil {
		return "", err
	}
	return entry.info.ID, nil
}

func (c *domainCache) refreshLoop() {
	timer := time.NewTimer(DomainCacheRefreshInterval)
	defer timer.Stop()
	for {
		select {
		case <-c.shutdownChan:
			return
		case <-timer.C:
			timer.Reset(DomainCacheRefreshInterval)
			err := c.refreshDomains()
			if err != nil {
				c.logger.Errorf("Error refreshing domain cache: %v", err)
			}
		}
	}
}

// this function only refresh the domains in the v2 table
// the domains in the v1 table will be refreshed if cache is stale
func (c *domainCache) refreshDomains() error {
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

	// make a copy of the existing domain cache, so we can calaulate diff and do compare and swap
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

	c.callbackLock.Lock()
	defer c.callbackLock.Unlock()
	c.triggerDomainChangePrepareCallbackLocked()
	c.cacheByID.Store(newCacheByID)
	c.cacheNameToID.Store(newCacheNameToID)
	c.triggerDomainChangeCallbackLocked(prevEntries, nextEntries)
	return nil
}

func (c *domainCache) loadDomain(name string, id string) (*persistence.GetDomainResponse, error) {
	resp, err := c.metadataMgr.GetDomain(&persistence.GetDomainRequest{Name: name, ID: id})
	if err == nil {
		if resp.TableVersion == persistence.DomainTableVersionV1 {
			// if loaded from V1 table
			// this means the FailoverNotificationVersion will be 0
			// and NotificationVersion has complete different meaning
			resp.FailoverNotificationVersion = 0
			resp.NotificationVersion = 0
		} else {
			// the result is from V2 table
			// this should not happen since background thread is refreshing.
			// if this actually happen, just discard the result
			// since we need to guarantee that domainNotificationVersion > all notification versions
			// inside the cache
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Domain: %v", name),
			}
		}
	}
	return resp, err
}

func (c *domainCache) updateNameToIDCache(cacheNameToID Cache, name string, id string) {
	cacheNameToID.Put(name, id)
}

func (c *domainCache) updateIDToDomainCache(cacheByID Cache, id string, record *DomainCacheEntry) (*DomainCacheEntry, *DomainCacheEntry, error) {
	elem, err := cacheByID.PutIfNotExist(id, newDomainCacheEntry(c.clusterMetadata))
	if err != nil {
		return nil, nil, err
	}
	entry := elem.(*DomainCacheEntry)

	entry.Lock()
	defer entry.Unlock()

	var prevDomain *DomainCacheEntry
	triggerCallback := c.clusterMetadata.IsGlobalDomainEnabled() &&
		// expiry will be non zero when the entry is initialized / valid
		!entry.expiry.IsZero() &&
		record.notificationVersion > entry.notificationVersion
	// expiry will be non zero when the entry is initialized / valid
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
	entry.expiry = c.timeSource.Now().Add(domainCacheEntryTTL)

	nextDomain := entry.duplicate()

	return prevDomain, nextDomain, nil
}

// getDomain retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *domainCache) getDomain(name string) (*DomainCacheEntry, error) {
	id, cacheHit := c.cacheNameToID.Load().(Cache).Get(name).(string)
	if cacheHit {
		return c.getDomainByID(id)
	}

	record, err := c.loadDomain(name, "")
	if err != nil {
		return nil, err
	}
	id = record.Info.ID
	_, newEntry, err := c.updateIDToDomainCache(c.cacheByID.Load().(Cache), id, c.buildEntryFromRecord(record))
	if err != nil {
		return nil, err
	}
	c.updateNameToIDCache(c.cacheNameToID.Load().(Cache), name, id)
	return newEntry, nil
}

// getDomainByID retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *domainCache) getDomainByID(id string) (*DomainCacheEntry, error) {
	now := c.timeSource.Now()
	var result *DomainCacheEntry

	entry, cacheHit := c.cacheByID.Load().(Cache).Get(id).(*DomainCacheEntry)
	if cacheHit {
		// Found the information in the cache, lets check if it needs to be refreshed before returning back
		entry.RLock()
		if !entry.isExpired(now) {
			result = entry.duplicate()
			entry.RUnlock()
			return result, nil
		}
		// cache expired, need to refresh
		entry.RUnlock()
	}

	record, err := c.loadDomain("", id)
	if err != nil {
		// err updating, use the existing record if record is valid
		// i.e. expiry is set
		if cacheHit {
			entry.RLock()
			defer entry.RUnlock()
			if !entry.expiry.IsZero() {
				return entry.duplicate(), nil
			}
		}
		return nil, err
	}

	_, newEntry, err := c.updateIDToDomainCache(c.cacheByID.Load().(Cache), id, c.buildEntryFromRecord(record))
	if err != nil {
		// err updating, use the existing record if record is valid
		// i.e. expiry is set
		if cacheHit {
			entry.RLock()
			defer entry.RUnlock()
			if !entry.expiry.IsZero() {
				return entry.duplicate(), nil
			}
		}
		return nil, err
	}
	c.updateNameToIDCache(c.cacheNameToID.Load().(Cache), newEntry.GetInfo().Name, id)
	return newEntry, nil
}

func (c *domainCache) triggerDomainChangePrepareCallbackLocked() {
	sw := c.metricsClient.StartTimer(metrics.DomainCacheScope, metrics.DomainCachePrepareCallbacksLatency)
	defer sw.Stop()

	for _, prepareCallback := range c.prepareCallbacks {
		prepareCallback()
	}
}

func (c *domainCache) triggerDomainChangeCallbackLocked(prevDomains []*DomainCacheEntry, nextDomains []*DomainCacheEntry) {
	sw := c.metricsClient.StartTimer(metrics.DomainCacheScope, metrics.DomainCacheCallbacksLatency)
	defer sw.Stop()

	for _, callback := range c.callbacks {
		callback(prevDomains, nextDomains)
	}
}

func (c *domainCache) buildEntryFromRecord(record *persistence.GetDomainResponse) *DomainCacheEntry {
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
	return newEntry
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
		Retention:  entry.config.Retention,
		EmitMetric: entry.config.EmitMetric,
	}
	result.replicationConfig = &persistence.DomainReplicationConfig{
		ActiveClusterName: entry.replicationConfig.ActiveClusterName,
	}
	for _, cluster := range entry.replicationConfig.Clusters {
		result.replicationConfig.Clusters = append(result.replicationConfig.Clusters, &*cluster)
	}
	result.configVersion = entry.configVersion
	result.failoverVersion = entry.failoverVersion
	result.isGlobalDomain = entry.isGlobalDomain
	result.failoverNotificationVersion = entry.failoverNotificationVersion
	result.notificationVersion = entry.notificationVersion
	result.expiry = entry.expiry
	return result
}

func (entry *DomainCacheEntry) isExpired(now time.Time) bool {
	return entry.expiry.IsZero() || now.After(entry.expiry)
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

// CanReplicateEvent return whether the workflows within this domain should be replicated
func (entry *DomainCacheEntry) CanReplicateEvent() bool {
	// frontend guarantee that the clusters always contains the active domain, so if the # of clusters is 1
	// then we do not need to send out any events for replication
	return entry.isGlobalDomain && len(entry.replicationConfig.Clusters) > 1
}

// GetDomainNotActiveErr return err if domain is not active, nil otherwise
func (entry *DomainCacheEntry) GetDomainNotActiveErr() error {
	if entry.IsDomainActive() {
		// domain is consider active
		return nil
	}
	return errors.NewDomainNotActiveError(entry.info.Name, entry.clusterMetadata.GetCurrentClusterName(), entry.replicationConfig.ActiveClusterName)
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
func CreateDomainCacheEntry(domainName string) *DomainCacheEntry {
	return &DomainCacheEntry{info: &persistence.DomainInfo{Name: domainName}}
}

// SampleRetentionKey is key to specify sample retention
var SampleRetentionKey = "sample_retention_days"

// SampleRateKey is key to specify sample rate
var SampleRateKey = "sample_retention_rate"

// GetRetentionDays returns retention in days for given workflow
func (entry *DomainCacheEntry) GetRetentionDays(workflowID string) int32 {
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
func (entry *DomainCacheEntry) IsSampledForLongerRetentionEnabled(workflowID string) bool {
	_, ok := entry.info.Data[SampleRateKey]
	return ok
}

// IsSampledForLongerRetention return should given workflow been sampled or not
func (entry *DomainCacheEntry) IsSampledForLongerRetention(workflowID string) bool {
	if sampledRateValue, ok := entry.info.Data[SampleRateKey]; ok {
		sampledRate, err := strconv.ParseFloat(sampledRateValue, 64)
		if err != nil {
			return false
		}

		h := fnv.New32a()
		h.Write([]byte(workflowID))
		hash := h.Sum32()

		r := float64(hash%1000) / float64(1000) // use 1000 so we support one decimal rate like 1.5%.
		if r < sampledRate {                    // sampled
			return true
		}
	}
	return false
}
