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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/persistence"

	"github.com/uber-common/bark"
)

const (
	domainCacheInitialSize     = 10 * 1024
	domainCacheMaxSize         = 16 * 1024
	domainCacheTTL             = time.Hour
	domainCacheRefreshInterval = 10 * time.Second
	domainCacheRefreshPageSize = 100

	domainCacheLocked   int32 = 0
	domainCacheReleased int32 = 1

	domainCacheInitialized int32 = 0
	domainCacheStarted     int32 = 1
	domainCacheStopped     int32 = 2
)

type (
	// function to be called when the domain cache is changed
	// the callback function will be called within the domain cache entry lock
	// make sure the callback function will not call domain cache again
	// in case of deadlock
	callbackFn func(prevDomain *DomainCacheEntry, nextDomain *DomainCacheEntry)

	// DomainCache is used the cache domain information and configuration to avoid making too many calls to cassandra.
	// This cache is mainly used by frontend for resolving domain names to domain uuids which are used throughout the
	// system.  Each domain entry is kept in the cache for one hour but also has an expiry of 10 seconds.  This results
	// in updating the domain entry every 10 seconds but in the case of a cassandra failure we can still keep on serving
	// requests using the stale entry from cache upto an hour
	DomainCache interface {
		common.Daemon
		RegisterDomainChangeCallback(shard int, initialNotificationVersion int64, fn callbackFn)
		UnregisterDomainChangeCallback(shard int)
		GetDomain(name string) (*DomainCacheEntry, error)
		GetDomainByID(id string) (*DomainCacheEntry, error)
		GetDomainID(name string) (string, error)
		GetDomainNotificationVersion() int64
		GetAllDomain() map[string]*DomainCacheEntry
	}

	domainCache struct {
		status          int32
		shutdownChan    chan struct{}
		cacheNameToID   Cache
		cacheByID       Cache
		metadataMgr     persistence.MetadataManager
		clusterMetadata cluster.Metadata
		timeSource      common.TimeSource
		logger          bark.Logger

		sync.RWMutex
		domainNotificationVersion int64
		callbacks                 map[int]callbackFn
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
func NewDomainCache(metadataMgr persistence.MetadataManager, clusterMetadata cluster.Metadata, logger bark.Logger) DomainCache {
	opts := &Options{}
	opts.InitialCapacity = domainCacheInitialSize
	opts.TTL = domainCacheTTL

	return &domainCache{
		status:          domainCacheInitialized,
		shutdownChan:    make(chan struct{}),
		cacheNameToID:   New(domainCacheMaxSize, opts),
		cacheByID:       New(domainCacheMaxSize, opts),
		metadataMgr:     metadataMgr,
		clusterMetadata: clusterMetadata,
		timeSource:      common.NewRealTimeSource(),
		logger:          logger,
		callbacks:       make(map[int]callbackFn),
	}
}

func newDomainCacheEntry(clusterMetadata cluster.Metadata) *DomainCacheEntry {
	return &DomainCacheEntry{clusterMetadata: clusterMetadata}
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

func (c *domainCache) GetDomainNotificationVersion() int64 {
	c.RLock()
	defer c.RUnlock()
	return c.domainNotificationVersion
}

func (c *domainCache) GetAllDomain() map[string]*DomainCacheEntry {
	result := make(map[string]*DomainCacheEntry)
	ite := c.cacheByID.Iterator()
	defer ite.Close()

	for ite.HasNext() {
		entry := ite.Next()
		id := entry.Key().(string)
		domainCacheEntry := entry.Value().(*DomainCacheEntry).duplicate()
		result[id] = domainCacheEntry
	}
	return result
}

// RegisterDomainChangeCallback set a domain change callback
// WARN: the callback function will be triggered by domain cache when holding the domain cache lock,
// make sure the callback function will not call domain cache again in case of dead lock
func (c *domainCache) RegisterDomainChangeCallback(shard int, initialNotificationVersion int64, fn callbackFn) {
	c.Lock()
	defer c.Unlock()

	c.callbacks[shard] = fn

	if c.domainNotificationVersion > initialNotificationVersion {
		domains := DomainCacheEntries{}
		for _, domain := range c.GetAllDomain() {
			domains = append(domains, domain)
		}
		// we mush notify the change in a ordered fashion
		// since history shard have to update the shard info
		// with domain change version.
		sort.Sort(domains)
		for _, domain := range domains {
			if domain.notificationVersion >= initialNotificationVersion {
				fn(nil, domain)
			}
		}
	}
}

// UnregisterDomainChangeCallback delete a domain failover callback
func (c *domainCache) UnregisterDomainChangeCallback(shard int) {
	c.Lock()
	defer c.Unlock()

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
	timer := time.NewTimer(domainCacheRefreshInterval)
	defer timer.Stop()
	for {
		select {
		case <-c.shutdownChan:
			return
		case <-timer.C:
			timer.Reset(domainCacheRefreshInterval)
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
	// this can guarentee that domains in the cache is not
	// more update to date then the metadata record
	metadata, err := c.metadataMgr.GetMetadata()
	if err != nil {
		return err
	}
	domainNotificationVersion := metadata.NotificationVersion
	c.Lock()
	c.domainNotificationVersion = domainNotificationVersion
	c.Unlock()

	var token []byte
	request := &persistence.ListDomainRequest{PageSize: domainCacheRefreshPageSize}
	var domains DomainCacheEntries
	continuePage := true

	for continuePage {
		request.NextPageToken = token
		response, err := c.metadataMgr.ListDomain(request)
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
	c.RLock()
	domainNotificationVersion = c.domainNotificationVersion
	c.RUnlock()

UpdateLoop:
	for _, domain := range domains {
		if domain.notificationVersion >= domainNotificationVersion {
			// this guarantee that domain change events before the
			// domainNotificationVersion is loaded into the cache.

			// the domain change events after the domainNotificationVersion
			// will be loaded into cache in the next refresh
			break UpdateLoop
		}
		c.updateIDToDomainCache(domain.info.ID, domain)
		c.updateNameToIDCache(domain.info.Name, domain.info.ID)
	}

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
			// the resule is from V2 table
			// this should not happen since backgroud thread is refreshing.
			// if this actually happen, just discard the result
			// since we need to guarantee that domainNotificationVersion > all notification versions
			// inside the cache
			return nil, &workflow.EntityNotExistsError{}
		}
	}
	return resp, err
}

func (c *domainCache) updateNameToIDCache(name string, id string) {
	c.cacheNameToID.Put(name, id)
}

func (c *domainCache) updateIDToDomainCache(id string, record *DomainCacheEntry) (*DomainCacheEntry, error) {
	elem, err := c.cacheByID.PutIfNotExist(id, newDomainCacheEntry(c.clusterMetadata))
	if err != nil {
		return nil, err
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
	entry.expiry = c.timeSource.Now().Add(domainCacheRefreshInterval)

	nextDomain := entry.duplicate()
	if triggerCallback {
		c.triggerDomainChangeCallback(prevDomain, nextDomain)
	}

	return nextDomain, nil
}

// getDomain retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *domainCache) getDomain(name string) (*DomainCacheEntry, error) {
	id, cacheHit := c.cacheNameToID.Get(name).(string)
	if cacheHit {
		return c.getDomainByID(id)
	}

	record, err := c.loadDomain(name, "")
	if err != nil {
		return nil, err
	}
	id = record.Info.ID
	newEntry, err := c.updateIDToDomainCache(id, c.buildEntryFromRecord(record))
	if err != nil {
		return nil, err
	}
	c.updateNameToIDCache(name, id)
	return newEntry, nil
}

// getDomainByID retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *domainCache) getDomainByID(id string) (*DomainCacheEntry, error) {
	now := c.timeSource.Now()
	var result *DomainCacheEntry

	entry, cacheHit := c.cacheByID.Get(id).(*DomainCacheEntry)
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

	newEntry, err := c.updateIDToDomainCache(id, c.buildEntryFromRecord(record))
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
	c.updateNameToIDCache(newEntry.GetInfo().Name, id)
	return newEntry, nil
}

func (c *domainCache) triggerDomainChangeCallback(prevDomain *DomainCacheEntry, nextDomain *DomainCacheEntry) {
	c.RLock()
	defer c.RUnlock()
	for _, callback := range c.callbacks {
		callback(prevDomain, nextDomain)
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
	result.info = &*entry.info
	result.config = &*entry.config
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

// ShouldReplicateEvent return whether the workflows within this domain should be replicated
func (entry *DomainCacheEntry) ShouldReplicateEvent() bool {
	// frontend guarantee that the clusters always contains the active domain, so if the # of clusters is 1
	// then we do not need to send out any events for replication
	return entry.clusterMetadata.GetCurrentClusterName() == entry.replicationConfig.ActiveClusterName &&
		entry.isGlobalDomain && len(entry.replicationConfig.Clusters) > 1
}

// GetDomainNotActiveErr return err if domain is not active, nil otherwise
func (entry *DomainCacheEntry) GetDomainNotActiveErr() error {
	if entry.IsDomainActive() {
		// domain is consider active
		return nil
	}
	return errors.NewDomainNotActiveError(entry.info.Name, entry.clusterMetadata.GetCurrentClusterName(), entry.replicationConfig.ActiveClusterName)
}
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
