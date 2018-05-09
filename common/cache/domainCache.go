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
	domainCacheInitialSize     = 1024
	domainCacheMaxSize         = 16 * 1024
	domainCacheTTL             = time.Hour
	domainEntryRefreshInterval = 10 * time.Second

	domainCacheLocked   int32 = 0
	domainCacheReleased int32 = 1
)

type (
	callbackFn func(prevDomain *DomainCacheEntry, nextDomain *DomainCacheEntry)

	// DomainCache is used the cache domain information and configuration to avoid making too many calls to cassandra.
	// This cache is mainly used by frontend for resolving domain names to domain uuids which are used throughout the
	// system.  Each domain entry is kept in the cache for one hour but also has an expiry of 10 seconds.  This results
	// in updating the domain entry every 10 seconds but in the case of a cassandra failure we can still keep on serving
	// requests using the stale entry from cache upto an hour
	DomainCache interface {
		RegisterDomainChangeCallback(shard int, fn callbackFn)
		UnregisterDomainChangeCallback(shard int)
		GetDomain(name string) (*DomainCacheEntry, error)
		GetDomainByID(id string) (*DomainCacheEntry, error)
		GetDomainID(name string) (string, error)
	}

	domainCache struct {
		cacheByName     Cache
		cacheByID       Cache
		metadataMgr     persistence.MetadataManager
		clusterMetadata cluster.Metadata
		timeSource      common.TimeSource
		logger          bark.Logger

		sync.RWMutex
		callbacks map[int]callbackFn
	}

	// DomainCacheEntry contains the info and config for a domain
	DomainCacheEntry struct {
		clusterMetadata cluster.Metadata

		sync.RWMutex
		info              *persistence.DomainInfo
		config            *persistence.DomainConfig
		replicationConfig *persistence.DomainReplicationConfig
		configVersion     int64
		failoverVersion   int64
		isGlobalDomain    bool
		expiry            time.Time
	}
)

// NewDomainCache creates a new instance of cache for holding onto domain information to reduce the load on persistence
func NewDomainCache(metadataMgr persistence.MetadataManager, clusterMetadata cluster.Metadata, logger bark.Logger) DomainCache {
	opts := &Options{}
	opts.InitialCapacity = domainCacheInitialSize
	opts.TTL = domainCacheTTL

	return &domainCache{
		cacheByName:     New(domainCacheMaxSize, opts),
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

// RegisterDomainChangeCallback set a domain failover callback, which will be when active domain for a domain changes
func (c *domainCache) RegisterDomainChangeCallback(shard int, fn callbackFn) {
	c.Lock()
	defer c.Unlock()

	c.callbacks[shard] = fn
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
	return c.getDomain(name, "", name, c.cacheByName)
}

// GetDomainByID retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *domainCache) GetDomainByID(id string) (*DomainCacheEntry, error) {
	if id == "" {
		return nil, &workflow.BadRequestError{Message: "DomainID is empty."}
	}
	return c.getDomain(id, id, "", c.cacheByID)
}

// GetDomainID retrieves domainID by using GetDomain
func (c *domainCache) GetDomainID(name string) (string, error) {
	entry, err := c.GetDomain(name)
	if err != nil {
		return "", err
	}
	return entry.info.ID, nil
}

// GetDomain retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *domainCache) getDomain(key, id, name string, cache Cache) (*DomainCacheEntry, error) {
	now := c.timeSource.Now()
	var result *DomainCacheEntry

	entry, cacheHit := cache.Get(key).(*DomainCacheEntry)
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

	// Cache entry not found, Let's create an entry and add it to cache
	if !cacheHit {
		elem, _ := cache.PutIfNotExist(key, newDomainCacheEntry(c.clusterMetadata))
		entry = elem.(*DomainCacheEntry)
	}

	// Now take a lock to update the entry
	entry.Lock()
	lockReleased := domainCacheLocked
	release := func() {
		if atomic.CompareAndSwapInt32(&lockReleased, domainCacheLocked, domainCacheReleased) {
			entry.Unlock()
		}
	}
	defer release()

	// Check again under the lock to make sure someone else did not update the entry
	if !entry.isExpired(now) {
		return entry.duplicate(), nil
	}

	response, err := c.metadataMgr.GetDomain(&persistence.GetDomainRequest{Name: name, ID: id})
	// Failed to get domain.  Return stale entry if we have one, otherwise just return error
	if err != nil {
		if !entry.expiry.IsZero() {
			return entry.duplicate(), nil
		}

		return nil, err
	}

	var prevDomain *DomainCacheEntry
	// expiry will be non zero when the entry is initialized / valid
	if c.clusterMetadata.IsGlobalDomainEnabled() && !entry.expiry.IsZero() {
		prevDomain = entry.duplicate()
	}

	entry.info = response.Info
	entry.config = response.Config
	entry.replicationConfig = response.ReplicationConfig
	entry.configVersion = response.ConfigVersion
	entry.failoverVersion = response.FailoverVersion
	entry.isGlobalDomain = response.IsGlobalDomain
	entry.expiry = now.Add(domainEntryRefreshInterval)
	nextDomain := entry.duplicate()

	release()
	if prevDomain != nil {
		c.triggerDomainChangeCallback(prevDomain, nextDomain)
	}

	return nextDomain, nil
}

func (c *domainCache) triggerDomainChangeCallback(prevDomain *DomainCacheEntry, nextDomain *DomainCacheEntry) {

	if prevDomain.configVersion < nextDomain.configVersion {
		return
	}

	c.RLock()
	defer c.RUnlock()
	for _, callback := range c.callbacks {
		callback(prevDomain, nextDomain)
	}
}

func (entry *DomainCacheEntry) duplicate() *DomainCacheEntry {
	result := newDomainCacheEntry(entry.clusterMetadata)
	result.info = entry.info
	result.config = entry.config
	result.replicationConfig = entry.replicationConfig
	result.configVersion = entry.configVersion
	result.failoverVersion = entry.failoverVersion
	result.isGlobalDomain = entry.isGlobalDomain
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
