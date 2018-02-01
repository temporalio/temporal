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
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"

	"github.com/uber-common/bark"
)

const (
	domainCacheInitialSize     = 1024
	domainCacheMaxSize         = 16 * 1024
	domainCacheTTL             = time.Hour
	domainEntryRefreshInterval = 10 * time.Second
)

type (
	// DomainCache is used the cache domain information and configuration to avoid making too many calls to cassandra.
	// This cache is mainly used by frontend for resolving domain names to domain uuids which are used throughout the
	// system.  Each domain entry is kept in the cache for one hour but also has an expiry of 10 seconds.  This results
	// in updating the domain entry every 10 seconds but in the case of a cassandra failure we can still keep on serving
	// requests using the stale entry from cache upto an hour
	DomainCache interface {
		GetDomain(name string) (*domainCacheEntry, error)
		GetDomainByID(id string) (*domainCacheEntry, error)
		GetDomainID(name string) (string, error)
	}

	domainCache struct {
		cacheByName Cache
		cacheByID   Cache
		metadataMgr persistence.MetadataManager
		timeSource  common.TimeSource
		logger      bark.Logger
	}

	domainCacheEntry struct {
		Info              *persistence.DomainInfo
		Config            *persistence.DomainConfig
		ReplicationConfig *persistence.DomainReplicationConfig
		expiry            time.Time
		sync.RWMutex
	}
)

// NewDomainCache creates a new instance of cache for holding onto domain information to reduce the load on persistence
func NewDomainCache(metadataMgr persistence.MetadataManager, logger bark.Logger) DomainCache {
	opts := &Options{}
	opts.InitialCapacity = domainCacheInitialSize
	opts.TTL = domainCacheTTL

	return &domainCache{
		cacheByName: New(domainCacheMaxSize, opts),
		cacheByID:   New(domainCacheMaxSize, opts),
		metadataMgr: metadataMgr,
		timeSource:  common.NewRealTimeSource(),
		logger:      logger,
	}
}

func newDomainCacheEntry() *domainCacheEntry {
	return &domainCacheEntry{}
}

// GetDomain retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *domainCache) GetDomain(name string) (*domainCacheEntry, error) {
	return c.getDomain(name, "", name, c.cacheByName)
}

// GetDomainByID retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *domainCache) GetDomainByID(id string) (*domainCacheEntry, error) {
	return c.getDomain(id, id, "", c.cacheByID)
}

// GetDomainID retrieves domainID by using GetDomain
func (c *domainCache) GetDomainID(name string) (string, error) {
	entry, err := c.GetDomain(name)
	if err != nil {
		return "", err
	}
	return entry.Info.ID, nil
}

// GetDomain retrieves the information from the cache if it exists, otherwise retrieves the information from metadata
// store and writes it to the cache with an expiry before returning back
func (c *domainCache) getDomain(key, id, name string, cache Cache) (*domainCacheEntry, error) {
	now := c.timeSource.Now()
	var result *domainCacheEntry

	entry, cacheHit := cache.Get(key).(*domainCacheEntry)
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
		elem, _ := cache.PutIfNotExist(key, newDomainCacheEntry())
		entry = elem.(*domainCacheEntry)
	}

	// Now take a lock to update the entry
	entry.Lock()
	defer entry.Unlock()

	// Check again under the lock to make sure someone else did not update the entry
	if entry.isExpired(now) {
		response, err := c.metadataMgr.GetDomain(&persistence.GetDomainRequest{
			Name: name,
			ID:   id,
		})

		// Failed to get domain.  Return stale entry if we have one, otherwise just return error
		if err != nil {
			if !entry.expiry.IsZero() {
				return entry, nil
			}

			return nil, err
		}

		entry.Info = response.Info
		entry.Config = response.Config
		entry.ReplicationConfig = response.ReplicationConfig
		entry.expiry = now.Add(domainEntryRefreshInterval)
	}

	return entry.duplicate(), nil
}

func (entry *domainCacheEntry) duplicate() *domainCacheEntry {
	result := newDomainCacheEntry()
	result.Info = entry.Info
	result.Config = entry.Config
	result.ReplicationConfig = entry.ReplicationConfig
	return result
}

func (entry *domainCacheEntry) isExpired(now time.Time) bool {
	return entry.expiry.IsZero() || now.After(entry.expiry)
}
