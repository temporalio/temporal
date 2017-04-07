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
	domainEntryRefreshInterval = int64(10 * time.Second)
)

type (
	// DomainCache is used the cache domain information and configuration to avoid making too many calls to cassandra.
	// This cache is mainly used by frontend for resolving domain names to domain uuids which are used throughout the
	// system.  Each domain entry is kept in the cache for one hour but also has an expiry of 10 seconds.  This results
	// in updating the domain entry every 10 seconds but in the case of a cassandra failure we can still keep on serving
	// requests using the stale entry from cache upto an hour
	DomainCache interface {
		GetDomain(name string) (*persistence.DomainInfo, *persistence.DomainConfig, error)
	}

	domainCache struct {
		Cache
		metadataMgr persistence.MetadataManager
		timeSource  common.TimeSource
		logger      bark.Logger
	}

	domainCacheEntry struct {
		info   *persistence.DomainInfo
		config *persistence.DomainConfig
		expiry int64
		sync.RWMutex
	}
)

// NewDomainCache creates a new instance of cache for holding onto domain information to reduce the load on persistence
func NewDomainCache(metadataMgr persistence.MetadataManager, logger bark.Logger) DomainCache {
	opts := &Options{}
	opts.InitialCapacity = domainCacheInitialSize
	opts.TTL = domainCacheTTL

	return &domainCache{
		Cache:       New(domainCacheMaxSize, opts),
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
func (c *domainCache) GetDomain(name string) (*persistence.DomainInfo, *persistence.DomainConfig, error) {
	now := c.timeSource.Now().UnixNano()
	refreshCache := false
	var info *persistence.DomainInfo
	var config *persistence.DomainConfig
	entry, cacheHit := c.Get(name).(*domainCacheEntry)
	if cacheHit {
		// Found the information in the cache, lets check if it needs to be refreshed before returning back
		entry.RLock()
		info = entry.info
		config = entry.config

		if entry.expiry == 0 || now >= entry.expiry {
			refreshCache = true
		}
		entry.RUnlock()
	}

	// Found a cache entry and no need to refresh.  Return immediately
	if cacheHit && !refreshCache {
		return info, config, nil
	}

	// Cache entry not found, Let's create an entry and add it to cache
	if !cacheHit {
		entry = c.PutIfNotExist(name, newDomainCacheEntry()).(*domainCacheEntry)
	}

	// Now take a lock to update the entry
	entry.Lock()
	defer entry.Unlock()

	// Check again under the lock to make sure someone else did not update the entry
	if entry.expiry == 0 || now >= entry.expiry {
		response, err := c.metadataMgr.GetDomain(&persistence.GetDomainRequest{
			Name: name,
		})

		// Failed to get domain.  Return stale entry if we have one, otherwise just return error
		if err != nil {
			if entry.expiry > 0 {
				return entry.info, entry.config, nil
			}

			return nil, nil, err
		}

		entry.info = response.Info
		entry.config = response.Config
		entry.expiry = now + domainEntryRefreshInterval
	}

	return entry.info, entry.config, nil
}
