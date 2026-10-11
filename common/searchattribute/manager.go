package searchattribute

import (
	"context"
	"maps"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
)

const (
	cacheRefreshTimeout               = 5 * time.Second
	cacheRefreshInterval              = 60 * time.Second
	cacheRefreshIfUnavailableInterval = 20 * time.Second
	cacheRefreshErrorInterval         = 1 * time.Second
)

type (
	managerImpl struct {
		logger                 log.Logger
		timeSource             clock.TimeSource
		clusterMetadataManager persistence.ClusterMetadataManager
		forceRefresh           dynamicconfig.BoolPropertyFn

		cacheUpdateMutex sync.Mutex
		cache            atomic.Value // of type cache
	}

	cache struct {
		// indexName -> NameTypeMap
		searchAttributes map[string]NameTypeMap
		dbVersion        int64
		expireOn         time.Time
		refreshErr       error
		retryAfter       time.Time
	}
)

var _ Manager = (*managerImpl)(nil)

func NewManager(
	timeSource clock.TimeSource,
	clusterMetadataManager persistence.ClusterMetadataManager,
	logger log.Logger,
	forceRefresh dynamicconfig.BoolPropertyFn,
) *managerImpl {
	var saCache atomic.Value
	saCache.Store(cache{
		searchAttributes: map[string]NameTypeMap{},
		dbVersion:        0,
		expireOn:         time.Time{},
	})

	return &managerImpl{
		logger:                 logger,
		timeSource:             timeSource,
		cache:                  saCache,
		clusterMetadataManager: clusterMetadataManager,
		forceRefresh:           forceRefresh,
	}
}

// GetSearchAttributes returns all search attributes (including system and build-in) for specified index.
// indexName can be an empty string for backward compatibility.
func (m *managerImpl) GetSearchAttributes(
	indexName string,
	forceRefreshCache bool,
) (NameTypeMap, error) {
	now := m.timeSource.Now()
	result := NewNameTypeMap(nil)
	saCache, err := m.refreshCache(forceRefreshCache, now)
	if err != nil {
		return result, err
	}
	if indexSearchAttributes, ok := saCache.searchAttributes[indexName]; ok {
		result.customSearchAttributes = maps.Clone(indexSearchAttributes.customSearchAttributes)
	}
	return result, nil
}

// needRefreshCache also returns the shared error when an expired snapshot is in cooldown.
func (m *managerImpl) needRefreshCache(saCache cache, forceRefreshCache bool, now time.Time) (bool, error) {
	if forceRefreshCache || m.forceRefresh() {
		return true, nil
	}
	// A failed forced refresh must not poison an otherwise usable snapshot.
	if !saCache.expireOn.Before(now) {
		return false, nil
	}
	if now.Before(saCache.retryAfter) {
		return false, saCache.refreshErr
	}
	return true, nil
}

func (m *managerImpl) refreshCache(forceRefreshCache bool, now time.Time) (cache, error) {
	//nolint:revive // cache value is always of type `cache`
	saCache := m.cache.Load().(cache)
	if refresh, err := m.needRefreshCache(saCache, forceRefreshCache, now); !refresh {
		return saCache, err
	}

	m.cacheUpdateMutex.Lock()
	defer m.cacheUpdateMutex.Unlock()
	//nolint:revive // cache value is always of type `cache`
	saCache = m.cache.Load().(cache)
	now = m.timeSource.Now()
	if refresh, err := m.needRefreshCache(saCache, forceRefreshCache, now); !refresh {
		return saCache, err
	}

	return m.refreshCacheLocked(saCache, now)
}

func (m *managerImpl) refreshCacheLocked(saCache cache, now time.Time) (cache, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cacheRefreshTimeout)
	defer cancel()
	if saCache.dbVersion == 0 {
		// if cache is cold, use the highest priority caller
		ctx = headers.SetCallerInfo(ctx, headers.SystemOperatorCallerInfo)
	} else {
		ctx = headers.SetCallerInfo(ctx, headers.SystemBackgroundHighCallerInfo)
	}

	clusterMetadata, err := m.clusterMetadataManager.GetCurrentClusterMetadata(ctx)
	saCache.refreshErr = nil
	saCache.retryAfter = time.Time{}
	if err != nil {
		switch err.(type) {
		case *serviceerror.NotFound:
			// NotFound means cluster metadata was never persisted and custom search attributes are not defined.
			// Ignore the error.
			saCache.expireOn = now.Add(cacheRefreshInterval)
			err = nil
		case *serviceerror.Unavailable:
			if saCache.dbVersion != 0 {
				// If persistence is Unavailable, but cache was loaded at least once, then ignore the error
				// and use existing cache for cacheRefreshIfUnavailableInterval.
				saCache.expireOn = now.Add(cacheRefreshIfUnavailableInterval)
				err = nil
			}
		}
		if err != nil {
			// Share the failed attempt without marking cold or expired metadata fresh.
			// Start the positive, jittered cooldown after I/O completes.
			saCache.refreshErr = err
			saCache.retryAfter = m.timeSource.Now().Add(cacheRefreshErrorInterval + time.Duration(rand.Int63n(int64(cacheRefreshErrorInterval))))
			m.logger.Error("failed to refresh search attributes cache", tag.Error(err))
		}
		m.cache.Store(saCache)
		return saCache, err
	}

	// clusterMetadata.Version <= saCache.dbVersion means DB is not changed.
	if clusterMetadata.Version <= saCache.dbVersion {
		saCache.expireOn = now.Add(cacheRefreshInterval)
		m.cache.Store(saCache)
		return saCache, nil
	}

	saCache = cache{
		searchAttributes: buildIndexNameTypeMap(clusterMetadata.GetIndexSearchAttributes()),
		expireOn:         now.Add(cacheRefreshInterval),
		dbVersion:        clusterMetadata.Version,
	}
	m.cache.Store(saCache)
	return saCache, nil
}

// SaveSearchAttributes saves search attributes to cluster metadata.
// indexName can be an empty string when Elasticsearch is not configured.
func (m *managerImpl) SaveSearchAttributes(
	ctx context.Context,
	indexName string,
	newCustomSearchAttributes map[string]enumspb.IndexedValueType,
) error {

	clusterMetadataResponse, err := m.clusterMetadataManager.GetCurrentClusterMetadata(ctx)
	if err != nil {
		return err
	}

	clusterMetadata := clusterMetadataResponse.ClusterMetadata
	if clusterMetadata.IndexSearchAttributes == nil {
		clusterMetadata.IndexSearchAttributes = map[string]*persistencespb.IndexSearchAttributes{indexName: nil}
	}
	clusterMetadata.IndexSearchAttributes[indexName] = &persistencespb.IndexSearchAttributes{CustomSearchAttributes: newCustomSearchAttributes}
	_, err = m.clusterMetadataManager.SaveClusterMetadata(ctx, &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: clusterMetadata,
		Version:         clusterMetadataResponse.Version,
	})
	// Flush local cache, even if there was an error, which is most likely version mismatch (=stale cache).
	// Keep network operations outside the lock, but order invalidation after any older refresh publication.
	m.cacheUpdateMutex.Lock()
	m.cache.Store(cache{})
	m.cacheUpdateMutex.Unlock()

	return err
}
