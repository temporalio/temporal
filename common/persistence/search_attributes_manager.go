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

package persistence

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/searchattribute"
)

const (
	searchAttributeCacheRefreshInterval = 60 * time.Second
)

type (
	// TODO (alex): move this to searchattribute package (after breaking package cycle)
	SearchAttributesManager struct {
		timeSource             clock.TimeSource
		clusterMetadataManager ClusterMetadataManager

		cacheUpdateMutex sync.Mutex
		cache            atomic.Value
	}

	searchAttributesCache struct {
		// indexName -> NameTypeMap
		searchAttributes map[string]searchattribute.NameTypeMap
		dbVersion        int64
		lastRefresh      time.Time
	}
)

var _ searchattribute.Manager = (*SearchAttributesManager)(nil)

var (
	ErrEmptyIndexName = errors.New("indexName is empty")
)

func NewSearchAttributesManager(
	timeSource clock.TimeSource,
	clusterMetadataManager ClusterMetadataManager,
) *SearchAttributesManager {

	var saCache atomic.Value
	saCache.Store(searchAttributesCache{
		searchAttributes: map[string]searchattribute.NameTypeMap{},
		dbVersion:        0,
		lastRefresh:      time.Time{},
	})

	return &SearchAttributesManager{
		timeSource:             timeSource,
		cache:                  saCache,
		clusterMetadataManager: clusterMetadataManager,
	}
}

// GetSearchAttributes returns all search attributes (including system and build-in) for specified index.
func (m *SearchAttributesManager) GetSearchAttributes(
	indexName string,
	forceRefreshCache bool,
) (searchattribute.NameTypeMap, error) {

	// Empty indexName means advanced visibility is not enabled.
	if indexName == "" {
		return searchattribute.NameTypeMap{}, nil
	}

	now := m.timeSource.Now()
	saCache := m.cache.Load().(searchAttributesCache)

	if m.needRefreshCache(saCache, forceRefreshCache, now) {
		m.cacheUpdateMutex.Lock()
		saCache = m.cache.Load().(searchAttributesCache)
		if m.needRefreshCache(saCache, forceRefreshCache, now) {
			var err error
			saCache, err = m.refreshCache(saCache, now)
			if err != nil {
				m.cacheUpdateMutex.Unlock()
				return searchattribute.NameTypeMap{}, err
			}
		}
		m.cacheUpdateMutex.Unlock()
	}

	indexSearchAttributes, ok := saCache.searchAttributes[indexName]
	if !ok {
		return searchattribute.NameTypeMap{}, nil
	}

	return indexSearchAttributes, nil
}

func (m *SearchAttributesManager) needRefreshCache(saCache searchAttributesCache, forceRefreshCache bool, now time.Time) bool {
	return forceRefreshCache || saCache.lastRefresh.Add(searchAttributeCacheRefreshInterval).Before(now)
}

func (m *SearchAttributesManager) refreshCache(saCache searchAttributesCache, now time.Time) (searchAttributesCache, error) {
	clusterMetadata, err := m.clusterMetadataManager.GetClusterMetadata()
	if err != nil {
		if _, isNotFoundErr := err.(*serviceerror.NotFound); !isNotFoundErr {
			return saCache, err
		}
	}

	// clusterMetadata == nil means cluster metadata was never persisted and search attributes are not defined.
	// clusterMetadata.Version <= saCache.dbVersion means DB is not changed.
	if clusterMetadata == nil || clusterMetadata.Version <= saCache.dbVersion {
		saCache.lastRefresh = now
		m.cache.Store(saCache)
		return saCache, nil
	}

	saCache = searchAttributesCache{
		searchAttributes: searchattribute.BuildIndexNameTypeMap(clusterMetadata.GetIndexSearchAttributes()),
		lastRefresh:      now,
		dbVersion:        clusterMetadata.Version,
	}
	m.cache.Store(saCache)
	return saCache, nil
}

// SaveSearchAttributes saves search attributes to cluster metadata.
func (m *SearchAttributesManager) SaveSearchAttributes(
	indexName string,
	newCustomSearchAttributes map[string]enumspb.IndexedValueType,
) error {

	// Empty indexName means advanced visibility is not enabled.
	if indexName == "" {
		return ErrEmptyIndexName
	}

	clusterMetadataResponse, err := m.clusterMetadataManager.GetClusterMetadata()
	if err != nil {
		return err
	}

	clusterMetadata := clusterMetadataResponse.ClusterMetadata
	if clusterMetadata.IndexSearchAttributes == nil {
		clusterMetadata.IndexSearchAttributes = map[string]*persistencespb.IndexSearchAttributes{indexName: nil}
	}
	clusterMetadata.IndexSearchAttributes[indexName] = &persistencespb.IndexSearchAttributes{CustomSearchAttributes: newCustomSearchAttributes}
	_, err = m.clusterMetadataManager.SaveClusterMetadata(&SaveClusterMetadataRequest{
		ClusterMetadata: clusterMetadata,
		Version:         clusterMetadataResponse.Version,
	})
	// Flush local cache, even if there was an error, which is most likely version mismatch (=stale cache).
	m.cache.Store(searchAttributesCache{})

	return err
}
