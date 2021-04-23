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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination metadata_mock.go

package cluster

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/searchattribute"
)

const (
	searchAttributeCacheRefreshInterval = 60 * time.Second
)

type (
	// Metadata provides information about clusters
	Metadata interface {
		searchattribute.Provider
		searchattribute.Saver

		// IsGlobalNamespaceEnabled whether the global namespace is enabled,
		// this attr should be discarded when cross DC is made public
		IsGlobalNamespaceEnabled() bool
		// IsMasterCluster whether current cluster is master cluster
		IsMasterCluster() bool
		// GetNextFailoverVersion return the next failover version for namespace failover
		GetNextFailoverVersion(string, int64) int64
		// IsVersionFromSameCluster return true if 2 version are used for the same cluster
		IsVersionFromSameCluster(version1 int64, version2 int64) bool
		// GetMasterClusterName return the master cluster name
		GetMasterClusterName() string
		// GetCurrentClusterName return the current cluster name
		GetCurrentClusterName() string
		// GetAllClusterInfo return the all cluster name -> corresponding info
		GetAllClusterInfo() map[string]config.ClusterInformation
		// ClusterNameForFailoverVersion return the corresponding cluster name for a given failover version
		ClusterNameForFailoverVersion(failoverVersion int64) string
	}

	searchAttributesCache struct {
		searchAttributes map[string]*persistencespb.IndexSearchAttributes
		dbVersion        int64
		lastRefresh      time.Time
	}

	metadataImpl struct {
		logger     log.Logger
		timeSource clock.TimeSource
		// EnableGlobalNamespace whether the global namespace is enabled,
		enableGlobalNamespace bool
		// failoverVersionIncrement is the increment of each cluster's version when failover happen
		failoverVersionIncrement int64
		// masterClusterName is the name of the master cluster, only the master cluster can register / update namespace
		// all clusters can do namespace failover
		masterClusterName string
		// currentClusterName is the name of the current cluster
		currentClusterName string
		// clusterInfo contains all cluster name -> corresponding information
		clusterInfo map[string]config.ClusterInformation
		// versionToClusterName contains all initial version -> corresponding cluster name
		versionToClusterName map[int64]string

		searchAttributesCacheUpdateMutex sync.Mutex
		searchAttributesCache            atomic.Value

		clusterMetadataManager persistence.ClusterMetadataManager
	}
)

// NewMetadata create a new instance of Metadata
func NewMetadata(
	logger log.Logger,
	timeSource clock.TimeSource,
	clusterMetadataManager persistence.ClusterMetadataManager,
	enableGlobalNamespace bool,
	failoverVersionIncrement int64,
	masterClusterName string,
	currentClusterName string,
	clusterInfo map[string]config.ClusterInformation,
) Metadata {

	if len(clusterInfo) == 0 {
		panic("Empty cluster information")
	} else if len(masterClusterName) == 0 {
		panic("Master cluster name is empty")
	} else if len(currentClusterName) == 0 {
		panic("Current cluster name is empty")
	} else if failoverVersionIncrement == 0 {
		panic("Version increment is 0")
	}

	versionToClusterName := make(map[int64]string)
	for clusterName, info := range clusterInfo {
		if failoverVersionIncrement <= info.InitialFailoverVersion || info.InitialFailoverVersion <= 0 {
			panic(fmt.Sprintf(
				"Version increment %v is smaller than initial version: %v.",
				failoverVersionIncrement,
				info.InitialFailoverVersion,
			))
		}
		if len(clusterName) == 0 {
			panic("Cluster name in all cluster names is empty")
		}
		versionToClusterName[info.InitialFailoverVersion] = clusterName

		if info.Enabled && info.RPCAddress == "" {
			panic(fmt.Sprintf("Cluster %v: RPCAddress is empty", clusterName))
		}
	}

	if _, ok := clusterInfo[currentClusterName]; !ok {
		panic("Current cluster is not specified in cluster info")
	}
	if _, ok := clusterInfo[masterClusterName]; !ok {
		panic("Master cluster is not specified in cluster info")
	}
	if len(versionToClusterName) != len(clusterInfo) {
		panic("Cluster info initial versions have duplicates")
	}

	var saCache atomic.Value
	saCache.Store(searchAttributesCache{})

	return &metadataImpl{
		logger:                   logger,
		enableGlobalNamespace:    enableGlobalNamespace,
		failoverVersionIncrement: failoverVersionIncrement,
		masterClusterName:        masterClusterName,
		currentClusterName:       currentClusterName,
		clusterInfo:              clusterInfo,
		versionToClusterName:     versionToClusterName,
		timeSource:               timeSource,
		searchAttributesCache:    saCache,
		clusterMetadataManager:   clusterMetadataManager,
	}
}

// IsGlobalNamespaceEnabled whether the global namespace is enabled,
// this attr should be discarded when cross DC is made public
func (m *metadataImpl) IsGlobalNamespaceEnabled() bool {
	return m.enableGlobalNamespace
}

// GetNextFailoverVersion return the next failover version based on input
func (m *metadataImpl) GetNextFailoverVersion(cluster string, currentFailoverVersion int64) int64 {
	info, ok := m.clusterInfo[cluster]
	if !ok {
		panic(fmt.Sprintf(
			"Unknown cluster name: %v with given cluster initial failover version map: %v.",
			cluster,
			m.clusterInfo,
		))
	}
	failoverVersion := currentFailoverVersion/m.failoverVersionIncrement*m.failoverVersionIncrement + info.InitialFailoverVersion
	if failoverVersion < currentFailoverVersion {
		return failoverVersion + m.failoverVersionIncrement
	}
	return failoverVersion
}

// IsVersionFromSameCluster return true if 2 version are used for the same cluster
func (m *metadataImpl) IsVersionFromSameCluster(version1 int64, version2 int64) bool {
	return (version1-version2)%m.failoverVersionIncrement == 0
}

func (m *metadataImpl) IsMasterCluster() bool {
	return m.masterClusterName == m.currentClusterName
}

// GetMasterClusterName return the master cluster name
func (m *metadataImpl) GetMasterClusterName() string {
	return m.masterClusterName
}

// GetCurrentClusterName return the current cluster name
func (m *metadataImpl) GetCurrentClusterName() string {
	return m.currentClusterName
}

// GetAllClusterInfo return the all cluster name -> corresponding information
func (m *metadataImpl) GetAllClusterInfo() map[string]config.ClusterInformation {
	return m.clusterInfo
}

// ClusterNameForFailoverVersion return the corresponding cluster name for a given failover version
func (m *metadataImpl) ClusterNameForFailoverVersion(failoverVersion int64) string {
	if failoverVersion == common.EmptyVersion {
		return m.currentClusterName
	}

	initialFailoverVersion := failoverVersion % m.failoverVersionIncrement
	// Failover version starts with 1.  Zero is an invalid value for failover version
	if initialFailoverVersion == common.EmptyVersion {
		initialFailoverVersion = m.failoverVersionIncrement
	}

	clusterName, ok := m.versionToClusterName[initialFailoverVersion]
	if !ok {
		panic(fmt.Sprintf(
			"Unknown initial failover version %v with given cluster initial failover version map: %v and failover version increment %v.",
			initialFailoverVersion,
			m.clusterInfo,
			m.failoverVersionIncrement,
		))
	}
	return clusterName
}

// GetSearchAttributes returns all search attributes (including system and build-in) for specified index.
func (m *metadataImpl) GetSearchAttributes(indexName string, bypassCache bool) (map[string]enumspb.IndexedValueType, error) {
	refreshCache := func(bypassCache bool, saCache searchAttributesCache, now time.Time) bool {
		return bypassCache ||
			saCache.lastRefresh.Add(searchAttributeCacheRefreshInterval).Before(now) ||
			saCache.searchAttributes == nil
	}

	now := m.timeSource.Now()
	saCache := m.searchAttributesCache.Load().(searchAttributesCache)

	if refreshCache(bypassCache, saCache, now) {
		m.searchAttributesCacheUpdateMutex.Lock()
		saCache = m.searchAttributesCache.Load().(searchAttributesCache)
		if refreshCache(bypassCache, saCache, now) {
			clusterMetadata, err := m.clusterMetadataManager.GetClusterMetadata()
			if err != nil {
				m.searchAttributesCacheUpdateMutex.Unlock()
				return nil, err
			}
			if clusterMetadata.Version > saCache.dbVersion {
				indexSearchAttributes := clusterMetadata.GetIndexSearchAttributes()

				// Append system search attributes to every index because they are not stored in metadata but cache should have everything ready to use.
				for _, customSearchAttributes := range indexSearchAttributes {
					searchattribute.AddSystemTo(customSearchAttributes.SearchAttributes)
				}

				saCache = searchAttributesCache{
					searchAttributes: indexSearchAttributes,
					lastRefresh:      now,
					dbVersion:        clusterMetadata.Version,
				}
			} else {
				saCache.lastRefresh = now
			}
			m.searchAttributesCache.Store(saCache)
		}
		m.searchAttributesCacheUpdateMutex.Unlock()
	}

	var searchAttributes map[string]enumspb.IndexedValueType
	if indexSearchAttributes, ok := saCache.searchAttributes[indexName]; ok {
		searchAttributes = indexSearchAttributes.GetSearchAttributes()
	}
	if searchAttributes == nil {
		return map[string]enumspb.IndexedValueType{}, nil
	}
	return searchAttributes, nil
}

// SaveSearchAttributes saves search attributes to cluster metadata.
func (m *metadataImpl) SaveSearchAttributes(indexName string, newCustomSearchAttributes map[string]enumspb.IndexedValueType) error {
	clusterMetadataResponse, err := m.clusterMetadataManager.GetClusterMetadata()
	if err != nil {
		return err
	}

	clusterMetadata := clusterMetadataResponse.ClusterMetadata
	if clusterMetadata.IndexSearchAttributes == nil {
		clusterMetadata.IndexSearchAttributes = map[string]*persistencespb.IndexSearchAttributes{indexName: nil}
	}
	clusterMetadata.IndexSearchAttributes[indexName] = &persistencespb.IndexSearchAttributes{SearchAttributes: newCustomSearchAttributes}
	_, err = m.clusterMetadataManager.SaveClusterMetadata(&persistence.SaveClusterMetadataRequest{
		ClusterMetadata: clusterMetadata,
		Version:         clusterMetadataResponse.Version,
	})
	if err != nil {
		return err
	}

	// Flush local cache.
	m.searchAttributesCache.Store(searchAttributesCache{})
	return nil
}
