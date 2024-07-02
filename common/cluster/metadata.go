// The MIT License
//
// Copyright (c) 2021 Temporal Technologies Inc.  All rights reservem.
//
// Copyright (c) 2021 Uber Technologies, Inc.
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
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/exp/maps"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/pingable"
	"go.temporal.io/server/internal/goro"
)

const (
	defaultClusterMetadataPageSize = 100
	refreshInterval                = time.Minute

	unknownClusterNamePrefix = "unknown-cluster-"
)

type (
	// Metadata provides information about the current cluster and other registered remote clusters.
	Metadata interface {
		pingable.Pingable

		// IsGlobalNamespaceEnabled whether the global namespace is enabled,
		// this attr should be discarded when cross DC is made public
		IsGlobalNamespaceEnabled() bool
		// IsMasterCluster whether current cluster is master cluster
		IsMasterCluster() bool
		// GetClusterID return the cluster ID, which is also the initial failover version
		GetClusterID() int64
		// GetNextFailoverVersion return the next failover version for namespace failover
		GetNextFailoverVersion(string, int64) int64
		// IsVersionFromSameCluster return true if 2 version are used for the same cluster
		IsVersionFromSameCluster(version1 int64, version2 int64) bool
		// GetMasterClusterName return the master cluster name
		GetMasterClusterName() string
		// GetCurrentClusterName return the current cluster name
		GetCurrentClusterName() string
		// GetAllClusterInfo return the all cluster name -> corresponding info
		GetAllClusterInfo() map[string]ClusterInformation
		// ClusterNameForFailoverVersion return the corresponding cluster name for a given failover version
		ClusterNameForFailoverVersion(isGlobalNamespace bool, failoverVersion int64) string
		// GetFailoverVersionIncrement return the Failover version increment value
		GetFailoverVersionIncrement() int64
		RegisterMetadataChangeCallback(callbackId any, cb CallbackFn)
		UnRegisterMetadataChangeCallback(callbackId any)
		Start()
		Stop()
	}

	CallbackFn func(oldClusterMetadata map[string]*ClusterInformation, newClusterMetadata map[string]*ClusterInformation)

	// Config contains the all cluster which participated in cross DC
	Config struct {
		EnableGlobalNamespace bool `yaml:"enableGlobalNamespace"`
		// FailoverVersionIncrement is the increment of each cluster version when failover happens.
		FailoverVersionIncrement int64 `yaml:"failoverVersionIncrement"`
		// MasterClusterName is the master cluster name, only the master cluster can register / update namespace
		// all clusters can do namespace failover.
		MasterClusterName string `yaml:"masterClusterName"`
		// CurrentClusterName is the name of the current cluster.
		CurrentClusterName string `yaml:"currentClusterName"`
		// ClusterInformation is a map from cluster name to corresponding information for each registered cluster.
		ClusterInformation map[string]ClusterInformation `yaml:"clusterInformation"`
		// Tags contains customized tags for the current cluster.
		Tags map[string]string `yaml:"tags"`
	}

	// ClusterInformation contains information for a single cluster.
	ClusterInformation struct {
		Enabled                bool  `yaml:"enabled"`
		InitialFailoverVersion int64 `yaml:"initialFailoverVersion"`
		// RPCAddress indicate the remote service address(Host:Port). Host can be DNS name.
		RPCAddress string `yaml:"rpcAddress"`
		// HTTPAddress indicates the address of the [go.temporal.io/server/service/frontend.HTTPAPIServer].
		// E.g. "localhost:7243".
		HTTPAddress string `yaml:"httpAddress"`
		// ClusterID allows to explicitly set the ID of the cluster. Optional.
		ClusterID  string            `yaml:"-"`
		ShardCount int32             `yaml:"-"` // Ignore this field when loading config.
		Tags       map[string]string `yaml:"-"` // Ignore this field. Use cluster.Config.Tags for customized tags.
		// private field to track cluster information updates
		version int64
	}

	metadataImpl struct {
		status               int32
		clusterMetadataStore persistence.ClusterMetadataManager
		refresher            *goro.Handle
		refreshDuration      dynamicconfig.DurationPropertyFn
		logger               log.Logger

		// Immutable fields

		// EnableGlobalNamespace whether the global namespace is enabled,
		enableGlobalNamespace bool
		// all clusters can do namespace failover
		masterClusterName string
		// currentClusterName is the name of the current cluster
		currentClusterName string
		// failoverVersionIncrement is the increment of each cluster's version when failover happen
		failoverVersionIncrement int64

		// Mutable fields

		clusterLock sync.RWMutex
		// clusterInfo contains all cluster name -> corresponding information
		clusterInfo map[string]ClusterInformation
		// versionToClusterName contains all initial version -> corresponding cluster name
		versionToClusterName map[int64]string

		clusterCallbackLock   sync.RWMutex
		clusterChangeCallback map[any]CallbackFn
	}
)

func NewMetadata(
	enableGlobalNamespace bool,
	failoverVersionIncrement int64,
	masterClusterName string,
	currentClusterName string,
	clusterInfo map[string]ClusterInformation,
	clusterMetadataStore persistence.ClusterMetadataManager,
	refreshDuration dynamicconfig.DurationPropertyFn,
	logger log.Logger,
) Metadata {
	if len(clusterInfo) == 0 {
		panic("Empty cluster information")
	} else if len(masterClusterName) == 0 {
		panic("Master cluster name is empty")
	} else if len(currentClusterName) == 0 {
		panic("Current cluster name is empty")
	} else if failoverVersionIncrement == 0 || failoverVersionIncrement > math.MaxInt32 {
		panic("Version increment <= 0 or > 2147483647")
	}

	versionToClusterName := updateVersionToClusterName(clusterInfo, failoverVersionIncrement)
	if _, ok := clusterInfo[currentClusterName]; !ok {
		panic("Current cluster is not specified in cluster info")
	}
	if _, ok := clusterInfo[masterClusterName]; !ok {
		panic("Master cluster is not specified in cluster info")
	}
	if len(versionToClusterName) != len(clusterInfo) {
		panic("Cluster info initial versions have duplicates")
	}

	copyClusterInfo := make(map[string]ClusterInformation)
	for k, v := range clusterInfo {
		copyClusterInfo[k] = v
	}
	if refreshDuration == nil {
		refreshDuration = dynamicconfig.GetDurationPropertyFn(refreshInterval)
	}
	return &metadataImpl{
		status:                   common.DaemonStatusInitialized,
		enableGlobalNamespace:    enableGlobalNamespace,
		failoverVersionIncrement: failoverVersionIncrement,
		masterClusterName:        masterClusterName,
		currentClusterName:       currentClusterName,
		clusterInfo:              copyClusterInfo,
		versionToClusterName:     versionToClusterName,
		clusterChangeCallback:    make(map[any]CallbackFn),
		clusterMetadataStore:     clusterMetadataStore,
		logger:                   logger,
		refreshDuration:          refreshDuration,
	}
}

func NewMetadataFromConfig(
	config *Config,
	clusterMetadataStore persistence.ClusterMetadataManager,
	dynamicCollection *dynamicconfig.Collection,
	logger log.Logger,
) Metadata {
	return NewMetadata(
		config.EnableGlobalNamespace,
		config.FailoverVersionIncrement,
		config.MasterClusterName,
		config.CurrentClusterName,
		config.ClusterInformation,
		clusterMetadataStore,
		dynamicconfig.ClusterMetadataRefreshInterval.Get(dynamicCollection),
		logger,
	)
}

func (m *metadataImpl) Start() {
	if !atomic.CompareAndSwapInt32(&m.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	// TODO: specify a timeout for the context
	ctx := headers.SetCallerInfo(
		context.TODO(),
		headers.SystemBackgroundCallerInfo,
	)
	err := m.refreshClusterMetadata(ctx)
	if err != nil {
		m.logger.Fatal("Unable to initialize cluster metadata cache", tag.Error(err))
	}
	m.refresher = goro.NewHandle(ctx).Go(m.refreshLoop)
}

func (m *metadataImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&m.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	m.refresher.Cancel()
	<-m.refresher.Done()
}

func (m *metadataImpl) GetPingChecks() []pingable.Check {
	return []pingable.Check{
		{
			Name: "cluster metadata lock",
			// we don't do any persistence ops under clusterLock, use a short timeout
			Timeout: 10 * time.Second,
			Ping: func() []pingable.Pingable {
				m.clusterLock.Lock()
				//lint:ignore SA2001 just checking if we can acquire the lock
				m.clusterLock.Unlock()
				return nil
			},
			MetricsName: metrics.DDClusterMetadataLockLatency.Name(),
		},
		{
			Name: "cluster metadata callback lock",
			// listeners get called under clusterCallbackLock, they may do some more work, but
			// not persistence ops.
			Timeout: 10 * time.Second,
			Ping: func() []pingable.Pingable {
				m.clusterCallbackLock.Lock()
				//lint:ignore SA2001 just checking if we can acquire the lock
				m.clusterCallbackLock.Unlock()
				return nil
			},
			MetricsName: metrics.DDClusterMetadataCallbackLockLatency.Name(),
		},
	}
}

func (m *metadataImpl) IsGlobalNamespaceEnabled() bool {
	return m.enableGlobalNamespace
}

func (m *metadataImpl) IsMasterCluster() bool {
	return m.masterClusterName == m.currentClusterName
}

func (m *metadataImpl) GetClusterID() int64 {
	m.clusterLock.RLock()
	defer m.clusterLock.RUnlock()

	info, ok := m.clusterInfo[m.currentClusterName]
	if !ok {
		panic(fmt.Sprintf(
			"Unknown cluster name: %v with given cluster initial failover version map: %v.",
			m.currentClusterName,
			m.clusterInfo,
		))
	}
	return info.InitialFailoverVersion
}

func (m *metadataImpl) GetNextFailoverVersion(clusterName string, currentFailoverVersion int64) int64 {
	m.clusterLock.RLock()
	defer m.clusterLock.RUnlock()

	info, ok := m.clusterInfo[clusterName]
	if !ok {
		panic(fmt.Sprintf(
			"Unknown cluster name: %v with given cluster initial failover version map: %v.",
			clusterName,
			m.clusterInfo,
		))
	}
	failoverVersion := currentFailoverVersion/m.failoverVersionIncrement*m.failoverVersionIncrement + info.InitialFailoverVersion
	if failoverVersion < currentFailoverVersion {
		return failoverVersion + m.failoverVersionIncrement
	}
	return failoverVersion
}

func (m *metadataImpl) IsVersionFromSameCluster(version1 int64, version2 int64) bool {
	return (version1-version2)%m.failoverVersionIncrement == 0
}

func (m *metadataImpl) GetMasterClusterName() string {
	return m.masterClusterName
}

func (m *metadataImpl) GetCurrentClusterName() string {
	return m.currentClusterName
}

func (m *metadataImpl) GetAllClusterInfo() map[string]ClusterInformation {
	m.clusterLock.RLock()
	defer m.clusterLock.RUnlock()

	result := make(map[string]ClusterInformation, len(m.clusterInfo))
	for k, v := range m.clusterInfo {
		result[k] = v
	}
	return result
}

func (m *metadataImpl) ClusterNameForFailoverVersion(isGlobalNamespace bool, failoverVersion int64) string {
	if failoverVersion == common.EmptyVersion {
		// Local namespace uses EmptyVersion. But local namespace could be promoted to global namespace. Once promoted,
		// workflows with EmptyVersion could be replicated to other clusters. The receiving cluster needs to know that
		// those workflows are not from their current cluster.
		if isGlobalNamespace {
			return unknownClusterNamePrefix + strconv.Itoa(int(failoverVersion))
		}
		return m.currentClusterName
	}

	if !isGlobalNamespace {
		panic(fmt.Sprintf(
			"ClusterMetadata encountered local namesapce with failover version %v",
			failoverVersion,
		))
	}

	initialFailoverVersion := failoverVersion % m.failoverVersionIncrement
	// Failover version starts with 1.  Zero is an invalid value for failover version
	if initialFailoverVersion == common.EmptyVersion {
		initialFailoverVersion = m.failoverVersionIncrement
	}

	m.clusterLock.RLock()
	defer m.clusterLock.RUnlock()
	clusterName, ok := m.versionToClusterName[initialFailoverVersion]
	if !ok {
		m.logger.Warn(fmt.Sprintf(
			"Unknown initial failover version %v with given cluster initial failover version map: %v and failover version increment %v.",
			initialFailoverVersion,
			m.clusterInfo,
			m.failoverVersionIncrement,
		))
		return unknownClusterNamePrefix + strconv.Itoa(int(initialFailoverVersion))
	}
	return clusterName
}

func (m *metadataImpl) GetFailoverVersionIncrement() int64 {
	return m.failoverVersionIncrement
}

func (m *metadataImpl) RegisterMetadataChangeCallback(callbackId any, cb CallbackFn) {
	m.clusterCallbackLock.Lock()
	m.clusterChangeCallback[callbackId] = cb
	m.clusterCallbackLock.Unlock()

	oldEntries := make(map[string]*ClusterInformation)
	newEntries := make(map[string]*ClusterInformation)
	m.clusterLock.RLock()
	for clusterName, clusterInfo := range m.clusterInfo {
		oldEntries[clusterName] = nil
		newEntries[clusterName] = ShallowCopyClusterInformation(&clusterInfo)
	}
	m.clusterLock.RUnlock()
	cb(oldEntries, newEntries)
}

func (m *metadataImpl) UnRegisterMetadataChangeCallback(callbackId any) {
	m.clusterCallbackLock.Lock()
	delete(m.clusterChangeCallback, callbackId)
	m.clusterCallbackLock.Unlock()
}

func (m *metadataImpl) refreshLoop(ctx context.Context) error {
	timer := time.NewTicker(m.refreshDuration())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			for err := m.refreshClusterMetadata(ctx); err != nil; err = m.refreshClusterMetadata(ctx) {
				m.logger.Error("Error refreshing remote cluster metadata", tag.Error(err))
				refreshTimer := time.NewTimer(m.refreshDuration() / 2)

				select {
				case <-refreshTimer.C:
				case <-ctx.Done():
					refreshTimer.Stop()
					return nil
				}
			}
		}
	}
}

func (m *metadataImpl) refreshClusterMetadata(ctx context.Context) error {
	clusterMetadataMap, err := m.listAllClusterMetadataFromDB(ctx)
	if err != nil {
		return err
	}

	oldEntries := make(map[string]*ClusterInformation)
	newEntries := make(map[string]*ClusterInformation)

	clusterInfoMap := m.GetAllClusterInfo()
	for clusterName, newClusterInfo := range clusterMetadataMap {
		oldClusterInfo, ok := clusterInfoMap[clusterName]
		if !ok {
			// handle new cluster registry
			oldEntries[clusterName] = nil
			newEntries[clusterName] = ShallowCopyClusterInformation(newClusterInfo)
		} else if newClusterInfo.version > oldClusterInfo.version {
			if newClusterInfo.Enabled == oldClusterInfo.Enabled &&
				newClusterInfo.RPCAddress == oldClusterInfo.RPCAddress &&
				newClusterInfo.HTTPAddress == oldClusterInfo.HTTPAddress &&
				newClusterInfo.InitialFailoverVersion == oldClusterInfo.InitialFailoverVersion &&
				newClusterInfo.ClusterID == oldClusterInfo.ClusterID &&
				maps.Equal(newClusterInfo.Tags, oldClusterInfo.Tags) {
				// key cluster info does not change
				continue
			}
			// handle updated cluster registry
			oldEntries[clusterName] = ShallowCopyClusterInformation(&oldClusterInfo)
			newEntries[clusterName] = ShallowCopyClusterInformation(newClusterInfo)
		}
	}
	for clusterName, oldClusterInfo := range clusterInfoMap {
		if _, ok := clusterMetadataMap[clusterName]; !ok {
			// removed cluster registry
			oldEntries[clusterName] = &oldClusterInfo
			newEntries[clusterName] = nil
		}
	}

	if len(oldEntries) > 0 {
		m.clusterLock.Lock()
		m.updateClusterInfoLocked(oldEntries, newEntries)
		m.updateFailoverVersionToClusterName()
		m.clusterLock.Unlock()

		m.clusterCallbackLock.RLock()
		defer m.clusterCallbackLock.RUnlock()
		for _, cb := range m.clusterChangeCallback {
			cb(oldEntries, newEntries)
		}
	}
	return nil
}

func (m *metadataImpl) updateClusterInfoLocked(
	oldClusterMetadata map[string]*ClusterInformation,
	newClusterMetadata map[string]*ClusterInformation,
) {
	for clusterName := range oldClusterMetadata {
		if oldClusterMetadata[clusterName] != nil && newClusterMetadata[clusterName] == nil {
			delete(m.clusterInfo, clusterName)
		} else {
			m.clusterInfo[clusterName] = *newClusterMetadata[clusterName]
		}
	}
}

func (m *metadataImpl) updateFailoverVersionToClusterName() {
	m.versionToClusterName = updateVersionToClusterName(m.clusterInfo, m.failoverVersionIncrement)
}

func updateVersionToClusterName(clusterInfo map[string]ClusterInformation, failoverVersionIncrement int64) map[int64]string {
	versionToClusterName := make(map[int64]string)
	for clusterName, info := range clusterInfo {
		if failoverVersionIncrement <= info.InitialFailoverVersion || info.InitialFailoverVersion <= 0 {
			panic(fmt.Sprintf(
				"Version increment %v is smaller than initial version: %v.",
				failoverVersionIncrement,
				clusterInfo,
			))
		}
		if len(clusterName) == 0 {
			panic("Cluster name needs to be defined in Cluster Information")
		}
		versionToClusterName[info.InitialFailoverVersion] = clusterName

		if info.Enabled && info.RPCAddress == "" {
			panic(fmt.Sprintf("Cluster %v: RPCAddress is empty", clusterName))
		}
		// It's ok if info.HTTPAddress is empty
	}
	return versionToClusterName
}

func (m *metadataImpl) listAllClusterMetadataFromDB(
	ctx context.Context,
) (map[string]*ClusterInformation, error) {
	result := make(map[string]*ClusterInformation)
	metadataStore := m.clusterMetadataStore
	if metadataStore == nil {
		return result, nil
	}

	iterator := GetAllClustersIter(ctx, metadataStore)
	for iterator.HasNext() {
		item, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		result[item.GetClusterName()] = ClusterInformationFromDB(item)
	}
	return result, nil
}

// GetAllClustersIter returns an iterator that can be used to iterate over all clusters in the metadata store.
func GetAllClustersIter(
	ctx context.Context,
	metadataStore persistence.ClusterMetadataManager,
) collection.Iterator[*persistence.GetClusterMetadataResponse] {
	paginationFn := func(paginationToken []byte) ([]*persistence.GetClusterMetadataResponse, []byte, error) {
		resp, err := metadataStore.ListClusterMetadata(
			ctx,
			&persistence.ListClusterMetadataRequest{
				PageSize:      defaultClusterMetadataPageSize,
				NextPageToken: paginationToken,
			},
		)
		if err != nil {
			return nil, nil, err
		}
		return resp.ClusterMetadata, resp.NextPageToken, nil
	}

	iterator := collection.NewPagingIterator(paginationFn)
	return iterator
}

func ClusterInformationFromDB(getClusterResp *persistence.GetClusterMetadataResponse) *ClusterInformation {
	return &ClusterInformation{
		Enabled:                getClusterResp.GetIsConnectionEnabled(),
		InitialFailoverVersion: getClusterResp.GetInitialFailoverVersion(),
		RPCAddress:             getClusterResp.GetClusterAddress(),
		HTTPAddress:            getClusterResp.GetHttpAddress(),
		ClusterID:              getClusterResp.GetClusterId(),
		ShardCount:             getClusterResp.GetHistoryShardCount(),
		Tags:                   getClusterResp.GetTags(),
		version:                getClusterResp.Version,
	}
}

// ShallowCopyClusterInformation returns a shallow copy of the given ClusterInformation. The [ClusterInformation.Tags]
// field is not deep-copied, so you must be careful when modifying it.
func ShallowCopyClusterInformation(information *ClusterInformation) *ClusterInformation {
	tmp := *information
	return &tmp
}
