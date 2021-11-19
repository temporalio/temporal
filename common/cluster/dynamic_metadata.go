// The MIT License
//
// Copyright (c) 2021 Temporal Technologies Inc.  All rights reserved.
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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination dynamic_metadata_mock.go

package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/internal/goro"
)

const (
	defaultClusterMetadataPageSize = 100
	refreshInterval                = time.Minute * 5
	refreshFailureInterval         = time.Second * 30
)

type (
	DynamicMetadata interface {
		common.Daemon

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
		GetAllClusterInfo() map[string]ClusterInformation
		// ClusterNameForFailoverVersion return the corresponding cluster name for a given failover version
		ClusterNameForFailoverVersion(failoverVersion int64) string
		// GetFailoverVersionIncrement return the Failover version increment value
		GetFailoverVersionIncrement() int64
		RegisterMetadataChangeCallback(callbackId string, cb CallbackFn)
		UnRegisterMetadataChangeCallback(callbackId string)
	}

	CallbackFn func(oldClusterMetadata map[string]*ClusterInformation, newClusterMetadata map[string]*ClusterInformation)

	// Config contains the all cluster which participated in cross DC
	Config struct {
		EnableGlobalNamespace bool `yaml:"enableGlobalNamespace"`
		// FailoverVersionIncrement is the increment of each cluster version when failover happens
		FailoverVersionIncrement int64 `yaml:"failoverVersionIncrement"`
		// MasterClusterName is the master cluster name, only the master cluster can register / update namespace
		// all clusters can do namespace failover
		MasterClusterName string `yaml:"masterClusterName"`
		// CurrentClusterName is the name of the current cluster
		CurrentClusterName string `yaml:"currentClusterName"`
		// ClusterInformation contains all cluster names to corresponding information about that cluster
		ClusterInformation map[string]ClusterInformation `yaml:"clusterInformation"`
	}

	// ClusterInformation contains the information about each cluster which participated in cross DC
	ClusterInformation struct {
		Enabled                bool  `yaml:"enabled"`
		InitialFailoverVersion int64 `yaml:"initialFailoverVersion"`
		// Address indicate the remote service address(Host:Port). Host can be DNS name.
		RPCAddress string `yaml:"rpcAddress"`
		// private field to track cluster infomation updates
		version int64
	}

	dynamicMetadataImpl struct {
		status               int32
		clusterMetadataStore persistence.ClusterMetadataManager
		refresher            *goro.Handle
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
		clusterChangeCallback map[string]CallbackFn
	}
)

func NewDynamicMetadata(
	enableGlobalNamespace bool,
	failoverVersionIncrement int64,
	masterClusterName string,
	currentClusterName string,
	clusterInfo map[string]ClusterInformation,
	clusterMetadataStore persistence.ClusterMetadataManager,
	logger log.Logger,
) DynamicMetadata {
	if len(clusterInfo) == 0 {
		panic("Empty cluster information")
	} else if len(masterClusterName) == 0 {
		panic("Master cluster name is empty")
	} else if len(currentClusterName) == 0 {
		panic("Current cluster name is empty")
	} else if failoverVersionIncrement == 0 {
		panic("Version increment is 0")
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
	return &dynamicMetadataImpl{
		status:                   common.DaemonStatusInitialized,
		enableGlobalNamespace:    enableGlobalNamespace,
		failoverVersionIncrement: failoverVersionIncrement,
		masterClusterName:        masterClusterName,
		currentClusterName:       currentClusterName,
		clusterInfo:              copyClusterInfo,
		versionToClusterName:     versionToClusterName,
		clusterChangeCallback:    make(map[string]CallbackFn),
		clusterMetadataStore:     clusterMetadataStore,
		logger:                   logger,
	}
}

func NewDynamicMetadataFromConfig(
	config *Config,
	clusterMetadataStore persistence.ClusterMetadataManager,
	logger log.Logger,
) DynamicMetadata {
	return NewDynamicMetadata(
		config.EnableGlobalNamespace,
		config.FailoverVersionIncrement,
		config.MasterClusterName,
		config.CurrentClusterName,
		config.ClusterInformation,
		clusterMetadataStore,
		logger,
	)
}

func NewDynamicMetadataForTest(
	config *Config,
) DynamicMetadata {
	return NewDynamicMetadata(
		config.EnableGlobalNamespace,
		config.FailoverVersionIncrement,
		config.MasterClusterName,
		config.CurrentClusterName,
		config.ClusterInformation,
		nil,
		log.NewNoopLogger(),
	)
}

func (d *dynamicMetadataImpl) Start() {
	if !atomic.CompareAndSwapInt32(&d.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	err := d.refreshClusterMetadata(context.Background())
	if err != nil {
		d.logger.Fatal("Unable to initialize cluster metadata cache", tag.Error(err))
	}
	d.refresher = goro.Go(context.Background(), d.refreshLoop)
}

func (d *dynamicMetadataImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&d.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	d.refresher.Cancel()
	<-d.refresher.Done()
}

func (d *dynamicMetadataImpl) IsGlobalNamespaceEnabled() bool {
	return d.enableGlobalNamespace
}

func (d *dynamicMetadataImpl) IsMasterCluster() bool {
	return d.masterClusterName == d.currentClusterName
}

func (d *dynamicMetadataImpl) GetNextFailoverVersion(clusterName string, currentFailoverVersion int64) int64 {
	d.clusterLock.RLock()
	defer d.clusterLock.RUnlock()

	info, ok := d.clusterInfo[clusterName]
	if !ok {
		panic(fmt.Sprintf(
			"Unknown cluster name: %v with given cluster initial failover version map: %v.",
			clusterName,
			d.clusterInfo,
		))
	}
	failoverVersion := currentFailoverVersion/d.failoverVersionIncrement*d.failoverVersionIncrement + info.InitialFailoverVersion
	if failoverVersion < currentFailoverVersion {
		return failoverVersion + d.failoverVersionIncrement
	}
	return failoverVersion
}

func (d *dynamicMetadataImpl) IsVersionFromSameCluster(version1 int64, version2 int64) bool {
	return (version1-version2)%d.failoverVersionIncrement == 0
}

func (d *dynamicMetadataImpl) GetMasterClusterName() string {
	return d.masterClusterName
}

func (d *dynamicMetadataImpl) GetCurrentClusterName() string {
	return d.currentClusterName
}

func (d *dynamicMetadataImpl) GetAllClusterInfo() map[string]ClusterInformation {
	d.clusterLock.RLock()
	defer d.clusterLock.RUnlock()

	result := make(map[string]ClusterInformation, len(d.clusterInfo))
	for k, v := range d.clusterInfo {
		result[k] = v
	}
	return result
}

func (d *dynamicMetadataImpl) ClusterNameForFailoverVersion(failoverVersion int64) string {
	if failoverVersion == common.EmptyVersion {
		return d.currentClusterName
	}

	initialFailoverVersion := failoverVersion % d.failoverVersionIncrement
	// Failover version starts with 1.  Zero is an invalid value for failover version
	if initialFailoverVersion == common.EmptyVersion {
		initialFailoverVersion = d.failoverVersionIncrement
	}

	d.clusterLock.RLock()
	defer d.clusterLock.RUnlock()
	clusterName, ok := d.versionToClusterName[initialFailoverVersion]
	if !ok {
		panic(fmt.Sprintf(
			"Unknown initial failover version %v with given cluster initial failover version map: %v and failover version increment %v.",
			initialFailoverVersion,
			d.clusterInfo,
			d.failoverVersionIncrement,
		))
	}
	return clusterName
}

func (d *dynamicMetadataImpl) GetFailoverVersionIncrement() int64 {
	return d.failoverVersionIncrement
}

func (d *dynamicMetadataImpl) RegisterMetadataChangeCallback(callbackId string, cb CallbackFn) {
	d.clusterCallbackLock.Lock()
	d.clusterChangeCallback[callbackId] = cb
	d.clusterCallbackLock.Unlock()

	oldEntries := make(map[string]*ClusterInformation)
	newEntries := make(map[string]*ClusterInformation)
	d.clusterLock.RLock()
	for clusterName, clusterInfo := range d.clusterInfo {
		oldEntries[clusterName] = nil
		newEntries[clusterName] = &ClusterInformation{
			Enabled:                clusterInfo.Enabled,
			InitialFailoverVersion: clusterInfo.InitialFailoverVersion,
			RPCAddress:             clusterInfo.RPCAddress,
			version:                clusterInfo.version,
		}
	}
	d.clusterLock.RUnlock()
	cb(oldEntries, newEntries)
}

func (d *dynamicMetadataImpl) UnRegisterMetadataChangeCallback(callbackId string) {
	d.clusterCallbackLock.Lock()
	delete(d.clusterChangeCallback, callbackId)
	d.clusterCallbackLock.Unlock()
}

func (d *dynamicMetadataImpl) refreshLoop(ctx context.Context) error {
	timer := time.NewTicker(refreshInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			for err := d.refreshClusterMetadata(ctx); err != nil; err = d.refreshClusterMetadata(ctx) {
				select {
				case <-ctx.Done():
					return nil
				default:
					d.logger.Error("Error refreshing remote cluster metadata", tag.Error(err))
					select {
					case <-time.After(refreshFailureInterval):
					case <-ctx.Done():
						return nil
					}
				}
			}
		}
	}
}

func (d *dynamicMetadataImpl) refreshClusterMetadata(_ context.Context) error {
	clusterMetadataMap, err := d.listAllClusterMetadata()
	if err != nil {
		return err
	}

	oldEntries := make(map[string]*ClusterInformation)
	newEntries := make(map[string]*ClusterInformation)

	clusterInfoMap := d.GetAllClusterInfo()
UpdateLoop:
	for clusterName, clusterMetadata := range clusterMetadataMap {
		clusterInfo, ok := clusterInfoMap[clusterName]
		if !ok {
			// handle new cluster registry
			oldEntries[clusterName] = nil
			newEntries[clusterName] = &ClusterInformation{
				Enabled:                clusterMetadata.Enabled,
				InitialFailoverVersion: clusterMetadata.InitialFailoverVersion,
				RPCAddress:             clusterMetadata.RPCAddress,
				version:                clusterMetadata.version,
			}
			continue UpdateLoop
		}

		if clusterMetadata.version > clusterInfo.version {
			// handle updated cluster registry
			oldEntries[clusterName] = &ClusterInformation{
				Enabled:                clusterInfo.Enabled,
				InitialFailoverVersion: clusterInfo.InitialFailoverVersion,
				RPCAddress:             clusterInfo.RPCAddress,
				version:                clusterInfo.version,
			}
			newEntries[clusterName] = &ClusterInformation{
				Enabled:                clusterMetadata.Enabled,
				InitialFailoverVersion: clusterMetadata.InitialFailoverVersion,
				RPCAddress:             clusterMetadata.RPCAddress,
				version:                clusterMetadata.version,
			}
		}
	}
	for clusterName, clusterInfo := range clusterInfoMap {
		if _, ok := clusterMetadataMap[clusterName]; !ok {
			// removed cluster registry
			oldEntries[clusterName] = &clusterInfo
			newEntries[clusterName] = nil
		}
	}

	if len(oldEntries) > 0 {
		d.clusterLock.Lock()
		d.updateClusterInfoLocked(oldEntries, newEntries)
		d.updateVersionToClusterNameLocked()
		d.clusterLock.Unlock()

		d.clusterCallbackLock.RLock()
		defer d.clusterCallbackLock.RUnlock()
		for _, cb := range d.clusterChangeCallback {
			cb(oldEntries, newEntries)
		}
	}
	return nil
}

func (d *dynamicMetadataImpl) updateClusterInfoLocked(
	oldClusterMetadata map[string]*ClusterInformation,
	newClusterMetadata map[string]*ClusterInformation,
) {
	for clusterName := range oldClusterMetadata {
		if oldClusterMetadata[clusterName] != nil && newClusterMetadata[clusterName] == nil {
			delete(d.clusterInfo, clusterName)
		} else {
			d.clusterInfo[clusterName] = *newClusterMetadata[clusterName]
		}
	}
}

func (d *dynamicMetadataImpl) updateVersionToClusterNameLocked() {
	d.versionToClusterName = updateVersionToClusterName(d.clusterInfo, d.failoverVersionIncrement)
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
			panic("Cluster name in all cluster names is empty")
		}
		versionToClusterName[info.InitialFailoverVersion] = clusterName

		if info.Enabled && info.RPCAddress == "" {
			panic(fmt.Sprintf("Cluster %v: RPCAddress is empty", clusterName))
		}
	}
	return versionToClusterName
}

func (d *dynamicMetadataImpl) listAllClusterMetadata() (map[string]*ClusterInformation, error) {
	result := make(map[string]*ClusterInformation)
	if d.clusterMetadataStore == nil {
		return result, nil
	}

	paginationFn := func(paginationToken []byte) ([]interface{}, []byte, error) {
		resp, err := d.clusterMetadataStore.ListClusterMetadata(
			&persistence.ListClusterMetadataRequest{PageSize: defaultClusterMetadataPageSize},
		)
		if err != nil {
			return nil, nil, err
		}
		var paginateItems []interface{}
		for _, clusterInfo := range resp.ClusterMetadata {
			paginateItems = append(paginateItems, clusterInfo)
		}
		return paginateItems, resp.NextPageToken, nil
	}

	iterator := collection.NewPagingIterator(paginationFn)
	for iterator.HasNext() {
		item, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		getClusterResp := item.(*persistence.GetClusterMetadataResponse)
		result[getClusterResp.GetClusterName()] = &ClusterInformation{
			Enabled:                getClusterResp.GetIsConnectionEnabled(),
			InitialFailoverVersion: getClusterResp.GetInitialFailoverVersion(),
			RPCAddress:             getClusterResp.GetClusterAddress(),
			version:                getClusterResp.Version,
		}
	}
	return result, nil
}
