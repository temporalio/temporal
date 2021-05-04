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

package client

import (
	"sync"

	"go.temporal.io/server/common/config"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resolver"
)

type (
	// Factory defines the interface for any implementation that can vend
	// persistence layer objects backed by a datastore. The actual datastore
	// is implementation detail hidden behind this interface
	Factory interface {
		// Close the factory
		Close()
		// NewTaskManager returns a new task manager
		NewTaskManager() (p.TaskManager, error)
		// NewShardManager returns a new shard manager
		NewShardManager() (p.ShardManager, error)
		// NewHistoryManager returns a new history manager
		NewHistoryManager() (p.HistoryManager, error)
		// NewMetadataManager returns a new metadata manager
		NewMetadataManager() (p.MetadataManager, error)
		// NewExecutionManager returns a new execution manager for a given shardID
		NewExecutionManager(shardID int32) (p.ExecutionManager, error)
		// NewVisibilityManager returns a new visibility manager
		NewVisibilityManager() (p.VisibilityManager, error)
		// NewNamespaceReplicationQueue returns a new queue for namespace replication
		NewNamespaceReplicationQueue() (p.NamespaceReplicationQueue, error)
		// NewClusterMetadata returns a new manager for cluster specific metadata
		NewClusterMetadataManager() (p.ClusterMetadataManager, error)
	}
	// DataStoreFactory is a low level interface to be implemented by a datastore
	// Examples of datastores are cassandra, mysql etc
	DataStoreFactory interface {
		// Close closes the factory
		Close()
		// NewTaskStore returns a new task store
		NewTaskStore() (p.TaskStore, error)
		// NewShardStore returns a new shard store
		NewShardStore() (p.ShardStore, error)
		// NewHistoryStore returns a new history store
		NewHistoryStore() (p.HistoryStore, error)
		// NewMetadataStore returns a new metadata store
		NewMetadataStore() (p.MetadataStore, error)
		// NewExecutionStore returns an execution store for given shardID
		NewExecutionStore(shardID int32) (p.ExecutionStore, error)
		// NewVisibilityStore returns a new visibility store
		NewVisibilityStore() (p.VisibilityStore, error)
		NewQueue(queueType p.QueueType) (p.Queue, error)
		// NewClusterMetadataStore returns a new metadata store
		NewClusterMetadataStore() (p.ClusterMetadataStore, error)
	}
	// AbstractDataStoreFactory creates a DataStoreFactory, can be used to implement custom datastore support outside
	// of the Temporal core.
	AbstractDataStoreFactory interface {
		NewFactory(cfg config.CustomDatastoreConfig, clusterName string, logger log.Logger) DataStoreFactory
	}

	// Datastore represents a datastore
	Datastore struct {
		factory   DataStoreFactory
		ratelimit quotas.RateLimiter
	}
	factoryImpl struct {
		sync.RWMutex
		config                   *config.Persistence
		abstractDataStoreFactory AbstractDataStoreFactory
		metricsClient            metrics.Client
		logger                   log.Logger
		datastores               map[storeType]Datastore
		clusterName              string
	}

	storeType int
)

const (
	storeTypeHistory storeType = iota + 1
	storeTypeTask
	storeTypeShard
	storeTypeMetadata
	storeTypeExecution
	storeTypeVisibility
	storeTypeQueue
	storeTypeClusterMetadata
)

var storeTypes = []storeType{
	storeTypeHistory,
	storeTypeTask,
	storeTypeShard,
	storeTypeMetadata,
	storeTypeExecution,
	storeTypeVisibility,
	storeTypeQueue,
	storeTypeClusterMetadata,
}

// NewFactory returns an implementation of factory that vends persistence objects based on
// specified configuration. This factory takes as input a config.Persistence object
// which specifies the datastore to be used for a given type of object. This config
// also contains config for individual datastores themselves.
//
// The objects returned by this factory enforce ratelimit and maxconns according to
// given configuration. In addition, all objects will emit metrics automatically
func NewFactory(
	cfg *config.Persistence,
	r resolver.ServiceResolver,
	persistenceMaxQPS dynamicconfig.IntPropertyFn,
	abstractDataStoreFactory AbstractDataStoreFactory,
	clusterName string,
	metricsClient metrics.Client,
	logger log.Logger,
) Factory {
	factory := &factoryImpl{
		config:                   cfg,
		abstractDataStoreFactory: abstractDataStoreFactory,
		metricsClient:            metricsClient,
		logger:                   logger,
		clusterName:              clusterName,
	}
	limiters := buildRateLimiters(cfg, persistenceMaxQPS)
	factory.init(clusterName, limiters, r)
	return factory
}

// NewTaskManager returns a new task manager
func (f *factoryImpl) NewTaskManager() (p.TaskManager, error) {
	ds := f.datastores[storeTypeTask]
	result, err := ds.factory.NewTaskStore()
	if err != nil {
		return nil, err
	}
	if ds.ratelimit != nil {
		result = p.NewTaskPersistenceRateLimitedClient(result, ds.ratelimit, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewTaskPersistenceMetricsClient(result, f.metricsClient, f.logger)
	}
	return result, nil
}

// NewShardManager returns a new shard manager
func (f *factoryImpl) NewShardManager() (p.ShardManager, error) {
	ds := f.datastores[storeTypeShard]
	result, err := ds.factory.NewShardStore()
	if err != nil {
		return nil, err
	}
	if ds.ratelimit != nil {
		result = p.NewShardPersistenceRateLimitedClient(result, ds.ratelimit, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewShardPersistenceMetricsClient(result, f.metricsClient, f.logger)
	}
	return result, nil
}

// NewHistoryManager returns a new history manager
func (f *factoryImpl) NewHistoryManager() (p.HistoryManager, error) {
	ds := f.datastores[storeTypeHistory]
	store, err := ds.factory.NewHistoryStore()
	if err != nil {
		return nil, err
	}
	result := p.NewHistoryV2ManagerImpl(store, f.logger, f.config.TransactionSizeLimit)
	if ds.ratelimit != nil {
		result = p.NewHistoryV2PersistenceRateLimitedClient(result, ds.ratelimit, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewHistoryV2PersistenceMetricsClient(result, f.metricsClient, f.logger)
	}
	return result, nil
}

// NewMetadataManager returns a new metadata manager
func (f *factoryImpl) NewMetadataManager() (p.MetadataManager, error) {
	var err error
	var store p.MetadataStore
	ds := f.datastores[storeTypeMetadata]
	store, err = ds.factory.NewMetadataStore()
	if err != nil {
		return nil, err
	}

	result := p.NewMetadataManagerImpl(store, f.logger, f.clusterName)
	if ds.ratelimit != nil {
		result = p.NewMetadataPersistenceRateLimitedClient(result, ds.ratelimit, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewMetadataPersistenceMetricsClient(result, f.metricsClient, f.logger)
	}
	return result, nil
}

// NewClusterMetadataManager returns a new cluster metadata manager
func (f *factoryImpl) NewClusterMetadataManager() (p.ClusterMetadataManager, error) {
	var err error
	var store p.ClusterMetadataStore
	ds := f.datastores[storeTypeClusterMetadata]
	store, err = ds.factory.NewClusterMetadataStore()
	if err != nil {
		return nil, err
	}

	result := p.NewClusterMetadataManagerImpl(store, f.logger)
	if ds.ratelimit != nil {
		result = p.NewClusterMetadataPersistenceRateLimitedClient(result, ds.ratelimit, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewClusterMetadataPersistenceMetricsClient(result, f.metricsClient, f.logger)
	}
	return result, nil
}

// NewExecutionManager returns a new execution manager for a given shardID
func (f *factoryImpl) NewExecutionManager(
	shardID int32,
) (p.ExecutionManager, error) {

	ds := f.datastores[storeTypeExecution]
	store, err := ds.factory.NewExecutionStore(shardID)
	if err != nil {
		return nil, err
	}
	result := p.NewExecutionManagerImpl(store, f.logger)
	if ds.ratelimit != nil {
		result = p.NewWorkflowExecutionPersistenceRateLimitedClient(result, ds.ratelimit, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewWorkflowExecutionPersistenceMetricsClient(result, f.metricsClient, f.logger)
	}
	return result, nil
}

// NewVisibilityManager returns a new visibility manager
func (f *factoryImpl) NewVisibilityManager() (p.VisibilityManager, error) {
	visConfig := f.config.VisibilityConfig
	if visConfig == nil {
		return nil, nil
	}

	ds, ok := f.datastores[storeTypeVisibility]
	if !ok {
		return nil, nil
	}

	store, err := ds.factory.NewVisibilityStore()
	if err != nil {
		return nil, err
	}

	result := p.NewVisibilityManagerImpl(store, visConfig.ValidSearchAttributes, f.logger)
	if ds.ratelimit != nil {
		result = p.NewVisibilityPersistenceRateLimitedClient(result, ds.ratelimit, f.logger)
	}

	if visConfig.EnableSampling() {
		result = p.NewVisibilitySamplingClient(result, visConfig, f.metricsClient, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewVisibilityPersistenceMetricsClient(result, f.metricsClient, f.logger)
	}

	return result, nil
}

func (f *factoryImpl) NewNamespaceReplicationQueue() (p.NamespaceReplicationQueue, error) {
	ds := f.datastores[storeTypeQueue]
	result, err := ds.factory.NewQueue(p.NamespaceReplicationQueueType)
	if err != nil {
		return nil, err
	}
	if ds.ratelimit != nil {
		result = p.NewQueuePersistenceRateLimitedClient(result, ds.ratelimit, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewQueuePersistenceMetricsClient(result, f.metricsClient, f.logger)
	}

	return p.NewNamespaceReplicationQueue(result, f.clusterName, f.metricsClient, f.logger), nil
}

// Close closes this factory
func (f *factoryImpl) Close() {
	ds := f.datastores[storeTypeExecution]
	ds.factory.Close()
	visDs, ok := f.datastores[storeTypeVisibility]
	if ok {
		visDs.factory.Close()
	}
}

func (f *factoryImpl) isCassandra() bool {
	cfg := f.config
	return cfg.DataStores[cfg.VisibilityStore].SQL == nil
}

func (f *factoryImpl) getCassandraConfig() *config.Cassandra {
	cfg := f.config
	return cfg.DataStores[cfg.VisibilityStore].Cassandra
}

func (f *factoryImpl) init(
	clusterName string,
	limiters map[string]quotas.RateLimiter,
	r resolver.ServiceResolver,
) {

	f.datastores = make(map[storeType]Datastore, len(storeTypes))

	defaultCfg := f.config.DataStores[f.config.DefaultStore]
	defaultDataStore := Datastore{ratelimit: limiters[f.config.DefaultStore]}
	switch {
	case defaultCfg.Cassandra != nil:
		defaultDataStore.factory = cassandra.NewFactory(*defaultCfg.Cassandra, r, clusterName, f.logger)
	case defaultCfg.SQL != nil:
		defaultDataStore.factory = sql.NewFactory(*defaultCfg.SQL, r, clusterName, f.logger)
	case defaultCfg.CustomDataStoreConfig != nil:
		defaultDataStore.factory = f.abstractDataStoreFactory.NewFactory(*defaultCfg.CustomDataStoreConfig, clusterName, f.logger)
	default:
		f.logger.Fatal("invalid config: one of cassandra or sql params must be specified for default data store")
	}
	for _, sType := range storeTypes {
		if sType != storeTypeVisibility {
			f.datastores[sType] = defaultDataStore
		}
	}

	if f.config.VisibilityStore != "" {
		visibilityCfg := f.config.DataStores[f.config.VisibilityStore]
		visibilityDataStore := Datastore{ratelimit: limiters[f.config.VisibilityStore]}
		switch {
		case visibilityCfg.Cassandra != nil:
			visibilityDataStore.factory = cassandra.NewFactory(*visibilityCfg.Cassandra, r, clusterName, f.logger)
		case visibilityCfg.SQL != nil:
			visibilityDataStore.factory = sql.NewFactory(*visibilityCfg.SQL, r, clusterName, f.logger)
		default:
			f.logger.Fatal("invalid config: one of cassandra or sql params must be specified for visibility store")
		}
		f.datastores[storeTypeVisibility] = visibilityDataStore
	}
}

func buildRateLimiters(
	cfg *config.Persistence,
	maxQPS dynamicconfig.IntPropertyFn,
) map[string]quotas.RateLimiter {

	result := make(map[string]quotas.RateLimiter, len(cfg.DataStores))
	for dsName := range cfg.DataStores {
		if maxQPS != nil && maxQPS() > 0 {
			result[dsName] = quotas.NewDefaultOutgoingDynamicRateLimiter(
				func() float64 { return float64(maxQPS()) },
			)
		}
	}
	return result
}
