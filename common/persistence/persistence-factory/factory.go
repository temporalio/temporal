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

package persistence

import (
	"sync"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/cassandra"
	"github.com/uber/cadence/common/persistence/sql"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/service/config"
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
		// NewHistoryV2Manager returns a new historyV2 manager
		NewHistoryV2Manager() (p.HistoryV2Manager, error)
		// NewMetadataManager returns a new metadata manager
		NewMetadataManager() (p.MetadataManager, error)
		// NewExecutionManager returns a new execution manager for a given shardID
		NewExecutionManager(shardID int) (p.ExecutionManager, error)
		// NewVisibilityManager returns a new visibility manager
		NewVisibilityManager() (p.VisibilityManager, error)
		// NewDomainReplicationQueue returns a new queue for domain replication
		NewDomainReplicationQueue() (p.DomainReplicationQueue, error)
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
		// NewHistoryV2Store returns a new historyV2 store
		NewHistoryV2Store() (p.HistoryV2Store, error)
		// NewMetadataStore returns a new metadata store
		NewMetadataStore() (p.MetadataStore, error)
		// NewExecutionStore returns an execution store for given shardID
		NewExecutionStore(shardID int) (p.ExecutionStore, error)
		// NewVisibilityStore returns a new visibility store
		NewVisibilityStore() (p.VisibilityStore, error)
		NewQueue(queueType common.QueueType) (p.Queue, error)
	}
	// Datastore represents a datastore
	Datastore struct {
		factory   DataStoreFactory
		ratelimit quotas.Limiter
	}
	factoryImpl struct {
		sync.RWMutex
		config        *config.Persistence
		metricsClient metrics.Client
		logger        log.Logger
		datastores    map[storeType]Datastore
		clusterName   string
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
)

var storeTypes = []storeType{
	storeTypeHistory,
	storeTypeTask,
	storeTypeShard,
	storeTypeMetadata,
	storeTypeExecution,
	storeTypeVisibility,
	storeTypeQueue,
}

// New returns an implementation of factory that vends persistence objects based on
// specified configuration. This factory takes as input a config.Persistence object
// which specifies the datastore to be used for a given type of object. This config
// also contains config for individual datastores themselves.
//
// The objects returned by this factory enforce ratelimit and maxconns according to
// given configuration. In addition, all objects will emit metrics automatically
func New(
	cfg *config.Persistence,
	clusterName string,
	metricsClient metrics.Client,
	logger log.Logger,
) Factory {
	factory := &factoryImpl{
		config:        cfg,
		metricsClient: metricsClient,
		logger:        logger,
		clusterName:   clusterName,
	}
	limiters := buildRatelimiters(cfg)
	factory.init(clusterName, limiters)
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
func (f *factoryImpl) NewHistoryV2Manager() (p.HistoryV2Manager, error) {
	ds := f.datastores[storeTypeHistory]
	store, err := ds.factory.NewHistoryV2Store()
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

	result := p.NewMetadataManagerImpl(store, f.logger)
	if ds.ratelimit != nil {
		result = p.NewMetadataPersistenceRateLimitedClient(result, ds.ratelimit, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewMetadataPersistenceMetricsClient(result, f.metricsClient, f.logger)
	}
	return result, nil
}

// NewExecutionManager returns a new execution manager for a given shardID
func (f *factoryImpl) NewExecutionManager(shardID int) (p.ExecutionManager, error) {
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
	ds := f.datastores[storeTypeVisibility]
	store, err := ds.factory.NewVisibilityStore()
	if err != nil {
		return nil, err
	}
	visConfig := f.config.VisibilityConfig
	if visConfig != nil && visConfig.EnableReadFromClosedExecutionV2() && f.isCassandra() {
		store, err = cassandra.NewVisibilityPersistenceV2(store, f.getCassandraConfig(), f.logger)
	}

	result := p.NewVisibilityManagerImpl(store, f.logger)
	if ds.ratelimit != nil {
		result = p.NewVisibilityPersistenceRateLimitedClient(result, ds.ratelimit, f.logger)
	}
	if visConfig != nil && visConfig.EnableSampling() {
		result = p.NewVisibilitySamplingClient(result, visConfig, f.metricsClient, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewVisibilityPersistenceMetricsClient(result, f.metricsClient, f.logger)
	}

	return result, nil
}

func (f *factoryImpl) NewDomainReplicationQueue() (p.DomainReplicationQueue, error) {
	ds := f.datastores[storeTypeQueue]
	result, err := ds.factory.NewQueue(common.DomainReplicationQueueType)
	if err != nil {
		return nil, err
	}
	if ds.ratelimit != nil {
		result = p.NewQueuePersistenceRateLimitedClient(result, ds.ratelimit, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewQueuePersistenceMetricsClient(result, f.metricsClient, f.logger)
	}

	return p.NewDomainReplicationQueue(result, f.clusterName, f.metricsClient, f.logger), nil
}

// Close closes this factory
func (f *factoryImpl) Close() {
	ds := f.datastores[storeTypeExecution]
	ds.factory.Close()
}

func (f *factoryImpl) isCassandra() bool {
	cfg := f.config
	return cfg.DataStores[cfg.VisibilityStore].SQL == nil
}

func (f *factoryImpl) getCassandraConfig() *config.Cassandra {
	cfg := f.config
	return cfg.DataStores[cfg.VisibilityStore].Cassandra
}

func (f *factoryImpl) init(clusterName string, limiters map[string]quotas.Limiter) {
	f.datastores = make(map[storeType]Datastore, len(storeTypes))
	defaultCfg := f.config.DataStores[f.config.DefaultStore]
	defaultDataStore := Datastore{ratelimit: limiters[f.config.DefaultStore]}
	switch {
	case defaultCfg.Cassandra != nil:
		defaultDataStore.factory = cassandra.NewFactory(*defaultCfg.Cassandra, clusterName, f.logger)
	case defaultCfg.SQL != nil:
		defaultDataStore.factory = sql.NewFactory(*defaultCfg.SQL, clusterName, f.logger)
	default:
		f.logger.Fatal("invalid config: one of cassandra or sql params must be specified")
	}

	for _, st := range storeTypes {
		if st != storeTypeVisibility {
			f.datastores[st] = defaultDataStore
		}
	}

	visibilityCfg := f.config.DataStores[f.config.VisibilityStore]
	visibilityDataStore := Datastore{ratelimit: limiters[f.config.VisibilityStore]}
	switch {
	case defaultCfg.Cassandra != nil:
		visibilityDataStore.factory = cassandra.NewFactory(*visibilityCfg.Cassandra, clusterName, f.logger)
	case visibilityCfg.SQL != nil:
		visibilityDataStore.factory = sql.NewFactory(*visibilityCfg.SQL, clusterName, f.logger)
	default:
		f.logger.Fatal("invalid config: one of cassandra or sql params must be specified")
	}

	f.datastores[storeTypeVisibility] = visibilityDataStore
}

func buildRatelimiters(cfg *config.Persistence) map[string]quotas.Limiter {
	result := make(map[string]quotas.Limiter, len(cfg.DataStores))
	for dsName, ds := range cfg.DataStores {
		qps := 0
		if ds.Cassandra != nil {
			qps = ds.Cassandra.MaxQPS
		}
		if ds.SQL != nil {
			qps = ds.SQL.MaxQPS
		}
		if qps > 0 {
			result[dsName] = quotas.NewSimpleRateLimiter(qps)
		}
	}
	return result
}
