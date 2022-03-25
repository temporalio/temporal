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
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/quotas"
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
		// NewMetadataManager returns a new metadata manager
		NewMetadataManager() (p.MetadataManager, error)
		// NewExecutionManager returns a new execution manager
		NewExecutionManager() (p.ExecutionManager, error)
		// NewNamespaceReplicationQueue returns a new queue for namespace replication
		NewNamespaceReplicationQueue() (p.NamespaceReplicationQueue, error)
		// NewClusterMetadataManager returns a new manager for cluster specific metadata
		NewClusterMetadataManager() (p.ClusterMetadataManager, error)
	}

	factoryImpl struct {
		dataStoreFactory DataStoreFactory
		config           *config.Persistence
		serializer       serialization.Serializer
		metricsClient    metrics.Client
		logger           log.Logger
		clusterName      string
		ratelimiter      quotas.RateLimiter
	}
)

// NewFactory returns an implementation of factory that vends persistence objects based on
// specified configuration. This factory takes as input a config.Persistence object
// which specifies the datastore to be used for a given type of object. This config
// also contains config for individual datastores themselves.
//
// The objects returned by this factory enforce ratelimit and maxconns according to
// given configuration. In addition, all objects will emit metrics automatically
func NewFactory(
	dataStoreFactory DataStoreFactory,
	cfg *config.Persistence,
	ratelimiter quotas.RateLimiter,
	serializer serialization.Serializer,
	clusterName string,
	metricsClient metrics.Client,
	logger log.Logger,
) Factory {
	return &factoryImpl{
		dataStoreFactory: dataStoreFactory,
		config:           cfg,
		serializer:       serializer,
		metricsClient:    metricsClient,
		logger:           logger,
		clusterName:      clusterName,
		ratelimiter:      ratelimiter,
	}
}

// NewTaskManager returns a new task manager
func (f *factoryImpl) NewTaskManager() (p.TaskManager, error) {
	taskStore, err := f.dataStoreFactory.NewTaskStore()
	if err != nil {
		return nil, err
	}

	result := p.NewTaskManager(taskStore, f.serializer)
	if f.ratelimiter != nil {
		result = p.NewTaskPersistenceRateLimitedClient(result, f.ratelimiter, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewTaskPersistenceMetricsClient(result, f.metricsClient, f.logger)
	}
	return result, nil
}

// NewShardManager returns a new shard manager
func (f *factoryImpl) NewShardManager() (p.ShardManager, error) {
	shardStore, err := f.dataStoreFactory.NewShardStore()
	if err != nil {
		return nil, err
	}

	result := p.NewShardManager(shardStore, f.serializer)
	if f.ratelimiter != nil {
		result = p.NewShardPersistenceRateLimitedClient(result, f.ratelimiter, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewShardPersistenceMetricsClient(result, f.metricsClient, f.logger)
	}
	return result, nil
}

// NewMetadataManager returns a new metadata manager
func (f *factoryImpl) NewMetadataManager() (p.MetadataManager, error) {
	store, err := f.dataStoreFactory.NewMetadataStore()
	if err != nil {
		return nil, err
	}

	result := p.NewMetadataManagerImpl(store, f.serializer, f.logger, f.clusterName)
	if f.ratelimiter != nil {
		result = p.NewMetadataPersistenceRateLimitedClient(result, f.ratelimiter, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewMetadataPersistenceMetricsClient(result, f.metricsClient, f.logger)
	}
	return result, nil
}

// NewClusterMetadataManager returns a new cluster metadata manager
func (f *factoryImpl) NewClusterMetadataManager() (p.ClusterMetadataManager, error) {
	store, err := f.dataStoreFactory.NewClusterMetadataStore()
	if err != nil {
		return nil, err
	}

	result := p.NewClusterMetadataManagerImpl(store, f.serializer, f.clusterName, f.logger)
	if f.ratelimiter != nil {
		result = p.NewClusterMetadataPersistenceRateLimitedClient(result, f.ratelimiter, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewClusterMetadataPersistenceMetricsClient(result, f.metricsClient, f.logger)
	}
	return result, nil
}

// NewExecutionManager returns a new execution manager
func (f *factoryImpl) NewExecutionManager() (p.ExecutionManager, error) {
	store, err := f.dataStoreFactory.NewExecutionStore()
	if err != nil {
		return nil, err
	}

	result := p.NewExecutionManager(store, f.serializer, f.logger, f.config.TransactionSizeLimit)
	if f.ratelimiter != nil {
		result = p.NewExecutionPersistenceRateLimitedClient(result, f.ratelimiter, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewExecutionPersistenceMetricsClient(result, f.metricsClient, f.logger)
	}
	return result, nil
}

func (f *factoryImpl) NewNamespaceReplicationQueue() (p.NamespaceReplicationQueue, error) {
	result, err := f.dataStoreFactory.NewQueue(p.NamespaceReplicationQueueType)
	if err != nil {
		return nil, err
	}

	if f.ratelimiter != nil {
		result = p.NewQueuePersistenceRateLimitedClient(result, f.ratelimiter, f.logger)
	}
	if f.metricsClient != nil {
		result = p.NewQueuePersistenceMetricsClient(result, f.metricsClient, f.logger)
	}

	return p.NewNamespaceReplicationQueue(result, f.serializer, f.clusterName, f.metricsClient, f.logger)
}

// Close closes this factory
func (f *factoryImpl) Close() {
	f.dataStoreFactory.Close()
}
