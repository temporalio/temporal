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
	"go.temporal.io/server/common/persistence/cassandra"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/resolver"
)

type (
	// DataStoreFactory is a low level interface to be implemented by a datastore
	// Examples of datastores are cassandra, mysql etc
	DataStoreFactory interface {
		// Close closes the factory
		Close()
		// NewTaskStore returns a new task store
		NewTaskStore() (p.TaskStore, error)
		// NewShardStore returns a new shard store
		NewShardStore() (p.ShardStore, error)
		// NewMetadataStore returns a new metadata store
		NewMetadataStore() (p.MetadataStore, error)
		// NewExecutionStore returns a new execution store
		NewExecutionStore() (p.ExecutionStore, error)
		NewQueue(queueType p.QueueType) (p.Queue, error)
		// NewClusterMetadataStore returns a new metadata store
		NewClusterMetadataStore() (p.ClusterMetadataStore, error)
	}

	// AbstractDataStoreFactory creates a DataStoreFactory, can be used to implement custom datastore support outside
	// of the Temporal core.
	AbstractDataStoreFactory interface {
		NewFactory(
			cfg config.CustomDatastoreConfig,
			r resolver.ServiceResolver,
			clusterName string,
			logger log.Logger,
			metricsClient metrics.Client,
		) DataStoreFactory
	}
)

func DataStoreFactoryProvider(
	clusterName ClusterName,
	r resolver.ServiceResolver,
	config *config.Persistence,
	abstractDataStoreFactory AbstractDataStoreFactory,
	logger log.Logger,
	metricsClient metrics.Client,
) (DataStoreFactory, *FaultInjectionDataStoreFactory) {

	var dataStoreFactory DataStoreFactory
	defaultCfg := config.DataStores[config.DefaultStore]
	switch {
	case defaultCfg.Cassandra != nil:
		dataStoreFactory = cassandra.NewFactory(*defaultCfg.Cassandra, r, string(clusterName), logger)
	case defaultCfg.SQL != nil:
		dataStoreFactory = sql.NewFactory(*defaultCfg.SQL, r, string(clusterName), logger)
	case defaultCfg.CustomDataStoreConfig != nil:
		dataStoreFactory = abstractDataStoreFactory.NewFactory(*defaultCfg.CustomDataStoreConfig, r, string(clusterName), logger, metricsClient)
	default:
		logger.Fatal("invalid config: one of cassandra or sql params must be specified for default data store")
	}

	var faultInjection *FaultInjectionDataStoreFactory
	if defaultCfg.FaultInjection != nil {
		dataStoreFactory = NewFaultInjectionDatastoreFactory(defaultCfg.FaultInjection, dataStoreFactory)
	}

	return dataStoreFactory, faultInjection
}
