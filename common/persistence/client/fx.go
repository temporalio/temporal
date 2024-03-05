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
	"time"

	"go.temporal.io/server/common/cluster"
	"go.uber.org/fx"

	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas"
)

type (
	// NewFactoryParams are the dependencies needed for FactoryProvider.
	NewFactoryParams struct {
		fx.In

		DataStoreFactory                   DataStoreFactory
		EventBlobCache                     persistence.XDCCache
		Cfg                                *config.Persistence
		PersistenceMaxQPS                  PersistenceMaxQps
		PersistenceNamespaceMaxQPS         PersistenceNamespaceMaxQps
		PersistencePerShardNamespaceMaxQPS PersistencePerShardNamespaceMaxQPS
		EnablePriorityRateLimiting         EnablePriorityRateLimiting
		OperatorRPSRatio                   OperatorRPSRatio
		ClusterName                        ClusterName
		ServiceName                        primitives.ServiceName
		MetricsHandler                     metrics.Handler
		Logger                             log.Logger
		HealthSignals                      persistence.HealthSignalAggregator
		DynamicRateLimitingParams          DynamicRateLimitingParams
	}

	// The below are type aliases which make it easy to distinguish between multiple instances of the same type or to
	// bind to one-off types without repeating their definitions.

	PersistenceMaxQps                  dynamicconfig.IntPropertyFn
	PersistenceNamespaceMaxQps         dynamicconfig.IntPropertyFnWithNamespaceFilter
	PersistencePerShardNamespaceMaxQPS dynamicconfig.IntPropertyFnWithNamespaceFilter
	EnablePriorityRateLimiting         dynamicconfig.BoolPropertyFn
	OperatorRPSRatio                   dynamicconfig.FloatPropertyFn
	DynamicRateLimitingParams          dynamicconfig.MapPropertyFn
	FactoryProviderFn                  func(NewFactoryParams) Factory
	ClusterName                        string
)

// Module provides all the clients from the persistence layer, given the dependencies specified by NewFactoryParams,
// by creating a single Factory, constructing its clients, adding their lifecycle hooks, and then providing them to the graph.
var Module = fx.Provide(
	DataStoreFactoryProvider,
	healthSignalAggregatorProvider,
	eventBlobCacheProvider,
	func(cfg *cluster.Config) ClusterName {
		return ClusterName(cfg.CurrentClusterName)
	},
	func(f Factory, lc fx.Lifecycle) (persistence.ClusterMetadataManager, error) {
		clusterMetadataManager, err := f.NewClusterMetadataManager()
		if err != nil {
			return nil, err
		}
		lc.Append(fx.StopHook(clusterMetadataManager.Close))
		return clusterMetadataManager, nil
	},
	func(f Factory, lc fx.Lifecycle) (persistence.MetadataManager, error) {
		metadataManager, err := f.NewMetadataManager()
		if err != nil {
			return nil, err
		}
		lc.Append(fx.StopHook(metadataManager.Close))
		return metadataManager, nil
	},
	func(f Factory, lc fx.Lifecycle) (persistence.TaskManager, error) {
		taskManager, err := f.NewTaskManager()
		if err != nil {
			return nil, err
		}
		lc.Append(fx.StopHook(taskManager.Close))
		return taskManager, nil
	},
	func(f Factory, lc fx.Lifecycle) (persistence.NamespaceReplicationQueue, error) {
		namespaceReplicationQueue, err := f.NewNamespaceReplicationQueue()
		if err != nil {
			return nil, err
		}
		lc.Append(fx.StopHook(namespaceReplicationQueue.Stop))
		return namespaceReplicationQueue, nil
	},
	func(f Factory, lc fx.Lifecycle) (persistence.ShardManager, error) {
		shardManager, err := f.NewShardManager()
		if err != nil {
			return nil, err
		}
		lc.Append(fx.StopHook(shardManager.Close))
		return shardManager, nil
	},
	func(f Factory, lc fx.Lifecycle) (persistence.ExecutionManager, error) {
		executionManager, err := f.NewExecutionManager()
		if err != nil {
			return nil, err
		}
		lc.Append(fx.StopHook(executionManager.Close))
		return executionManager, nil
	},
	func(f Factory, lc fx.Lifecycle) (persistence.HistoryTaskQueueManager, error) {
		manager, err := f.NewHistoryTaskQueueManager()
		if err != nil {
			return nil, err
		}
		return manager, nil
	},
)

// FactoryProvider is similar to NewFactory, but it also provides a request rate limiter based on the params.
// TODO: get rid of FactoryProviderFn and make this function un-exported.
// Clients should provide their own factory just by using fx graph modifications on the factory directly instead of
// needing to override a factory provider.
func FactoryProvider(
	params NewFactoryParams,
) Factory {
	var requestRatelimiter quotas.RequestRateLimiter
	if params.PersistenceMaxQPS != nil && params.PersistenceMaxQPS() > 0 {
		if params.EnablePriorityRateLimiting != nil && params.EnablePriorityRateLimiting() {
			requestRatelimiter = NewPriorityRateLimiter(
				params.PersistenceNamespaceMaxQPS,
				params.PersistenceMaxQPS,
				params.PersistencePerShardNamespaceMaxQPS,
				RequestPriorityFn,
				params.OperatorRPSRatio,
				params.HealthSignals,
				params.DynamicRateLimitingParams,
				params.MetricsHandler,
				params.Logger,
			)
		} else {
			requestRatelimiter = NewNoopPriorityRateLimiter(params.PersistenceMaxQPS)
		}
	}

	return NewFactory(
		params.DataStoreFactory,
		params.Cfg,
		requestRatelimiter,
		serialization.NewSerializer(),
		params.EventBlobCache,
		string(params.ClusterName),
		params.MetricsHandler,
		params.Logger,
		params.HealthSignals,
	)
}

func eventBlobCacheProvider(
	dc *dynamicconfig.Collection,
) persistence.XDCCache {
	return persistence.NewEventsBlobCache(
		dc.GetIntProperty(dynamicconfig.XDCCacheMaxSizeBytes, 8*1024*1024)(),
		20*time.Second,
	)
}

func healthSignalAggregatorProvider(
	dynamicCollection *dynamicconfig.Collection,
	metricsHandler metrics.Handler,
	logger log.ThrottledLogger,
) persistence.HealthSignalAggregator {
	if dynamicCollection.GetBoolProperty(dynamicconfig.PersistenceHealthSignalMetricsEnabled, true)() {
		return persistence.NewHealthSignalAggregatorImpl(
			dynamicCollection.GetBoolProperty(dynamicconfig.PersistenceHealthSignalAggregationEnabled, true)(),
			dynamicCollection.GetDurationProperty(dynamicconfig.PersistenceHealthSignalWindowSize, 10*time.Second)(),
			dynamicCollection.GetIntProperty(dynamicconfig.PersistenceHealthSignalBufferSize, 5000)(),
			metricsHandler,
			dynamicCollection.GetIntProperty(dynamicconfig.ShardRPSWarnLimit, 50),
			dynamicCollection.GetFloat64Property(dynamicconfig.ShardPerNsRPSWarnPercent, 0.8),
			logger,
		)
	}

	return persistence.NoopHealthSignalAggregator
}
