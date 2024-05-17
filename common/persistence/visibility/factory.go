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

package visibility

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/persistence/visibility/store/sql"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

type VisibilityStoreFactory interface {
	NewVisibilityStore(
		cfg config.CustomDatastoreConfig,
		r resolver.ServiceResolver,
		logger log.Logger,
		metricsHandler metrics.Handler,
	) (store.VisibilityStore, error)
}

func NewManager(
	persistenceCfg config.Persistence,
	persistenceResolver resolver.ServiceResolver,
	customVisibilityStoreFactory VisibilityStoreFactory,

	esClient esclient.Client,
	esProcessorConfig *elasticsearch.ProcessorConfig,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,

	maxReadQPS dynamicconfig.IntPropertyFn,
	maxWriteQPS dynamicconfig.IntPropertyFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
	enableReadFromSecondaryVisibility dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	secondaryVisibilityWritingMode dynamicconfig.StringPropertyFn,
	visibilityDisableOrderByClause dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	visibilityEnableManualPagination dynamicconfig.BoolPropertyFnWithNamespaceFilter,

	metricsHandler metrics.Handler,
	logger log.Logger,
) (manager.VisibilityManager, error) {
	visibilityManager, err := newVisibilityManagerFromDataStoreConfig(
		persistenceCfg.GetVisibilityStoreConfig(),
		persistenceResolver,
		customVisibilityStoreFactory,
		esClient,
		esProcessorConfig,
		searchAttributesProvider,
		searchAttributesMapperProvider,
		maxReadQPS,
		maxWriteQPS,
		operatorRPSRatio,
		visibilityDisableOrderByClause,
		visibilityEnableManualPagination,
		metricsHandler,
		logger,
	)
	if err != nil {
		return nil, err
	}
	if visibilityManager == nil {
		logger.Fatal("invalid config: visibility store must be configured")
		return nil, nil
	}

	secondaryVisibilityManager, err := newVisibilityManagerFromDataStoreConfig(
		persistenceCfg.GetSecondaryVisibilityStoreConfig(),
		persistenceResolver,
		customVisibilityStoreFactory,
		esClient,
		esProcessorConfig,
		searchAttributesProvider,
		searchAttributesMapperProvider,
		maxReadQPS,
		maxWriteQPS,
		operatorRPSRatio,
		visibilityDisableOrderByClause,
		visibilityEnableManualPagination,
		metricsHandler,
		logger,
	)
	if err != nil {
		return nil, err
	}

	if secondaryVisibilityManager != nil {
		isPrimaryAdvancedSQL := false
		isSecondaryAdvancedSQL := false
		switch visibilityManager.GetStoreNames()[0] {
		case mysql.PluginName, postgresql.PluginName, postgresql.PluginNamePGX, sqlite.PluginName:
			isPrimaryAdvancedSQL = true
		}
		switch secondaryVisibilityManager.GetStoreNames()[0] {
		case mysql.PluginName, postgresql.PluginName, postgresql.PluginNamePGX, sqlite.PluginName:
			isSecondaryAdvancedSQL = true
		}
		if isPrimaryAdvancedSQL && !isSecondaryAdvancedSQL {
			logger.Fatal("invalid config: dual visibility combination not supported")
			return nil, nil
		}

		managerSelector := newDefaultManagerSelector(
			visibilityManager,
			secondaryVisibilityManager,
			enableReadFromSecondaryVisibility,
			secondaryVisibilityWritingMode,
		)
		return NewVisibilityManagerDual(
			visibilityManager,
			secondaryVisibilityManager,
			managerSelector,
		), nil
	}

	return visibilityManager, nil
}

func newVisibilityManager(
	visStore store.VisibilityStore,
	maxReadQPS dynamicconfig.IntPropertyFn,
	maxWriteQPS dynamicconfig.IntPropertyFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
	metricsHandler metrics.Handler,
	visibilityPluginNameTag metrics.Tag,
	logger log.Logger,
) manager.VisibilityManager {
	if visStore == nil {
		return nil
	}
	logger.Info(
		"creating new visibility manager",
		tag.NewStringTag(visibilityPluginNameTag.Key(), visibilityPluginNameTag.Value()),
		tag.NewStringTag("visibility_index_name", visStore.GetIndexName()),
	)
	var visManager manager.VisibilityManager = newVisibilityManagerImpl(visStore, logger)

	// wrap with rate limiter
	visManager = NewVisibilityManagerRateLimited(
		visManager,
		maxReadQPS,
		maxWriteQPS,
		operatorRPSRatio,
	)
	// wrap with metrics client
	visManager = NewVisibilityManagerMetrics(
		visManager,
		metricsHandler,
		logger,
		visibilityPluginNameTag,
	)
	return visManager
}

//nolint:revive // too many arguments
func newVisibilityManagerFromDataStoreConfig(
	dsConfig config.DataStore,
	persistenceResolver resolver.ServiceResolver,
	customVisibilityStoreFactory VisibilityStoreFactory,

	esClient esclient.Client,
	esProcessorConfig *elasticsearch.ProcessorConfig,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,

	maxReadQPS dynamicconfig.IntPropertyFn,
	maxWriteQPS dynamicconfig.IntPropertyFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
	visibilityDisableOrderByClause dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	visibilityEnableManualPagination dynamicconfig.BoolPropertyFnWithNamespaceFilter,

	metricsHandler metrics.Handler,
	logger log.Logger,
) (manager.VisibilityManager, error) {
	visStore, err := newVisibilityStoreFromDataStoreConfig(
		dsConfig,
		persistenceResolver,
		customVisibilityStoreFactory,
		esClient,
		esProcessorConfig,
		searchAttributesProvider,
		searchAttributesMapperProvider,
		visibilityDisableOrderByClause,
		visibilityEnableManualPagination,
		metricsHandler,
		logger,
	)
	if err != nil {
		return nil, err
	}
	if visStore == nil {
		return nil, nil
	}
	return newVisibilityManager(
		visStore,
		maxReadQPS,
		maxWriteQPS,
		operatorRPSRatio,
		metricsHandler,
		metrics.VisibilityPluginNameTag(visStore.GetName()),
		logger,
	), nil
}

func newVisibilityStoreFromDataStoreConfig(
	dsConfig config.DataStore,
	persistenceResolver resolver.ServiceResolver,
	customVisibilityStoreFactory VisibilityStoreFactory,

	esClient esclient.Client,
	esProcessorConfig *elasticsearch.ProcessorConfig,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	visibilityDisableOrderByClause dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	visibilityEnableManualPagination dynamicconfig.BoolPropertyFnWithNamespaceFilter,

	metricsHandler metrics.Handler,
	logger log.Logger,
) (store.VisibilityStore, error) {
	var (
		visStore store.VisibilityStore
		err      error
	)
	if dsConfig.SQL != nil {
		visStore, err = sql.NewSQLVisibilityStore(
			*dsConfig.SQL,
			persistenceResolver,
			searchAttributesProvider,
			searchAttributesMapperProvider,
			logger,
			metricsHandler,
		)
	} else if dsConfig.Elasticsearch != nil {
		visStore = newElasticsearchVisibilityStore(
			dsConfig.Elasticsearch.GetVisibilityIndex(),
			esClient,
			esProcessorConfig,
			searchAttributesProvider,
			searchAttributesMapperProvider,
			visibilityDisableOrderByClause,
			visibilityEnableManualPagination,
			metricsHandler,
			logger,
		)
	} else if dsConfig.CustomDataStoreConfig != nil {
		visStore, err = customVisibilityStoreFactory.NewVisibilityStore(
			*dsConfig.CustomDataStoreConfig,
			persistenceResolver,
			logger,
			metricsHandler,
		)
	}
	return visStore, err
}

func newElasticsearchVisibilityStore(
	defaultIndexName string,
	esClient esclient.Client,
	esProcessorConfig *elasticsearch.ProcessorConfig,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	visibilityDisableOrderByClause dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	visibilityEnableManualPagination dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	metricsHandler metrics.Handler,
	logger log.Logger,
) store.VisibilityStore {
	if esClient == nil {
		return nil
	}

	var (
		esProcessor           elasticsearch.Processor
		esProcessorAckTimeout dynamicconfig.DurationPropertyFn
	)
	if esProcessorConfig != nil {
		esProcessor = elasticsearch.NewProcessor(esProcessorConfig, esClient, logger, metricsHandler)
		esProcessor.Start()
		esProcessorAckTimeout = esProcessorConfig.ESProcessorAckTimeout
	}
	s := elasticsearch.NewVisibilityStore(
		esClient,
		defaultIndexName,
		searchAttributesProvider,
		searchAttributesMapperProvider,
		esProcessor,
		esProcessorAckTimeout,
		visibilityDisableOrderByClause,
		visibilityEnableManualPagination,
		metricsHandler)
	return s
}
