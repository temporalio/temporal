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
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/persistence/visibility/store/sql"
	"go.temporal.io/server/common/persistence/visibility/store/standard"
	"go.temporal.io/server/common/persistence/visibility/store/standard/cassandra"
	standardSql "go.temporal.io/server/common/persistence/visibility/store/standard/sql"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

func NewManager(
	persistenceCfg config.Persistence,
	persistenceResolver resolver.ServiceResolver,

	defaultIndexName string,
	secondaryVisibilityIndexName string,
	esClient esclient.Client,
	esProcessorConfig *elasticsearch.ProcessorConfig,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,

	standardVisibilityPersistenceMaxReadQPS dynamicconfig.IntPropertyFn,
	standardVisibilityPersistenceMaxWriteQPS dynamicconfig.IntPropertyFn,
	advancedVisibilityPersistenceMaxReadQPS dynamicconfig.IntPropertyFn,
	advancedVisibilityPersistenceMaxWriteQPS dynamicconfig.IntPropertyFn,
	enableAdvancedVisibilityRead dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	advancedVisibilityWritingMode dynamicconfig.StringPropertyFn,
	enableReadFromSecondaryAdvancedVisibility dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	enableWriteToSecondaryAdvancedVisibility dynamicconfig.BoolPropertyFn,
	visibilityDisableOrderByClause dynamicconfig.BoolPropertyFn,

	metricsHandler metrics.Handler,
	logger log.Logger,
) (manager.VisibilityManager, error) {
	stdVisibilityManager, err := NewStandardManager(
		persistenceCfg,
		persistenceResolver,
		searchAttributesProvider,
		searchAttributesMapperProvider,
		standardVisibilityPersistenceMaxReadQPS,
		standardVisibilityPersistenceMaxWriteQPS,
		metricsHandler,
		logger,
	)
	if err != nil {
		return nil, err
	}

	advVisibilityManager, err := NewAdvancedManager(
		defaultIndexName,
		esClient,
		esProcessorConfig,
		searchAttributesProvider,
		searchAttributesMapperProvider,
		advancedVisibilityPersistenceMaxReadQPS,
		advancedVisibilityPersistenceMaxWriteQPS,
		visibilityDisableOrderByClause,
		metricsHandler,
		logger,
	)
	if err != nil {
		return nil, err
	}

	secondaryVisibilityManager, err := NewAdvancedManager(
		secondaryVisibilityIndexName,
		esClient,
		esProcessorConfig,
		searchAttributesProvider,
		searchAttributesMapperProvider,
		advancedVisibilityPersistenceMaxReadQPS,
		advancedVisibilityPersistenceMaxWriteQPS,
		visibilityDisableOrderByClause,
		metricsHandler,
		logger,
	)
	if err != nil {
		return nil, err
	}

	if stdVisibilityManager == nil && advVisibilityManager == nil {
		logger.Fatal("invalid config: one of standard or advanced visibility must be configured")
		return nil, nil
	}

	if stdVisibilityManager != nil && secondaryVisibilityManager != nil {
		logger.Fatal("invalid config: secondary visibility store cannot be used with standard visibility")
		return nil, nil
	}

	if stdVisibilityManager != nil && advVisibilityManager == nil {
		return stdVisibilityManager, nil
	}

	if stdVisibilityManager == nil && advVisibilityManager != nil {
		if secondaryVisibilityManager == nil {
			return advVisibilityManager, nil
		}

		// Dual write to primary and secondary ES indices.
		managerSelector := NewESManagerSelector(
			advVisibilityManager,
			secondaryVisibilityManager,
			enableReadFromSecondaryAdvancedVisibility,
			enableWriteToSecondaryAdvancedVisibility)

		return NewVisibilityManagerDual(
			advVisibilityManager,
			secondaryVisibilityManager,
			managerSelector,
		), nil
	}

	// Dual write to standard and advanced visibility.
	managerSelector := NewSQLToESManagerSelector(
		stdVisibilityManager,
		advVisibilityManager,
		enableAdvancedVisibilityRead,
		advancedVisibilityWritingMode)
	return NewVisibilityManagerDual(
		stdVisibilityManager,
		advVisibilityManager,
		managerSelector,
	), nil
}

func NewStandardManager(
	persistenceCfg config.Persistence,
	persistenceResolver resolver.ServiceResolver,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,

	standardVisibilityPersistenceMaxReadQPS dynamicconfig.IntPropertyFn,
	standardVisibilityPersistenceMaxWriteQPS dynamicconfig.IntPropertyFn,

	metricsHandler metrics.Handler,
	logger log.Logger,
) (manager.VisibilityManager, error) {

	stdVisibilityStore, err := newStandardVisibilityStore(
		persistenceCfg,
		persistenceResolver,
		searchAttributesProvider,
		searchAttributesMapperProvider,
		logger)
	if err != nil {
		return nil, err
	}

	return newVisibilityManager(
		stdVisibilityStore,
		standardVisibilityPersistenceMaxReadQPS,
		standardVisibilityPersistenceMaxWriteQPS,
		metricsHandler,
		metrics.StandardVisibilityTypeTag(),
		logger), nil
}

func NewAdvancedManager(
	defaultIndexName string,
	esClient esclient.Client,
	esProcessorConfig *elasticsearch.ProcessorConfig,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,

	advancedVisibilityPersistenceMaxReadQPS dynamicconfig.IntPropertyFn,
	advancedVisibilityPersistenceMaxWriteQPS dynamicconfig.IntPropertyFn,
	visibilityDisableOrderByClause dynamicconfig.BoolPropertyFn,

	metricsHandler metrics.Handler,
	logger log.Logger,
) (manager.VisibilityManager, error) {
	if defaultIndexName == "" {
		return nil, nil
	}

	advVisibilityStore := newAdvancedVisibilityStore(
		defaultIndexName,
		esClient,
		esProcessorConfig,
		searchAttributesProvider,
		searchAttributesMapperProvider,
		visibilityDisableOrderByClause,
		metricsHandler,
		logger)

	return newVisibilityManager(
		advVisibilityStore,
		advancedVisibilityPersistenceMaxReadQPS,
		advancedVisibilityPersistenceMaxWriteQPS,
		metricsHandler,
		metrics.AdvancedVisibilityTypeTag(),
		logger,
	), nil
}

func newVisibilityManager(
	store store.VisibilityStore,
	maxReadQPS dynamicconfig.IntPropertyFn,
	maxWriteQPS dynamicconfig.IntPropertyFn,
	metricsHandler metrics.Handler,
	tag metrics.Tag,
	logger log.Logger,
) manager.VisibilityManager {
	if store == nil {
		return nil
	}

	var manager manager.VisibilityManager = newVisibilityManagerImpl(store, logger)

	// wrap with rate limiter
	manager = NewVisibilityManagerRateLimited(
		manager,
		maxReadQPS,
		maxWriteQPS)
	// wrap with metrics client
	manager = NewVisibilityManagerMetrics(
		manager,
		metricsHandler,
		logger,
		tag)

	return manager
}

func newStandardVisibilityStore(
	persistenceCfg config.Persistence,
	persistenceResolver resolver.ServiceResolver,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	logger log.Logger,
) (store.VisibilityStore, error) {
	// If standard visibility is not configured.
	if persistenceCfg.VisibilityStore == "" {
		return nil, nil
	}

	visibilityStoreCfg := persistenceCfg.DataStores[persistenceCfg.VisibilityStore]

	var (
		visStore   store.VisibilityStore
		isStandard bool
		err        error
	)
	switch {
	case visibilityStoreCfg.Cassandra != nil:
		visStore, err = cassandra.NewVisibilityStore(*visibilityStoreCfg.Cassandra, persistenceResolver, logger)
		isStandard = true
	case visibilityStoreCfg.SQL != nil:
		switch visibilityStoreCfg.SQL.PluginName {
		case mysql.PluginNameV8:
			isStandard = false
			visStore, err = sql.NewSQLVisibilityStore(
				*visibilityStoreCfg.SQL,
				persistenceResolver,
				searchAttributesProvider,
				searchAttributesMapperProvider,
				logger,
			)
		default:
			isStandard = true
			visStore, err = standardSql.NewSQLVisibilityStore(*visibilityStoreCfg.SQL, persistenceResolver, logger)
		}
	}

	if err != nil {
		return nil, err
	}

	if visStore == nil {
		logger.Fatal("invalid config: one of cassandra or sql params must be specified for visibility store")
		return nil, nil
	}

	if isStandard {
		return standard.NewVisibilityStore(visStore), nil
	}
	return visStore, nil
}

func newAdvancedVisibilityStore(
	defaultIndexName string,
	esClient esclient.Client,
	esProcessorConfig *elasticsearch.ProcessorConfig,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	visibilityDisableOrderByClause dynamicconfig.BoolPropertyFn,
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
		metricsHandler)
	return s
}
