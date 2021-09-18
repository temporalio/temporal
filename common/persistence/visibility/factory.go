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
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/cassandra"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/persistence/visibility/store/sql"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

func NewManager(
	persistenceCfg config.Persistence,
	persistenceResolver resolver.ServiceResolver,

	defaultIndexName string,
	esClient esclient.Client,
	esProcessorConfig *elasticsearch.ProcessorConfig,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapper searchattribute.Mapper,

	standardVisibilityPersistenceMaxReadQPS dynamicconfig.IntPropertyFn,
	standardVisibilityPersistenceMaxWriteQPS dynamicconfig.IntPropertyFn,
	advancedVisibilityPersistenceMaxReadQPS dynamicconfig.IntPropertyFn,
	advancedVisibilityPersistenceMaxWriteQPS dynamicconfig.IntPropertyFn,
	enableAdvancedVisibilityRead dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	advancedVisibilityWritingMode dynamicconfig.StringPropertyFn,

	metricsClient metrics.Client,
	logger log.Logger,
) (manager.VisibilityManager, error) {
	stdVisibilityManager, err := NewStandardManager(
		persistenceCfg,
		persistenceResolver,
		standardVisibilityPersistenceMaxReadQPS,
		standardVisibilityPersistenceMaxWriteQPS,
		metricsClient,
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
		searchAttributesMapper,
		advancedVisibilityPersistenceMaxReadQPS,
		advancedVisibilityPersistenceMaxWriteQPS,
		metricsClient,
		logger,
	)
	if err != nil {
		return nil, err
	}

	if stdVisibilityManager == nil && advVisibilityManager == nil {
		logger.Fatal("invalid config: one of standard or advanced visibility must be configured")
		return nil, nil
	}

	if stdVisibilityManager != nil && advVisibilityManager == nil {
		return stdVisibilityManager, nil
	}

	if stdVisibilityManager == nil && advVisibilityManager != nil {
		return advVisibilityManager, nil
	}

	// If both visibilities are configured use dual write.
	return NewVisibilityManagerDual(
		stdVisibilityManager,
		advVisibilityManager,
		enableAdvancedVisibilityRead,
		advancedVisibilityWritingMode,
	), nil
}

func NewStandardManager(
	persistenceCfg config.Persistence,
	persistenceResolver resolver.ServiceResolver,

	standardVisibilityPersistenceMaxReadQPS dynamicconfig.IntPropertyFn,
	standardVisibilityPersistenceMaxWriteQPS dynamicconfig.IntPropertyFn,

	metricsClient metrics.Client,
	logger log.Logger,
) (*visibilityManager, error) {

	stdVisibilityStore, err := newStandardVisibilityStore(
		persistenceCfg,
		persistenceResolver,
		logger)
	if err != nil {
		return nil, err
	}

	var stdVisibilityManager *visibilityManager
	if stdVisibilityStore != nil {
		stdVisibilityManager = newVisibilityManager(
			stdVisibilityStore,
			standardVisibilityPersistenceMaxReadQPS,
			standardVisibilityPersistenceMaxWriteQPS,
			metricsClient,
			metrics.StandardVisibilityTypeTag(),
			logger)
	}
	return stdVisibilityManager, nil
}

func NewAdvancedManager(
	defaultIndexName string,
	esClient esclient.Client,
	esProcessorConfig *elasticsearch.ProcessorConfig,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapper searchattribute.Mapper,

	advancedVisibilityPersistenceMaxReadQPS dynamicconfig.IntPropertyFn,
	advancedVisibilityPersistenceMaxWriteQPS dynamicconfig.IntPropertyFn,

	metricsClient metrics.Client,
	logger log.Logger,
) (*visibilityManager, error) {
	advVisibilityStore := newAdvancedVisibilityStore(
		defaultIndexName,
		esClient,
		esProcessorConfig,
		searchAttributesProvider,
		searchAttributesMapper,
		metricsClient,
		logger)

	var advVisibilityManager *visibilityManager
	if advVisibilityStore != nil {
		advVisibilityManager = newVisibilityManager(
			advVisibilityStore,
			advancedVisibilityPersistenceMaxReadQPS,
			advancedVisibilityPersistenceMaxWriteQPS,
			metricsClient,
			metrics.AdvancedVisibilityTypeTag(),
			logger)
	}
	return advVisibilityManager, nil
}

func newStandardVisibilityStore(
	persistenceCfg config.Persistence,
	persistenceResolver resolver.ServiceResolver,
	logger log.Logger,
) (store.VisibilityStore, error) {
	// If standard visibility is not configured.
	if persistenceCfg.VisibilityStore == "" {
		return nil, nil
	}

	visibilityStoreCfg := persistenceCfg.DataStores[persistenceCfg.VisibilityStore]

	var (
		store store.VisibilityStore
		err   error
	)
	switch {
	case visibilityStoreCfg.Cassandra != nil:
		store, err = cassandra.NewVisibilityStore(*visibilityStoreCfg.Cassandra, persistenceResolver, logger)
	case visibilityStoreCfg.SQL != nil:
		store, err = sql.NewSQLVisibilityStore(*visibilityStoreCfg.SQL, persistenceResolver, logger)
	}

	if err != nil {
		return nil, err
	}

	if store == nil {
		logger.Fatal("invalid config: one of cassandra or sql params must be specified for visibility store")
		return nil, nil
	}

	return store, nil
}

func newAdvancedVisibilityStore(
	defaultIndexName string,
	esClient esclient.Client,
	esProcessorConfig *elasticsearch.ProcessorConfig,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapper searchattribute.Mapper,
	metricsClient metrics.Client,
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
		esProcessor = elasticsearch.NewProcessor(esProcessorConfig, esClient, logger, metricsClient)
		esProcessor.Start()
		esProcessorAckTimeout = esProcessorConfig.ESProcessorAckTimeout
	}
	s := elasticsearch.NewVisibilityStore(
		esClient,
		defaultIndexName,
		searchAttributesProvider,
		searchAttributesMapper,
		esProcessor,
		esProcessorAckTimeout,
		metricsClient)
	return s
}
