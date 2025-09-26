package visibility

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/persistence/visibility/store/sql"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

type VisibilityStoreFactory interface {
	NewVisibilityStore(
		cfg config.CustomDatastoreConfig,
		saProvider searchattribute.Provider,
		saMapperProvider searchattribute.MapperProvider,
		nsRegistry namespace.Registry,
		r resolver.ServiceResolver,
		logger log.Logger,
		metricsHandler metrics.Handler,
	) (store.VisibilityStore, error)
}

func NewManager(
	persistenceCfg config.Persistence,
	persistenceResolver resolver.ServiceResolver,
	customVisibilityStoreFactory VisibilityStoreFactory,

	esProcessorConfig *elasticsearch.ProcessorConfig,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	namespaceRegistry namespace.Registry,

	maxReadQPS dynamicconfig.IntPropertyFn,
	maxWriteQPS dynamicconfig.IntPropertyFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
	slowQueryThreshold dynamicconfig.DurationPropertyFn,
	enableReadFromSecondaryVisibility dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	visibilityEnableShadowReadMode dynamicconfig.BoolPropertyFn,
	secondaryVisibilityWritingMode dynamicconfig.StringPropertyFn,
	visibilityDisableOrderByClause dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	visibilityEnableManualPagination dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	visibilityEnableUnifiedQueryConverter dynamicconfig.BoolPropertyFn,

	metricsHandler metrics.Handler,
	logger log.Logger,
) (manager.VisibilityManager, error) {
	visibilityManager, err := newVisibilityManagerFromDataStoreConfig(
		persistenceCfg.GetVisibilityStoreConfig(),
		persistenceResolver,
		customVisibilityStoreFactory,
		esProcessorConfig,
		searchAttributesProvider,
		searchAttributesMapperProvider,
		namespaceRegistry,
		maxReadQPS,
		maxWriteQPS,
		operatorRPSRatio,
		slowQueryThreshold,
		visibilityDisableOrderByClause,
		visibilityEnableManualPagination,
		visibilityEnableUnifiedQueryConverter,
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
		esProcessorConfig,
		searchAttributesProvider,
		searchAttributesMapperProvider,
		namespaceRegistry,
		maxReadQPS,
		maxWriteQPS,
		operatorRPSRatio,
		slowQueryThreshold,
		visibilityDisableOrderByClause,
		visibilityEnableManualPagination,
		visibilityEnableUnifiedQueryConverter,
		metricsHandler,
		logger,
	)
	if err != nil {
		return nil, err
	}

	if secondaryVisibilityManager != nil {
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
			visibilityEnableShadowReadMode,
		), nil
	}

	return visibilityManager, nil
}

func newVisibilityManager(
	visStore store.VisibilityStore,
	maxReadQPS dynamicconfig.IntPropertyFn,
	maxWriteQPS dynamicconfig.IntPropertyFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
	slowQueryThreshold dynamicconfig.DurationPropertyFn,
	metricsHandler metrics.Handler,
	visibilityPluginNameTag metrics.Tag,
	visibilityIndexNameTag metrics.Tag,
	logger log.Logger,
) manager.VisibilityManager {
	if visStore == nil {
		return nil
	}
	logger.Info(
		"creating new visibility manager",
		tag.NewStringTag(visibilityPluginNameTag.Key(), visibilityPluginNameTag.Value()),
		tag.NewStringTag(visibilityIndexNameTag.Key(), visibilityIndexNameTag.Value()),
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
		slowQueryThreshold,
		visibilityPluginNameTag,
		visibilityIndexNameTag,
	)
	return visManager
}

//nolint:revive // too many arguments
func newVisibilityManagerFromDataStoreConfig(
	dsConfig config.DataStore,
	persistenceResolver resolver.ServiceResolver,
	customVisibilityStoreFactory VisibilityStoreFactory,

	esProcessorConfig *elasticsearch.ProcessorConfig,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	namespaceRegistry namespace.Registry,

	maxReadQPS dynamicconfig.IntPropertyFn,
	maxWriteQPS dynamicconfig.IntPropertyFn,
	operatorRPSRatio dynamicconfig.FloatPropertyFn,
	slowQueryThreshold dynamicconfig.DurationPropertyFn,
	visibilityDisableOrderByClause dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	visibilityEnableManualPagination dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	visibilityEnableUnifiedQueryConverter dynamicconfig.BoolPropertyFn,

	metricsHandler metrics.Handler,
	logger log.Logger,
) (manager.VisibilityManager, error) {
	visStore, err := newVisibilityStoreFromDataStoreConfig(
		dsConfig,
		persistenceResolver,
		customVisibilityStoreFactory,
		esProcessorConfig,
		searchAttributesProvider,
		searchAttributesMapperProvider,
		namespaceRegistry,
		visibilityDisableOrderByClause,
		visibilityEnableManualPagination,
		visibilityEnableUnifiedQueryConverter,
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
		slowQueryThreshold,
		metricsHandler,
		metrics.VisibilityPluginNameTag(visStore.GetName()),
		metrics.VisibilityIndexNameTag(visStore.GetIndexName()),
		logger,
	), nil
}

func newVisibilityStoreFromDataStoreConfig(
	dsConfig config.DataStore,
	persistenceResolver resolver.ServiceResolver,
	customVisibilityStoreFactory VisibilityStoreFactory,

	esProcessorConfig *elasticsearch.ProcessorConfig,
	searchAttributesProvider searchattribute.Provider,
	searchAttributesMapperProvider searchattribute.MapperProvider,
	namespaceRegistry namespace.Registry,
	visibilityDisableOrderByClause dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	visibilityEnableManualPagination dynamicconfig.BoolPropertyFnWithNamespaceFilter,
	visibilityEnableUnifiedQueryConverter dynamicconfig.BoolPropertyFn,

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
			visibilityEnableUnifiedQueryConverter,
			logger,
			metricsHandler,
		)
	} else if dsConfig.Elasticsearch != nil {
		visStore, err = elasticsearch.NewVisibilityStore(
			dsConfig.Elasticsearch,
			esProcessorConfig,
			searchAttributesProvider,
			searchAttributesMapperProvider,
			visibilityDisableOrderByClause,
			visibilityEnableManualPagination,
			visibilityEnableUnifiedQueryConverter,
			metricsHandler,
			logger,
		)
	} else if dsConfig.CustomDataStoreConfig != nil {
		if customVisibilityStoreFactory == nil {
			logger.Fatal("custom visibility store factory must be defined")
			return nil, nil
		}
		visStore, err = customVisibilityStoreFactory.NewVisibilityStore(
			*dsConfig.CustomDataStoreConfig,
			searchAttributesProvider,
			searchAttributesMapperProvider,
			namespaceRegistry,
			persistenceResolver,
			logger,
			metricsHandler,
		)
	}
	return visStore, err
}
