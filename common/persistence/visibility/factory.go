package visibility

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/persistence/visibility/store/sql"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
	"go.uber.org/fx"
)

type VisibilityStoreFactory interface {
	NewVisibilityStore(
		cfg config.CustomDatastoreConfig,
		saProvider searchattribute.Provider,
		saMapperProvider searchattribute.MapperProvider,
		nsRegistry namespace.Registry,
		chasmRegistry *chasm.Registry,
		r resolver.ServiceResolver,
		logger log.Logger,
		metricsHandler metrics.Handler,
	) (store.VisibilityStore, error)
}

type ManagerConfig struct {
	EsProcessorConfig *elasticsearch.ProcessorConfig

	MaxReadQPS                     dynamicconfig.IntPropertyFn
	MaxWriteQPS                    dynamicconfig.IntPropertyFn
	OperatorRPSRatio               dynamicconfig.FloatPropertyFn
	SlowQueryThreshold             dynamicconfig.DurationPropertyFn
	EnableReadFromSecondary        dynamicconfig.BoolPropertyFnWithNamespaceFilter
	EnableShadowReadMode           dynamicconfig.BoolPropertyFn
	SecondaryVisibilityWritingMode dynamicconfig.StringPropertyFn
	DisableOrderByClause           dynamicconfig.BoolPropertyFnWithNamespaceFilter
	EnableManualPagination         dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

type ManagerParams struct {
	fx.In

	PersistenceCfg               *config.Persistence
	PersistenceResolver          resolver.ServiceResolver
	CustomVisibilityStoreFactory VisibilityStoreFactory

	SearchAttributesProvider       searchattribute.Provider
	SearchAttributesMapperProvider searchattribute.MapperProvider
	NamespaceRegistry              namespace.Registry
	ChasmRegistry                  *chasm.Registry

	MetricsHandler metrics.Handler
	Logger         log.Logger
	Serializer     serialization.Serializer
}

func NewManager(
	params *ManagerParams,
	managerConfig *ManagerConfig,
) (manager.VisibilityManager, error) {
	visibilityManager, err := newVisibilityManagerFromDataStoreConfig(
		params.PersistenceCfg.GetVisibilityStoreConfig(),
		params,
		managerConfig,
	)
	if err != nil {
		return nil, err
	}
	if visibilityManager == nil {
		params.Logger.Fatal("invalid config: visibility store must be configured")
		return nil, nil
	}

	secondaryVisibilityManager, err := newVisibilityManagerFromDataStoreConfig(
		params.PersistenceCfg.GetSecondaryVisibilityStoreConfig(),
		params,
		managerConfig,
	)
	if err != nil {
		return nil, err
	}

	if secondaryVisibilityManager != nil {
		managerSelector := newDefaultManagerSelector(
			visibilityManager,
			secondaryVisibilityManager,
			managerConfig.EnableReadFromSecondary,
			managerConfig.SecondaryVisibilityWritingMode,
		)
		return NewVisibilityManagerDual(
			visibilityManager,
			secondaryVisibilityManager,
			managerSelector,
			managerConfig.EnableShadowReadMode,
		), nil
	}

	return visibilityManager, nil
}

func newVisibilityManager(
	visStore store.VisibilityStore,
	visibilityPluginNameTag metrics.Tag,
	visibilityIndexNameTag metrics.Tag,
	params *ManagerParams,
	managerConfig *ManagerConfig,
) manager.VisibilityManager {
	if visStore == nil {
		return nil
	}
	params.Logger.Info(
		"creating new visibility manager",
		tag.String(visibilityPluginNameTag.Key, visibilityPluginNameTag.Value),
		tag.String(visibilityIndexNameTag.Key, visibilityIndexNameTag.Value),
	)
	var visManager manager.VisibilityManager = newVisibilityManagerImpl(
		visStore,
		params.Logger,
		params.SearchAttributesMapperProvider,
		params.ChasmRegistry,
	)

	// wrap with rate limiter
	visManager = NewVisibilityManagerRateLimited(
		visManager,
		managerConfig.MaxReadQPS,
		managerConfig.MaxWriteQPS,
		managerConfig.OperatorRPSRatio,
	)
	// wrap with metrics client
	visManager = NewVisibilityManagerMetrics(
		visManager,
		params.MetricsHandler,
		params.Logger,
		managerConfig.SlowQueryThreshold,
		visibilityPluginNameTag,
		visibilityIndexNameTag,
	)
	return visManager
}

func newVisibilityManagerFromDataStoreConfig(
	dsConfig config.DataStore,
	params *ManagerParams,
	managerConfig *ManagerConfig,
) (manager.VisibilityManager, error) {
	visStore, err := newVisibilityStoreFromDataStoreConfig(dsConfig, params, managerConfig)
	if err != nil {
		return nil, err
	}
	if visStore == nil {
		return nil, nil
	}
	return newVisibilityManager(
		visStore,
		metrics.VisibilityPluginNameTag(visStore.GetName()),
		metrics.VisibilityIndexNameTag(visStore.GetIndexName()),
		params,
		managerConfig,
	), nil
}

func newVisibilityStoreFromDataStoreConfig(
	dsConfig config.DataStore,
	params *ManagerParams,
	managerConfig *ManagerConfig,
) (store.VisibilityStore, error) {
	var (
		visStore store.VisibilityStore
		err      error
	)
	if dsConfig.SQL != nil {
		visStore, err = sql.NewSQLVisibilityStore(
			*dsConfig.SQL,
			params.PersistenceResolver,
			params.SearchAttributesProvider,
			params.SearchAttributesMapperProvider,
			params.ChasmRegistry,
			params.Logger,
			params.MetricsHandler,
			params.Serializer,
		)
	} else if dsConfig.Elasticsearch != nil {
		visStore, err = elasticsearch.NewVisibilityStore(
			dsConfig.Elasticsearch,
			managerConfig.EsProcessorConfig,
			params.SearchAttributesProvider,
			params.SearchAttributesMapperProvider,
			params.ChasmRegistry,
			managerConfig.DisableOrderByClause,
			managerConfig.EnableManualPagination,
			params.MetricsHandler,
			params.Logger,
		)
	} else if dsConfig.CustomDataStoreConfig != nil {
		customFactory := params.CustomVisibilityStoreFactory
		if customFactory == nil {
			params.Logger.Fatal("custom visibility store factory must be defined")
			return nil, nil
		}
		visStore, err = customFactory.NewVisibilityStore(
			*dsConfig.CustomDataStoreConfig,
			params.SearchAttributesProvider,
			params.SearchAttributesMapperProvider,
			params.NamespaceRegistry,
			params.ChasmRegistry,
			params.PersistenceResolver,
			params.Logger,
			params.MetricsHandler,
		)
	}
	return visStore, err
}
