//go:build !temporal_minimal || temporal_with_elasticsearch

package visibility

import (
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/esconfig"
)

func newElasticsearchVisibilityStore(
	cfg *esconfig.Config,
	params *ManagerParams,
	managerConfig *ManagerConfig,
) (store.VisibilityStore, error) {
	return elasticsearch.NewVisibilityStore(
		cfg,
		managerConfig.EsProcessorConfig,
		params.SearchAttributesProvider,
		params.SearchAttributesMapperProvider,
		params.NamespaceRegistry,
		params.ChasmRegistry,
		managerConfig.DisableOrderByClause,
		managerConfig.EnableManualPagination,
		params.MetricsHandler,
		params.Logger,
	)
}
