package nsregistry

import (
	"context"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/fx"
)

var RegistryLifetimeHooksModule = fx.Options(
	fx.Invoke(RegistryLifetimeHooks),
	fx.Invoke(InitializeSearchAttributeMappingsHook),
)

func RegistryLifetimeHooks(
	lc fx.Lifecycle,
	registry namespace.Registry,
) {
	lc.Append(fx.StartStopHook(registry.Start, registry.Stop))
}

func InitializeSearchAttributeMappingsHook(
	lc fx.Lifecycle,
	cfg *config.Config,
	clusterMetadata cluster.Metadata,
	metadataManager persistence.MetadataManager,
	clusterMetadataManager persistence.ClusterMetadataManager,
	logger log.Logger,
) {
	visDataStore := cfg.Persistence.GetVisibilityStoreConfig()
	if visDataStore.Elasticsearch == nil {
		return
	}

	visibilityIndexName := visDataStore.GetIndexName()
	currentClusterName := clusterMetadata.GetCurrentClusterName()

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return InitializeSearchAttributeMappings(
				ctx,
				metadataManager,
				clusterMetadataManager,
				currentClusterName,
				visibilityIndexName,
				logger,
			)
		},
	})
}
