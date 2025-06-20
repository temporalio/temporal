package cluster

import (
	"context"

	"go.temporal.io/server/common/pingable"
	"go.uber.org/fx"
)

var MetadataLifetimeHooksModule = fx.Options(
	fx.Provide(NewMetadataFromConfig),
	fx.Invoke(MetadataLifetimeHooks),
	fx.Provide(fx.Annotate(
		func(p Metadata) pingable.Pingable { return p },
		fx.ResultTags(`group:"deadlockDetectorRoots"`),
	)),
)

func MetadataLifetimeHooks(
	lc fx.Lifecycle,
	clusterMetadata Metadata,
) {
	lc.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				clusterMetadata.Start()
				return nil
			},
			OnStop: func(context.Context) error {
				clusterMetadata.Stop()
				return nil
			},
		},
	)
}
