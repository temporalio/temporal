package pprof

import (
	"context"

	"go.uber.org/fx"
)

// Requires *config.PProf available in container.
var Module = fx.Options(
	fx.Provide(NewInitializer),
	fx.Invoke(LifetimeHooks),
)

func LifetimeHooks(
	lc fx.Lifecycle,
	pprof *PProfInitializerImpl,
) {
	lc.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				return pprof.Start()
			},
			// todo: refactor pprof to gracefully shutdown http server
			// OnStop: func(ctx context.Context) error {
			// 	pprof.Stop()
			// 	return nil
			// },
		},
	)
}
