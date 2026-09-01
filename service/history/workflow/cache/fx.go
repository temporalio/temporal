package cache

import (
	"context"

	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewHostLevelCache),
	fx.Invoke(func(
		lc fx.Lifecycle,
		cache Cache,
	) {
		lc.Append(fx.Hook{
			OnStop: func(_ context.Context) error {
				ci, ok := cache.(*cacheImpl)
				if ok {
					ci.stop()
				}
				return nil
			},
		})
	}),
)
