package cache

import (
	"context"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/configs"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(func(
		config *configs.Config,
		logger log.Logger,
		handler metrics.Handler,
		testHooks testhooks.TestHooks,
	) Cache {
		return NewHostLevelCacheWithTestHooks(config, logger, handler, testHooks)
	}),
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
