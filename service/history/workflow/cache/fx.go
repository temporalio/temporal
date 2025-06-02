package cache

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/configs"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(func(config *configs.Config, logger log.Logger, handler metrics.Handler) Cache {
		return NewHostLevelCache(config, logger, handler)
	}),
)
