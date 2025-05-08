package events

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(func(executionManager persistence.ExecutionManager, config *configs.Config, handler metrics.Handler, logger log.Logger) Cache {
		return NewHostLevelEventsCache(executionManager, config, handler, logger, false)
	}),
)
