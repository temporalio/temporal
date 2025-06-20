package archival

import (
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/configs"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewArchiver),
	fx.Provide(func(config *configs.Config) quotas.RateLimiter {
		return quotas.NewDefaultOutgoingRateLimiter(quotas.RateFn(config.ArchivalBackendMaxRPS))
	}),
)
