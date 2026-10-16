package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/fx"
)

var errGoMemoryLimitDetection = errors.New("detect container memory limit")

func goMemoryLimitLifetimeHooks(lc fx.Lifecycle, cfg *config.Config, logger log.Logger) {
	lc.Append(newGoMemoryLimitHook(cfg.Global.GoMemoryLimit, logger, memlimit.FromCgroup))
}

func newGoMemoryLimitHook(cfg config.GoMemoryLimit, logger log.Logger, provider memlimit.Provider) fx.Hook {
	if cfg.Ratio == 0 {
		return fx.Hook{}
	}

	var cancel context.CancelFunc
	return fx.Hook{
		OnStart: func(context.Context) error {
			// The start-hook context expires after startup, but refresh must run until Stop.
			var ctx context.Context
			ctx, cancel = context.WithCancel(context.Background())
			_, err := memlimit.Set(
				memlimit.WithRatio(cfg.Ratio),
				memlimit.WithRefreshInterval(ctx, cfg.RefreshInterval),
				memlimit.WithProvider(func() (uint64, error) {
					limit, err := provider()
					if err != nil && !errors.Is(err, memlimit.ErrNoLimit) {
						logger.Warn("Unable to detect container memory limit; keeping the current Go memory limit", tag.Error(err))
						return 0, fmt.Errorf("%w: %w", errGoMemoryLimitDetection, err)
					}
					return limit, err
				}),
			)
			if err != nil && !errors.Is(err, errGoMemoryLimitDetection) {
				cancel()
				return fmt.Errorf("configure Go memory limit: %w", err)
			}
			return nil
		},
		OnStop: func(context.Context) error {
			cancel()
			return nil
		},
	}
}
