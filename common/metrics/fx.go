package metrics

import (
	"context"

	"go.uber.org/fx"
)

var RuntimeMetricsReporterModule = fx.Options(
	RuntimeMetricsReporterLifetimeHooksModule,
)
var RuntimeMetricsReporterLifetimeHooksModule = fx.Options(
	fx.Invoke(RuntimeMetricsReporterLifetimeHooks),
)

func RuntimeMetricsReporterLifetimeHooks(
	lc fx.Lifecycle,
	reporter *RuntimeMetricsReporter,
) {
	lc.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				reporter.Start()
				return nil
			},
			OnStop: func(context.Context) error {
				reporter.Stop()
				return nil
			},
		},
	)
}
