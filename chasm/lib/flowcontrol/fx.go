package flowcontrol

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/flowcontrol/concurrency"
	"go.uber.org/fx"
)

var HistoryModule = fx.Module(
	"flowcontrol-history",
	fx.Provide(
		concurrency.NewHandler,
		newLibrary,
	),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)
