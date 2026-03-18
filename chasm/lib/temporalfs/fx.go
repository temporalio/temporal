package temporalfs

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var HistoryModule = fx.Module(
	"temporalfs-history",
	fx.Provide(
		ConfigProvider,
		newHandler,
		newChunkGCTaskExecutor,
		newManifestCompactTaskExecutor,
		newQuotaCheckTaskExecutor,
		newLibrary,
	),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)
