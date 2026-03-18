package temporalfs

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.uber.org/fx"
)

var HistoryModule = fx.Module(
	"temporalfs-history",
	fx.Provide(
		ConfigProvider,
		fx.Annotate(
			func(logger log.Logger) FSStoreProvider {
				return NewInMemoryStoreProvider(logger)
			},
			fx.As(new(FSStoreProvider)),
		),
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
