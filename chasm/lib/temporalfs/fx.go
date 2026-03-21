package temporalfs

import (
	"context"
	"os"
	"path/filepath"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.uber.org/fx"
)

var HistoryModule = fx.Module(
	"temporalfs-history",
	fx.Provide(
		ConfigProvider,
		fx.Annotate(
			func(lc fx.Lifecycle, logger log.Logger) FSStoreProvider {
				dataDir := filepath.Join(os.TempDir(), "temporalfs")
				provider := NewPebbleStoreProvider(dataDir, logger)
				lc.Append(fx.Hook{
					OnStop: func(_ context.Context) error {
						return provider.Close()
					},
				})
				return provider
			},
			fx.As(new(FSStoreProvider)),
		),
		fx.Annotate(
			newNoopWorkflowExistenceChecker,
			fx.As(new(WorkflowExistenceChecker)),
		),
		newHandler,
		newChunkGCTaskExecutor,
		newManifestCompactTaskExecutor,
		newQuotaCheckTaskExecutor,
		newOwnerCheckTaskExecutor,
		newDataCleanupTaskExecutor,
		newLibrary,
	),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)
