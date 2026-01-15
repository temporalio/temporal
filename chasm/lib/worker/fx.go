package worker

import (
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	"go.uber.org/fx"
)

func Register(
	registry *chasm.Registry,
	library *Library,
) error {
	return registry.Register(library)
}

// HistoryClientProvider wraps the resource.HistoryClient to implement HistoryClient interface.
// Only used in history service where HistoryClient is available.
func HistoryClientProvider(client resource.HistoryClient) HistoryClient {
	return client
}

// Module is the shared module for all services (frontend, history, etc.).
// Provides library registration for tdbg and archetypeID conversion.
// HistoryClient is optional and only available in history service.
var Module = fx.Module(
	"worker",
	fx.Provide(ConfigProvider),
	fx.Provide(NewLibrary),
	fx.Invoke(Register),
)

// HistoryModule extends the base module with history-specific providers.
// Use this in history service to enable activity rescheduling.
var HistoryModule = fx.Module(
	"worker-history",
	fx.Provide(HistoryClientProvider),
)

func NewWorkerServiceClient(
	dc *dynamicconfig.Collection,
	rpcFactory common.RPCFactory,
	monitor membership.Monitor,
	persistenceConfig *config.Persistence,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (workerstatepb.WorkerServiceClient, error) {
	// This wrapper returns the interface type expected by consumers
	return workerstatepb.NewWorkerServiceLayeredClient(
		dc,
		rpcFactory,
		monitor,
		persistenceConfig,
		logger,
		metricsHandler,
	)
}

var FrontendModule = fx.Module(
	"worker-frontend",
	fx.Provide(NewWorkerServiceClient),
)
