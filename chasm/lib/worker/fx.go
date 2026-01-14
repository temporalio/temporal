package worker

import (
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/fx"
)

func Register(
	registry *chasm.Registry,
	library *Library,
) error {
	return registry.Register(library)
}

// HistoryClientProvider wraps the generated history client to implement HistoryClient interface.
func HistoryClientProvider(client historyservice.HistoryServiceClient) HistoryClient {
	return client
}

var HistoryModule = fx.Module(
	"worker-history",
	fx.Provide(ConfigProvider),
	fx.Provide(HistoryClientProvider),
	fx.Provide(NewLibrary),
	fx.Invoke(Register),
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
