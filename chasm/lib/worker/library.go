package worker

import (
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary

	handler                 *handler
	leaseExpiryTaskExecutor *LeaseExpiryTaskExecutor
}

func NewLibrary(
	logger log.Logger,
	config *Config,
	metricsHandler metrics.Handler,
	historyClient HistoryClient,
	namespaceRegistry namespace.Registry,
) *Library {
	return &Library{
		handler:                 newHandler(metricsHandler),
		leaseExpiryTaskExecutor: NewLeaseExpiryTaskExecutor(logger, metricsHandler, historyClient, namespaceRegistry),
	}
}

func (l *Library) Name() string {
	return "worker"
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Worker](string(Archetype)),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask(
			"expiry",
			l.leaseExpiryTaskExecutor,
			l.leaseExpiryTaskExecutor,
		),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	workerstatepb.RegisterWorkerServiceServer(server, l.handler)
}
