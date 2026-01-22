package worker

import (
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary

	handler                 *handler
	leaseExpiryTaskExecutor *LeaseExpiryTaskExecutor
}

// LibraryParams defines dependencies for NewLibrary.
// HistoryClient and NamespaceRegistry are optional - only available in history service.
// When nil, activity rescheduling on lease expiry is disabled.
type LibraryParams struct {
	fx.In

	Logger            log.Logger
	Config            *Config
	MetricsHandler    metrics.Handler
	HistoryClient     HistoryClient     `optional:"true"`
	NamespaceRegistry namespace.Registry `optional:"true"`
}

func NewLibrary(params LibraryParams) *Library {
	return &Library{
		handler:                 newHandler(params.MetricsHandler, params.Logger),
		leaseExpiryTaskExecutor: NewLeaseExpiryTaskExecutor(params.Logger, params.Config, params.MetricsHandler, params.HistoryClient, params.NamespaceRegistry),
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
