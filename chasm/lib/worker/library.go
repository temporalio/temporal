package worker

import (
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary

	handler                   *handler
	leaseExpiryTaskExecutor   *LeaseExpiryTaskExecutor
	workerCleanupTaskExecutor *WorkerCleanupTaskExecutor
}

func NewLibrary(
	logger log.Logger,
	config *Config,
) *Library {
	return &Library{
		handler:                   newHandler(),
		leaseExpiryTaskExecutor:   NewLeaseExpiryTaskExecutor(logger, config),
		workerCleanupTaskExecutor: NewWorkerCleanupTaskExecutor(logger),
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
