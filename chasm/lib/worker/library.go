package worker

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary

	leaseExpiryTaskExecutor   *LeaseExpiryTaskExecutor
	workerCleanupTaskExecutor *WorkerCleanupTaskExecutor
}

func NewLibrary(
	logger log.Logger,
) *Library {
	return &Library{
		leaseExpiryTaskExecutor:   NewLeaseExpiryTaskExecutor(logger),
		workerCleanupTaskExecutor: NewWorkerCleanupTaskExecutor(logger),
	}
}

func (l *Library) Name() string {
	return "worker"
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Worker]("Worker"),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask(
			"LeaseExpiryTask",
			l.leaseExpiryTaskExecutor,
			l.leaseExpiryTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"WorkerCleanupTask",
			l.workerCleanupTaskExecutor,
			l.workerCleanupTaskExecutor,
		),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	// No gRPC services for Worker currently
}
