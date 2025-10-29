package workersession

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary

	leaseExpiryTaskExecutor    *LeaseExpiryTaskExecutor
	sessionCleanupTaskExecutor *SessionCleanupTaskExecutor
}

func NewLibrary(
	logger log.Logger,
) *Library {
	return &Library{
		leaseExpiryTaskExecutor:    NewLeaseExpiryTaskExecutor(logger),
		sessionCleanupTaskExecutor: NewSessionCleanupTaskExecutor(logger),
	}
}

func (l *Library) Name() string {
	return "workersession"
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*WorkerSession]("WorkerSession"),
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
			"SessionCleanupTask",
			l.sessionCleanupTaskExecutor,
			l.sessionCleanupTaskExecutor,
		),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	// No gRPC services for WorkerSession currently
}
