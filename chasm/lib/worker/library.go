package worker

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary

	leaseExpiryTaskExecutor *LeaseExpiryTaskExecutor
}

func NewLibrary(
	logger log.Logger,
	config *Config,
) *Library {
	return &Library{
		leaseExpiryTaskExecutor: NewLeaseExpiryTaskExecutor(logger),
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
	// No gRPC services for Worker currently
}
