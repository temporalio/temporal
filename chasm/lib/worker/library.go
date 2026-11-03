package worker

import (
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary

	handler                 *handler
	leaseExpiryTaskExecutor *leaseExpiryTaskExecutor
}

// NewNilLibrary registers persisted component and task types for offline readers.
func NewNilLibrary() *Library {
	return &Library{}
}

func newLibrary(handler *handler) *Library {
	return &Library{handler: handler, leaseExpiryTaskExecutor: &leaseExpiryTaskExecutor{}}
}

func (l *Library) Name() string {
	return libraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*WorkerGroup](groupComponentName),
		// Detached workers can run recovery tasks after the group closes,
		// while the execution is retained.
		chasm.NewRegistrableComponent[*Worker](workerComponentName, chasm.WithDetached()),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask("expiry", l.leaseExpiryTaskExecutor),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	workerstatepb.RegisterWorkerServiceServer(server, l.handler)
}
