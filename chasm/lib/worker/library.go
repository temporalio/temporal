package worker

import (
	"time"

	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary

	config                  *Config
	handler                 *handler
	leaseExpiryTaskExecutor *leaseExpiryTaskExecutor
}

// NewNilLibrary registers persisted component and task types for offline readers.
func NewNilLibrary() *Library {
	return &Library{}
}

func newLibrary(handler *handler, config *Config) *Library {
	return &Library{config: config, handler: handler, leaseExpiryTaskExecutor: &leaseExpiryTaskExecutor{}}
}

func (l *Library) Name() string {
	return libraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	var groupOptions []chasm.RegistrableComponentOption
	if l.config != nil {
		groupOptions = append(groupOptions, chasm.WithRetention(func(ns *namespace.Namespace) time.Duration {
			return l.config.GroupRetention(ns.Name().String())
		}))
	}
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*WorkerGroup](groupComponentName, groupOptions...),
		// Detached workers can run recovery tasks after the group closes,
		// while the execution is retained.
		chasm.NewRegistrableComponent[*Worker](workerComponentName, chasm.WithDetached()),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask(
			"expiry",
			l.leaseExpiryTaskExecutor,
			chasm.WithSingletonTask(chasm.SingletonTaskModeReplace),
		),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	workerstatepb.RegisterWorkerServiceServer(server, l.handler)
}
