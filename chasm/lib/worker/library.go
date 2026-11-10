package worker

import "go.temporal.io/server/chasm"

type Library struct {
	chasm.UnimplementedLibrary

	leaseExpiryTaskExecutor *leaseExpiryTaskExecutor
}

// NewNilLibrary registers persisted component and task types for offline readers.
func NewNilLibrary() *Library {
	return &Library{}
}

func NewLibrary() *Library {
	return &Library{leaseExpiryTaskExecutor: &leaseExpiryTaskExecutor{}}
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
		chasm.NewRegistrablePureTask(
			"expiry",
			l.leaseExpiryTaskExecutor,
			chasm.WithSingletonTask(chasm.SingletonTaskModeReplace),
		),
	}
}
