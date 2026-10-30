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
		chasm.NewRegistrableComponent[*Worker](workerComponentName),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrablePureTask("expiry", l.leaseExpiryTaskExecutor),
	}
}
