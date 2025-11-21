package callback

import (
	"go.temporal.io/server/chasm"
	"google.golang.org/grpc"
)

type (
	Library struct {
		chasm.UnimplementedLibrary

		InvocationTaskExecutor *InvocationTaskExecutor
		BackoffTaskExecutor    *BackoffTaskExecutor
	}
)

func newLibrary(
	InvocationTaskExecutor *InvocationTaskExecutor,
	BackoffTaskExecutor *BackoffTaskExecutor,
) *Library {
	return &Library{
		InvocationTaskExecutor: InvocationTaskExecutor,
		BackoffTaskExecutor:    BackoffTaskExecutor,
	}
}

func (l *Library) Name() string {
	return "callback"
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Callback]("callback"),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"invoke",
			l.InvocationTaskExecutor,
			l.InvocationTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"backoff",
			l.BackoffTaskExecutor,
			l.BackoffTaskExecutor,
		),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
}
