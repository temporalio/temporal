package callback

import (
	"go.temporal.io/server/chasm"
	"google.golang.org/grpc"
)

type (
	Library struct {
		chasm.UnimplementedLibrary

		InvocationTaskHandler *invocationTaskHandler
		BackoffTaskHandler    *backoffTaskHandler
	}
)

func newLibrary(
	InvocationTaskHandler *invocationTaskHandler,
	BackoffTaskHandler *backoffTaskHandler,
) *Library {
	return &Library{
		InvocationTaskHandler: InvocationTaskHandler,
		BackoffTaskHandler:    BackoffTaskHandler,
	}
}

func (l *Library) Name() string {
	return chasm.CallbackLibraryName
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Callback](
			chasm.CallbackComponentName,
			chasm.WithDetached(),
		),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"invoke",
			l.InvocationTaskHandler,
		),
		chasm.NewRegistrablePureTask(
			"backoff",
			l.BackoffTaskHandler,
		),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
}
