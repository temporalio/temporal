package callback

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

type Library struct {
	fx.In

	chasm.UnimplementedLibrary

	InvocationTaskExecutor *InvocationTaskExecutor
	BackoffTaskExecutor    *BackoffTaskExecutor
}

func NewLibrary(
	invocationTaskExecutor *InvocationTaskExecutor,
	backoffTaskExecutor *BackoffTaskExecutor,
) *Library {
	return &Library{
		InvocationTaskExecutor: invocationTaskExecutor,
		BackoffTaskExecutor:    backoffTaskExecutor,
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
			"invocation",
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
