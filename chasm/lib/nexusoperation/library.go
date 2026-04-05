package nexusoperation

import (
	"go.temporal.io/server/chasm"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary

	OperationInvocationTaskHandler *OperationInvocationTaskHandler
	OperationBackoffTaskHandler    *OperationBackoffTaskHandler
	OperationTimeoutTaskHandler    *OperationTimeoutTaskHandler

	CancellationTaskHandler        *CancellationTaskHandler
	CancellationBackoffTaskHandler *CancellationBackoffTaskHandler
}

func newLibrary() *Library {
	return &Library{}
}

func (l *Library) Name() string {
	return "nexusoperation"
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Operation]("operation"),
		chasm.NewRegistrableComponent[*Operation]("cancellation"),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask("invocation", l.OperationInvocationTaskHandler),
		chasm.NewRegistrablePureTask("invocationBackoff", l.OperationBackoffTaskHandler),
		chasm.NewRegistrablePureTask("scheduleToCloseTimeout", l.OperationTimeoutTaskHandler),
		chasm.NewRegistrableSideEffectTask("cancellation", l.CancellationTaskHandler),
		chasm.NewRegistrablePureTask("cancellationBackoff", l.CancellationBackoffTaskHandler),
	}
}

func (l *Library) RegisterServices(_ *grpc.Server) {
}
