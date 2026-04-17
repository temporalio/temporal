package nexusoperation

import (
	"go.temporal.io/server/chasm"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary

	operationBackoffTaskHandler                *operationBackoffTaskHandler
	operationInvocationTaskHandler             *operationInvocationTaskHandler
	operationScheduleToCloseTimeoutTaskHandler *operationScheduleToCloseTimeoutTaskHandler
	operationScheduleToStartTimeoutTaskHandler *operationScheduleToStartTimeoutTaskHandler
	operationStartToCloseTimeoutTaskHandler    *operationStartToCloseTimeoutTaskHandler

	cancellationInvocationTaskHandler *cancellationInvocationTaskHandler
	cancellationBackoffTaskHandler    *cancellationBackoffTaskHandler
}

func newLibrary(
	operationBackoffTaskHandler *operationBackoffTaskHandler,
	operationInvocationTaskHandler *operationInvocationTaskHandler,
	operationScheduleToCloseTimeoutTaskHandler *operationScheduleToCloseTimeoutTaskHandler,
	operationScheduleToStartTimeoutTaskHandler *operationScheduleToStartTimeoutTaskHandler,
	operationStartToCloseTimeoutTaskHandler *operationStartToCloseTimeoutTaskHandler,
	cancellationInvocationTaskHandler *cancellationInvocationTaskHandler,
	cancellationBackoffTaskHandler *cancellationBackoffTaskHandler,
) *Library {
	return &Library{
		operationBackoffTaskHandler:                operationBackoffTaskHandler,
		operationInvocationTaskHandler:             operationInvocationTaskHandler,
		operationScheduleToCloseTimeoutTaskHandler: operationScheduleToCloseTimeoutTaskHandler,
		operationScheduleToStartTimeoutTaskHandler: operationScheduleToStartTimeoutTaskHandler,
		operationStartToCloseTimeoutTaskHandler:    operationStartToCloseTimeoutTaskHandler,
		cancellationInvocationTaskHandler:          cancellationInvocationTaskHandler,
		cancellationBackoffTaskHandler:             cancellationBackoffTaskHandler,
	}
}

func (l *Library) Name() string {
	return "nexusoperation"
}

func (l *Library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Operation]("operation"),
		chasm.NewRegistrableComponent[*Cancellation]("cancellation"),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"invocation",
			l.operationInvocationTaskHandler,
			chasm.WithSideEffectTaskGroup(TaskGroupName),
		),
		chasm.NewRegistrablePureTask("invocationBackoff", l.operationBackoffTaskHandler),
		chasm.NewRegistrablePureTask("scheduleToStartTimeout", l.operationScheduleToStartTimeoutTaskHandler),
		chasm.NewRegistrablePureTask("startToCloseTimeout", l.operationStartToCloseTimeoutTaskHandler),
		chasm.NewRegistrablePureTask("scheduleToCloseTimeout", l.operationScheduleToCloseTimeoutTaskHandler),
		chasm.NewRegistrableSideEffectTask(
			"cancellation",
			l.cancellationInvocationTaskHandler,
			chasm.WithSideEffectTaskGroup(TaskGroupName),
		),
		chasm.NewRegistrablePureTask("cancellationBackoff", l.cancellationBackoffTaskHandler),
	}
}

func (l *Library) RegisterServices(_ *grpc.Server) {
}
