package nexusoperation

import (
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"google.golang.org/grpc"
)

// componentOnlyLibrary registers just the components without task executors or gRPC handlers.
// Used in the frontend to enable component ref serialization.
type componentOnlyLibrary struct {
	chasm.UnimplementedLibrary
}

func newComponentOnlyLibrary() *componentOnlyLibrary {
	return &componentOnlyLibrary{}
}

func (l *componentOnlyLibrary) Name() string {
	return "nexusoperation"
}

func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Operation](
			"operation",
			chasm.WithSearchAttributes(
				EndpointSearchAttribute,
				ServiceSearchAttribute,
				OperationSearchAttribute,
				RequestIDSearchAttribute,
				StatusSearchAttribute,
			),
			chasm.WithBusinessIDAlias("OperationId"),
		),
		chasm.NewRegistrableComponent[*Cancellation]("cancellation"),
	}
}

type Library struct {
	componentOnlyLibrary

	handler *handler

	operationBackoffTaskHandler                *operationBackoffTaskHandler
	operationInvocationTaskHandler             *operationInvocationTaskHandler
	operationScheduleToCloseTimeoutTaskHandler *operationScheduleToCloseTimeoutTaskHandler
	operationScheduleToStartTimeoutTaskHandler *operationScheduleToStartTimeoutTaskHandler
	operationStartToCloseTimeoutTaskHandler    *operationStartToCloseTimeoutTaskHandler

	cancellationInvocationTaskHandler *cancellationInvocationTaskHandler
	cancellationBackoffTaskHandler    *cancellationBackoffTaskHandler
}

func newLibrary(
	handler *handler,
	operationBackoffTaskHandler *operationBackoffTaskHandler,
	operationInvocationTaskHandler *operationInvocationTaskHandler,
	operationScheduleToCloseTimeoutTaskHandler *operationScheduleToCloseTimeoutTaskHandler,
	operationScheduleToStartTimeoutTaskHandler *operationScheduleToStartTimeoutTaskHandler,
	operationStartToCloseTimeoutTaskHandler *operationStartToCloseTimeoutTaskHandler,
	cancellationInvocationTaskHandler *cancellationInvocationTaskHandler,
	cancellationBackoffTaskHandler *cancellationBackoffTaskHandler,
) *Library {
	return &Library{
		handler:                                    handler,
		operationBackoffTaskHandler:                operationBackoffTaskHandler,
		operationInvocationTaskHandler:             operationInvocationTaskHandler,
		operationScheduleToCloseTimeoutTaskHandler: operationScheduleToCloseTimeoutTaskHandler,
		operationScheduleToStartTimeoutTaskHandler: operationScheduleToStartTimeoutTaskHandler,
		operationStartToCloseTimeoutTaskHandler:    operationStartToCloseTimeoutTaskHandler,
		cancellationInvocationTaskHandler:          cancellationInvocationTaskHandler,
		cancellationBackoffTaskHandler:             cancellationBackoffTaskHandler,
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"invocation",
			l.operationInvocationTaskHandler,
			chasm.WithTaskGroup(TaskGroupName),
		),
		chasm.NewRegistrablePureTask("invocationBackoff", l.operationBackoffTaskHandler),
		chasm.NewRegistrablePureTask("scheduleToStartTimeout", l.operationScheduleToStartTimeoutTaskHandler),
		chasm.NewRegistrablePureTask("startToCloseTimeout", l.operationStartToCloseTimeoutTaskHandler),
		chasm.NewRegistrablePureTask("scheduleToCloseTimeout", l.operationScheduleToCloseTimeoutTaskHandler),
		chasm.NewRegistrableSideEffectTask(
			"cancellation",
			l.cancellationInvocationTaskHandler,
			chasm.WithTaskGroup(TaskGroupName),
		),
		chasm.NewRegistrablePureTask("cancellationBackoff", l.cancellationBackoffTaskHandler),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&nexusoperationpb.NexusOperationService_ServiceDesc, l.handler)
}
