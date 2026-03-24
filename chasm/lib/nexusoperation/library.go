package nexusoperation

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"google.golang.org/grpc"
)

// componentOnlyLibrary registers just the components without task executors or gRPC handlers.
// Used in the frontend to enable component ref serialization.
type componentOnlyLibrary struct {
	chasm.UnimplementedLibrary
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

	OperationInvocationTaskExecutor             *OperationInvocationTaskExecutor
	OperationBackoffTaskExecutor                *OperationBackoffTaskExecutor
	OperationScheduleToStartTimeoutTaskExecutor *OperationScheduleToStartTimeoutTaskExecutor
	OperationStartToCloseTimeoutTaskExecutor    *OperationStartToCloseTimeoutTaskExecutor
	OperationScheduleToCloseTimeoutTaskExecutor *OperationScheduleToCloseTimeoutTaskExecutor

	CancellationTaskExecutor        *CancellationTaskExecutor
	CancellationBackoffTaskExecutor *CancellationBackoffTaskExecutor
}

func newLibrary(
	handler *handler,
	invocationTaskExecutor *OperationInvocationTaskExecutor,
	backoffTaskExecutor *OperationBackoffTaskExecutor,
	scheduleToStartTimeoutTaskExecutor *OperationScheduleToStartTimeoutTaskExecutor,
	startToCloseTimeoutTaskExecutor *OperationStartToCloseTimeoutTaskExecutor,
	scheduleToCloseTimeoutTaskExecutor *OperationScheduleToCloseTimeoutTaskExecutor,
	cancellationTaskExecutor *CancellationTaskExecutor,
	cancellationBackoffTaskExecutor *CancellationBackoffTaskExecutor,
) *Library {
	return &Library{
		handler:                                     handler,
		OperationInvocationTaskExecutor:             invocationTaskExecutor,
		OperationBackoffTaskExecutor:                backoffTaskExecutor,
		OperationScheduleToStartTimeoutTaskExecutor: scheduleToStartTimeoutTaskExecutor,
		OperationStartToCloseTimeoutTaskExecutor:    startToCloseTimeoutTaskExecutor,
		OperationScheduleToCloseTimeoutTaskExecutor: scheduleToCloseTimeoutTaskExecutor,
		CancellationTaskExecutor:                    cancellationTaskExecutor,
		CancellationBackoffTaskExecutor:             cancellationBackoffTaskExecutor,
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask[*Operation, *nexusoperationpb.InvocationTask]("invocation", l.OperationInvocationTaskExecutor, l.OperationInvocationTaskExecutor),
		chasm.NewRegistrablePureTask[*Operation, *nexusoperationpb.InvocationBackoffTask]("invocationBackoff", l.OperationBackoffTaskExecutor, l.OperationBackoffTaskExecutor),
		chasm.NewRegistrablePureTask[*Operation, *nexusoperationpb.ScheduleToStartTimeoutTask]("scheduleToStartTimeout", l.OperationScheduleToStartTimeoutTaskExecutor, l.OperationScheduleToStartTimeoutTaskExecutor),
		chasm.NewRegistrablePureTask[*Operation, *nexusoperationpb.StartToCloseTimeoutTask]("startToCloseTimeout", l.OperationStartToCloseTimeoutTaskExecutor, l.OperationStartToCloseTimeoutTaskExecutor),
		chasm.NewRegistrablePureTask[*Operation, *nexusoperationpb.ScheduleToCloseTimeoutTask]("scheduleToCloseTimeout", l.OperationScheduleToCloseTimeoutTaskExecutor, l.OperationScheduleToCloseTimeoutTaskExecutor),
		chasm.NewRegistrableSideEffectTask[*Cancellation, *nexusoperationpb.CancellationTask]("cancellation", l.CancellationTaskExecutor, l.CancellationTaskExecutor),
		chasm.NewRegistrablePureTask[*Cancellation, *nexusoperationpb.CancellationBackoffTask]("cancellationBackoff", l.CancellationBackoffTaskExecutor, l.CancellationBackoffTaskExecutor),
	}
}

func (l *Library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&nexusoperationpb.NexusOperationService_ServiceDesc, l.handler)
}
