package nexusoperation

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary

	OperationInvocationTaskExecutor             *OperationInvocationTaskExecutor
	OperationBackoffTaskExecutor                *OperationBackoffTaskExecutor
	OperationScheduleToStartTimeoutTaskExecutor *OperationScheduleToStartTimeoutTaskExecutor
	OperationStartToCloseTimeoutTaskExecutor    *OperationStartToCloseTimeoutTaskExecutor
	OperationScheduleToCloseTimeoutTaskExecutor *OperationScheduleToCloseTimeoutTaskExecutor

	CancellationTaskExecutor        *CancellationTaskExecutor
	CancellationBackoffTaskExecutor *CancellationBackoffTaskExecutor
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
		chasm.NewRegistrableComponent[*Cancellation]("cancellation"),
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

func (l *Library) RegisterServices(_ *grpc.Server) {
}
