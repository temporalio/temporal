package nexusoperation

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"google.golang.org/grpc"
)

type Library struct {
	chasm.UnimplementedLibrary

	OperationBackoffTaskHandler                *OperationBackoffTaskHandler
	OperationInvocationTaskHandler             *OperationInvocationTaskHandler
	OperationScheduleToCloseTimeoutTaskHandler *OperationScheduleToCloseTimeoutTaskHandler
	OperationScheduleToStartTimeoutTaskHandler *OperationScheduleToStartTimeoutTaskHandler
	OperationStartToCloseTimeoutTaskHandler    *OperationStartToCloseTimeoutTaskHandler

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
		chasm.NewRegistrableComponent[*Cancellation]("cancellation"),
	}
}

func (l *Library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask[*Operation, *nexusoperationpb.InvocationTask]("invocation", l.OperationInvocationTaskHandler),
		chasm.NewRegistrablePureTask[*Operation, *nexusoperationpb.InvocationBackoffTask]("invocationBackoff", l.OperationBackoffTaskHandler),
		chasm.NewRegistrablePureTask[*Operation, *nexusoperationpb.ScheduleToStartTimeoutTask]("scheduleToStartTimeout", l.OperationScheduleToStartTimeoutTaskHandler),
		chasm.NewRegistrablePureTask[*Operation, *nexusoperationpb.StartToCloseTimeoutTask]("startToCloseTimeout", l.OperationStartToCloseTimeoutTaskHandler),
		chasm.NewRegistrablePureTask[*Operation, *nexusoperationpb.ScheduleToCloseTimeoutTask]("scheduleToCloseTimeout", l.OperationScheduleToCloseTimeoutTaskHandler),
		chasm.NewRegistrableSideEffectTask[*Cancellation, *nexusoperationpb.CancellationTask]("cancellation", l.CancellationTaskHandler),
		chasm.NewRegistrablePureTask[*Cancellation, *nexusoperationpb.CancellationBackoffTask]("cancellationBackoff", l.CancellationBackoffTaskHandler),
	}
}

func (l *Library) RegisterServices(_ *grpc.Server) {
}
