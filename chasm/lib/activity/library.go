package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"google.golang.org/grpc"
)

type library struct {
	chasm.UnimplementedLibrary

	handler                            *handler
	ActivityDispatchTaskExecutor       *activityDispatchTaskExecutor
	ScheduleToStartTimeoutTaskExecutor *scheduleToStartTimeoutTaskExecutor
	ScheduleToCloseTimeoutTaskExecutor *scheduleToCloseTimeoutTaskExecutor
	StartToCloseTimeoutTaskExecutor    *startToCloseTimeoutTaskExecutor
}

func newLibrary(
	handler *handler,
	ActivityDispatchTaskExecutor *activityDispatchTaskExecutor,
	ScheduleToStartTimeoutTaskExecutor *scheduleToStartTimeoutTaskExecutor,
	ScheduleToCloseTimeoutTaskExecutor *scheduleToCloseTimeoutTaskExecutor,
	StartToCloseTimeoutTaskExecutor *startToCloseTimeoutTaskExecutor,
) *library {
	return &library{
		handler:                            handler,
		ActivityDispatchTaskExecutor:       ActivityDispatchTaskExecutor,
		ScheduleToStartTimeoutTaskExecutor: ScheduleToStartTimeoutTaskExecutor,
		ScheduleToCloseTimeoutTaskExecutor: ScheduleToCloseTimeoutTaskExecutor,
		StartToCloseTimeoutTaskExecutor:    StartToCloseTimeoutTaskExecutor,
	}
}

func (l *library) Name() string {
	return "activity"
}

func (l *library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&activitypb.ActivityService_ServiceDesc, l.handler)
}

func (l *library) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Activity]("activity"),
	}
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask[*Activity, *activitypb.ActivityDispatchTask](
			"dispatchActivity",
			l.ActivityDispatchTaskExecutor,
			l.ActivityDispatchTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"scheduleToStartTimer",
			l.ScheduleToStartTimeoutTaskExecutor,
			l.ScheduleToStartTimeoutTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"scheduleToCloseTimer",
			l.ScheduleToCloseTimeoutTaskExecutor,
			l.ScheduleToCloseTimeoutTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"startToCloseTimer",
			l.StartToCloseTimeoutTaskExecutor,
			l.StartToCloseTimeoutTaskExecutor,
		),
	}
}
