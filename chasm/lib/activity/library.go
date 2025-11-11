package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"google.golang.org/grpc"
)

type library struct {
	chasm.UnimplementedLibrary

	handler                            *handler
	activityDispatchTaskExecutor       *activityDispatchTaskExecutor
	scheduleToStartTimeoutTaskExecutor *scheduleToStartTimeoutTaskExecutor
	scheduleToCloseTimeoutTaskExecutor *scheduleToCloseTimeoutTaskExecutor
	startToCloseTimeoutTaskExecutor    *startToCloseTimeoutTaskExecutor
}

func newLibrary(
	handler *handler,
	activityDispatchTaskExecutor *activityDispatchTaskExecutor,
	scheduleToStartTimeoutTaskExecutor *scheduleToStartTimeoutTaskExecutor,
	scheduleToCloseTimeoutTaskExecutor *scheduleToCloseTimeoutTaskExecutor,
	startToCloseTimeoutTaskExecutor *startToCloseTimeoutTaskExecutor,
) *library {
	return &library{
		handler:                            handler,
		activityDispatchTaskExecutor:       activityDispatchTaskExecutor,
		scheduleToStartTimeoutTaskExecutor: scheduleToStartTimeoutTaskExecutor,
		scheduleToCloseTimeoutTaskExecutor: scheduleToCloseTimeoutTaskExecutor,
		startToCloseTimeoutTaskExecutor:    startToCloseTimeoutTaskExecutor,
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
			"dispatch",
			l.activityDispatchTaskExecutor,
			l.activityDispatchTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"scheduleToStartTimer",
			l.scheduleToStartTimeoutTaskExecutor,
			l.scheduleToStartTimeoutTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"scheduleToCloseTimer",
			l.scheduleToCloseTimeoutTaskExecutor,
			l.scheduleToCloseTimeoutTaskExecutor,
		),
		chasm.NewRegistrablePureTask(
			"startToCloseTimer",
			l.startToCloseTimeoutTaskExecutor,
			l.startToCloseTimeoutTaskExecutor,
		),
	}
}
