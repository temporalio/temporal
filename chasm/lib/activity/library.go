package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"google.golang.org/grpc"
)

type componentOnlyLibrary struct {
	chasm.UnimplementedLibrary
}

func newComponentOnlyLibrary() *componentOnlyLibrary {
	return &componentOnlyLibrary{}
}

func (l *componentOnlyLibrary) Name() string {
	return "activity"
}

func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Activity]("activity",
			chasm.WithSearchAttributes(
				TypeSearchAttribute,
				StatusSearchAttribute,
				TaskQueueSearchAttribute,
			),
			chasm.WithBusinessIDAlias("ActivityId"),
		),
	}
}

type library struct {
	componentOnlyLibrary

	handler                            *handler
	activityDispatchTaskExecutor       *activityDispatchTaskExecutor
	scheduleToStartTimeoutTaskExecutor *scheduleToStartTimeoutTaskExecutor
	scheduleToCloseTimeoutTaskExecutor *scheduleToCloseTimeoutTaskExecutor
	startToCloseTimeoutTaskExecutor    *startToCloseTimeoutTaskExecutor
	heartbeatTimeoutTaskExecutor       *heartbeatTimeoutTaskExecutor
}

func newLibrary(
	handler *handler,
	activityDispatchTaskExecutor *activityDispatchTaskExecutor,
	scheduleToStartTimeoutTaskExecutor *scheduleToStartTimeoutTaskExecutor,
	scheduleToCloseTimeoutTaskExecutor *scheduleToCloseTimeoutTaskExecutor,
	startToCloseTimeoutTaskExecutor *startToCloseTimeoutTaskExecutor,
	heartbeatTimeoutTaskExecutor *heartbeatTimeoutTaskExecutor,
) *library {
	return &library{
		handler:                            handler,
		activityDispatchTaskExecutor:       activityDispatchTaskExecutor,
		scheduleToStartTimeoutTaskExecutor: scheduleToStartTimeoutTaskExecutor,
		scheduleToCloseTimeoutTaskExecutor: scheduleToCloseTimeoutTaskExecutor,
		startToCloseTimeoutTaskExecutor:    startToCloseTimeoutTaskExecutor,
		heartbeatTimeoutTaskExecutor:       heartbeatTimeoutTaskExecutor,
	}
}

func (l *library) RegisterServices(server *grpc.Server) {
	server.RegisterService(&activitypb.ActivityService_ServiceDesc, l.handler)
}

func (l *library) Tasks() []*chasm.RegistrableTask {
	return []*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
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
		chasm.NewRegistrablePureTask(
			"heartbeatTimer",
			l.heartbeatTimeoutTaskExecutor,
			l.heartbeatTimeoutTaskExecutor,
		),
	}
}
