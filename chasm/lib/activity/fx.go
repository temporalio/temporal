package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/resource"
	"go.uber.org/fx"
)

var HistoryModule = fx.Module(
	"activity-history",
	fx.Provide(
		ConfigProvider,
		newActivityDispatchTaskExecutor,
		newScheduleToStartTimeoutTaskExecutor,
		newScheduleToCloseTimeoutTaskExecutor,
		newStartToCloseTimeoutTaskExecutor,
		newHandler,
		newLibrary,
	),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)

var FrontendModule = fx.Module(
	"activity-frontend",
	fx.Provide(activitypb.NewActivityServiceLayeredClient),
	fx.Provide(NewFrontendHandler),
	fx.Provide(resource.SearchAttributeValidatorProvider),
	fx.Invoke(func(registry *chasm.Registry) error {
		return registry.Register(&componentOnlyLibrary{})
	}),
)

// componentOnlyLibrary only registers the Activity component. Used by frontend which needs to
// serialize ComponentRefs but doesn't need task executors.
type componentOnlyLibrary struct {
	chasm.UnimplementedLibrary
}

func (l *componentOnlyLibrary) Name() string {
	return "activity"
}

func (l *componentOnlyLibrary) Components() []*chasm.RegistrableComponent {
	return []*chasm.RegistrableComponent{
		chasm.NewRegistrableComponent[*Activity]("activity"),
	}
}
