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
		newHeartbeatTimeoutTaskExecutor,
		newHandler,
		newLibrary,
	),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)

var FrontendModule = fx.Module(
	"activity-frontend",
	fx.Provide(ConfigProvider),
	fx.Provide(activitypb.NewActivityServiceLayeredClient),
	fx.Provide(NewFrontendHandler),
	fx.Provide(resource.SearchAttributeValidatorProvider),
	fx.Invoke(func(registry *chasm.Registry) error {
		// Frontend needs to register the component in order to serialize ComponentRefs, but doesn't
		// need task executors.
		return registry.Register(newComponentOnlyLibrary())
	}),
)
