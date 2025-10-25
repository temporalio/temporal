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
		newActivityDispatchTaskExecutor,
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
)
