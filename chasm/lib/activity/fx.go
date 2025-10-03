package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.uber.org/fx"
)

var HistoryModule = fx.Module(
	"activity-history",
	fx.Provide(
		newHandler,
		newLibrary,
	),
	fx.Invoke(func(l *library, registry *chasm.Registry) {
		registry.Register(l)
	}),
)

var FrontendModule = fx.Module(
	"activity-frontend",
	fx.Provide(activitypb.NewActivityServiceLayeredClient),
	fx.Provide(NewFrontendHandler),
)
