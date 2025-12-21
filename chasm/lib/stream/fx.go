package stream

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/stream/gen/streampb/v1"
	"go.uber.org/fx"
)

var FrontendModule = fx.Module(
	"stream-frontend",
	fx.Provide(streampb.NewStreamServiceLayeredClient),
	fx.Provide(NewFrontendHandler),
)

var HistoryModule = fx.Module(
	"stream-history",
	fx.Provide(newHandler),
	fx.Provide(newLibrary),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)
