package tquserdata

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tquserdata/gen/tquserdatapb/v1"
	"go.uber.org/fx"
)

var HistoryModule = fx.Module(
	"chasm.lib.tquserdata.history",
	fx.Provide(newHandler, newLibrary),
	fx.Invoke(func(l *library, registry *chasm.Registry) error {
		return registry.Register(l)
	}),
)

var MatchingModule = fx.Module(
	"chasm.lib.tquserdata.matching",
	fx.Provide(tquserdatapb.NewTaskQueueUserDataServiceLayeredClient),
)
