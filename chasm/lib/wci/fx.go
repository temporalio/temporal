package wci

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

func Register(registry *chasm.Registry, library *Library) error {
	return registry.Register(library)
}

// HistoryModule needs a ComputeClient; history provides one (service/history/wcicompute).
var HistoryModule = fx.Module(
	"chasm.lib.wci",
	fx.Provide(newHandler),
	fx.Provide(NewFetchSnapshotTaskHandler),
	fx.Provide(NewDeferredDecisionTaskHandler),
	fx.Provide(NewResizeTaskHandler),
	fx.Provide(NewInvokeTaskHandler),
	fx.Provide(NewValidateSpecTaskHandler),
	fx.Provide(NewSignalVersionWorkflowTaskHandler),
	fx.Provide(NewMigrateToWorkflowTaskHandler),
	fx.Provide(NewLibrary),
	fx.Invoke(Register),
)

// ComponentModule registers the components and tasks without handlers, for services that only
// route to or list WCI executions.
var ComponentModule = fx.Module(
	"chasm.lib.wci.components",
	fx.Invoke(func(registry *chasm.Registry) error {
		return registry.Register(NewNilLibrary())
	}),
)
