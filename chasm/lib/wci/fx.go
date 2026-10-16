package wci

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

func Register(registry *chasm.Registry, library *Library) error {
	return registry.Register(library)
}

// Module needs a ComputeClient; history provides one (service/history/wcicompute).
var Module = fx.Module(
	"chasm.lib.wci",
	fx.Provide(newHandler),
	fx.Provide(NewFetchSnapshotTaskHandler),
	fx.Provide(NewDeferredDecisionTaskHandler),
	fx.Provide(NewResizeTaskHandler),
	fx.Provide(NewInvokeTaskHandler),
	fx.Provide(NewValidateSpecTaskHandler),
	fx.Provide(NewSignalVersionWorkflowTaskHandler),
	fx.Provide(NewLibrary),
	fx.Invoke(Register),
)
