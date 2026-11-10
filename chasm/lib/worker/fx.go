package worker

import (
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.uber.org/fx"
)

func Register(
	registry *chasm.Registry,
	library *Library,
) error {
	return registry.Register(library)
}

var Module = fx.Module(
	"chasm.lib.worker",
	fx.Provide(configProvider),
	fx.Provide(newHandler),
	fx.Provide(newLibrary),
	fx.Invoke(Register),
)

var FrontendModule = fx.Module(
	"chasm.lib.worker.frontend",
	fx.Provide(workerstatepb.NewWorkerServiceLayeredClient),
	fx.Invoke(func(registry *chasm.Registry) error {
		return registry.Register(NewNilLibrary())
	}),
)
