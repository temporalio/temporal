package worker

import (
	"go.temporal.io/server/chasm"
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
	fx.Provide(newHandler),
	fx.Provide(newLibrary),
	fx.Invoke(Register),
)
