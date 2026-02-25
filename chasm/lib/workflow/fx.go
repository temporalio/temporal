package workflow

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.workflow",
	fx.Provide(NewLibrary),
	fx.Invoke(func(registry *chasm.Registry, library *Library) error {
		return registry.Register(library)
	}),
)
