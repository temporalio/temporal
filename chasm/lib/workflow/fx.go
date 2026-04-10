package workflow

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.workflow",
	fx.Provide(NewRegistry),
	fx.Provide(newLibrary),
	fx.Invoke(func(registry *chasm.Registry, library *library) error {
		return registry.Register(library)
	}),
)
