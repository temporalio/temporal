package workflow

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/workflow/command"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.workflow",
	command.Module,
	fx.Provide(NewLibrary),
	fx.Invoke(func(registry *chasm.Registry, library *Library) error {
		return registry.Register(library)
	}),
)
