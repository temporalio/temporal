package workflow

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.uber.org/fx"
)

type nexusCommandHandlerDeps struct {
	fx.In

	Library          *Library
	EndpointRegistry commonnexus.EndpointRegistry `optional:"true"`
	Config           *nexusoperation.Config       `optional:"true"`
}

var Module = fx.Module(
	"chasm.lib.workflow",
	fx.Provide(NewLibrary),
	fx.Invoke(func(registry *chasm.Registry, library *Library) error {
		return registry.Register(library)
	}),
	fx.Invoke(func(deps nexusCommandHandlerDeps) error {
		if deps.EndpointRegistry == nil || deps.Config == nil {
			// Dependencies not available, skip registration.
			// This happens in services that don't have nexus operation support.
			return nil
		}
		return nexusoperation.RegisterCommandHandlers(
			deps.Library.commandHandlers,
			deps.EndpointRegistry,
			deps.Config,
		)
	}),
)
