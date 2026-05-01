package workflow

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.workflow",
	fx.Provide(NewRegistry),
	fx.Provide(newLibrary),
	fx.Invoke(func(
		chasmRegistry *chasm.Registry,
		library *library,
		config *nexusoperation.Config,
	) error {
		if err := library.registry.Register(
			newNexusLibrary(config, chasmRegistry.NexusEndpointProcessor),
		); err != nil {
			return err
		}
		return chasmRegistry.Register(library)
	}),
)
