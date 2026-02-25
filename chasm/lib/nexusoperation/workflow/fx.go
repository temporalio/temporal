package workflow

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/chasm/lib/workflow/command"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.nexusoperations.workflow",
	nexusoperation.Module,
	fx.Invoke(func(
		registry *command.Registry,
		config *nexusoperation.Config,
		chasmRegistry *chasm.Registry,
	) error {
		return registerCommandHandlers(registry, config, chasmRegistry.NexusEndpointProcessor)
	}),
)
