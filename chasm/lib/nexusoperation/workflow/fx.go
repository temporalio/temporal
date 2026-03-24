package workflow

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.nexusoperations.workflow",
	nexusoperation.Module,
	fx.Invoke(func(
		registry *chasmworkflow.Registry,
		config *nexusoperation.Config,
		chasmRegistry *chasm.Registry,
	) error {
		return registerCommandHandlers(registry, config, chasmRegistry.NexusEndpointProcessor)
	}),
	fx.Invoke(func(
		registry *chasmworkflow.Registry,
		config *nexusoperation.Config,
		chasmRegistry *chasm.Registry,
	) error {
		return registerEvents(registry, config, chasmRegistry.NexusEndpointProcessor)
	}),
)
