package workflow

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/chasm/lib/workflow/workflowregistry"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.nexusoperations.workflow",
	nexusoperation.Module,
	fx.Invoke(func(
		registry *workflowregistry.Registry,
		config *nexusoperation.Config,
		chasmRegistry *chasm.Registry,
	) error {
		return registerCommandHandlers(registry, config, chasmRegistry.NexusEndpointProcessor)
	}),
	fx.Invoke(func(
		registry *workflowregistry.Registry,
		config *nexusoperation.Config,
		chasmRegistry *chasm.Registry,
	) error {
		return registerEvents(registry, config, chasmRegistry.NexusEndpointProcessor)
	}),
)
