package workflow

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.nexusoperation.workflow",
	fx.Invoke(func(
		registry *chasmworkflow.Registry,
		config *nexusoperation.Config,
		chasmRegistry *chasm.Registry,
	) error {
		return registry.Register(newLibrary(config, chasmRegistry.NexusEndpointProcessor))
	}),
)
