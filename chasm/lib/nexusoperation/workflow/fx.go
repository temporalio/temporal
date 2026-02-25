package workflow

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/chasm/lib/workflow/command"
	"go.uber.org/fx"
)

type registerParams struct {
	fx.In

	Registry      *command.Registry
	Config        *nexusoperation.Config
	ChasmRegistry *chasm.Registry
}

var Module = fx.Module(
	"chasm.lib.nexusoperations.workflow",
	nexusoperation.Module,
	fx.Invoke(func(p registerParams) error {
		return registerCommandHandlers(p.Registry, p.Config, p.ChasmRegistry.NexusEndpointProcessor)
	}),
)
