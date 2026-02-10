package workflow

import (
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/chasm/lib/workflow/command"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.uber.org/fx"
)

type registerParams struct {
	fx.In

	Registry         *command.Registry
	Config           *nexusoperation.Config
	EndpointRegistry commonnexus.EndpointRegistry `optional:"true"`
}

var Module = fx.Module(
	"chasm.lib.nexusoperations.workflow",
	nexusoperation.Module,
	fx.Invoke(func(p registerParams) error {
		if p.EndpointRegistry == nil {
			return nil
		}
		return registerCommandHandlers(p.Registry, p.Config, p.EndpointRegistry)
	}),
)
