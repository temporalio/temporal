package workflow

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.workflow",
	fx.Provide(NewLibrary),
	fx.Invoke(func(registry *chasm.Registry, library *Library) error {
		return registry.Register(library)
	}),
	fx.Invoke(func(
		l *Library,
		endpointRegistry commonnexus.EndpointRegistry,
		config *nexusoperation.Config,
	) error {
		return nexusoperation.RegisterCommandHandlers(l.commandHandlers, endpointRegistry, config)
	}),
)
