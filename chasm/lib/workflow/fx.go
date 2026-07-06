package workflow

import (
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.workflow",
	fx.Provide(NewConfig),
	fx.Provide(NewRegistry),
	fx.Provide(newLibrary),
	fx.Invoke(func(
		chasmRegistry *chasm.Registry,
		library *Library,
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

// HistoryHandlerModule wires the workflow library's Nexus handler to the
// history service. Only include this in services that provide
// historyservice.HistoryServiceServer (the history service).
var HistoryHandlerModule = fx.Invoke(func(library *Library, historyHandler historyservice.HistoryServiceServer) {
	library.workflowServiceNexusHandler.setHistoryHandler(historyHandler)
})
