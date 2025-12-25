package workflow

import (
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.workflow",
	fx.Provide(NewConfig),
	fx.Provide(NewLibrary),
	fx.Invoke(func(registry *chasm.Registry, library *Library) error {
		return registry.Register(library)
	}),
	fx.Invoke(func(library *Library, historyHandler historyservice.HistoryServiceServer) {
		library.workflowServiceNexusHandler.setHistoryHandler(historyHandler)
	}),
)
