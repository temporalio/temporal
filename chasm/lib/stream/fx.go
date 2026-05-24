package stream

import (
	"go.uber.org/fx"

	"go.temporal.io/server/chasm"
)

// Register registers the stream Library with the chasm Registry.
func Register(registry *chasm.Registry, library *Library) error {
	return registry.Register(library)
}

// Module is the fx module for the native-streams chasm library.
var Module = fx.Module(
	"chasm.lib.stream",
	fx.Provide(NewSweepExpiredTaskHandler),
	fx.Provide(NewAbortCleanupTaskHandler),
	fx.Provide(NewCloseCleanupTaskHandler),
	fx.Provide(NewOwnerWorkflowCloseTaskHandler),
	fx.Provide(NewPublisherDedupSweepTaskHandler),
	fx.Provide(NewDeliveryTaskHandler),
	fx.Provide(NewLibrary),
	fx.Invoke(Register),
)
