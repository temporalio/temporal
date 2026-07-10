package tquserdata

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

// Module registers the task queue user data CHASM library with the global
// registry.
var Module = fx.Module(
	"chasm.lib.tquserdata",
	fx.Invoke(func(registry *chasm.Registry) error {
		return registry.Register(Library)
	}),
)
