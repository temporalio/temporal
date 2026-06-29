package nsrepl

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

func register(
	registry *chasm.Registry,
	library *Library,
) error {
	return registry.Register(library)
}

var Module = fx.Module(
	"chasm.lib.nsrepl",
	fx.Provide(newApplyLocalTaskHandler),
	fx.Provide(newApplyPeerTaskHandler),
	fx.Provide(newHandler),
	fx.Provide(newLibrary),
	fx.Invoke(register),
)
