package namespacereplication

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
	"chasm.lib.namespacereplication",
	fx.Provide(newApplyLocalTaskHandler),
	// Default peer transport (admin RPC). Provided as the PeerApplier interface so
	// a deployment can override it via fx (e.g. fx.Decorate in the history service
	// options) without changing any peer fan-out policy.
	fx.Provide(newAdminClientPeerApplier),
	fx.Provide(newApplyPeerTaskHandler),
	fx.Provide(newLibrary),
	fx.Invoke(register),
)
