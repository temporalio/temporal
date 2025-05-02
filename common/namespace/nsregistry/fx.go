package nsregistry

import (
	"go.temporal.io/server/common/namespace"
	"go.uber.org/fx"
)

var RegistryLifetimeHooksModule = fx.Options(
	fx.Invoke(RegistryLifetimeHooks),
)

func RegistryLifetimeHooks(
	lc fx.Lifecycle,
	registry namespace.Registry,
) {
	lc.Append(fx.StartStopHook(registry.Start, registry.Stop))
}
