package callback

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

func Register(
	registry *chasm.Registry,
	library *Library,
) error {
	return registry.Register(library)
}

var Module = fx.Module(
	"chasm.lib.callback",
	fx.Provide(ConfigProvider),
	fx.Provide(NewInvocationTaskExecutor),
	fx.Provide(NewBackoffTaskExecutor),
	fx.Provide(NewLibrary),
	fx.Invoke(Register),
)
