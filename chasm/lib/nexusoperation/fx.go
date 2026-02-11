package nexusoperation

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.nexusoperations",
	fx.Provide(configProvider),
	fx.Provide(NewOperationInvocationTaskExecutor),
	fx.Provide(NewOperationBackoffTaskExecutor),
	fx.Provide(NewOperationTimeoutTaskExecutor),
	fx.Provide(NewCancellationTaskExecutor),
	fx.Provide(NewCancellationBackoffTaskExecutor),
	fx.Provide(newLibrary),
	fx.Invoke(register),
)

func register(
	registry *chasm.Registry,
	library *Library,
) error {
	return registry.Register(library)
}
