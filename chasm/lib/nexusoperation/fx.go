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
	fx.Provide(NewOperationScheduleToStartTimeoutTaskExecutor),
	fx.Provide(NewOperationStartToCloseTimeoutTaskExecutor),
	fx.Provide(NewOperationScheduleToCloseTimeoutTaskExecutor),
	fx.Provide(NewCancellationTaskExecutor),
	fx.Provide(NewCancellationBackoffTaskExecutor),
	fx.Provide(newLibrary),
	fx.Invoke(register),
	fx.Invoke(registerCommandHandlers),
)

func register(
	registry *chasm.Registry,
	library *Library,
) error {
	return registry.Register(library)
}
