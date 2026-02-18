package nexusoperation

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.nexusoperations",
	fx.Provide(configProvider),
	fx.Provide(NewCancellationBackoffTaskHandler),
	fx.Provide(NewCancellationTaskHandler),
	fx.Provide(NewOperationBackoffTaskHandler),
	fx.Provide(NewOperationInvocationTaskHandler),
	fx.Provide(NewOperationScheduleToCloseTimeoutTaskHandler),
	fx.Provide(NewOperationScheduleToStartTimeoutTaskHandler),
	fx.Provide(NewOperationStartToCloseTimeoutTaskHandler),
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
