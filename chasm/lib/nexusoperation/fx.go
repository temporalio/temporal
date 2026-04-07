package nexusoperation

import (
	"go.temporal.io/server/chasm"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.nexusoperation",
	fx.Provide(configProvider),
	fx.Provide(newCancellationBackoffTaskHandler),
	fx.Provide(newCancellationTaskHandler),
	fx.Provide(newOperationBackoffTaskHandler),
	fx.Provide(newOperationInvocationTaskHandler),
	fx.Provide(newOperationScheduleToCloseTimeoutTaskHandler),
	fx.Provide(newOperationScheduleToStartTimeoutTaskHandler),
	fx.Provide(newOperationStartToCloseTimeoutTaskHandler),
	fx.Provide(newLibrary),
	fx.Invoke(register),
)

func register(
	registry *chasm.Registry,
	library *Library,
) error {
	return registry.Register(library)
}
