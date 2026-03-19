package nexusoperation

import (
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.nexusoperation",
	fx.Provide(configProvider),
	fx.Provide(newHandler),
	fx.Provide(NewOperationInvocationTaskExecutor),
	fx.Provide(NewOperationBackoffTaskExecutor),
	fx.Provide(NewOperationScheduleToStartTimeoutTaskExecutor),
	fx.Provide(NewOperationStartToCloseTimeoutTaskExecutor),
	fx.Provide(NewOperationScheduleToCloseTimeoutTaskExecutor),
	fx.Provide(NewCancellationTaskExecutor),
	fx.Provide(NewCancellationBackoffTaskExecutor),
	fx.Provide(newLibrary),
	fx.Invoke(register),
)

var FrontendModule = fx.Module(
	"chasm.lib.nexusoperation.frontend",
	fx.Provide(nexusoperationpb.NewNexusOperationServiceLayeredClient),
	fx.Provide(NewFrontendHandler),
)

func register(
	registry *chasm.Registry,
	library *Library,
) error {
	return registry.Register(library)
}
