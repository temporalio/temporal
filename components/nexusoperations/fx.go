package nexusoperations

import (
	commonnexus "go.temporal.io/server/common/nexus"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"component.nexusoperations",
	commonnexus.Module,
	fx.Provide(ConfigProvider),
	fx.Provide(CallbackTokenGeneratorProvider),
	fx.Invoke(RegisterStateMachines),
	fx.Invoke(RegisterTaskSerializers),
	fx.Invoke(RegisterEventDefinitions),
	fx.Invoke(RegisterExecutor),
)

func CallbackTokenGeneratorProvider() *commonnexus.CallbackTokenGenerator {
	return commonnexus.NewCallbackTokenGenerator()
}
