package scheduler

import (
	"go.uber.org/fx"
)

var Module = fx.Module(
	"component.callbacks",
	fx.Provide(ConfigProvider),
	fx.Provide(ActiveExecutorOptionsProvider),
	fx.Provide(StandbyExecutorOptionsProvider),
	fx.Invoke(RegisterTaskSerializers),
	fx.Invoke(RegisterStateMachine),
	fx.Invoke(RegisterExecutor),
)

func ActiveExecutorOptionsProvider() ActiveExecutorOptions {
	return ActiveExecutorOptions{}
}

func StandbyExecutorOptionsProvider() StandbyExecutorOptions {
	return StandbyExecutorOptions{}
}
