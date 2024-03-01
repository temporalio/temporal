package callbacks

import (
	"go.uber.org/fx"
)

var Module = fx.Module(
	"plugin.callbacks",
	fx.Provide(ConfigProvider),
	fx.Invoke(RegisterTaskSerializer),
	fx.Invoke(RegisterStateMachine),
	fx.Invoke(RegisterExecutor),
)
