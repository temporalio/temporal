package command

import "go.uber.org/fx"

var Module = fx.Module(
	"chasm.lib.workflow.command",
	fx.Provide(NewRegistry),
)
