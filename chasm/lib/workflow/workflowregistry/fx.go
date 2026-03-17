package workflowregistry

import "go.uber.org/fx"

var Module = fx.Module(
	"chasm.lib.workflow.workflowregistry",
	fx.Provide(NewRegistry),
)
