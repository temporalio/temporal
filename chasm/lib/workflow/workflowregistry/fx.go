package workflowregistry

import (
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"chasm.lib.workflow.workflowregistry",
	fx.Provide(NewRegistry),
	fx.Provide(func(r *Registry) chasmworkflow.EventRegistry { return r }),
)
