package adminbatcher

import (
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/worker/batcher"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.uber.org/fx"
)

type workerComponent struct {
	activities *batcher.Activities
}

var Module = fx.Options(
	fx.Provide(fx.Annotate(newComponent, fx.ResultTags(workercommon.WorkerComponentTag))),
)

func newComponent(
	deps batcher.ActivityDeps,
	dc *dynamicconfig.Collection,
	namespaceRegistry namespace.Registry,
) workercommon.WorkerComponent {
	return &workerComponent{
		activities: batcher.NewActivities(deps, dc, validateAndResolveNSForAdminBatch(namespaceRegistry)),
	}
}

func (c *workerComponent) RegisterWorkflow(registry sdkworker.Registry) {
	registry.RegisterWorkflowWithOptions(Workflow, workflow.RegisterOptions{Name: WorkflowTypeName})
}

func (c *workerComponent) DedicatedWorkflowWorkerOptions() *workercommon.DedicatedWorkerOptions {
	return nil
}

func (c *workerComponent) RegisterActivities(registry sdkworker.Registry) {
	registry.RegisterActivity(c.activities)
}

func (c *workerComponent) DedicatedActivityWorkerOptions() *workercommon.DedicatedWorkerOptions {
	return &workercommon.DedicatedWorkerOptions{
		TaskQueue: primitives.AdminBatchActivityTQ,
	}
}
