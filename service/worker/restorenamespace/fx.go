package restorenamespace

import (
	"go.temporal.io/sdk/activity"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.uber.org/fx"
)

type (
	component struct {
		activities *Activities
	}

	componentParams struct {
		fx.In
		DynamicCollection *dynamicconfig.Collection
		VisibilityManager manager.VisibilityManager
		NamespaceRegistry namespace.Registry
		ClientBean        client.Bean
		ClusterMetadata   cluster.Metadata
		MetricsHandler    metrics.Handler
		Logger            log.Logger
		ThrottledLogger   log.ThrottledLogger
	}
)

var Module = workercommon.AnnotateWorkerComponentProvider(newComponent)

func newComponent(params componentParams) workercommon.WorkerComponent {
	return &component{
		activities: NewActivities(
			params.VisibilityManager,
			params.NamespaceRegistry,
			params.ClientBean,
			params.ClusterMetadata.GetCurrentClusterName(),
			dynamicconfig.NamespaceRestoreRefreshRPS.Get(params.DynamicCollection),
			dynamicconfig.NamespaceRestoreRefreshConcurrency.Get(params.DynamicCollection),
			params.MetricsHandler,
			params.Logger,
			params.ThrottledLogger,
		),
	}
}

func (c *component) RegisterWorkflow(registry sdkworker.Registry) {
	registry.RegisterWorkflowWithOptions(RestoreNamespaceWorkflow, workflow.RegisterOptions{Name: WorkflowName})
}

func (c *component) DedicatedWorkflowWorkerOptions() *workercommon.DedicatedWorkerOptions {
	return nil
}

func (c *component) RegisterActivities(registry sdkworker.Registry) {
	registry.RegisterActivityWithOptions(c.activities.RefreshExecutions, activity.RegisterOptions{Name: RefreshExecutionsActivityName})
	registry.RegisterActivityWithOptions(c.activities.CompleteRestore, activity.RegisterOptions{Name: CompleteRestoreActivityName})
}

func (c *component) DedicatedActivityWorkerOptions() *workercommon.DedicatedWorkerOptions {
	return nil
}
