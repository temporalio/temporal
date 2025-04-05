package systemconfig

import (
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/persistence"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.uber.org/fx"
)

type (
	initParams struct {
		fx.In
		ClusterMetadataManager persistence.ClusterMetadataManager
		ClusterMetadata        cluster.Metadata
	}

	fxResult struct {
		fx.Out
		Component workercommon.WorkerComponent `group:"workerComponent"`
	}

	clusterWorkerComponent struct {
		initParams
	}
)

var Module = fx.Options(
	fx.Provide(NewResult),
)

func NewResult(params initParams) fxResult {
	component := &clusterWorkerComponent{
		initParams: params,
	}
	return fxResult{
		Component: component,
	}
}

func (wc *clusterWorkerComponent) RegisterWorkflow(registry sdkworker.Registry) {
	registry.RegisterWorkflowWithOptions(UpdateFailoverVersionIncrementWorkflow, workflow.RegisterOptions{Name: updateFailoverVersionIncrementWorkflowName})
}

func (wc *clusterWorkerComponent) DedicatedWorkflowWorkerOptions() *workercommon.DedicatedWorkerOptions {
	// Use default worker
	return nil
}

func (wc *clusterWorkerComponent) RegisterActivities(registry sdkworker.Registry) {
	registry.RegisterActivity(wc.activities())
}

func (wc *clusterWorkerComponent) DedicatedActivityWorkerOptions() *workercommon.DedicatedWorkerOptions {
	return nil
}

func (wc *clusterWorkerComponent) activities() *activities {
	return &activities{
		currentClusterName:     wc.ClusterMetadata.GetCurrentClusterName(),
		clusterMetadataManager: wc.ClusterMetadataManager,
	}
}
