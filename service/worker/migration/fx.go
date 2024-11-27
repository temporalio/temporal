package migration

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"

	serverClient "go.temporal.io/server/client"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	workercommon "go.temporal.io/server/service/worker/common"
)

type (
	initParams struct {
		fx.In
		PersistenceConfig         *config.Persistence
		ExecutionManager          persistence.ExecutionManager
		NamespaceRegistry         namespace.Registry
		HistoryClient             resource.HistoryClient
		FrontendClient            workflowservice.WorkflowServiceClient
		ClientFactory             serverClient.Factory
		ClientBean                serverClient.Bean
		NamespaceReplicationQueue persistence.NamespaceReplicationQueue
		TaskManager               persistence.TaskManager
		Logger                    log.Logger
		MetricsHandler            metrics.Handler
	}

	fxResult struct {
		fx.Out
		Component workercommon.WorkerComponent `group:"workerComponent"`
	}

	replicationWorkerComponent struct {
		initParams
	}
)

var Module = fx.Options(
	fx.Provide(NewResult),
)

func NewResult(params initParams) fxResult {
	component := &replicationWorkerComponent{
		initParams: params,
	}
	return fxResult{
		Component: component,
	}
}

func (wc *replicationWorkerComponent) RegisterWorkflow(registry sdkworker.Registry) {
	registry.RegisterWorkflowWithOptions(ForceReplicationWorkflow, workflow.RegisterOptions{Name: forceReplicationWorkflowName})
	registry.RegisterWorkflowWithOptions(NamespaceHandoverWorkflow, workflow.RegisterOptions{Name: namespaceHandoverWorkflowName})
	registry.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{Name: forceTaskQueueUserDataReplicationWorkflow})
}

func (wc *replicationWorkerComponent) DedicatedWorkflowWorkerOptions() *workercommon.DedicatedWorkerOptions {
	// Use default worker
	return nil
}

func (wc *replicationWorkerComponent) RegisterActivities(registry sdkworker.Registry) {
	registry.RegisterActivity(wc.activities())
}

func (wc *replicationWorkerComponent) DedicatedActivityWorkerOptions() *workercommon.DedicatedWorkerOptions {
	return &workercommon.DedicatedWorkerOptions{
		TaskQueue: primitives.MigrationActivityTQ,
		Options: sdkworker.Options{
			BackgroundActivityContext: headers.SetCallerType(context.Background(), headers.CallerTypePreemptable),
		},
	}
}

func (wc *replicationWorkerComponent) activities() *activities {
	return &activities{
		historyShardCount:              wc.PersistenceConfig.NumHistoryShards,
		executionManager:               wc.ExecutionManager,
		namespaceRegistry:              wc.NamespaceRegistry,
		historyClient:                  wc.HistoryClient,
		frontendClient:                 wc.FrontendClient,
		clientFactory:                  wc.ClientFactory,
		clientBean:                     wc.ClientBean,
		namespaceReplicationQueue:      wc.NamespaceReplicationQueue,
		taskManager:                    wc.TaskManager,
		logger:                         wc.Logger,
		metricsHandler:                 wc.MetricsHandler,
		forceReplicationMetricsHandler: wc.MetricsHandler.WithTags(metrics.WorkflowTypeTag(forceReplicationWorkflowName)),
	}
}
