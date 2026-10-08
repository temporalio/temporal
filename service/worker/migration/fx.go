package migration

import (
	"context"
	"fmt"

	otellog "go.opentelemetry.io/otel/log"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	serverClient "go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.uber.org/fx"
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
		ClusterMetadata           cluster.Metadata
		NamespaceReplicationQueue persistence.NamespaceReplicationQueue
		TaskManager               persistence.TaskManager
		Logger                    log.Logger
		EventLogger               otellog.Logger
		MetricsHandler            metrics.Handler
		DynamicCollection         *dynamicconfig.Collection
		WorkflowVerifier          WorkflowVerifier
		ChasmRegistry             *chasm.Registry
		SDKClientFactory          sdk.ClientFactory
	}

	fxResult struct {
		fx.Out
		Component workercommon.WorkerComponent `group:"workerComponent"`
	}

	replicationWorkerComponent struct {
		initParams
	}

	// shardedWorkerComponent registers the sharded force-replication
	// workflows and the activities they call on a dedicated task queue.
	// It holds its own *activities so its activity registration targets
	// the sharded TQ rather than the legacy MigrationActivityTQ.
	shardedWorkerComponent struct {
		activities *shardedActivities
	}
)

var Module = fx.Options(
	fx.Provide(NewResult),
	fx.Provide(NewShardedResult),
	fx.Provide(workflowVerifierProvider),
)

func NewResult(params initParams) fxResult {
	return fxResult{
		Component: &replicationWorkerComponent{initParams: params},
	}
}

// NewShardedResult constructs the sharded WorkerComponent.
func NewShardedResult(params initParams) (fxResult, error) {
	a, err := newShardedActivities(params)
	if err != nil {
		return fxResult{}, err
	}
	return fxResult{
		Component: &shardedWorkerComponent{activities: a},
	}, nil
}

func (wc *replicationWorkerComponent) RegisterWorkflow(registry sdkworker.Registry) {
	registry.RegisterWorkflowWithOptions(CatchupWorkflow, workflow.RegisterOptions{Name: catchupWorkflowName})
	registry.RegisterWorkflowWithOptions(ForceReplicationWorkflow, workflow.RegisterOptions{Name: forceReplicationWorkflowName})
	registry.RegisterWorkflowWithOptions(ForceReplicationWorkflowV2, workflow.RegisterOptions{Name: forceReplicationWorkflowV2Name})
	registry.RegisterWorkflowWithOptions(NamespaceHandoverWorkflow, workflow.RegisterOptions{Name: namespaceHandoverWorkflowName})
	registry.RegisterWorkflowWithOptions(NamespaceHandoverWorkflowV2, workflow.RegisterOptions{Name: namespaceHandoverWorkflowV2Name})
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

func (sc *shardedWorkerComponent) RegisterWorkflow(registry sdkworker.Registry) {
	registry.RegisterWorkflowWithOptions(ShardedForceReplicationWorkflow, workflow.RegisterOptions{
		Name: shardedForceReplicationWorkflowName,
	})
	registry.RegisterWorkflowWithOptions(shardedForceReplicationWorker, workflow.RegisterOptions{
		Name: shardedForceReplicationWorkerName,
	})
	registry.RegisterWorkflowWithOptions(shardedTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{
		Name: shardedTaskQueueUserDataReplicationWorkflowName,
	})
	registry.RegisterActivityWithOptions(sc.activities, activity.RegisterOptions{
		Name: shardedActivityPrefix,
	})
}

func (sc *shardedWorkerComponent) DedicatedWorkflowWorkerOptions() *workercommon.DedicatedWorkerOptions {
	// Workflow + activity share the same TQ so the workflow's default
	// ExecuteActivity (no explicit TaskQueue) routes to our dedicated
	// activity worker rather than the default-TQ worker. Without a
	// dedicated workflow worker here the workflow would land on
	// default-worker-tq and its activities would pile up on the (separate,
	// our-TQ) dedicated activity worker, unscheduled.
	//
	// LocalActivityWorkerOnly is essential: by default a worker polls for
	// both workflow and activity tasks on its TQ. Since the activity
	// worker (a separate sdkworker.Worker) also polls this TQ and is the
	// one that owns the registered activities, leaving activity polling
	// enabled here means this worker races for activity tasks and
	// dispatches them with no registrations — ActivityNotRegisteredError,
	// "Supported types: []".
	return &workercommon.DedicatedWorkerOptions{
		TaskQueue: primitives.MigrationShardedActivityTQ,
		Options: sdkworker.Options{
			LocalActivityWorkerOnly: true,
		},
	}
}

func (sc *shardedWorkerComponent) RegisterActivities(registry sdkworker.Registry) {
	registry.RegisterActivityWithOptions(sc.activities, activity.RegisterOptions{
		Name: shardedActivityPrefix,
	})
}

func (sc *shardedWorkerComponent) DedicatedActivityWorkerOptions() *workercommon.DedicatedWorkerOptions {
	return &workercommon.DedicatedWorkerOptions{
		TaskQueue: primitives.MigrationShardedActivityTQ,
		Options: sdkworker.Options{
			BackgroundActivityContext: headers.SetCallerType(context.Background(), headers.CallerTypePreemptable),
		},
	}
}

func workflowVerifierProvider() WorkflowVerifier {
	return func(
		_ context.Context,
		_ *verifyReplicationTasksRequest,
		_ adminservice.AdminServiceClient,
		_ adminservice.AdminServiceClient,
		_ *namespace.Namespace,
		_ *ExecutionInfo,
		_ *adminservice.DescribeMutableStateResponse,
	) (verifyResult, error) {
		return verifyResult{
			status: verified,
		}, nil
	}
}

func (wc *replicationWorkerComponent) activities() *activities {
	return &activities{
		HistoryShardCount:                wc.PersistenceConfig.NumHistoryShards,
		executionManager:                 wc.ExecutionManager,
		NamespaceRegistry:                wc.NamespaceRegistry,
		HistoryClient:                    wc.HistoryClient,
		frontendClient:                   wc.FrontendClient,
		clientFactory:                    wc.ClientFactory,
		clientBean:                       wc.ClientBean,
		namespaceReplicationQueue:        wc.NamespaceReplicationQueue,
		taskManager:                      wc.TaskManager,
		Logger:                           wc.Logger,
		EventLogger:                      wc.EventLogger,
		MetricsHandler:                   wc.MetricsHandler,
		forceReplicationMetricsHandler:   wc.MetricsHandler.WithTags(metrics.WorkflowTypeTag(forceReplicationWorkflowName)),
		generateMigrationTaskViaFrontend: dynamicconfig.WorkerGenerateMigrationTaskViaFrontend.Get(wc.DynamicCollection),
		enableHistoryRateLimiter:         dynamicconfig.WorkerEnableHistoryRateLimiter.Get(wc.DynamicCollection),
		emitNamespaceLifecycleEvents:     dynamicconfig.EmitNamespaceLifecycleEvents.Get(wc.DynamicCollection),
		workflowVerifier:                 wc.WorkflowVerifier,
		chasmRegistry:                    wc.ChasmRegistry,
	}
}

// newShardedActivities builds the activity set used only by the sharded
// workflow workers. The local admin client is cached by ClientBean at startup.
// Routing through the bean (rather than constructing a fresh wrapper via
// NewLocalAdminClientWithTimeout) reuses the same retry+metric wrapper
// every other consumer in the process sees.
func newShardedActivities(params initParams) (*shardedActivities, error) {
	localCluster := params.ClusterMetadata.GetCurrentClusterName()
	localAdmin, err := params.ClientBean.GetRemoteAdminClient(localCluster)
	if err != nil {
		return nil, fmt.Errorf("migration: local admin client missing from ClientBean for cluster %q: %w", localCluster, err)
	}
	return &shardedActivities{activities: &activities{
		HistoryShardCount:                params.PersistenceConfig.NumHistoryShards,
		executionManager:                 params.ExecutionManager,
		NamespaceRegistry:                params.NamespaceRegistry,
		HistoryClient:                    params.HistoryClient,
		frontendClient:                   params.FrontendClient,
		adminClient:                      localAdmin,
		clientFactory:                    params.ClientFactory,
		clientBean:                       params.ClientBean,
		clusterMetadata:                  params.ClusterMetadata,
		namespaceReplicationQueue:        params.NamespaceReplicationQueue,
		taskManager:                      params.TaskManager,
		Logger:                           params.Logger,
		EventLogger:                      params.EventLogger,
		MetricsHandler:                   params.MetricsHandler,
		forceReplicationMetricsHandler:   params.MetricsHandler.WithTags(metrics.WorkflowTypeTag(shardedForceReplicationWorkflowName)),
		generateMigrationTaskViaFrontend: dynamicconfig.WorkerGenerateMigrationTaskViaFrontend.Get(params.DynamicCollection),
		enableHistoryRateLimiter:         dynamicconfig.WorkerEnableHistoryRateLimiter.Get(params.DynamicCollection),
		emitNamespaceLifecycleEvents:     dynamicconfig.EmitNamespaceLifecycleEvents.Get(params.DynamicCollection),
		workflowVerifier:                 params.WorkflowVerifier,
		chasmRegistry:                    params.ChasmRegistry,
		sdkClientFactory:                 params.SDKClientFactory,
	}}, nil
}
