package migration

import (
	"context"
	"fmt"

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
		activities *activities
	}

	// shardedWorkerComponent registers the sharded force-replication
	// workflow + ReplicateBatch activity on their dedicated TQ. Holds an
	// *activities-sized clone so its activity registration is isolated
	// from the default-TQ worker — sharded inject paths don't accidentally
	// land on the legacy MigrationActivityTQ.
	shardedWorkerComponent struct {
		activities *activities
	}
)

var Module = fx.Options(
	fx.Provide(NewResult),
	fx.Provide(NewShardedResult),
	fx.Provide(workflowVerifierProvider),
)

func NewResult(params initParams) (fxResult, error) {
	a, err := newActivitiesFromParams(params, forceReplicationWorkflowName)
	if err != nil {
		return fxResult{}, err
	}
	return fxResult{
		Component: &replicationWorkerComponent{
			initParams: params,
			activities: a,
		},
	}, nil
}

// NewShardedResult constructs the sharded WorkerComponent. The component
// owns its own *activities clone so registration against the sharded TQ
// doesn't bleed into the legacy worker.
func NewShardedResult(params initParams) (fxResult, error) {
	a, err := newActivitiesFromParams(params, shardedForceReplicationWorkflowName)
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
	// DisableAlreadyRegisteredCheck because the sharded WorkerComponent
	// shares the *activities method set; whichever component registers
	// first on the default worker wins (per the worker.go upgrade-hack
	// pass), and the second component's reflection-based registration
	// would otherwise panic on every method name. The default worker
	// isn't dispatched to by either workflow — both have dedicated
	// activity workers — so winner-takes-all is fine.
	registry.RegisterActivityWithOptions(wc.activities, activity.RegisterOptions{
		DisableAlreadyRegisteredCheck: true,
	})
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
	registry.RegisterWorkflowWithOptions(ForceTaskQueueUserDataReplicationWorkflow, workflow.RegisterOptions{
		Name: forceTaskQueueUserDataReplicationWorkflow,
	})
	// Local activities dispatch from the workflow worker's own registry, so
	// the ones invoked via ExecuteLocalActivity (GetMetadata,
	// DescribeTargetCluster) need to be visible here too. Registering the
	// whole *activities set mirrors the activity-worker registration; the
	// workflow worker has LocalActivityWorkerOnly=true so this doesn't
	// race the activity worker for regular activity tasks.
	registry.RegisterActivityWithOptions(sc.activities, activity.RegisterOptions{
		DisableAlreadyRegisteredCheck: true,
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
	// See replicationWorkerComponent.RegisterActivities — both components
	// share the *activities method set, so the second registration on the
	// default worker would otherwise panic.
	registry.RegisterActivityWithOptions(sc.activities, activity.RegisterOptions{
		DisableAlreadyRegisteredCheck: true,
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

// newActivitiesFromParams builds the shared *activities struct from the
// fx params. workflowTypeName tags the forceReplicationMetricsHandler so
// the legacy and sharded variants emit force-replication metrics under
// distinct workflow_type tags.
//
// adminClient is the local admin client cached by ClientBean at startup.
// Routing through the bean (rather than constructing a fresh wrapper via
// NewLocalAdminClientWithTimeout) reuses the same retry+metric wrapper
// every other consumer in the process sees, and guarantees adminClient
// is non-nil so the inject and verify paths can use it without nil
// guarding. A lookup failure indicates ClusterMetadata is misconfigured;
// surfacing it as an fx error fails app start cleanly rather than mid-run.
func newActivitiesFromParams(params initParams, workflowTypeName string) (*activities, error) {
	localCluster := params.ClusterMetadata.GetCurrentClusterName()
	localAdmin, err := params.ClientBean.GetRemoteAdminClient(localCluster)
	if err != nil {
		return nil, fmt.Errorf("migration: local admin client missing from ClientBean for cluster %q: %w", localCluster, err)
	}
	return &activities{
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
		MetricsHandler:                   params.MetricsHandler,
		forceReplicationMetricsHandler:   params.MetricsHandler.WithTags(metrics.WorkflowTypeTag(workflowTypeName)),
		generateMigrationTaskViaFrontend: dynamicconfig.WorkerGenerateMigrationTaskViaFrontend.Get(params.DynamicCollection),
		enableHistoryRateLimiter:         dynamicconfig.WorkerEnableHistoryRateLimiter.Get(params.DynamicCollection),
		workflowVerifier:                 params.WorkflowVerifier,
		chasmRegistry:                    params.ChasmRegistry,
		sdkClientFactory:                 params.SDKClientFactory,
	}, nil
}
