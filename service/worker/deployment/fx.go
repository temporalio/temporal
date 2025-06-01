package deployment

import (
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.uber.org/fx"
)

type (
	workerComponent struct {
		activityDeps activityDeps
	}

	activityDeps struct {
		fx.In
		MetricsHandler   metrics.Handler
		Logger           log.Logger
		ClientFactory    sdk.ClientFactory
		MatchingClient   resource.MatchingClient
		DeploymentClient DeploymentStoreClient
	}

	fxResult struct {
		fx.Out
		Component workercommon.PerNSWorkerComponent `group:"perNamespaceWorkerComponent"`
	}
)

var Module = fx.Options(
	fx.Provide(NewResult),
	fx.Provide(DeploymentStoreClientProvider),
)

func DeploymentStoreClientProvider(
	logger log.Logger,
	historyClient resource.HistoryClient,
	visibilityManager manager.VisibilityManager,
	dc *dynamicconfig.Collection,
) DeploymentStoreClient {
	return &DeploymentClientImpl{
		logger:                logger,
		historyClient:         historyClient,
		visibilityManager:     visibilityManager,
		maxIDLengthLimit:      dynamicconfig.MaxIDLengthLimit.Get(dc),
		visibilityMaxPageSize: dynamicconfig.FrontendVisibilityMaxPageSize.Get(dc),
		reachabilityCache: newReachabilityCache(
			metrics.NoopMetricsHandler,
			visibilityManager,
			dynamicconfig.ReachabilityCacheOpenWFsTTL.Get(dc)(),
			dynamicconfig.ReachabilityCacheClosedWFsTTL.Get(dc)(),
		),
		maxTaskQueuesInDeployment: dynamicconfig.MatchingMaxTaskQueuesInDeployment.Get(dc),
	}
}

func NewResult(
	dc *dynamicconfig.Collection,
	params activityDeps,
) fxResult {
	return fxResult{
		Component: &workerComponent{
			activityDeps: params,
		},
	}
}

func (s *workerComponent) DedicatedWorkerOptions(ns *namespace.Namespace) *workercommon.PerNSDedicatedWorkerOptions {
	return &workercommon.PerNSDedicatedWorkerOptions{
		Enabled: true,
	}
}

func (s *workerComponent) Register(registry sdkworker.Registry, ns *namespace.Namespace, details workercommon.RegistrationDetails) func() {
	registry.RegisterWorkflowWithOptions(DeploymentWorkflow, workflow.RegisterOptions{Name: DeploymentWorkflowType})
	registry.RegisterWorkflowWithOptions(DeploymentSeriesWorkflow, workflow.RegisterOptions{Name: DeploymentSeriesWorkflowType})

	deploymentActivities := &DeploymentActivities{
		namespace:        ns,
		deploymentClient: s.activityDeps.DeploymentClient,
		matchingClient:   s.activityDeps.MatchingClient,
	}
	registry.RegisterActivity(deploymentActivities)

	deploymentSeriesActivities := &DeploymentSeriesActivities{
		namespace:        ns,
		deploymentClient: s.activityDeps.DeploymentClient,
	}
	registry.RegisterActivity(deploymentSeriesActivities)

	return nil
}
