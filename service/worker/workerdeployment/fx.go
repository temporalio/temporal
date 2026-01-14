package workerdeployment

import (
	"time"

	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/testing/testhooks"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.uber.org/fx"
)

type DeploymentWorkflowVersion int64

const (
	// Versions of workflow logic. When introducing a new version, consider generating a new
	// history for TestReplays using generate_history.sh.

	// Represents the state before the versioning API's received the option of becoming async in nature
	InitialVersion DeploymentWorkflowVersion = iota
	// SetCurrent and SetRamping and DeleteVersion APIs are async
	AsyncSetCurrentAndRamping
	// Version Data has its own revision number with TaskQueue registration being async as well
	VersionDataRevisionNumber
)

type (
	workerComponent struct {
		activityDeps  activityDeps
		dynamicConfig *dynamicconfig.Collection
	}

	activityDeps struct {
		fx.In
		MetricsHandler         metrics.Handler
		Logger                 log.Logger
		ClientFactory          sdk.ClientFactory
		MatchingClient         resource.MatchingClient
		HistoryClient          resource.HistoryClient
		WorkerDeploymentClient Client
	}

	fxResult struct {
		fx.Out
		Component workercommon.PerNSWorkerComponent `group:"perNamespaceWorkerComponent"`
	}
)

var Module = fx.Options(
	fx.Provide(NewResult),
	fx.Provide(ClientProvider),
)

func ClientProvider(
	logger log.Logger,
	historyClient resource.HistoryClient,
	matchingClient resource.MatchingClient,
	visibilityManager manager.VisibilityManager,
	dc *dynamicconfig.Collection,
	testHooks testhooks.TestHooks,
	metricsHandler metrics.Handler,
) Client {
	return &ClientImpl{
		logger:                           logger,
		historyClient:                    historyClient,
		visibilityManager:                visibilityManager,
		matchingClient:                   matchingClient,
		maxIDLengthLimit:                 dynamicconfig.MaxIDLengthLimit.Get(dc),
		visibilityMaxPageSize:            dynamicconfig.FrontendVisibilityMaxPageSize.Get(dc),
		maxTaskQueuesInDeploymentVersion: dynamicconfig.MatchingMaxTaskQueuesInDeploymentVersion.Get(dc),
		maxDeployments:                   dynamicconfig.MatchingMaxDeployments.Get(dc),
		testHooks:                        testHooks,
		metricsHandler:                   metricsHandler,
	}
}

func NewResult(
	dc *dynamicconfig.Collection,
	params activityDeps,
) fxResult {
	return fxResult{
		Component: &workerComponent{
			activityDeps:  params,
			dynamicConfig: dc,
		},
	}
}

func (s *workerComponent) DedicatedWorkerOptions(ns *namespace.Namespace) *workercommon.PerNSDedicatedWorkerOptions {
	return &workercommon.PerNSDedicatedWorkerOptions{
		Enabled: true,
	}
}

func (s *workerComponent) Register(registry sdkworker.Registry, ns *namespace.Namespace, details workercommon.RegistrationDetails) func() {
	workflowVersionGetter := func() DeploymentWorkflowVersion {
		val := DeploymentWorkflowVersion(dynamicconfig.MatchingDeploymentWorkflowVersion.Get(s.dynamicConfig)(ns.Name().String()))
		return val
	}

	versionWorkflow := func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentVersionWorkflowArgs) error {
		refreshIntervalGetter := func() time.Duration {
			return dynamicconfig.VersionDrainageStatusRefreshInterval.Get(s.dynamicConfig)(ns.Name().String())
		}
		visibilityGracePeriodGetter := func() time.Duration {
			return dynamicconfig.VersionDrainageStatusVisibilityGracePeriod.Get(s.dynamicConfig)(ns.Name().String())
		}
		return VersionWorkflow(ctx, workflowVersionGetter, refreshIntervalGetter, visibilityGracePeriodGetter, args)
	}
	registry.RegisterWorkflowWithOptions(versionWorkflow, workflow.RegisterOptions{Name: WorkerDeploymentVersionWorkflowType})

	deploymentWorkflow := func(ctx workflow.Context, args *deploymentspb.WorkerDeploymentWorkflowArgs) error {
		maxVersionsGetter := func() int {
			return dynamicconfig.MatchingMaxVersionsInDeployment.Get(s.dynamicConfig)(ns.Name().String())
		}
		return Workflow(ctx, workflowVersionGetter, maxVersionsGetter, args)
	}
	registry.RegisterWorkflowWithOptions(deploymentWorkflow, workflow.RegisterOptions{Name: WorkerDeploymentWorkflowType})

	versionActivities := &VersionActivities{
		activityDeps: s.activityDeps,
		namespace:    ns,
	}
	registry.RegisterActivity(versionActivities)

	activities := &Activities{
		activityDeps: s.activityDeps,
		namespace:    ns,
	}
	registry.RegisterActivity(activities)
	return nil
}
