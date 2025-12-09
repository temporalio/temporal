package scheduler

import (
	"fmt"
	"math"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/searchattribute/sadefs"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.uber.org/fx"
)

const (
	WorkflowType      = "temporal-sys-scheduler-workflow"
	NamespaceDivision = "TemporalScheduler"
)

// VisibilityBaseListQuery will select schedules handled by both V1 scheduler and
// V2 CHASM scheduler.
var VisibilityBaseListQuery = fmt.Sprintf(
	"%s IN ('%s', '%d') AND %s = '%s'",
	sadefs.TemporalNamespaceDivision,
	NamespaceDivision,
	chasm.SchedulerArchetypeID,
	sadefs.ExecutionStatus,
	enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
)

type (
	workerComponent struct {
		specBuilder              *SpecBuilder // workflow dep
		activityDeps             activityDeps
		enabledForNs             dynamicconfig.BoolPropertyFnWithNamespaceFilter
		globalNSStartWorkflowRPS dynamicconfig.TypedSubscribableWithNamespaceFilter[float64]
		maxBlobSize              dynamicconfig.IntPropertyFnWithNamespaceFilter
		localActivitySleepLimit  dynamicconfig.DurationPropertyFnWithNamespaceFilter
	}

	activityDeps struct {
		fx.In
		MetricsHandler metrics.Handler
		Logger         log.Logger
		HistoryClient  resource.HistoryClient
		FrontendClient workflowservice.WorkflowServiceClient
	}

	fxResult struct {
		fx.Out
		Component workercommon.PerNSWorkerComponent `group:"perNamespaceWorkerComponent"`
	}
)

var Module = fx.Options(
	fx.Provide(NewResult),
	// SpecBuilder is provided as part of chasm Scheduler module at top level.
)

func NewResult(
	dc *dynamicconfig.Collection,
	specBuilder *SpecBuilder,
	params activityDeps,
) fxResult {
	return fxResult{
		Component: &workerComponent{
			specBuilder:              specBuilder,
			activityDeps:             params,
			enabledForNs:             dynamicconfig.WorkerEnableScheduler.Get(dc),
			globalNSStartWorkflowRPS: dynamicconfig.SchedulerNamespaceStartWorkflowRPS.Subscribe(dc),
			maxBlobSize:              dynamicconfig.BlobSizeLimitError.Get(dc),
			localActivitySleepLimit:  dynamicconfig.SchedulerLocalActivitySleepLimit.Get(dc),
		},
	}
}

func (s *workerComponent) DedicatedWorkerOptions(ns *namespace.Namespace) *workercommon.PerNSDedicatedWorkerOptions {
	return &workercommon.PerNSDedicatedWorkerOptions{
		Enabled: s.enabledForNs(ns.Name().String()),
	}
}

func (s *workerComponent) Register(registry sdkworker.Registry, ns *namespace.Namespace, details workercommon.RegistrationDetails) func() {
	wfFunc := func(ctx workflow.Context, args *schedulespb.StartScheduleArgs) error {
		return schedulerWorkflowWithSpecBuilder(ctx, args, s.specBuilder)
	}
	registry.RegisterWorkflowWithOptions(wfFunc, workflow.RegisterOptions{Name: WorkflowType})

	activities, cleanup := s.newActivities(ns.Name(), ns.ID(), details)
	registry.RegisterActivity(activities)
	return cleanup
}

func (s *workerComponent) newActivities(name namespace.Name, id namespace.ID, details workercommon.RegistrationDetails) (*activities, func()) {
	const burstRatio = 1.0

	lim := quotas.NewRateLimiter(1, 1)
	cb := func(rps float64) {
		localRPS := rps * float64(details.Multiplicity) / float64(details.TotalWorkers)
		burst := max(1, int(math.Ceil(localRPS*burstRatio)))
		lim.SetRateBurst(localRPS, burst)
	}
	initialRPS, cancel := s.globalNSStartWorkflowRPS(name.String(), cb)
	cb(initialRPS)

	return &activities{
		activityDeps:             s.activityDeps,
		namespace:                name,
		namespaceID:              id,
		startWorkflowRateLimiter: lim,
		maxBlobSize:              func() int { return s.maxBlobSize(name.String()) },
		localActivitySleepLimit:  func() time.Duration { return s.localActivitySleepLimit(name.String()) },
	}, cancel
}
