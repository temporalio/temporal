package batcher

import (
	"go.temporal.io/api/workflowservice/v1"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/quotas/calculator"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.uber.org/fx"
)

const (
	// BatchWFTypeName is the workflow type
	BatchWFTypeName         = "temporal-sys-batch-workflow"
	BatchWFTypeProtobufName = "temporal-sys-batch-workflow-protobuf"
	NamespaceDivision       = "TemporalBatcher"
	AdminNamespaceDivision  = "TemporalAdminBatcher"
)

type (
	AdminBatcherRateLimiter quotas.RequestRateLimiter

	workerComponent struct {
		activityDeps   activityDeps
		dc             *dynamicconfig.Collection
		enabledFeature dynamicconfig.BoolPropertyFnWithNamespaceFilter
	}

	activityDeps struct {
		fx.In
		MetricsHandler          metrics.Handler
		Logger                  log.Logger
		ClientFactory           sdk.ClientFactory
		FrontendClient          workflowservice.WorkflowServiceClient
		AdminClient             adminservice.AdminServiceClient
		HistoryClient           resource.HistoryClient
		AdminBatcherRateLimiter AdminBatcherRateLimiter
	}

	fxResult struct {
		fx.Out
		Component workercommon.PerNSWorkerComponent `group:"perNamespaceWorkerComponent"`
	}
)

var Module = fx.Options(
	fx.Provide(AdminBatcherRateLimiterProvider),
	fx.Provide(NewResult),
)

func AdminBatcherRateLimiterProvider(
	dc *dynamicconfig.Collection,
	serviceResolver membership.ServiceResolver,
	logger log.Logger,
) AdminBatcherRateLimiter {
	return quotas.NewRequestRateLimiterAdapter(
		quotas.NewDefaultOutgoingRateLimiter(
			calculator.NewLoggedCalculator(
				calculator.ClusterAwareQuotaCalculator{
					MemberCounter:    serviceResolver,
					PerInstanceQuota: dynamicconfig.AdminBatcherHostRPS.Get(dc),
					GlobalQuota:      dynamicconfig.AdminBatcherGlobalRPS.Get(dc),
				},
				log.With(logger, tag.ComponentAdminBatcher, tag.ScopeHost),
			).GetQuota,
		),
	)
}

func NewResult(
	dc *dynamicconfig.Collection,
	params activityDeps,
) fxResult {
	return fxResult{
		Component: &workerComponent{
			activityDeps:   params,
			dc:             dc,
			enabledFeature: dynamicconfig.EnableBatcherNamespace.Get(dc),
		},
	}
}

func (s *workerComponent) DedicatedWorkerOptions(ns *namespace.Namespace) *workercommon.PerNSDedicatedWorkerOptions {
	namespaceName := ns.Name().String()
	enableFeature := s.enabledFeature(namespaceName)
	return &workercommon.PerNSDedicatedWorkerOptions{
		Enabled: enableFeature,
	}
}

func (s *workerComponent) Register(registry sdkworker.Registry, ns *namespace.Namespace, _ workercommon.RegistrationDetails) func() {
	// Register the batch workflow with both the proto-qualified and unqualified types.
	// TODO(spkane31): Remove the proto-qualified type and call the unqualified type from the frontend after the 1.30 release.
	registry.RegisterWorkflowWithOptions(BatchWorkflowProtobuf, workflow.RegisterOptions{Name: BatchWFTypeName})
	// Newer version of the batch workflow which was rewritten to accept a proto struct as input.
	registry.RegisterWorkflowWithOptions(BatchWorkflowProtobuf, workflow.RegisterOptions{Name: BatchWFTypeProtobufName})
	registry.RegisterActivity(s.activities(ns.Name(), ns.ID()))
	return nil
}

func (s *workerComponent) activities(name namespace.Name, id namespace.ID) *activities {
	return &activities{
		activityDeps: s.activityDeps,
		namespace:    name,
		namespaceID:  id,
		rps:          dynamicconfig.BatcherRPS.Get(s.dc),
		concurrency:  dynamicconfig.BatcherConcurrency.Get(s.dc),
	}
}
