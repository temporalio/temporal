package interceptor

import (
	"context"
	"testing"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	tallyprom "github.com/uber-go/tally/v4/prometheus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/api"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/quotas/quotastest"
	"go.temporal.io/server/common/testing/protorequire"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

func TestTelemetryConcurrentLimitGroup(t *testing.T) {
	t.Parallel()

	namespaceRPS := &serviceerror.ResourceExhausted{
		Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
		Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
	}
	systemRPS := &serviceerror.ResourceExhausted{
		Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
		Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
	}
	unrelatedConcurrent := *ErrNamespaceCountLimitServerBusy
	for _, tc := range []struct {
		name       string
		request    any
		method     string
		reject     bool
		handlerErr *serviceerror.ResourceExhausted
		wantGroup  string
	}{
		{
			name: "regular workflow rejection", method: "PollWorkflowTaskQueue", reject: true, wantGroup: "default",
			request: &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: "test-namespace", TaskQueue: &taskqueuepb.TaskQueue{Name: "regular-tq"},
			},
		},
		{
			name: "sticky internal workflow rejection", method: "PollWorkflowTaskQueue", reject: true, wantGroup: "internal_per_ns",
			request: &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: "test-namespace", TaskQueue: &taskqueuepb.TaskQueue{
					Name: "sticky-tq", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: primitives.PerNSWorkerTaskQueue,
				},
			},
		},
		{
			name: "internal activity rejection", method: "PollActivityTaskQueue", reject: true, wantGroup: "internal_per_ns",
			request: &workflowservice.PollActivityTaskQueueRequest{
				Namespace: "test-namespace", TaskQueue: &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
			},
		},
		{
			name: "internal nexus rejection", method: "PollNexusTaskQueue", reject: true, wantGroup: "internal_per_ns",
			request: &workflowservice.PollNexusTaskQueueRequest{
				Namespace: "test-namespace", TaskQueue: &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
			},
		},
		{
			name: "query rejection", method: "QueryWorkflow", reject: true, wantGroup: "default",
			request: &workflowservice.QueryWorkflowRequest{Namespace: "test-namespace"},
		},
		{
			name: "regular poll namespace RPS", method: "PollWorkflowTaskQueue", handlerErr: namespaceRPS, wantGroup: "not_applicable",
			request: &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: "test-namespace", TaskQueue: &taskqueuepb.TaskQueue{Name: "regular-tq"},
			},
		},
		{
			name: "internal poll namespace RPS", method: "PollWorkflowTaskQueue", handlerErr: namespaceRPS, wantGroup: "not_applicable",
			request: &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: "test-namespace", TaskQueue: &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
			},
		},
		{
			name: "internal poll system RPS", method: "PollWorkflowTaskQueue", handlerErr: systemRPS, wantGroup: "not_applicable",
			request: &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: "test-namespace", TaskQueue: &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
			},
		},
		{
			name: "unrelated concurrent error with identical fields", method: "PollWorkflowTaskQueue", handlerErr: &unrelatedConcurrent, wantGroup: "not_applicable",
			request: &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: "test-namespace", TaskQueue: &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registry := namespace.NewMockRegistry(gomock.NewController(t))
			registry.EXPECT().GetNamespace(namespace.Name("test-namespace")).Return(&namespace.Namespace{}, nil).AnyTimes()
			logger := log.NewNoopLogger()
			logAllErrors := dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false)
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)
			telemetry := NewTelemetryInterceptor(registry, mh.WithTags(metrics.ServiceNameTag(primitives.FrontendService)),
				logger, logAllErrors, NewRequestErrorHandler(logger, logAllErrors))
			limit := 1
			if tc.reject {
				limit = 0
			}
			quotas := ConcurrentRequestQuotas{
				PerInstance: dynamicconfig.GetIntPropertyFnFilteredByNamespace(limit),
				Global:      dynamicconfig.GetIntPropertyFnFilteredByNamespace(0),
			}
			fullMethod := api.WorkflowServicePrefix + tc.method
			limiter := NewConcurrentRequestLimitInterceptor(registry, quotastest.NewFakeMemberCounter(1), logger,
				quotas, quotas, map[string]int{fullMethod: 1})
			info := &grpc.UnaryServerInfo{FullMethod: fullMethod}
			called := false
			_, err := telemetry.UnaryIntercept(context.Background(), tc.request, info, func(ctx context.Context, req any) (any, error) {
				return limiter.Intercept(ctx, req, info, func(context.Context, any) (any, error) {
					called = true
					return nil, tc.handlerErr
				})
			})
			wantErr := tc.handlerErr
			if tc.reject {
				wantErr = ErrNamespaceCountLimitServerBusy
			}
			require.ErrorIs(t, err, wantErr)
			require.Equal(t, !tc.reject, called)
			protorequire.ProtoEqual(t, wantErr.Status().Proto(), serviceerror.ToStatus(err).Proto())

			recordings := capture.SnapshotMetric(metrics.ServiceErrResourceExhaustedCounter.Name())
			require.Len(t, recordings, 1)
			require.Equal(t, int64(1), recordings[0].Value)
			require.Equal(t, map[string]string{
				"namespace": "test-namespace", "operation": tc.method, "service_name": "frontend",
				"resource_exhausted_cause": wantErr.Cause.String(), "resource_exhausted_scope": wantErr.Scope.String(),
				"concurrency_limit_group": tc.wantGroup,
			}, recordings[0].Tags)
			failures := 0
			if wantErr.Scope == enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM {
				failures = 1
			}
			require.Len(t, capture.SnapshotMetric(metrics.ServiceFailures.Name()), failures)
			require.Len(t, capture.SnapshotMetric(metrics.ServiceErrorWithType.Name()), 1)
			for name, samples := range capture.Snapshot() {
				if name == metrics.ServiceErrResourceExhaustedCounter.Name() || name == metrics.ServicePendingRequests.Name() {
					continue
				}
				for _, sample := range samples {
					require.NotContains(t, sample.Tags, "concurrency_limit_group", name)
				}
			}
		})
	}
}

func TestResourceExhaustedConcurrentLimitGroupsPrometheus(t *testing.T) {
	t.Parallel()

	for _, rpsFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "concurrency first", true: "RPS first"}[rpsFirst], func(t *testing.T) {
			t.Parallel()
			registry := prom.NewRegistry()
			var registrationErrors []error
			reporter := tallyprom.NewReporter(tallyprom.Options{
				Registerer: registry,
				OnRegisterError: func(err error) {
					registrationErrors = append(registrationErrors, err)
				},
			})
			scope, closer := tally.NewRootScope(tally.ScopeOptions{
				CachedReporter: reporter, Separator: "_", OmitCardinalityMetrics: true,
			}, 0)
			mh := metrics.NewTallyMetricsHandler(metrics.ClientConfig{}, scope).WithTags(
				metrics.NamespaceTag("test-namespace"), metrics.OperationTag("PollWorkflowTaskQueue"),
			)
			errorHandler := NewRequestErrorHandler(log.NewNoopLogger(), dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false))
			emitRPS := func() {
				errorHandler.HandleError(nil, "", mh, nil, &serviceerror.ResourceExhausted{
					Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
					Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
				}, "test-namespace")
			}
			if rpsFirst {
				emitRPS()
			}
			for _, queue := range []string{"regular-tq", primitives.PerNSWorkerTaskQueue} {
				errorHandler.HandleError(&workflowservice.PollWorkflowTaskQueueRequest{
					TaskQueue: &taskqueuepb.TaskQueue{Name: queue},
				}, "", mh, nil, ErrNamespaceCountLimitServerBusy, "test-namespace")
			}
			if !rpsFirst {
				emitRPS()
			}
			require.NoError(t, closer.Close())
			require.Empty(t, registrationErrors)
			families, err := registry.Gather()
			require.NoError(t, err)
			groups := make(map[string]string)
			for _, family := range families {
				if family.GetName() != metrics.ServiceErrResourceExhaustedCounter.Name() {
					continue
				}
				for _, sample := range family.GetMetric() {
					labels := make(map[string]string)
					for _, label := range sample.GetLabel() {
						labels[label.GetName()] = label.GetValue()
					}
					require.Contains(t, labels, "concurrency_limit_group")
					require.Equal(t, "test-namespace", labels["namespace"])
					require.Equal(t, "PollWorkflowTaskQueue", labels["operation"])
					require.Equal(t, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE.String(), labels["resource_exhausted_scope"])
					require.InDelta(t, 1, sample.GetCounter().GetValue(), 0.001)
					groups[labels["concurrency_limit_group"]] = labels["resource_exhausted_cause"]
				}
			}
			require.Equal(t, map[string]string{
				"default":         enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT.String(),
				"internal_per_ns": enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT.String(),
				"not_applicable":  enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT.String(),
			}, groups)
		})
	}
}
