package interceptor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/primitives"
)

func TestRecordErrorMetricsResourceExhausted(t *testing.T) {
	t.Parallel()

	regularPoll := &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: "test-namespace", TaskQueue: &taskqueuepb.TaskQueue{Name: "regular-tq"},
	}
	internalPoll := &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: "test-namespace", TaskQueue: &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
	}
	unrelatedConcurrent := *ErrNamespaceCountLimitServerBusy
	for _, tc := range []struct {
		name       string
		request    *workflowservice.PollWorkflowTaskQueueRequest
		err        error
		wantCause  enumspb.ResourceExhaustedCause
		wantScope  enumspb.ResourceExhaustedScope
		wantGroup  string
		wantOrigin string
	}{
		{
			name:       "regular workflow concurrent limit",
			request:    regularPoll,
			err:        ErrNamespaceCountLimitServerBusy,
			wantCause:  enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			wantScope:  enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			wantGroup:  "default",
			wantOrigin: "local",
		},
		{
			name:       "internal workflow concurrent limit",
			request:    internalPoll,
			err:        ErrNamespaceCountLimitServerBusy,
			wantCause:  enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			wantScope:  enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			wantGroup:  "internal_per_ns",
			wantOrigin: "local",
		},
		{
			name:       "namespace rate limit",
			request:    regularPoll,
			err:        ErrNamespaceRateLimitServerBusy,
			wantCause:  enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
			wantScope:  enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			wantGroup:  "not_applicable",
			wantOrigin: "local",
		},
		{
			name:       "service rate limit",
			request:    regularPoll,
			err:        RateLimitServerBusy,
			wantCause:  enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
			wantScope:  enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			wantGroup:  "not_applicable",
			wantOrigin: "local",
		},
		{
			name:       "wrapped workflow concurrent limit",
			request:    regularPoll,
			err:        fmt.Errorf("poll workflow task queue: %w", ErrNamespaceCountLimitServerBusy),
			wantCause:  enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			wantScope:  enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			wantGroup:  "default",
			wantOrigin: "local",
		},
		{
			name:       "downstream workflow concurrent limit",
			request:    internalPoll,
			err:        serviceerror.FromStatus(serviceerror.ToStatus(ErrNamespaceCountLimitServerBusy)),
			wantCause:  enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			wantScope:  enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			wantGroup:  "not_applicable",
			wantOrigin: "downstream",
		},
		{
			name:       "downstream service rate limit",
			request:    regularPoll,
			err:        serviceerror.FromStatus(serviceerror.ToStatus(RateLimitServerBusy)),
			wantCause:  enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
			wantScope:  enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			wantGroup:  "not_applicable",
			wantOrigin: "downstream",
		},
		{
			name:    "internal poll namespace RPS",
			request: internalPoll,
			err: &serviceerror.ResourceExhausted{
				Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
				Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			},
			wantCause:  enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
			wantScope:  enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			wantGroup:  "not_applicable",
			wantOrigin: "downstream",
		},
		{
			name:    "internal poll system RPS",
			request: internalPoll,
			err: &serviceerror.ResourceExhausted{
				Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
				Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			},
			wantCause:  enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
			wantScope:  enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			wantGroup:  "not_applicable",
			wantOrigin: "downstream",
		},
		{
			name:       "unrelated concurrent error with identical fields",
			request:    internalPoll,
			err:        &unrelatedConcurrent,
			wantCause:  enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
			wantScope:  enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			wantGroup:  "not_applicable",
			wantOrigin: "downstream",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)
			recordErrorMetrics(tc.request, mh.WithTags(
				metrics.NamespaceTag("test-namespace"),
				metrics.OperationTag("PollWorkflowTaskQueue"),
				metrics.ServiceNameTag(primitives.FrontendService),
			), tc.err, false)

			recordings := capture.SnapshotMetric(metrics.ServiceErrResourceExhaustedCounter.Name())
			require.Len(t, recordings, 1)
			require.Equal(t, int64(1), recordings[0].Value)
			require.Equal(t, map[string]string{
				"namespace": "test-namespace", "operation": "PollWorkflowTaskQueue", "service_name": "frontend",
				"resource_exhausted_cause": tc.wantCause.String(), "resource_exhausted_scope": tc.wantScope.String(),
				"concurrency_limit_group": tc.wantGroup, "resource_exhausted_origin": tc.wantOrigin,
			}, recordings[0].Tags)
		})
	}
}
