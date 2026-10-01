package interceptor

import (
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

func TestRecordErrorMetricsConcurrentLimitGroup(t *testing.T) {
	t.Parallel()

	internalPoll := &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: "test-namespace", TaskQueue: &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
	}
	unrelatedConcurrent := *ErrNamespaceCountLimitServerBusy
	for _, tc := range []struct {
		name      string
		request   *workflowservice.PollWorkflowTaskQueueRequest
		err       *serviceerror.ResourceExhausted
		wantGroup string
	}{
		{
			name: "regular workflow concurrent limit",
			request: &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: "test-namespace", TaskQueue: &taskqueuepb.TaskQueue{Name: "regular-tq"},
			},
			err:       ErrNamespaceCountLimitServerBusy,
			wantGroup: "default",
		},
		{
			name:      "internal workflow concurrent limit",
			request:   internalPoll,
			err:       ErrNamespaceCountLimitServerBusy,
			wantGroup: "internal_per_ns",
		},
		{
			name:    "internal poll namespace RPS",
			request: internalPoll,
			err: &serviceerror.ResourceExhausted{
				Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
				Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			},
			wantGroup: "not_applicable",
		},
		{
			name:    "internal poll system RPS",
			request: internalPoll,
			err: &serviceerror.ResourceExhausted{
				Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
				Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			},
			wantGroup: "not_applicable",
		},
		{
			name:      "unrelated concurrent error with identical fields",
			request:   internalPoll,
			err:       &unrelatedConcurrent,
			wantGroup: "not_applicable",
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
				"resource_exhausted_cause": tc.err.Cause.String(), "resource_exhausted_scope": tc.err.Scope.String(),
				"concurrency_limit_group": tc.wantGroup,
			}, recordings[0].Tags)
		})
	}
}
