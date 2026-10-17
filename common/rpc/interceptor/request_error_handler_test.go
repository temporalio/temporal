package interceptor

import (
	"fmt"
	"testing"
	"time"

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

func TestRecordErrorMetricsFlooredConcurrentLimitGroup(t *testing.T) {
	t.Parallel()

	internalPoll := &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: "test-namespace", TaskQueue: &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue},
	}
	clonedConcurrent := *ErrNamespaceCountLimitServerBusy
	messageMismatch := *ErrNamespaceCountLimitServerBusy
	messageMismatch.Message = "another resource limit exceeded"
	causeMismatch := *ErrNamespaceCountLimitServerBusy
	causeMismatch.Cause = enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT
	scopeMismatch := *ErrNamespaceCountLimitServerBusy
	scopeMismatch.Scope = enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM
	flooredConcurrent := addRetryInfo(t, ErrNamespaceCountLimitServerBusy, 2*time.Second)
	for _, tc := range []struct {
		name      string
		request   *workflowservice.PollWorkflowTaskQueueRequest
		err       error
		cause     enumspb.ResourceExhaustedCause
		scope     enumspb.ResourceExhaustedScope
		wantGroup string
	}{
		{
			name:      "floored concurrent limit",
			request:   internalPoll,
			err:       flooredConcurrent,
			cause:     ErrNamespaceCountLimitServerBusy.Cause,
			scope:     ErrNamespaceCountLimitServerBusy.Scope,
			wantGroup: "internal_per_ns",
		},
		{
			name:      "wrapped floored concurrent limit",
			request:   internalPoll,
			err:       fmt.Errorf("wrapped concurrent limit: %w", flooredConcurrent),
			cause:     ErrNamespaceCountLimitServerBusy.Cause,
			scope:     ErrNamespaceCountLimitServerBusy.Scope,
			wantGroup: "internal_per_ns",
		},
		{
			name:      "identical fields without retry info",
			request:   internalPoll,
			err:       &clonedConcurrent,
			cause:     clonedConcurrent.Cause,
			scope:     clonedConcurrent.Scope,
			wantGroup: "not_applicable",
		},
		{
			name:      "different message",
			request:   internalPoll,
			err:       addRetryInfo(t, &messageMismatch, 2*time.Second),
			cause:     messageMismatch.Cause,
			scope:     messageMismatch.Scope,
			wantGroup: "not_applicable",
		},
		{
			name:      "different cause",
			request:   internalPoll,
			err:       addRetryInfo(t, &causeMismatch, 2*time.Second),
			cause:     causeMismatch.Cause,
			scope:     causeMismatch.Scope,
			wantGroup: "not_applicable",
		},
		{
			name:      "different scope",
			request:   internalPoll,
			err:       addRetryInfo(t, &scopeMismatch, 2*time.Second),
			cause:     scopeMismatch.Cause,
			scope:     scopeMismatch.Scope,
			wantGroup: "not_applicable",
		},
		{
			name: "floored concurrent limit for regular poll",
			request: &workflowservice.PollWorkflowTaskQueueRequest{
				Namespace: "test-namespace", TaskQueue: &taskqueuepb.TaskQueue{Name: "regular-tq"},
			},
			err:       flooredConcurrent,
			cause:     ErrNamespaceCountLimitServerBusy.Cause,
			scope:     ErrNamespaceCountLimitServerBusy.Scope,
			wantGroup: "default",
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
			require.Equal(t, map[string]string{
				"namespace": "test-namespace", "operation": "PollWorkflowTaskQueue", "service_name": "frontend",
				"resource_exhausted_cause": tc.cause.String(), "resource_exhausted_scope": tc.scope.String(),
				"concurrency_limit_group": tc.wantGroup,
			}, recordings[0].Tags)
		})
	}
}
