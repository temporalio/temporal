package interceptor

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/primitives"
)

func TestRecordErrorMetricsResourceExhausted(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  *serviceerror.ResourceExhausted
	}{
		{
			name: "namespace concurrent limit",
			err:  ErrNamespaceCountLimitServerBusy,
		},
		{
			name: "namespace RPS limit",
			err: &serviceerror.ResourceExhausted{
				Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
				Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			},
		},
		{
			name: "system RPS limit",
			err: &serviceerror.ResourceExhausted{
				Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
				Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)
			recordErrorMetrics(mh.WithTags(
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
			}, recordings[0].Tags)
		})
	}
}
