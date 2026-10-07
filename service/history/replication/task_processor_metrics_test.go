package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
)

func TestTaskProcessorResourceExhaustedMetrics(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		err  *serviceerror.ResourceExhausted
		tags map[string]string
	}{
		{
			name: "namespace concurrent limit",
			err: &serviceerror.ResourceExhausted{
				Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
				Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			},
			tags: map[string]string{
				"operation":                "HistoryReplicationTask",
				"resource_exhausted_cause": "ConcurrentLimit",
				"resource_exhausted_scope": "Namespace",
				"concurrency_limit_group":  "not_applicable",
			},
		},
		{
			name: "system RPS limit",
			err: &serviceerror.ResourceExhausted{
				Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT,
				Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM,
			},
			tags: map[string]string{
				"operation":                "HistoryReplicationTask",
				"resource_exhausted_cause": "RpsLimit",
				"resource_exhausted_scope": "System",
				"concurrency_limit_group":  "not_applicable",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)
			processor := &taskProcessorImpl{metricsHandler: metricsHandler}

			processor.emitTaskMetrics(metrics.HistoryReplicationTaskScope, tc.err)

			require.Equal(t, []*metricstest.CapturedRecording{{
				Value: int64(1),
				Tags:  tc.tags,
			}}, capture.SnapshotMetric(metrics.ServiceErrResourceExhaustedCounter.Name()))
			require.Equal(t, []*metricstest.CapturedRecording{{
				Value: int64(1),
				Tags:  map[string]string{"operation": "HistoryReplicationTask"},
			}}, capture.SnapshotMetric(metrics.ReplicationTasksFailed.Name()))
		})
	}
}
