package replicator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace/nsreplication"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRecordTaskQueueUserDataOutcomeWithoutValidVisibilityTime(t *testing.T) {
	for _, test := range []struct {
		name           string
		visibilityTime *timestamppb.Timestamp
	}{
		{name: "missing"},
		{name: "invalid", visibilityTime: &timestamppb.Timestamp{Seconds: 253402300800}},
	} {
		t.Run(test.name, func(t *testing.T) {
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			ctx := nsreplication.WithTaskMetricsContext(context.Background(), nsreplication.TaskMetricsContext{
				SourceCluster:  "cluster-a",
				TargetCluster:  "cluster-b",
				Transport:      nsreplication.LegacyMetricsTransport,
				VisibilityTime: test.visibilityTime,
			})

			recordTaskQueueUserDataOutcome(
				ctx,
				handler,
				&replicationspb.TaskQueueUserDataAttributes{NamespaceId: "namespace-id"},
				taskQueueUserDataMetricsOutcomeApplied,
			)

			require.Len(t, capture.Snapshot()[metrics.TaskQueueUserDataReplicationApplyOutcomes.Name()], 1)
			require.Empty(t, capture.Snapshot()[metrics.TaskQueueUserDataReplicationApplyEndToEndLatency.Name()])
		})
	}
}

func TestRecordTaskQueueUserDataOutcomeClampsFutureVisibilityTime(t *testing.T) {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	ctx := nsreplication.WithTaskMetricsContext(context.Background(), nsreplication.TaskMetricsContext{
		SourceCluster:  "cluster-a",
		TargetCluster:  "cluster-b",
		Transport:      nsreplication.LegacyMetricsTransport,
		VisibilityTime: timestamppb.New(time.Now().Add(time.Hour)),
	})

	recordTaskQueueUserDataOutcome(
		ctx,
		handler,
		&replicationspb.TaskQueueUserDataAttributes{NamespaceId: "namespace-id"},
		taskQueueUserDataMetricsOutcomeApplied,
	)

	latencies := capture.Snapshot()[metrics.TaskQueueUserDataReplicationApplyEndToEndLatency.Name()]
	require.Len(t, latencies, 1)
	require.Equal(t, time.Duration(0), latencies[0].Value)
}
