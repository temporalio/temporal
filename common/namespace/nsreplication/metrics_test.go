package nsreplication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	namespacepb "go.temporal.io/api/namespace/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRecordNamespaceReplicationOutcome(t *testing.T) {
	task := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_CREATE,
		Info:               &namespacepb.NamespaceInfo{Name: "payments"},
	}
	metadata := TaskMetricsContext{
		SourceCluster:  "cluster-a",
		TargetCluster:  "cluster-b",
		Transport:      LegacyMetricsTransport,
		VisibilityTime: timestamppb.New(time.Now().Add(-time.Minute)),
	}

	for _, outcome := range []string{
		metricsOutcomeApplied,
		metricsOutcomeNoChange,
		metricsOutcomeNotAdmitted,
		metricsOutcomeTerminalFailure,
	} {
		t.Run(outcome, func(t *testing.T) {
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			recordOutcome(WithTaskMetricsContext(context.Background(), metadata), handler, task, outcome)

			snapshot := capture.Snapshot()
			outcomes := snapshot[metrics.NamespaceReplicationApplyOutcomes.Name()]
			require.Len(t, outcomes, 1)
			require.Equal(t, int64(1), outcomes[0].Value)
			require.Equal(t, "payments", outcomes[0].Tags[metrics.NamespaceTag("").Key])
			require.Equal(t, "cluster-a", outcomes[0].Tags[metrics.SourceClusterTag("").Key])
			require.Equal(t, "cluster-b", outcomes[0].Tags[metrics.TargetClusterTag("").Key])
			require.Equal(t, LegacyMetricsTransport, outcomes[0].Tags[metrics.TransportTag("").Key])
			require.Equal(t, "create", outcomes[0].Tags[metrics.OperationTag("").Key])
			require.Equal(t, outcome, outcomes[0].Tags[metrics.OutcomeTag("").Key])

			latencies := snapshot[metrics.NamespaceReplicationApplyEndToEndLatency.Name()]
			require.Len(t, latencies, 1)
			require.GreaterOrEqual(t, latencies[0].Value.(time.Duration), time.Minute)
		})
	}
}

func TestRecordNamespaceReplicationOutcomeWithoutValidVisibilityTime(t *testing.T) {
	task := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_UPDATE,
		Info:               &namespacepb.NamespaceInfo{Name: "payments"},
	}

	tests := []struct {
		name           string
		visibilityTime *timestamppb.Timestamp
	}{
		{name: "missing"},
		{name: "invalid", visibilityTime: &timestamppb.Timestamp{Seconds: 253402300800}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			ctx := WithTaskMetricsContext(context.Background(), TaskMetricsContext{
				SourceCluster:  "cluster-a",
				TargetCluster:  "cluster-b",
				Transport:      LegacyMetricsTransport,
				VisibilityTime: test.visibilityTime,
			})

			recordOutcome(ctx, handler, task, metricsOutcomeApplied)

			snapshot := capture.Snapshot()
			require.Len(t, snapshot[metrics.NamespaceReplicationApplyOutcomes.Name()], 1)
			require.Empty(t, snapshot[metrics.NamespaceReplicationApplyEndToEndLatency.Name()])
		})
	}
}

func TestRecordNamespaceReplicationOutcomeRequiresMetricsContext(t *testing.T) {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()

	recordOutcome(context.Background(), handler, nil, metricsOutcomeTerminalFailure)

	require.Empty(t, capture.Snapshot())
}

func TestRecordNamespaceReplicationOutcomeClampsFutureVisibilityTime(t *testing.T) {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	ctx := WithTaskMetricsContext(context.Background(), TaskMetricsContext{
		SourceCluster:  "cluster-a",
		TargetCluster:  "cluster-b",
		Transport:      LegacyMetricsTransport,
		VisibilityTime: timestamppb.New(time.Now().Add(time.Hour)),
	})

	RecordLegacyTerminalFailure(ctx, handler, nil)

	latencies := capture.Snapshot()[metrics.NamespaceReplicationApplyEndToEndLatency.Name()]
	require.Len(t, latencies, 1)
	require.Equal(t, time.Duration(0), latencies[0].Value)
}
