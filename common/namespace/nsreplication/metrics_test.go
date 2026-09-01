package nsreplication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetrics "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	namespacepb "go.temporal.io/api/namespace/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testOpenTelemetryProvider struct {
	meter otelmetric.Meter
}

func (p *testOpenTelemetryProvider) GetMeter() otelmetric.Meter {
	return p.meter
}

func (p *testOpenTelemetryProvider) Stop(log.Logger) {}

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

func TestRecordNamespaceReplicationOutcomeExcludesNamespaceTag(t *testing.T) {
	scope := tally.NewTestScope("", nil)
	handler := metrics.NewTallyMetricsHandler(metrics.ClientConfig{
		ExcludeTags: map[string][]string{
			metrics.NamespaceTag("").Key: {},
		},
	}, scope)
	ctx := WithTaskMetricsContext(context.Background(), TaskMetricsContext{
		SourceCluster:  "cluster-a",
		TargetCluster:  "cluster-b",
		Transport:      LegacyMetricsTransport,
		VisibilityTime: timestamppb.New(time.Now().Add(-time.Minute)),
	})
	task := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_CREATE,
		Info:               &namespacepb.NamespaceInfo{Name: "payments"},
	}

	recordOutcome(ctx, handler, task, metricsOutcomeApplied)

	snapshot := scope.Snapshot()
	require.Len(t, snapshot.Counters(), 1)
	for _, counter := range snapshot.Counters() {
		require.Equal(t, "_tag_excluded_", counter.Tags()[metrics.NamespaceTag("").Key])
	}
	require.Len(t, snapshot.Timers(), 1)
	for _, timer := range snapshot.Timers() {
		require.Equal(t, "_tag_excluded_", timer.Tags()[metrics.NamespaceTag("").Key])
	}
}

func TestRecordNamespaceReplicationOutcomeExcludesNamespaceTagOpenTelemetry(t *testing.T) {
	reader := sdkmetrics.NewManualReader()
	provider := sdkmetrics.NewMeterProvider(sdkmetrics.WithReader(reader))
	handler, err := metrics.NewOtelMetricsHandler(
		log.NewTestLogger(),
		&testOpenTelemetryProvider{meter: provider.Meter("test")},
		metrics.ClientConfig{
			ExcludeTags: map[string][]string{
				metrics.NamespaceTag("").Key: {},
			},
		},
		false,
	)
	require.NoError(t, err)
	ctx := WithTaskMetricsContext(context.Background(), TaskMetricsContext{
		SourceCluster:  "cluster-a",
		TargetCluster:  "cluster-b",
		Transport:      LegacyMetricsTransport,
		VisibilityTime: timestamppb.New(time.Now().Add(-time.Minute)),
	})
	task := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_CREATE,
		Info:               &namespacepb.NamespaceInfo{Name: "payments"},
	}

	recordOutcome(ctx, handler, task, metricsOutcomeApplied)

	var resourceMetrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &resourceMetrics))
	found := map[string]bool{}
	for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
		for _, metric := range scopeMetrics.Metrics {
			switch metric.Name {
			case metrics.NamespaceReplicationApplyOutcomes.Name():
				data, ok := metric.Data.(metricdata.Sum[int64])
				require.True(t, ok)
				require.Len(t, data.DataPoints, 1)
				requireExcludedNamespaceAttribute(t, data.DataPoints[0].Attributes)
				found[metric.Name] = true
			case metrics.NamespaceReplicationApplyEndToEndLatency.Name():
				data, ok := metric.Data.(metricdata.Histogram[int64])
				require.True(t, ok)
				require.Len(t, data.DataPoints, 1)
				requireExcludedNamespaceAttribute(t, data.DataPoints[0].Attributes)
				found[metric.Name] = true
			default:
			}
		}
	}
	require.True(t, found[metrics.NamespaceReplicationApplyOutcomes.Name()])
	require.True(t, found[metrics.NamespaceReplicationApplyEndToEndLatency.Name()])
}

func requireExcludedNamespaceAttribute(t *testing.T, attributes attribute.Set) {
	t.Helper()
	value, ok := attributes.Value(attribute.Key(metrics.NamespaceTag("").Key))
	require.True(t, ok)
	require.Equal(t, "_tag_excluded_", value.AsString())
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
