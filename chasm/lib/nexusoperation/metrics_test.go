package nexusoperation

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestOperationBackendMetricTag(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  NexusMetricTagConfig
		wantTag bool
	}{
		{name: "unset"},
		{name: "disabled", config: NexusMetricTagConfig{IncludeBackendTag: false}},
		{name: "enabled", config: NexusMetricTagConfig{IncludeBackendTag: true}, wantTag: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, workflowOperation := range []bool{false, true} {
				name := "standalone"
				if workflowOperation {
					name = "workflow"
				}
				t.Run(name, func(t *testing.T) {
					handler := metricstest.NewCaptureHandler()
					capture := handler.StartCapture()
					defer handler.StopCapture(capture)
					ctx := &chasm.MockContext{
						HandleNamespaceEntry: func() *namespace.Namespace {
							return namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{Name: "ns-name"}, nil, false, nil, 0)
						},
						HandleMetricsHandler: func() metrics.Handler { return handler },
						GoCtx: context.WithValue(context.Background(), OperationContextKey, &OperationContext{
							MetricTagConfig: dynamicconfig.GetTypedPropertyFn(tc.config),
						}),
					}
					op := newTestOperation()
					if workflowOperation {
						op.Store = chasm.NewMockParentPtr[OperationStore](&mockStoreComponent{})
					}
					closeTime := defaultTime.Add(2 * time.Second)
					op.emitOnSucceededMetrics(ctx, closeTime)
					op.emitOnFailedMetrics(ctx, closeTime)
					op.emitOnCanceledMetrics(ctx, closeTime)
					op.emitOnTerminatedMetrics(ctx, closeTime)
					op.emitOnTimedOutMetrics(ctx, closeTime, "SCHEDULE_TO_CLOSE")
					op.StartedTime = timestamppb.New(defaultTime.Add(time.Second))
					op.emitOnSucceededMetrics(ctx, closeTime)
					snapshot := capture.Snapshot()
					require.ElementsMatch(t, []string{
						"nexus_operation_success",
						"nexus_operation_fail",
						"nexus_operation_cancel",
						"nexus_operation_terminate",
						"nexus_operation_timeout",
						"nexus_operation_schedule_to_close_latency",
						"nexus_operation_schedule_to_start_latency",
						"nexus_operation_start_to_close_latency",
					}, slices.Collect(maps.Keys(snapshot)))
					for metric, recordings := range snapshot {
						for _, recording := range recordings {
							if tc.wantTag {
								require.Equal(t, "chasm", recording.Tags["backend"], metric)
							} else {
								require.NotContains(t, recording.Tags, "backend", metric)
							}
						}
					}
				})
			}
		})
	}
}

func TestBackendMetricTagDisabledByDefault(t *testing.T) {
	require.False(t, MetricTagConfiguration.Get(dynamicconfig.NewNoopCollection())().IncludeBackendTag)
}
