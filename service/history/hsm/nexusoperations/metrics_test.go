package nexusoperations

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics/metricstest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestOperationBackendMetricTag(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  *Config
		wantTag bool
	}{
		{name: "nil config"},
		{name: "unset", config: &Config{}},
		{name: "disabled", config: &Config{MetricTagConfig: dynamicconfig.GetTypedPropertyFn(chasmnexus.NexusMetricTagConfig{IncludeBackendTag: false})}},
		{name: "enabled", config: &Config{MetricTagConfig: dynamicconfig.GetTypedPropertyFn(chasmnexus.NexusMetricTagConfig{IncludeBackendTag: true})}, wantTag: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			defer handler.StopCapture(capture)
			scheduledTime := time.Now().UTC()
			op := Operation{NexusOperationInfo: &persistencespb.NexusOperationInfo{
				ScheduledTime: timestamppb.New(scheduledTime),
				StartedTime:   timestamppb.New(scheduledTime.Add(time.Second)),
			}}
			tagConfig := tc.config.ResolvedMetricTagConfig()
			closeTime := scheduledTime.Add(2 * time.Second)
			emitOperationSucceeded(handler, tagConfig, op, "namespace", "workflow", closeTime)
			emitOperationFailed(handler, tagConfig, op, "namespace", "workflow", closeTime)
			emitOperationCanceled(handler, tagConfig, op, "namespace", "workflow", closeTime)
			emitOperationTimedOut(handler, tagConfig, op, "namespace", "workflow", "SCHEDULE_TO_CLOSE", closeTime)
			emitScheduleToStartLatency(handler, tagConfig, op, "namespace", "workflow", op.StartedTime.AsTime())
			snapshot := capture.Snapshot()
			require.ElementsMatch(t, []string{
				"nexus_operation_success",
				"nexus_operation_fail",
				"nexus_operation_cancel",
				"nexus_operation_timeout",
				"nexus_operation_schedule_to_close_latency",
				"nexus_operation_schedule_to_start_latency",
				"nexus_operation_start_to_close_latency",
			}, slices.Collect(maps.Keys(snapshot)))
			for metric, recordings := range snapshot {
				for _, recording := range recordings {
					if tc.wantTag {
						require.Equal(t, "hsm", recording.Tags["backend"], metric)
					} else {
						require.NotContains(t, recording.Tags, "backend", metric)
					}
				}
			}
		})
	}
}

func TestBackendMetricTagDisabledByDefault(t *testing.T) {
	require.False(t, MetricTagConfiguration.Get(dynamicconfig.NewNoopCollection())().IncludeBackendTag)
}
