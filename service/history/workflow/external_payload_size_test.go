package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
)

func TestCalculateExternalPayloadSize_NoExternalPayloads(t *testing.T) {
	events := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			WorkflowExecutionStartedEventAttributes: historypb.WorkflowExecutionStartedEventAttributes_builder{
				Input: commonpb.Payloads_builder{
					Payloads: []*commonpb.Payload{
						commonpb.Payload_builder{
							Data: []byte("test data"),
						}.Build(),
					},
				}.Build(),
			}.Build(),
		}.Build(),
	}

	size, count, err := CalculateExternalPayloadSize(events, metrics.NoopMetricsHandler)
	require.NoError(t, err)
	assert.Equal(t, int64(0), size)
	assert.Equal(t, int64(0), count)
}

func TestCalculateExternalPayloadSize_WithExternalPayloads(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	events := []*historypb.HistoryEvent{
		historypb.HistoryEvent_builder{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			WorkflowExecutionStartedEventAttributes: historypb.WorkflowExecutionStartedEventAttributes_builder{
				Input: commonpb.Payloads_builder{
					Payloads: []*commonpb.Payload{
						commonpb.Payload_builder{
							Data: []byte("reference"),
							ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
								commonpb.Payload_ExternalPayloadDetails_builder{
									SizeBytes: 1024,
								}.Build(),
								commonpb.Payload_ExternalPayloadDetails_builder{
									SizeBytes: 2048,
								}.Build(),
							},
						}.Build(),
					},
				}.Build(),
			}.Build(),
		}.Build(),
		historypb.HistoryEvent_builder{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			ActivityTaskCompletedEventAttributes: historypb.ActivityTaskCompletedEventAttributes_builder{
				Result: commonpb.Payloads_builder{
					Payloads: []*commonpb.Payload{
						commonpb.Payload_builder{
							Data: []byte("result"),
							ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
								commonpb.Payload_ExternalPayloadDetails_builder{
									SizeBytes: 512,
								}.Build(),
							},
						}.Build(),
					},
				}.Build(),
			}.Build(),
		}.Build(),
	}

	size, count, err := CalculateExternalPayloadSize(events, metricsHandler)
	require.NoError(t, err)
	assert.Equal(t, int64(1024+2048+512), size)
	assert.Equal(t, int64(3), count)

	snapshot := capture.Snapshot()

	histogramRecs := snapshot[metrics.ExternalPayloadUploadSize.Name()]
	require.Len(t, histogramRecs, 3)
	assert.Equal(t, int64(1024), histogramRecs[0].Value)
	assert.Equal(t, int64(2048), histogramRecs[1].Value)
	assert.Equal(t, int64(512), histogramRecs[2].Value)
}

func TestCalculateExternalPayloadSize_EmptyEvents(t *testing.T) {
	events := []*historypb.HistoryEvent{}

	size, count, err := CalculateExternalPayloadSize(events, metrics.NoopMetricsHandler)
	require.NoError(t, err)
	assert.Equal(t, int64(0), size)
	assert.Equal(t, int64(0), count)
}
