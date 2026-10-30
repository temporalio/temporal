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
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					Input: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{
							{
								Data: []byte("test data"),
							},
						},
					},
				},
			},
		},
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
		{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					Input: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{
							{
								Data: []byte("reference"),
								ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
									{
										SizeBytes: 1024,
									},
									{
										SizeBytes: 2048,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
				ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
					Result: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{
							{
								Data: []byte("result"),
								ExternalPayloads: []*commonpb.Payload_ExternalPayloadDetails{
									{
										SizeBytes: 512,
									},
								},
							},
						},
					},
				},
			},
		},
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
