package historybuilder

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
)

func TestEventAttachmentsCountBufferedLinksOnce(t *testing.T) {
	t.Parallel()
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	link := &commonpb.Link{Variant: &commonpb.Link_Callback_{Callback: &commonpb.Link_Callback{}}}
	builder := New(clock.NewEventTimeSource(), nil, 1, common.FirstEventID, []*historypb.HistoryEvent{
		{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, Links: []*commonpb.Link{link}},
	}, handler.WithTags(metrics.OperationTag("test")), "test-namespace", dynamicconfig.GetIntPropertyFn(0))

	event := builder.AddHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, func(event *historypb.HistoryEvent) {
		event.Links = []*commonpb.Link{link, link, {}}
	})
	require.Equal(t, common.BufferedEventID, event.GetEventId())
	builder.FlushBufferToCurrentBatch()
	require.Len(t, builder.memLatestBatch, 2)

	snapshot := capture.Snapshot()
	linkCounts := snapshot[metrics.HistoryEventLinksAttached.Name()]
	require.Len(t, linkCounts, 2)
	byVariant := make(map[string]any)
	for _, recording := range linkCounts {
		require.Equal(t, "EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED", recording.Tags["event_type"])
		require.Equal(t, "test-namespace", recording.Tags["namespace"])
		byVariant[recording.Tags["link_variant"]] = recording.Value
	}
	require.Equal(t, map[string]any{"callback": int64(2), "unknown": int64(1)}, byVariant)
	linkHistogram := snapshot[metrics.HistoryEventLinksPerEvent.Name()]
	require.Len(t, linkHistogram, 1)
	require.Equal(t, int64(3), linkHistogram[0].Value)
	require.Empty(t, snapshot[metrics.HistoryEventCompletionCallbacksAttached.Name()])
}

func TestEventAttachmentsCountAcceptedUpdateLinksAndCallbacks(t *testing.T) {
	t.Parallel()
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	builder := New(clock.NewEventTimeSource(), nil, 1, common.FirstEventID, nil, handler, "test-namespace", dynamicconfig.GetIntPropertyFn(0))
	link := &commonpb.Link{Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: &commonpb.Link_WorkflowEvent{}}}
	event := builder.AddWorkflowExecutionUpdateAcceptedEvent("update", "message", 1, &updatepb.Request{
		CompletionCallbacks: []*commonpb.Callback{
			{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{}}, Links: []*commonpb.Link{link}},
			{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{}}},
		},
	})
	require.Len(t, event.GetLinks(), 1)

	snapshot := capture.Snapshot()
	links := snapshot[metrics.HistoryEventLinksAttached.Name()]
	require.Len(t, links, 1)
	require.Equal(t, "workflow_event", links[0].Tags["link_variant"])
	require.Equal(t, int64(1), links[0].Value)
	callbacks := snapshot[metrics.HistoryEventCompletionCallbacksAttached.Name()]
	require.Len(t, callbacks, 1)
	require.Equal(t, int64(2), callbacks[0].Value)
	require.Equal(t, map[string]string{
		"namespace":  "test-namespace",
		"event_type": "EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED",
	}, callbacks[0].Tags)
}

func TestLinkVariantTagValues(t *testing.T) {
	t.Parallel()
	var link commonpb.Link
	variant := link.ProtoReflect().Descriptor().Oneofs().ByName("variant")
	require.NotNil(t, variant)
	fields := variant.Fields()
	for i := range fields.Len() {
		field := fields.Get(i)
		link.ProtoReflect().Mutable(field)
		require.Equal(t, string(field.Name()), linkVariant(&link))
	}
}
