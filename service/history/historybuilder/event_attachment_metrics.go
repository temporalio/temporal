package historybuilder

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/metrics"
)

func (b *EventStore) emitEventAttachmentMetrics(event *historypb.HistoryEvent) {
	if b.metricsHandler == nil {
		return
	}

	links := event.GetLinks()
	callbackCount := completionCallbackCount(event)
	if len(links) == 0 && callbackCount == 0 {
		return
	}

	eventTypeTag := metrics.EventTypeTag(enumspb.EventType_name[int32(event.GetEventType())])
	if len(links) > 0 {
		handler := metrics.HistoryEventLinksAttached.With(b.metricsHandler)
		countByVariant := make(map[string]int64, len(links))
		for _, link := range links {
			countByVariant[linkVariant(link)]++
		}
		for variant, count := range countByVariant {
			handler.Record(count, b.namespaceTag, eventTypeTag, metrics.Tag{Key: "link_variant", Value: variant})
		}
		metrics.HistoryEventLinksPerEvent.With(b.metricsHandler).Record(
			int64(len(links)),
			b.namespaceTag,
			eventTypeTag,
		)
	}
	if callbackCount > 0 {
		metrics.HistoryEventCompletionCallbacksAttached.With(b.metricsHandler).Record(
			int64(callbackCount),
			b.namespaceTag,
			eventTypeTag,
		)
	}
}

// linkVariant maps a link to a closed set of tag values. Keep in sync with the variants handled by
// common/links.validateFields.
func linkVariant(link *commonpb.Link) string {
	switch link.GetVariant().(type) {
	case *commonpb.Link_WorkflowEvent_:
		return "workflow_event"
	case *commonpb.Link_Callback_:
		return "callback"
	case *commonpb.Link_BatchJob_:
		return "batch_job"
	case *commonpb.Link_NexusOperation_:
		return "nexus_operation"
	case *commonpb.Link_Activity_:
		return "activity"
	case *commonpb.Link_Workflow_:
		return "workflow"
	default:
		return "unknown"
	}
}

// completionCallbackCount returns the number of completion callbacks carried by an event.
// Completion callbacks live on the event attributes rather than on event.Links.
//
// WorkflowExecutionUpdateAdmitted and WorkflowExecutionUpdateAccepted never both carry the request
// for the same update: in the normal flow only the Accepted event has an AcceptedRequest, and after
// a reset the Admitted event carries it while AcceptedRequest is nil. So counting both does not
// double count.
func completionCallbackCount(event *historypb.HistoryEvent) int {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
		return len(event.GetWorkflowExecutionStartedEventAttributes().GetCompletionCallbacks())
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED:
		attrs := event.GetWorkflowExecutionOptionsUpdatedEventAttributes()
		count := len(attrs.GetAttachedCompletionCallbacks())
		for _, updateOptions := range attrs.GetWorkflowUpdateOptions() {
			count += len(updateOptions.GetAttachedCompletionCallbacks())
		}
		return count
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED:
		return len(event.GetWorkflowExecutionUpdateAdmittedEventAttributes().GetRequest().GetCompletionCallbacks())
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:
		return len(event.GetWorkflowExecutionUpdateAcceptedEventAttributes().GetAcceptedRequest().GetCompletionCallbacks())
	default:
		return 0
	}
}
