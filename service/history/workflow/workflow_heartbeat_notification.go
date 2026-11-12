package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/metrics"
)

// isWorkflowHeartbeatNotificationInputType reports whether an event type is eligible for a Workflow Task heartbeat notification.
// Buffering alone does not make an event eligible; for example, ActivityTaskStarted records progress but does not normally require a new Workflow Task.
func isWorkflowHeartbeatNotificationInputType(eventType enumspb.EventType) bool {
	switch eventType {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_TIMER_FIRED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED:
		return true
	default:
		return false
	}
}

func (c *ContextImpl) emitWorkflowHeartbeatNotificationBufferedInputMetrics(
	newBufferedEvents []*historypb.HistoryEvent,
) {
	if len(newBufferedEvents) == 0 || !c.MutableState.HasStartedWorkflowTask() {
		return
	}

	eventTypes := make([]string, 0, len(newBufferedEvents))
	for _, event := range newBufferedEvents {
		if isWorkflowHeartbeatNotificationInputType(event.GetEventType()) {
			eventTypes = append(eventTypes, event.GetEventType().String())
		}
	}
	if len(eventTypes) == 0 {
		return
	}

	// TODO: Relaxed accuracy for the estimate to avoid iterating and comparing buffered events:
	// any prior buffered event counts, not only workflow-heartbeat-notification-eligible ones.
	hadBufferedInput := c.MutableState.NumBufferedEvents() > len(newBufferedEvents)
	// TODO: Account for pending Updates. Only events are counted for now because UpdateRegistry is not a
	// read-only method, so the first-input estimate is slightly exaggerated depending on the namespace's Update traffic.
	firstBufferedInput := !hadBufferedInput
	namespaceEntry := c.MutableState.GetNamespaceEntry()
	if namespaceEntry == nil {
		return
	}
	handler := c.metricsHandler.WithTags(metrics.NamespaceTag(namespaceEntry.Name().String()))

	for _, eventType := range eventTypes {
		inputHandler := handler.WithTags(metrics.StringTag("event_type", eventType))
		metrics.WorkflowHeartbeatNotificationBufferedInput.With(inputHandler).Record(1)
		if firstBufferedInput {
			metrics.WorkflowHeartbeatNotificationEstimatedFirstBufferedInput.With(inputHandler).Record(1)
			firstBufferedInput = false
		}
	}
}
