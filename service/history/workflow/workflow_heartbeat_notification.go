package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/metrics"
)

// IsWorkflowHeartbeatNotificationInputType reports whether an event type is eligible for a Workflow Task heartbeat notification.
// Buffering alone does not make an event eligible; for example, ActivityTaskStarted records progress but does not normally require a new Workflow Task.
func IsWorkflowHeartbeatNotificationInputType(eventType enumspb.EventType) bool {
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
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED:
		return true
	default:
		return false
	}
}

func (c *ContextImpl) emitWorkflowHeartbeatNotificationBufferedInputMetrics(
	newBufferedEvents []*historypb.HistoryEvent,
) {
	if !c.MutableState.HasStartedWorkflowTask() {
		return
	}

	eventTypes := make([]string, 0, len(newBufferedEvents))
	for _, event := range newBufferedEvents {
		if IsWorkflowHeartbeatNotificationInputType(event.GetEventType()) {
			eventTypes = append(eventTypes, enumspb.EventType_name[int32(event.GetEventType())])
		}
	}
	if len(eventTypes) == 0 {
		return
	}

	// The closed transaction's builder contains both prior buffered events and this mutation's new events.
	bufferedInputCount := 0
	hadBufferedInput := c.MutableState.HasAnyBufferedEvent(func(event *historypb.HistoryEvent) bool {
		if IsWorkflowHeartbeatNotificationInputType(event.GetEventType()) {
			bufferedInputCount++
		}
		return bufferedInputCount > len(eventTypes)
	})
	// TODO: Account for pending Updates without changing Registry state during metrics collection.
	firstBufferedInput := !hadBufferedInput
	handler := c.metricsHandler.WithTags(metrics.NamespaceTag(c.MutableState.GetNamespaceEntry().Name().String()))

	for _, eventType := range eventTypes {
		inputHandler := handler.WithTags(metrics.StringTag("input_type", eventType))
		metrics.WorkflowHeartbeatNotificationBufferedInput.With(inputHandler).Record(1)
		if firstBufferedInput {
			metrics.WorkflowHeartbeatNotificationEstimatedFirstBufferedInput.With(inputHandler).Record(1)
			firstBufferedInput = false
		}
	}
}
