package workflow

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/metrics"
)

// BufferedWHNInputType returns the input type for an event eligible for WHN.
func BufferedWHNInputType(eventType enumspb.EventType) string {
	switch eventType {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
		return "signal"
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:
		return "cancellation_request"
	case enumspb.EVENT_TYPE_TIMER_FIRED:
		return "timer"
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		return "activity_result"
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:
		return "child_workflow_result"
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED:
		return "nexus_operation_result"
	default:
		return ""
	}
}

func (c *ContextImpl) emitBufferedWHNInputMetrics(
	ctx context.Context,
	newBufferedEvents []*historypb.HistoryEvent,
) {
	if !c.MutableState.HasStartedWorkflowTask() {
		return
	}

	inputTypes := make([]string, 0, len(newBufferedEvents))
	for _, event := range newBufferedEvents {
		if inputType := BufferedWHNInputType(event.GetEventType()); inputType != "" {
			inputTypes = append(inputTypes, inputType)
		}
	}
	if len(inputTypes) == 0 {
		return
	}

	// The closed transaction's builder contains both prior buffered events and this mutation's new events.
	bufferedInputCount := 0
	hadBufferedInput := c.MutableState.HasAnyBufferedEvent(func(event *historypb.HistoryEvent) bool {
		if BufferedWHNInputType(event.GetEventType()) != "" {
			bufferedInputCount++
		}
		return bufferedInputCount > len(inputTypes)
	})
	firstBufferedInput := !hadBufferedInput && !c.UpdateRegistry(ctx).HasOutgoingMessages(false)
	handler := c.metricsHandler.WithTags(metrics.NamespaceTag(c.MutableState.GetNamespaceEntry().Name().String()))

	for _, inputType := range inputTypes {
		inputHandler := handler.WithTags(metrics.StringTag("input_type", inputType))
		metrics.WorkflowHeartbeatNotificationBufferedInput.With(inputHandler).Record(1)
		if firstBufferedInput {
			metrics.WorkflowHeartbeatNotificationFirstBufferedInput.With(inputHandler).Record(1)
			firstBufferedInput = false
		}
	}
}
