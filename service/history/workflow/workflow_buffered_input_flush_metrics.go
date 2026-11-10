package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/metrics"
)

// emitWorkflowBufferedInputFlushLatency records the time eligible inputs waited for a committed history flush.
func (c *ContextImpl) emitWorkflowBufferedInputFlushLatency(events []*historypb.HistoryEvent) {
	// Event timestamps use workflow time, which may differ from wall time in time-skipped workflows.
	if len(events) == 0 || c.MutableState.GetExecutionInfo().GetTimeSkippingInfo() != nil {
		return
	}

	now := c.MutableState.Now()
	handler := c.metricsHandler.WithTags(metrics.NamespaceTag(c.MutableState.GetNamespaceEntry().Name().String()))
	for _, event := range events {
		if event.GetEventTime() == nil || !isBufferedWorkflowTaskInputEvent(event.GetEventType()) {
			continue
		}
		latency := now.Sub(event.GetEventTime().AsTime())
		if latency < 0 {
			continue
		}
		metrics.WorkflowBufferedInputFlushLatency.With(handler).Record(
			latency,
			metrics.StringTag("event_type", enumspb.EventType_name[int32(event.GetEventType())]),
		)
	}
}

func isBufferedWorkflowTaskInputEvent(eventType enumspb.EventType) bool {
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
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED:
		return true
	default:
		return false
	}
}
