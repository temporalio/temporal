package api

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	historyi "go.temporal.io/server/service/history/interfaces"
)

type WHNInputType string

const (
	WHNInputTypeSignal               WHNInputType = "signal"
	WHNInputTypeCancellationRequest  WHNInputType = "cancellation_request"
	WHNInputTypeTimer                WHNInputType = "timer"
	WHNInputTypeActivityResult       WHNInputType = "activity_result"
	WHNInputTypeChildWorkflowResult  WHNInputType = "child_workflow_result"
	WHNInputTypeNexusOperationResult WHNInputType = "nexus_operation_result"
	WHNInputTypeUpdate               WHNInputType = "update"
)

func isBufferedWorkflowInputEvent(eventType enumspb.EventType) bool {
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

func HasBufferedWHNInputEvent(mutableState historyi.MutableState) bool {
	return mutableState.HasAnyBufferedEvent(func(event *historypb.HistoryEvent) bool {
		return isBufferedWorkflowInputEvent(event.GetEventType())
	})
}
