package workflow

import (
	"slices"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/metrics"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// WorkflowHeartbeatNotificationUpdateAdmittedMetricTag is a dummy event type: admitting an Update writes no history
// event (WorkflowExecutionUpdateAdmitted is only written when reapplying on reset).
const WorkflowHeartbeatNotificationUpdateAdmittedMetricTag = "UpdateAdmitted"

const (
	workflowHeartbeatNotificationSignal              = "signal"
	workflowHeartbeatNotificationCancellation        = "cancellation"
	workflowHeartbeatNotificationTimer               = "timer"
	workflowHeartbeatNotificationActivityResult      = "activity_result"
	workflowHeartbeatNotificationChildWorkflowResult = "child_workflow_result"
	workflowHeartbeatNotificationNexusResult         = "nexus_result"
	workflowHeartbeatNotificationUpdate              = "update"
)

// workflowHeartbeatNotificationInputTypeForEvent maps an eligible event to its configuration category.
func workflowHeartbeatNotificationInputTypeForEvent(eventType enumspb.EventType) string {
	switch eventType {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
		return workflowHeartbeatNotificationSignal
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:
		return workflowHeartbeatNotificationCancellation
	case enumspb.EVENT_TYPE_TIMER_FIRED:
		return workflowHeartbeatNotificationTimer
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		return workflowHeartbeatNotificationActivityResult
	case enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:
		return workflowHeartbeatNotificationChildWorkflowResult
	case enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED:
		return workflowHeartbeatNotificationNexusResult
	default:
		return ""
	}
}

func workflowHeartbeatNotificationInputTypeForName(eventType string) string {
	if eventType == WorkflowHeartbeatNotificationUpdateAdmittedMetricTag {
		return workflowHeartbeatNotificationUpdate
	}
	if value, ok := enumspb.EventType_value[eventType]; ok {
		return workflowHeartbeatNotificationInputTypeForEvent(enumspb.EventType(value))
	}
	return ""
}

// isWorkflowHeartbeatNotificationInputEnabled checks the namespace rollout and input type filter.
func isWorkflowHeartbeatNotificationInputEnabled(shardCtx historyi.ShardContext, namespaceName, eventType string) bool {
	config := shardCtx.GetConfig()
	if !config.EnableWorkflowHeartbeatNotification(namespaceName) {
		return false
	}
	category := workflowHeartbeatNotificationInputTypeForName(eventType)
	if category == "" {
		return false
	}
	allowed := config.WorkflowHeartbeatNotificationInputTypes(namespaceName)
	return slices.ContainsFunc(allowed, func(value string) bool {
		return strings.EqualFold(value, category)
	})
}

// IsWorkflowHeartbeatNotificationInputType reports whether an event type is eligible for a Workflow Task heartbeat notification.
// Buffering alone does not make an event eligible; for example, ActivityTaskStarted records progress but does not normally require a new Workflow Task.
func IsWorkflowHeartbeatNotificationInputType(eventType enumspb.EventType) bool {
	return workflowHeartbeatNotificationInputTypeForEvent(eventType) != ""
}

func (c *ContextImpl) handleBufferedInputsForWorkflowHeartbeatNotification(
	shardCtx historyi.ShardContext,
	newBufferedEvents []*historypb.HistoryEvent,
) {
	if len(newBufferedEvents) == 0 || !c.MutableState.HasStartedWorkflowTask() {
		return
	}

	filteredNewBufferedEventTypes := make([]string, 0, len(newBufferedEvents))
	for _, event := range newBufferedEvents {
		if IsWorkflowHeartbeatNotificationInputType(event.GetEventType()) {
			filteredNewBufferedEventTypes = append(filteredNewBufferedEventTypes, event.GetEventType().String())
		}
	}
	if len(filteredNewBufferedEventTypes) == 0 {
		return
	}

	// TODO: Relaxed accuracy for the estimate to avoid iterating and comparing buffered events:
	// any prior buffered event counts, not only workflow-heartbeat-notification-eligible ones.
	hadBufferedInput := c.MutableState.NumBufferedEvents() > len(newBufferedEvents)
	firstBufferedInput := !hadBufferedInput && !c.hasPendingUpdateForWorkflowHeartbeatNotification()
	namespaceEntry := c.MutableState.GetNamespaceEntry()
	if namespaceEntry == nil {
		return
	}
	namespaceName := namespaceEntry.Name().String()
	handler := c.metricsHandler.WithTags(metrics.NamespaceTag(namespaceName))

	// Rollout settings control WHN dispatch, while these metrics count all eligible buffered inputs.
	for i, eventType := range filteredNewBufferedEventTypes {
		inputHandler := handler.WithTags(metrics.EventTypeTag(eventType))
		metrics.WorkflowHeartbeatNotificationBufferedInput.With(inputHandler).Record(1)
		if firstBufferedInput && i == 0 {
			metrics.WorkflowHeartbeatNotificationEstimatedFirstBufferedInput.With(inputHandler).Record(1)
		}
	}
	if firstBufferedInput {
		SendWorkflowHeartbeatNotification(shardCtx, namespaceName, filteredNewBufferedEventTypes[0])
	}
}

func SendWorkflowHeartbeatNotification(shardCtx historyi.ShardContext, namespaceName, eventType string) {
	if !isWorkflowHeartbeatNotificationInputEnabled(shardCtx, namespaceName, eventType) {
		return
	}
}

// hasPendingUpdateForWorkflowHeartbeatNotification checks for unsent Updates in the existing, current-version Registry.
// It reads c.updateRegistry directly; calling c.UpdateRegistry could clear or create a Registry.
func (c *ContextImpl) hasPendingUpdateForWorkflowHeartbeatNotification() bool {
	return c.updateRegistry != nil &&
		c.updateRegistry.FailoverVersion() == c.MutableState.GetCurrentVersion() &&
		c.updateRegistry.HasOutgoingMessages(false)
}
