package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/tasktoken"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// WorkflowHeartbeatNotificationUpdateAdmittedMetricTag is a dummy event type: admitting an Update writes no history
// event (WorkflowExecutionUpdateAdmitted is only written when reapplying on reset).
const WorkflowHeartbeatNotificationUpdateAdmittedMetricTag = "UpdateAdmitted"

// isWorkflowHeartbeatNotificationEnabled checks rollout.
func isWorkflowHeartbeatNotificationEnabled(
	shardCtx historyi.ShardContext, namespaceName, workflowID string) bool {
	config := shardCtx.GetConfig()
	rolloutPercent := config.WorkflowHeartbeatNotificationRolloutPercent(namespaceName)
	return dynamicconfig.RolloutAccepts(
		[]byte(namespaceName+"\x00"+workflowID),
		rolloutPercent,
	)
}

// IsWorkflowHeartbeatNotificationInputType reports whether an event type is eligible for a Workflow Task heartbeat notification.
// An event type that is never buffered does not need WHN. Buffering alone does not make an event eligible;
// for example, ActivityTaskStarted records progress but does not normally require a new Workflow Task.
func IsWorkflowHeartbeatNotificationInputType(eventType enumspb.EventType) bool {
	switch eventType {
	case
		// signal
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		// cancel
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		// timer
		enumspb.EVENT_TYPE_TIMER_FIRED,
		// child workflow
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		// nexus operation
		enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED,
		// todo: activity results may need a different handling mechanism for potential high unnecessary traffic it may trigger
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:
		return true
	default:
		return false
	}
}

// HasBufferedWorkflowHeartbeatNotificationInput reports whether a prior WHN-eligible event is buffered.
// Events in newBufferedEvents are excluded by pointer identity.
func HasBufferedWorkflowHeartbeatNotificationInput(ms historyi.MutableState, newBufferedEvents []*historypb.HistoryEvent) bool {
	newEventSet := make(map[*historypb.HistoryEvent]struct{}, len(newBufferedEvents))
	for _, event := range newBufferedEvents {
		newEventSet[event] = struct{}{}
	}
	return ms.HasAnyBufferedEvent(func(event *historypb.HistoryEvent) bool {
		if _, ok := newEventSet[event]; ok {
			return false
		}
		return IsWorkflowHeartbeatNotificationInputType(event.GetEventType())
	})
}

// handleWorkflowHeartbeatNotification handles buffered events waiting for the next Workflow Task to reach the worker.
// Admitted Updates can also trigger WHN while waiting for that task, through the Update path.
func (c *ContextImpl) handleWorkflowHeartbeatNotification(
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

	hadBufferedInput := HasBufferedWorkflowHeartbeatNotificationInput(c.MutableState, newBufferedEvents)
	firstBufferedInput := !hadBufferedInput && !c.hasPendingUpdateForWorkflowHeartbeatNotification()
	namespaceEntry := c.MutableState.GetNamespaceEntry()
	if namespaceEntry == nil {
		return
	}
	namespaceName := namespaceEntry.Name().String()
	handler := c.metricsHandler.WithTags(metrics.NamespaceTag(namespaceName))

	// The rollout setting controls WHN dispatch, while these metrics count all eligible buffered inputs.
	for i, eventType := range filteredNewBufferedEventTypes {
		inputHandler := handler.WithTags(metrics.EventTypeTag(eventType))
		metrics.WorkflowHeartbeatNotificationBufferedInput.With(inputHandler).Record(1)
		if firstBufferedInput && i == 0 {
			metrics.WorkflowHeartbeatNotificationEstimatedFirstBufferedInput.With(inputHandler).Record(1)
		}
	}
	if firstBufferedInput {
		SendWorkflowHeartbeatNotification(shardCtx, namespaceName, c.workflowKey.WorkflowID)
	}
}

// SendWorkflowHeartbeatNotification applies the workflow-sticky rollout before notifying the worker.
// Callers must first verify that an eligible input is waiting behind the started Workflow Task.
// It runs on the critical path of customer-facing APIs, so it must be low latency.
func SendWorkflowHeartbeatNotification(shardCtx historyi.ShardContext, namespaceName, workflowID string) {
	if !isWorkflowHeartbeatNotificationEnabled(shardCtx, namespaceName, workflowID) {
		return
	}
	// TODO(whn): Dispatch asynchronously to the worker holding the started Workflow Task.
}

// hasPendingUpdateForWorkflowHeartbeatNotification checks for unsent Updates in the existing, current-version Registry.
// It reads c.updateRegistry directly; calling c.UpdateRegistry could clear or create a Registry.
func (c *ContextImpl) hasPendingUpdateForWorkflowHeartbeatNotification() bool {
	return c.updateRegistry != nil &&
		c.updateRegistry.FailoverVersion() == c.MutableState.GetCurrentVersion() &&
		c.updateRegistry.HasOutgoingMessages(false)
}

// RebuildCurrentWorkflowTaskToken rebuilds the task token given to the worker for the started workflow task.
// It returns nil if no workflow task is started or its started clock is unknown.
func RebuildCurrentWorkflowTaskToken(key definition.WorkflowKey, info *persistencespb.WorkflowExecutionInfo) *tokenspb.Task {
	if info.GetWorkflowTaskStartedEventId() == common.EmptyEventID || info.GetWorkflowTaskStartedClock() == nil {
		return nil
	}
	return tasktoken.NewWorkflowTaskToken(
		key.NamespaceID,
		key.WorkflowID,
		key.RunID,
		info.GetWorkflowTaskScheduledEventId(),
		info.GetWorkflowTaskStartedEventId(),
		common.CloneProto(info.GetWorkflowTaskStartedTime()),
		info.GetWorkflowTaskAttempt(),
		common.CloneProto(info.GetWorkflowTaskStartedClock()),
		info.GetWorkflowTaskVersion(),
	)
}
