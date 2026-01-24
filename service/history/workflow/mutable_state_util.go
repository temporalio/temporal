package workflow

import (
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/tasks"
)

func convertSyncActivityInfos(
	now time.Time,
	workflowKey definition.WorkflowKey,
	activityInfos map[int64]*persistencespb.ActivityInfo,
	inputs map[int64]struct{},
	targetClusters []string,
) []tasks.Task {
	outputs := make([]tasks.Task, 0, len(inputs))
	for item := range inputs {
		activityInfo, ok := activityInfos[item]
		if ok {
			outputs = append(outputs, &tasks.SyncActivityTask{
				WorkflowKey:         workflowKey,
				Version:             activityInfo.GetVersion(),
				ScheduledEventID:    activityInfo.GetScheduledEventId(),
				VisibilityTimestamp: now,
				TargetClusters:      targetClusters,
			})
		}
	}
	return outputs
}

func SanitizeMutableState(
	workflowMutableState *persistencespb.WorkflowMutableState,
) {
	// Some values stored in mutable state are cluster or shard specific.
	// E.g task status (if task is created or not), taskID (derived from shard rangeID), txnID (derived from shard rangeID), etc.
	// Those fields should not be replicated across clusters and should be sanitized.
	sanitizeExecutionInfo(workflowMutableState.GetExecutionInfo())

	sanitizeChildExecutionInfo(workflowMutableState.GetChildExecutionInfos())

	rootNode := persistencespb.StateMachineNode_builder{
		Children: workflowMutableState.GetExecutionInfo().GetSubStateMachinesByType(),
	}.Build()
	SanitizeStateMachineNode(rootNode)
}

func SanitizeMutableStateMutation(
	mutableStateMutation *persistencespb.WorkflowMutableStateMutation,
) {
	sanitizeExecutionInfo(mutableStateMutation.GetExecutionInfo())
	sanitizeChildExecutionInfo(mutableStateMutation.GetUpdatedChildExecutionInfos())
}

func sanitizeExecutionInfo(
	executionInfo *persistencespb.WorkflowExecutionInfo,
) {
	executionInfo.SetWorkflowExecutionTimerTaskStatus(TimerTaskStatusNone)
	executionInfo.SetLastFirstEventTxnId(common.EmptyEventTaskID)
	executionInfo.SetCloseVisibilityTaskId(common.EmptyEventTaskID)
	executionInfo.SetCloseTransferTaskId(common.EmptyEventTaskID)
	// TODO: after adding cluster to clock info, no need to reset clock here
	executionInfo.ClearParentClock()

	// Timer tasks are generated locally, do not sync them.
	executionInfo.SetStateMachineTimers(nil)
	executionInfo.SetTaskGenerationShardClockTimestamp(common.EmptyEventTaskID)
}

func sanitizeChildExecutionInfo(
	pendingChildInfo map[int64]*persistencespb.ChildExecutionInfo,
) {
	for _, childExecutionInfo := range pendingChildInfo {
		childExecutionInfo.ClearClock()
	}
}

func SanitizeStateMachineNode(
	node *persistencespb.StateMachineNode,
) {
	if node == nil {
		return
	}

	// Node TransitionCount is cluster local information used for detecting staleness.
	// Reset it to 1 since we are creating a new mutable state.
	node.SetTransitionCount(1)

	for _, stateMachineMap := range node.GetChildren() {
		for _, childNode := range stateMachineMap.GetMachinesById() {
			SanitizeStateMachineNode(childNode)
		}
	}
}
