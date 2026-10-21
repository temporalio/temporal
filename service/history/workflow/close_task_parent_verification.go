package workflow

import (
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/persistence/versionhistory"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
)

// CanSkipParentVerification reports whether the local close task has already completed
// for the incoming history. Call it before backfill or applying the snapshot changes local state.
func CanSkipParentVerification(
	shard historyi.ShardContext,
	localMutableState historyi.MutableState,
	sourceVersionHistories *historyspb.VersionHistories,
) (bool, error) {
	if localMutableState.GetExecutionState().GetState() != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED || !localMutableState.IsWorkflow() {
		return false, nil
	}
	localHistory, err := versionhistory.GetCurrentVersionHistory(localMutableState.GetExecutionInfo().VersionHistories)
	if err != nil {
		return false, err
	}
	sourceHistory, err := versionhistory.GetCurrentVersionHistory(sourceVersionHistories)
	if err != nil {
		return false, err
	}
	return versionhistory.IsEqualVersionHistoryItems(localHistory.Items, sourceHistory.Items) &&
		!closeTransferTaskPending(shard, localMutableState), nil
}

func closeTransferTaskPending(shard historyi.ShardContext, mutableState historyi.MutableState) bool {
	closeTaskID := mutableState.GetExecutionInfo().CloseTransferTaskId
	if closeTaskID == 0 {
		return false
	}
	queueState, ok := shard.GetQueueState(tasks.CategoryTransfer)
	if !ok {
		return true
	}
	return !queues.IsTaskAcked(&tasks.CloseExecutionTask{
		WorkflowKey: mutableState.GetWorkflowKey(),
		TaskID:      closeTaskID,
	}, queueState)
}
