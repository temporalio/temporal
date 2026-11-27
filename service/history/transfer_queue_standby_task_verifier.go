package history

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

func (t *transferQueueStandbyTaskExecutor) verifyChasmSideEffectTransferTask(
	ctx context.Context,
	task *tasks.ChasmTask,
	ms historyi.MutableState,
) (any, error) {
	isTaskInTree, isValid, err := validateChasmSideEffectTask(ctx, ms, task)
	if err != nil {
		return nil, err
	}
	if !isTaskInTree || !isValid {
		// Replication has removed the logical task, or the component reports it
		// invalid — drop the physical task.
		return nil, nil
	}

	// Task still exists in the tree; retry until the active cluster executes
	// and replicates the resulting state change.
	return ms.ChasmTree(), nil
}

func (t *transferQueueStandbyTaskExecutor) verifyActivityTask(
	ctx context.Context,
	transferTask *tasks.ActivityTask,
	mutableState historyi.MutableState,
) (any, error) {
	if !mutableState.IsWorkflowExecutionRunning() {
		return nil, nil
	}

	activityInfo, ok := mutableState.GetActivityInfo(transferTask.ScheduledEventID)
	if !ok {
		return nil, nil
	}

	if activityInfo.Paused {
		return nil, nil
	}

	if activityInfo.Stamp != transferTask.Stamp {
		return nil, consts.ErrStaleReference
	}

	err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), activityInfo.Version, transferTask.Version, transferTask)
	if err != nil {
		return nil, err
	}

	if activityInfo.StartedEventId == common.EmptyEventID {
		return newActivityTaskPostActionInfo(mutableState, activityInfo)
	}

	return nil, nil
}

func (t *transferQueueStandbyTaskExecutor) verifyWorkflowTask(
	ctx context.Context,
	transferTask *tasks.WorkflowTask,
	mutableState historyi.MutableState,
) (any, error) {
	if !mutableState.IsWorkflowExecutionRunning() {
		return nil, nil
	}

	wtInfo := mutableState.GetWorkflowTaskByID(transferTask.ScheduledEventID)
	if wtInfo == nil {
		return nil, nil
	}
	if transferTask.Stamp != wtInfo.Stamp {
		return nil, consts.ErrStaleReference
	}

	_, scheduleToStartTimeout := mutableState.TaskQueueScheduleToStartTimeout(transferTask.TaskQueue)
	// Task queue is ignored here because at standby, always use original normal task queue,
	// disregards the transferTask.TaskQueue which could be sticky.
	// NOTE: scheduleToStart timeout is respected. If workflow was sticky before namespace become standby,
	// transferTask.TaskQueue is sticky, and there is timer already created for this timeout.
	// Use this sticky timeout as TTL.
	taskQueue := &taskqueuepb.TaskQueue{
		Name: mutableState.GetExecutionInfo().TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}

	err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), wtInfo.Version, transferTask.Version, transferTask)
	if err != nil {
		return nil, err
	}

	if wtInfo.StartedEventID == common.EmptyEventID {
		return newWorkflowTaskPostActionInfo(
			mutableState,
			scheduleToStartTimeout.AsDuration(),
			taskQueue,
		)
	}

	return nil, nil
}

func (t *transferQueueStandbyTaskExecutor) verifyCancelExecution(
	ctx context.Context,
	transferTask *tasks.CancelExecutionTask,
	mutableState historyi.MutableState,
) (any, error) {
	if !mutableState.IsWorkflowExecutionRunning() {
		return nil, nil
	}

	requestCancelInfo, ok := mutableState.GetRequestCancelInfo(transferTask.InitiatedEventID)
	if !ok {
		return nil, nil
	}

	err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), requestCancelInfo.Version, transferTask.Version, transferTask)
	if err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (t *transferQueueStandbyTaskExecutor) verifySignalExecution(
	ctx context.Context,
	transferTask *tasks.SignalExecutionTask,
	mutableState historyi.MutableState,
) (any, error) {
	if !mutableState.IsWorkflowExecutionRunning() {
		return nil, nil
	}

	signalInfo, ok := mutableState.GetSignalInfo(transferTask.InitiatedEventID)
	if !ok {
		return nil, nil
	}

	err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), signalInfo.Version, transferTask.Version, transferTask)
	if err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}
