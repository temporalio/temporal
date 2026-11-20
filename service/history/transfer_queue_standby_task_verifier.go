package history

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
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

type closeExecutionVerificationInfo struct {
	request            *historyservice.VerifyChildExecutionCompletionRecordedRequest
	childWorkflowState string
}

type startChildExecutionVerificationInfo struct {
	request       *historyservice.VerifyFirstWorkflowTaskScheduledRequest
	namespaceName namespace.Name
}

func (t *transferQueueStandbyTaskExecutor) verifyCloseExecution(
	ctx context.Context,
	task *tasks.CloseExecutionTask,
	mutableState historyi.MutableState,
) (any, error) {
	if mutableState.IsWorkflowExecutionRunning() {
		return nil, nil
	}
	closeVersion, err := mutableState.GetCloseVersion()
	if err != nil {
		return nil, err
	}
	if err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), closeVersion, task.Version, task); err != nil {
		return nil, err
	}
	info := mutableState.GetExecutionInfo()
	if task.SkipParentVerification || !mutableState.HasParentExecution() || info.NewExecutionRunId != "" {
		return nil, nil
	}
	completionEvent, err := mutableState.GetCompletionEvent(ctx)
	if err != nil {
		return nil, err
	}
	if ndc.IsTerminatedByResetter(completionEvent) {
		return nil, nil
	}
	return &closeExecutionVerificationInfo{
		request: &historyservice.VerifyChildExecutionCompletionRecordedRequest{
			NamespaceId:            info.ParentNamespaceId,
			ParentExecution:        &commonpb.WorkflowExecution{WorkflowId: info.ParentWorkflowId, RunId: info.ParentRunId},
			ChildExecution:         &commonpb.WorkflowExecution{WorkflowId: task.WorkflowID, RunId: task.RunID},
			ParentInitiatedId:      info.ParentInitiatedId,
			ParentInitiatedVersion: info.ParentInitiatedVersion,
			Clock:                  common.CloneProto(info.ParentClock),
		},
		childWorkflowState: mutableState.GetExecutionState().GetState().String(),
	}, nil
}

func (t *transferQueueStandbyTaskExecutor) verifyStartChildExecution(
	ctx context.Context,
	task *tasks.StartChildExecutionTask,
	mutableState historyi.MutableState,
) (any, error) {
	info, ok := mutableState.GetChildExecutionInfo(task.InitiatedEventID)
	if !ok {
		return nil, nil
	}
	if err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), info.Version, task.Version, task); err != nil {
		return nil, err
	}
	started := info.StartedEventId != common.EmptyEventID
	if !mutableState.IsWorkflowExecutionRunning() && (!started || info.ParentClosePolicy != enumspb.PARENT_CLOSE_POLICY_ABANDON) {
		// Closed parents only verify children that were started with the abandon policy.
		return nil, nil
	}
	if !started {
		return &startChildExecutionPostActionInfo{}, nil
	}
	return &startChildExecutionVerificationInfo{
		request: &historyservice.VerifyFirstWorkflowTaskScheduledRequest{
			NamespaceId:       info.NamespaceId,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: info.StartedWorkflowId, RunId: info.StartedRunId},
			Clock:             common.CloneProto(info.Clock),
		},
		namespaceName: namespace.Name(info.Namespace),
	}, nil
}
