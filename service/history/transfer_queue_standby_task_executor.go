package history

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	recordChildCompletionVerificationFailedMsg = "Failed to verify child execution completion recoreded"
	firstWorkflowTaskVerificationFailedMsg     = "Failed to verify first workflow task scheduled"
)

type (
	transferQueueStandbyTaskExecutor struct {
		*transferQueueTaskExecutorBase

		clusterName string
		clientBean  client.Bean
	}

	verificationErr struct {
		msg string
		err error
	}
)

func newTransferQueueStandbyTaskExecutor(
	shard historyi.ShardContext,
	workflowCache wcache.Cache,
	logger log.Logger,
	metricProvider metrics.Handler,
	clusterName string,
	historyRawClient resource.HistoryRawClient,
	matchingRawClient resource.MatchingRawClient,
	visibilityManager manager.VisibilityManager,
	chasmEngine chasm.Engine,
	clientBean client.Bean,
) queues.Executor {
	return &transferQueueStandbyTaskExecutor{
		transferQueueTaskExecutorBase: newTransferQueueTaskExecutorBase(
			shard,
			workflowCache,
			logger,
			metricProvider,
			historyRawClient,
			matchingRawClient,
			visibilityManager,
			chasmEngine,
		),
		clusterName: clusterName,
		clientBean:  clientBean,
	}
}

func (t *transferQueueStandbyTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) queues.ExecuteResponse {
	task := executable.GetTask()
	taskType := queues.GetStandbyTransferTaskTypeTagValue(task)
	metricsTags := []metrics.Tag{
		getNamespaceTagByID(t.shardContext.GetNamespaceRegistry(), task.GetNamespaceID()),
		metrics.TaskTypeTag(taskType),
		metrics.OperationTag(taskType), // for backward compatibility
	}

	var err error
	switch task := task.(type) {
	case *tasks.ActivityTask:
		err = t.processActivityTask(ctx, task)
	case *tasks.WorkflowTask:
		err = t.processWorkflowTask(ctx, task)
	case *tasks.CancelExecutionTask:
		err = t.processCancelExecution(ctx, task)
	case *tasks.SignalExecutionTask:
		err = t.processSignalExecution(ctx, task)
	case *tasks.StartChildExecutionTask:
		err = t.processStartChildExecution(ctx, task)
	case *tasks.ResetWorkflowTask:
		// no reset needed for standby
		// TODO: add error logs
		err = nil
	case *tasks.CloseExecutionTask:
		err = t.processCloseExecution(ctx, task)
	case *tasks.DeleteExecutionTask:
		err = t.processDeleteExecutionTask(ctx, task, false)
	case *tasks.ChasmTask:
		err = t.executeChasmSideEffectTransferTask(ctx, task)
	default:
		err = errUnknownTransferTask
	}

	return queues.ExecuteResponse{
		ExecutionMetricTags: metricsTags,
		ExecutedAsActive:    false,
		ExecutionErr:        err,
	}
}

func (t *transferQueueStandbyTaskExecutor) executeChasmSideEffectTransferTask(
	ctx context.Context,
	task *tasks.ChasmTask,
) error {
	actionFn := func(
		ctx context.Context,
		wfContext historyi.WorkflowContext,
		ms historyi.MutableState,
	) (any, error) {
		return validateChasmSideEffectTask(
			ctx,
			ms,
			task,
		)
	}

	return t.processTransfer(
		ctx,
		true,
		task,
		actionFn,
		getStandbyPostActionFn(
			task,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(task.GetType()),
			// TODO - replace this with a method for CHASM components
			t.checkWorkflowStillExistOnSourceBeforeDiscard,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processActivityTask(
	ctx context.Context,
	transferTask *tasks.ActivityTask,
) error {
	processTaskIfClosed := false
	actionFn := func(_ context.Context, wfContext historyi.WorkflowContext, mutableState historyi.MutableState) (interface{}, error) {
		activityInfo, ok := mutableState.GetActivityInfo(transferTask.ScheduledEventID)
		if !ok {
			return nil, nil
		}

		if activityInfo.Stamp != transferTask.Stamp || activityInfo.Paused {
			return nil, nil // drop the task
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

	return t.processTransfer(
		ctx,
		processTaskIfClosed,
		transferTask,
		actionFn,
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			t.pushActivity,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processWorkflowTask(
	ctx context.Context,
	transferTask *tasks.WorkflowTask,
) error {
	actionFn := func(_ context.Context, wfContext historyi.WorkflowContext, mutableState historyi.MutableState) (interface{}, error) {
		wtInfo := mutableState.GetWorkflowTaskByID(transferTask.ScheduledEventID)
		if wtInfo == nil {
			return nil, nil
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

	return t.processTransfer(
		ctx,
		false,
		transferTask,
		actionFn,
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			t.pushWorkflowTask,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processCloseExecution(
	ctx context.Context,
	transferTask *tasks.CloseExecutionTask,
) error {
	processTaskIfClosed := true
	actionFn := func(ctx context.Context, wfContext historyi.WorkflowContext, mutableState historyi.MutableState) (interface{}, error) {
		if mutableState.IsWorkflowExecutionRunning() {
			// this can happen if workflow is reset.
			return nil, nil
		}

		executionInfo := mutableState.GetExecutionInfo()

		closeVersion, err := mutableState.GetCloseVersion()
		if err != nil {
			return nil, err
		}
		err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), closeVersion, transferTask.Version, transferTask)
		if err != nil {
			return nil, err
		}

		// verify if parent got the completion event
		verifyCompletionRecorded := mutableState.HasParentExecution() && executionInfo.NewExecutionRunId == ""
		if verifyCompletionRecorded {
			// load close event only if needed.
			completionEvent, err := mutableState.GetCompletionEvent(ctx)
			if err != nil {
				return nil, err
			}

			verifyCompletionRecorded = verifyCompletionRecorded && !ndc.IsTerminatedByResetter(completionEvent)
		}

		if verifyCompletionRecorded {
			now := t.getCurrentTime()
			taskTime := transferTask.GetVisibilityTime()
			localVerificationTime := taskTime.Add(t.config.MaxLocalParentWorkflowVerificationDuration())

			resendParent := now.After(localVerificationTime) && mutableState.IsTransitionHistoryEnabled() && mutableState.CurrentVersionedTransition() != nil

			_, err := t.historyRawClient.VerifyChildExecutionCompletionRecorded(ctx, &historyservice.VerifyChildExecutionCompletionRecordedRequest{
				NamespaceId: executionInfo.ParentNamespaceId,
				ParentExecution: &commonpb.WorkflowExecution{
					WorkflowId: executionInfo.ParentWorkflowId,
					RunId:      executionInfo.ParentRunId,
				},
				ChildExecution: &commonpb.WorkflowExecution{
					WorkflowId: transferTask.WorkflowID,
					RunId:      transferTask.RunID,
				},
				ParentInitiatedId:      executionInfo.ParentInitiatedId,
				ParentInitiatedVersion: executionInfo.ParentInitiatedVersion,
				Clock:                  executionInfo.ParentClock,
				ResendParent:           resendParent,
			})
			switch err.(type) {
			case nil, *serviceerror.NamespaceNotFound, *serviceerror.Unimplemented:
				// Case 1: Target workflow is in the desired state.
				return nil, nil
			case *serviceerror.NotFound, *serviceerror.WorkflowNotReady:
				// Case 2: Target workflow is not in the desired state.
				// Returning a non-nil pointer as postActionInfo here to indicate that verification is not done yet.
				return &verifyCompletionRecordedPostActionInfo{
					parentWorkflowKey: &definition.WorkflowKey{
						NamespaceID: executionInfo.ParentNamespaceId,
						WorkflowID:  executionInfo.ParentWorkflowId,
						RunID:       executionInfo.ParentRunId,
					},
				}, nil
			default:
				// Case 3: Verification itself failed.
				// NOTE: Returning an error as postActionInfo here so that post action can decide whether to retry or not.
				// Post action will propagate the error to upper layer to backoff and emit metrics properly if retry is needed.
				// NOTE: Wrapping the error as a verification error to prevent mutable state from being cleared and reloaded upon retry.
				// That's unnecessary as the error is in the target workflow, not this workflow.
				return &verificationErr{
					msg: recordChildCompletionVerificationFailedMsg,
					err: err,
				}, nil
			}
		}
		return nil, nil
	}

	return t.processTransfer(
		ctx,
		processTaskIfClosed,
		transferTask,
		actionFn,
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			t.checkParentWorkflowStillExistOnSourceBeforeDiscard,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processCancelExecution(
	ctx context.Context,
	transferTask *tasks.CancelExecutionTask,
) error {
	processTaskIfClosed := false
	actionFn := func(_ context.Context, wfContext historyi.WorkflowContext, mutableState historyi.MutableState) (interface{}, error) {
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

	return t.processTransfer(
		ctx,
		processTaskIfClosed,
		transferTask,
		actionFn,
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			t.checkWorkflowStillExistOnSourceBeforeDiscard,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processSignalExecution(
	ctx context.Context,
	transferTask *tasks.SignalExecutionTask,
) error {
	processTaskIfClosed := false
	actionFn := func(_ context.Context, wfContext historyi.WorkflowContext, mutableState historyi.MutableState) (interface{}, error) {
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

	return t.processTransfer(
		ctx,
		processTaskIfClosed,
		transferTask,
		actionFn,
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			t.checkWorkflowStillExistOnSourceBeforeDiscard,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processStartChildExecution(
	ctx context.Context,
	transferTask *tasks.StartChildExecutionTask,
) error {
	processTaskIfClosed := true
	actionFn := func(ctx context.Context, wfContext historyi.WorkflowContext, mutableState historyi.MutableState) (interface{}, error) {
		childWorkflowInfo, ok := mutableState.GetChildExecutionInfo(transferTask.InitiatedEventID)
		if !ok {
			return nil, nil
		}

		err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), childWorkflowInfo.Version, transferTask.Version, transferTask)
		if err != nil {
			return nil, err
		}

		workflowClosed := !mutableState.IsWorkflowExecutionRunning()
		childStarted := childWorkflowInfo.StartedEventId != common.EmptyEventID
		childAbandon := childWorkflowInfo.ParentClosePolicy == enumspb.PARENT_CLOSE_POLICY_ABANDON

		if workflowClosed && !(childStarted && childAbandon) {
			// NOTE: ideally for workflowClosed, child not started, parent close policy is abandon case,
			// we should continue to start the child workflow in active cluster, so standby logic also need to
			// perform the verification. However, we can't do that due to some technial reasons.
			// Please check the comments in processStartChildExecution in transferQueueActiveTaskExecutor.go
			// for details.
			return nil, nil
		}

		if !childStarted {
			return &struct{}{}, nil
		}

		targetNamespaceID := childWorkflowInfo.NamespaceId
		if targetNamespaceID == "" {
			// This is for backward compatibility.
			// Old mutable state may not have the target namespace ID set in childWorkflowInfo.

			targetNamespaceEntry, err := t.registry.GetNamespace(namespace.Name(childWorkflowInfo.Namespace))
			if err != nil {
				return nil, err
			}
			targetNamespaceID = targetNamespaceEntry.ID().String()
		}

		_, err = t.historyRawClient.VerifyFirstWorkflowTaskScheduled(ctx, &historyservice.VerifyFirstWorkflowTaskScheduledRequest{
			NamespaceId: targetNamespaceID,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: childWorkflowInfo.StartedWorkflowId,
				RunId:      childWorkflowInfo.StartedRunId,
			},
			Clock: childWorkflowInfo.Clock,
		})
		switch err.(type) {
		case nil, *serviceerror.NamespaceNotFound, *serviceerror.Unimplemented:
			// Case 1: Target workflow is in the desired state.
			return nil, nil
		case *serviceerror.NotFound, *serviceerror.WorkflowNotReady:
			// Case 2:Ttarget workflow is not in the desired state.
			// Return a non-nil pointer as postActionInfo here to indicate that verification is not done yet.
			return &struct{}{}, nil
		default:
			// Case 3: Verification itself failed.
			// NOTE: Returning an error as postActionInfo here so that post action can decide whether to retry or not.
			// Post action will propagate the error to upper layer to backoff and emit metrics properly if retry is needed.
			// NOTE: Wrapping the error as a verification error to prevent mutable state from being cleared and reloaded upon retry.
			// That's unnecessary as the error is in the target workflow, not this workflow.
			return &verificationErr{
				msg: recordChildCompletionVerificationFailedMsg,
				err: err,
			}, nil
		}
	}

	return t.processTransfer(
		ctx,
		processTaskIfClosed,
		transferTask,
		actionFn,
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			t.checkWorkflowStillExistOnSourceBeforeDiscard,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processTransfer(
	ctx context.Context,
	processTaskIfClosed bool,
	taskInfo tasks.Task,
	actionFn standbyActionFn,
	postActionFn standbyPostActionFn,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	nsRecord, err := t.shardContext.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(taskInfo.GetNamespaceID()))
	if err != nil {
		return err
	}
	if !nsRecord.IsOnCluster(t.clusterName) {
		// namespace is not replicated to local cluster, ignore corresponding tasks
		return nil
	}

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, taskInfo)
	if err != nil {
		return err
	}
	defer func() {
		var verificationErr *verificationErr
		if retError == consts.ErrTaskRetry || errors.As(retError, &verificationErr) {
			release(nil)
		} else {
			release(retError)
		}
	}()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, taskInfo, t.metricHandler, t.logger)
	if err != nil || mutableState == nil {
		return err
	}

	if !processTaskIfClosed && !mutableState.IsWorkflowExecutionRunning() {
		// workflow already finished, no need to process transfer task.
		return nil
	}

	postActionInfo, err := actionFn(ctx, weContext, mutableState)
	if err != nil {
		return err
	}

	// NOTE: do not access anything related mutable state after this lock release
	release(nil)
	return postActionFn(ctx, taskInfo, postActionInfo, t.logger)
}

func (t *transferQueueStandbyTaskExecutor) pushActivity(
	ctx context.Context,
	task tasks.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {
	if postActionInfo == nil {
		return nil
	}

	pushActivityInfo := postActionInfo.(*activityTaskPostActionInfo)
	return t.transferQueueTaskExecutorBase.pushActivity(
		ctx,
		task.(*tasks.ActivityTask),
		pushActivityInfo.activityTaskScheduleToStartTimeout,
		pushActivityInfo.versionDirective,
		pushActivityInfo.priority,
		historyi.TransactionPolicyPassive,
	)
}

func (t *transferQueueStandbyTaskExecutor) pushWorkflowTask(
	ctx context.Context,
	task tasks.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {
	if postActionInfo == nil {
		return nil
	}

	pushwtInfo := postActionInfo.(*workflowTaskPostActionInfo)
	return t.transferQueueTaskExecutorBase.pushWorkflowTask(
		ctx,
		task.(*tasks.WorkflowTask),
		pushwtInfo.taskqueue,
		pushwtInfo.workflowTaskScheduleToStartTimeout,
		pushwtInfo.versionDirective,
		pushwtInfo.priority,
		historyi.TransactionPolicyPassive,
	)
}

// TODO: deprecate this function and always use t.Now()
// Only test code sets t.clusterName to be non-current cluster name
// and advance the time by setting calling shardContext.SetCurrentTime.
func (t *transferQueueStandbyTaskExecutor) getCurrentTime() time.Time {
	return t.shardContext.GetCurrentTime(t.clusterName)
}

func (e *verificationErr) Error() string {
	return fmt.Sprintf("%v: %v", e.msg, e.err.Error())
}

func (e *verificationErr) Unwrap() error {
	return e.err
}

func (t *transferQueueStandbyTaskExecutor) checkWorkflowStillExistOnSourceBeforeDiscard(
	ctx context.Context,
	taskInfo tasks.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {
	if postActionInfo == nil {
		return nil
	}
	if !isWorkflowExistOnSource(ctx, taskWorkflowKey(taskInfo), logger, t.clusterName, t.clientBean, t.shardContext.GetNamespaceRegistry()) {
		return standbyTransferTaskPostActionTaskDiscarded(ctx, taskInfo, nil, logger)
	}
	return standbyTransferTaskPostActionTaskDiscarded(ctx, taskInfo, postActionInfo, logger)
}

func (t *transferQueueStandbyTaskExecutor) checkParentWorkflowStillExistOnSourceBeforeDiscard(
	ctx context.Context,
	taskInfo tasks.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {
	if postActionInfo == nil {
		return nil
	}
	pushActivityInfo, ok := postActionInfo.(*verifyCompletionRecordedPostActionInfo)
	if !ok || pushActivityInfo.parentWorkflowKey == nil {
		return standbyTransferTaskPostActionTaskDiscarded(ctx, taskInfo, postActionInfo, logger)
	}

	if !isWorkflowExistOnSource(ctx, *pushActivityInfo.parentWorkflowKey, logger, t.clusterName, t.clientBean, t.shardContext.GetNamespaceRegistry()) {
		return standbyTransferTaskPostActionTaskDiscarded(ctx, taskInfo, nil, logger)
	}
	return standbyTransferTaskPostActionTaskDiscarded(ctx, taskInfo, postActionInfo, logger)
}
