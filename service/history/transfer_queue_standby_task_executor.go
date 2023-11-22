// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	recordChildCompletionVerificationFailedMsg = "Failed to verify child execution completion recoreded"
	firstWorkflowTaskVerificationFailedMsg     = "Failed to verify first workflow task scheduled"
)

type (
	transferQueueStandbyTaskExecutor struct {
		*transferQueueTaskExecutorBase

		clusterName        string
		nDCHistoryResender xdc.NDCHistoryResender
	}

	verificationErr struct {
		msg string
		err error
	}
)

func newTransferQueueStandbyTaskExecutor(
	shard shard.Context,
	workflowCache wcache.Cache,
	nDCHistoryResender xdc.NDCHistoryResender,
	logger log.Logger,
	metricProvider metrics.Handler,
	clusterName string,
	historyRawClient resource.HistoryRawClient,
	matchingRawClient resource.MatchingRawClient,
	visibilityManager manager.VisibilityManager,
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
		),
		clusterName:        clusterName,
		nDCHistoryResender: nDCHistoryResender,
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
	default:
		err = errUnknownTransferTask
	}

	return queues.ExecuteResponse{
		ExecutionMetricTags: metricsTags,
		ExecutedAsActive:    false,
		ExecutionErr:        err,
	}
}

func (t *transferQueueStandbyTaskExecutor) processActivityTask(
	ctx context.Context,
	transferTask *tasks.ActivityTask,
) error {
	processTaskIfClosed := false
	actionFn := func(_ context.Context, wfContext workflow.Context, mutableState workflow.MutableState) (interface{}, error) {
		activityInfo, ok := mutableState.GetActivityInfo(transferTask.ScheduledEventID)
		if !ok {
			return nil, nil
		}

		err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), activityInfo.Version, transferTask.Version, transferTask)
		if err != nil {
			return nil, err
		}

		if activityInfo.StartedEventId == common.EmptyEventID {
			return newActivityTaskPostActionInfo(mutableState, activityInfo.ScheduleToStartTimeout.AsDuration(), activityInfo.UseCompatibleVersion)
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
			t.config.StandbyTaskMissingEventsResendDelay(transferTask.GetType()),
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			t.fetchHistoryFromRemote,
			t.pushActivity,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processWorkflowTask(
	ctx context.Context,
	transferTask *tasks.WorkflowTask,
) error {
	actionFn := func(_ context.Context, wfContext workflow.Context, mutableState workflow.MutableState) (interface{}, error) {
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
			t.config.StandbyTaskMissingEventsResendDelay(transferTask.GetType()),
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			t.fetchHistoryFromRemote,
			t.pushWorkflowTask,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processCloseExecution(
	ctx context.Context,
	transferTask *tasks.CloseExecutionTask,
) error {
	processTaskIfClosed := true
	actionFn := func(ctx context.Context, wfContext workflow.Context, mutableState workflow.MutableState) (interface{}, error) {
		if mutableState.IsWorkflowExecutionRunning() {
			// this can happen if workflow is reset.
			return nil, nil
		}

		executionInfo := mutableState.GetExecutionInfo()

		lastWriteVersion, err := mutableState.GetLastWriteVersion()
		if err != nil {
			return nil, err
		}
		err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), lastWriteVersion, transferTask.Version, transferTask)
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
			})
			switch err.(type) {
			case nil, *serviceerror.NamespaceNotFound, *serviceerror.Unimplemented:
				return nil, nil
			case *serviceerror.NotFound, *serviceerror.WorkflowNotReady:
				return verifyChildCompletionRecordedInfo, nil
			default:
				// NOTE: we do not return the error directly here as it will cause the mutable state to be cleared and reloaded upon retry
				// it's unnecessary as the error is in the target workflow, not this workflow.
				return nil, &verificationErr{
					msg: recordChildCompletionVerificationFailedMsg,
					err: err,
				}
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
			t.config.StandbyTaskMissingEventsResendDelay(transferTask.GetType()),
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			standbyTaskPostActionNoOp,
			standbyTransferTaskPostActionTaskDiscarded,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processCancelExecution(
	ctx context.Context,
	transferTask *tasks.CancelExecutionTask,
) error {
	processTaskIfClosed := false
	actionFn := func(_ context.Context, wfContext workflow.Context, mutableState workflow.MutableState) (interface{}, error) {
		requestCancelInfo, ok := mutableState.GetRequestCancelInfo(transferTask.InitiatedEventID)
		if !ok {
			return nil, nil
		}

		err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), requestCancelInfo.Version, transferTask.Version, transferTask)
		if err != nil {
			return nil, err
		}

		return getHistoryResendInfo(mutableState)
	}

	return t.processTransfer(
		ctx,
		processTaskIfClosed,
		transferTask,
		actionFn,
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(transferTask.GetType()),
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			t.fetchHistoryFromRemote,
			standbyTransferTaskPostActionTaskDiscarded,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processSignalExecution(
	ctx context.Context,
	transferTask *tasks.SignalExecutionTask,
) error {
	processTaskIfClosed := false
	actionFn := func(_ context.Context, wfContext workflow.Context, mutableState workflow.MutableState) (interface{}, error) {
		signalInfo, ok := mutableState.GetSignalInfo(transferTask.InitiatedEventID)
		if !ok {
			return nil, nil
		}

		err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), signalInfo.Version, transferTask.Version, transferTask)
		if err != nil {
			return nil, err
		}

		return getHistoryResendInfo(mutableState)
	}

	return t.processTransfer(
		ctx,
		processTaskIfClosed,
		transferTask,
		actionFn,
		getStandbyPostActionFn(
			transferTask,
			t.getCurrentTime,
			t.config.StandbyTaskMissingEventsResendDelay(transferTask.GetType()),
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			t.fetchHistoryFromRemote,
			standbyTransferTaskPostActionTaskDiscarded,
		),
	)
}

func (t *transferQueueStandbyTaskExecutor) processStartChildExecution(
	ctx context.Context,
	transferTask *tasks.StartChildExecutionTask,
) error {
	processTaskIfClosed := true
	actionFn := func(ctx context.Context, wfContext workflow.Context, mutableState workflow.MutableState) (interface{}, error) {
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
			historyResendInfo, err := getHistoryResendInfo(mutableState)
			if err != nil {
				return nil, err
			}
			return &startChildExecutionPostActionInfo{
				historyResendInfo: historyResendInfo,
			}, nil
		}

		_, err = t.historyRawClient.VerifyFirstWorkflowTaskScheduled(ctx, &historyservice.VerifyFirstWorkflowTaskScheduledRequest{
			NamespaceId: transferTask.TargetNamespaceID,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: childWorkflowInfo.StartedWorkflowId,
				RunId:      childWorkflowInfo.StartedRunId,
			},
			Clock: childWorkflowInfo.Clock,
		})
		switch err.(type) {
		case nil, *serviceerror.NamespaceNotFound, *serviceerror.Unimplemented:
			return nil, nil
		case *serviceerror.NotFound, *serviceerror.WorkflowNotReady:
			return &startChildExecutionPostActionInfo{}, nil
		default:
			// NOTE: we do not return the error directly here as it will cause the mutable state to be cleared and reloaded upon retry
			// it's unnecessary as the error is in the target workflow, not this workflow.
			return nil, &verificationErr{
				msg: recordChildCompletionVerificationFailedMsg,
				err: err,
			}
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
			t.config.StandbyTaskMissingEventsResendDelay(transferTask.GetType()),
			t.config.StandbyTaskMissingEventsDiscardDelay(transferTask.GetType()),
			t.startChildExecutionResendPostAction,
			standbyTransferTaskPostActionTaskDiscarded,
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

	if !mutableState.IsWorkflowExecutionRunning() && !processTaskIfClosed {
		// workflow already finished, no need to process transfer task.
		return nil
	}

	historyResendInfo, err := actionFn(ctx, weContext, mutableState)
	if err != nil {
		return err
	}

	// NOTE: do not access anything related mutable state after this lock release
	release(nil)
	return postActionFn(ctx, taskInfo, historyResendInfo, t.logger)
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
	timeout := pushActivityInfo.activityTaskScheduleToStartTimeout
	return t.transferQueueTaskExecutorBase.pushActivity(
		ctx,
		task.(*tasks.ActivityTask),
		timeout,
		pushActivityInfo.versionDirective,
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
	)
}

func (t *transferQueueStandbyTaskExecutor) startChildExecutionResendPostAction(
	ctx context.Context,
	taskInfo tasks.Task,
	postActionInfo interface{},
	log log.Logger,
) error {
	if postActionInfo == nil {
		return nil
	}

	historyResendInfo := postActionInfo.(*startChildExecutionPostActionInfo).historyResendInfo
	if historyResendInfo != nil {
		return t.fetchHistoryFromRemote(ctx, taskInfo, historyResendInfo, log)
	}

	return standbyTaskPostActionNoOp(ctx, taskInfo, postActionInfo, log)
}

func (t *transferQueueStandbyTaskExecutor) fetchHistoryFromRemote(
	ctx context.Context,
	taskInfo tasks.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {
	var resendInfo *historyResendInfo
	switch postActionInfo := postActionInfo.(type) {
	case nil:
		return nil
	case *historyResendInfo:
		resendInfo = postActionInfo
	case *activityTaskPostActionInfo:
		resendInfo = postActionInfo.historyResendInfo
	case *workflowTaskPostActionInfo:
		resendInfo = postActionInfo.historyResendInfo
	default:
		logger.Fatal("unknown post action info for fetching remote history", tag.Value(postActionInfo))
	}

	remoteClusterName, err := getRemoteClusterName(
		t.currentClusterName,
		t.registry,
		taskInfo.GetNamespaceID(),
	)
	if err != nil {
		return err
	}

	scope := t.metricHandler.WithTags(metrics.OperationTag(metrics.HistoryRereplicationByTransferTaskScope))
	scope.Counter(metrics.ClientRequests.GetMetricName()).Record(1)
	startTime := time.Now().UTC()
	defer func() { scope.Timer(metrics.ClientLatency.GetMetricName()).Record(time.Since(startTime)) }()

	if resendInfo.lastEventID == common.EmptyEventID || resendInfo.lastEventVersion == common.EmptyVersion {
		t.logger.Error("Error re-replicating history from remote: transferQueueStandbyProcessor encountered empty historyResendInfo.",
			tag.ShardID(t.shardContext.GetShardID()),
			tag.WorkflowNamespaceID(taskInfo.GetNamespaceID()),
			tag.WorkflowID(taskInfo.GetWorkflowID()),
			tag.WorkflowRunID(taskInfo.GetRunID()),
			tag.SourceCluster(remoteClusterName))

		return consts.ErrTaskRetry
	}

	// NOTE: history resend may take long time and its timeout is currently
	// controlled by a separate dynamicconfig config: StandbyTaskReReplicationContextTimeout
	if err = t.nDCHistoryResender.SendSingleWorkflowHistory(
		ctx,
		remoteClusterName,
		namespace.ID(taskInfo.GetNamespaceID()),
		taskInfo.GetWorkflowID(),
		taskInfo.GetRunID(),
		resendInfo.lastEventID,
		resendInfo.lastEventVersion,
		0,
		0,
	); err != nil {
		if _, isNotFound := err.(*serviceerror.NamespaceNotFound); isNotFound {
			// Don't log NamespaceNotFound error because it is valid case, and return error to stop retrying.
			return err
		}
		t.logger.Error("Error re-replicating history from remote.",
			tag.ShardID(t.shardContext.GetShardID()),
			tag.WorkflowNamespaceID(taskInfo.GetNamespaceID()),
			tag.WorkflowID(taskInfo.GetWorkflowID()),
			tag.WorkflowRunID(taskInfo.GetRunID()),
			tag.SourceCluster(remoteClusterName),
			tag.Error(err))
	}

	// Return retryable error, so task processing will retry.
	return consts.ErrTaskRetry
}

func (t *transferQueueStandbyTaskExecutor) getCurrentTime() time.Time {
	return t.shardContext.GetCurrentTime(t.clusterName)
}

func (e *verificationErr) Error() string {
	return fmt.Sprintf("%v: %v", e.msg, e.err.Error())
}

func (e *verificationErr) Unwrap() error {
	return e.err
}
