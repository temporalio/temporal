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
	"fmt"
	"time"

	"github.com/pborman/uuid"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/sdk"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/worker/archiver"
	"go.temporal.io/server/service/worker/parentclosepolicy"
)

type (
	transferQueueActiveTaskExecutor struct {
		*transferQueueTaskExecutorBase

		workflowResetter        *workflowResetterImpl
		parentClosePolicyClient parentclosepolicy.Client
	}
)

func newTransferQueueActiveTaskExecutor(
	shard shard.Context,
	workflowCache workflow.Cache,
	archivalClient archiver.Client,
	sdkClientFactory sdk.ClientFactory,
	logger log.Logger,
	metricProvider metrics.MetricsHandler,
	config *configs.Config,
	matchingClient matchingservice.MatchingServiceClient,
) queues.Executor {
	return &transferQueueActiveTaskExecutor{
		transferQueueTaskExecutorBase: newTransferQueueTaskExecutorBase(
			shard,
			workflowCache,
			archivalClient,
			logger,
			metricProvider,
			matchingClient,
		),
		workflowResetter: newWorkflowResetter(
			shard,
			workflowCache,
			logger,
		),
		parentClosePolicyClient: parentclosepolicy.NewClient(
			shard.GetMetricsClient(),
			shard.GetLogger(),
			sdkClientFactory,
			config.NumParentClosePolicySystemWorkflows(),
		),
	}
}

func (t *transferQueueActiveTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) ([]metrics.Tag, bool, error) {
	task := executable.GetTask()
	taskType := queues.GetActiveTransferTaskTypeTagValue(task)
	metricsTags := []metrics.Tag{
		getNamespaceTagByID(t.shard.GetNamespaceRegistry(), task.GetNamespaceID()),
		metrics.TaskTypeTag(taskType),
		metrics.OperationTag(taskType), // for backward compatibility
	}

	var err error
	switch task := task.(type) {
	case *tasks.ActivityTask:
		err = t.processActivityTask(ctx, task)
	case *tasks.WorkflowTask:
		err = t.processWorkflowTask(ctx, task)
	case *tasks.CloseExecutionTask:
		err = t.processCloseExecution(ctx, task)
	case *tasks.CancelExecutionTask:
		err = t.processCancelExecution(ctx, task)
	case *tasks.SignalExecutionTask:
		err = t.processSignalExecution(ctx, task)
	case *tasks.StartChildExecutionTask:
		err = t.processStartChildExecution(ctx, task)
	case *tasks.ResetWorkflowTask:
		err = t.processResetWorkflow(ctx, task)
	case *tasks.DeleteExecutionTask:
		err = t.processDeleteExecutionTask(ctx, task)
	default:
		err = errUnknownTransferTask
	}

	return metricsTags, true, err
}

func (t *transferQueueActiveTaskExecutor) processDeleteExecutionTask(ctx context.Context,
	task *tasks.DeleteExecutionTask) error {
	return t.transferQueueTaskExecutorBase.processDeleteExecutionTask(ctx, task,
		t.config.TransferProcessorEnsureCloseBeforeDelete())
}

func (t *transferQueueActiveTaskExecutor) processActivityTask(
	ctx context.Context,
	task *tasks.ActivityTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, weContext, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	ai, ok := mutableState.GetActivityInfo(task.ScheduledEventID)
	if !ok {
		return nil
	}
	ok = VerifyTaskVersion(t.shard, t.logger, mutableState.GetNamespaceEntry(), ai.Version, task.Version, task)
	if !ok {
		return nil
	}

	timeout := timestamp.DurationValue(ai.ScheduleToStartTimeout)

	// NOTE: do not access anything related mutable state after this lock release
	// release the context lock since we no longer need mutable state and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	return t.pushActivity(ctx, task, &timeout)
}

func (t *transferQueueActiveTaskExecutor) processWorkflowTask(
	ctx context.Context,
	task *tasks.WorkflowTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, weContext, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	workflowTask, found := mutableState.GetWorkflowTaskInfo(task.ScheduledEventID)
	if !found {
		return nil
	}
	ok := VerifyTaskVersion(t.shard, t.logger, mutableState.GetNamespaceEntry(), workflowTask.Version, task.Version, task)
	if !ok {
		return nil
	}

	executionInfo := mutableState.GetExecutionInfo()

	// NOTE: previously this section check whether mutable state has enabled
	// sticky workflowTask, if so convert the workflowTask to a sticky workflowTask.
	// that logic has a bug which timer task for that sticky workflowTask is not generated
	// the correct logic should check whether the workflow task is a sticky workflowTask
	// task or not.
	var taskQueue *taskqueuepb.TaskQueue
	taskScheduleToStartTimeoutSeconds := int64(0)
	if mutableState.GetExecutionInfo().TaskQueue != task.TaskQueue {
		// this workflowTask is an sticky workflowTask
		// there shall already be an timer set
		taskQueue = &taskqueuepb.TaskQueue{
			Name: task.TaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_STICKY,
		}
		taskScheduleToStartTimeoutSeconds = int64(timestamp.DurationValue(executionInfo.StickyScheduleToStartTimeout).Seconds())
	} else {
		taskQueue = &taskqueuepb.TaskQueue{
			Name: task.TaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}
		workflowRunTimeout := timestamp.DurationValue(executionInfo.WorkflowRunTimeout)
		taskScheduleToStartTimeoutSeconds = int64(workflowRunTimeout.Round(time.Second).Seconds())
	}

	originalTaskQueue := mutableState.GetExecutionInfo().TaskQueue
	// NOTE: do not access anything related mutable state after this lock release
	// release the context lock since we no longer need mutable state and
	// the rest of logic is making RPC call, which takes time.
	release(nil)

	err = t.pushWorkflowTask(ctx, task, taskQueue, timestamp.DurationFromSeconds(taskScheduleToStartTimeoutSeconds))

	if _, ok := err.(*serviceerrors.StickyWorkerUnavailable); ok {
		// sticky worker is unavailable, switch to original task queue
		taskQueue = &taskqueuepb.TaskQueue{
			// do not use task.TaskQueue which is sticky, use original task queue from mutable state
			Name: originalTaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}

		// Continue to use sticky schedule_to_start timeout as TTL for the matching task. Because the schedule_to_start
		// timeout timer task is already created which will timeout this task if no worker pick it up in 5s anyway.
		// There is no need to reset sticky, because if this task is picked by new worker, the new worker will reset
		// the sticky queue to a new one. However, if worker is completely down, that schedule_to_start timeout task
		// will re-create a new non-sticky task and reset sticky.
		err = t.pushWorkflowTask(ctx, task, taskQueue, timestamp.DurationFromSeconds(taskScheduleToStartTimeoutSeconds))
	}
	return err
}

func (t *transferQueueActiveTaskExecutor) processCloseExecution(
	ctx context.Context,
	task *tasks.CloseExecutionTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, weContext, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	// DeleteAfterClose is set to true when this close execution task was generated as part of delete open workflow execution procedure.
	// Delete workflow execution is started by user API call and should be done regardless of current workflow version.
	if !task.DeleteAfterClose {
		lastWriteVersion, err := mutableState.GetLastWriteVersion()
		if err != nil {
			return err
		}
		ok := VerifyTaskVersion(t.shard, t.logger, mutableState.GetNamespaceEntry(), lastWriteVersion, task.Version, task)
		if !ok {
			return nil
		}
	}

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowID(),
		RunId:      task.GetRunID(),
	}
	executionInfo := mutableState.GetExecutionInfo()
	executionState := mutableState.GetExecutionState()
	var completionEvent *historypb.HistoryEvent // needed to report close event to parent workflow
	replyToParentWorkflow := mutableState.HasParentExecution() && executionInfo.NewExecutionRunId == ""
	if replyToParentWorkflow {
		// only load close event if needed.
		completionEvent, err = mutableState.GetCompletionEvent(ctx)
		if err != nil {
			return err
		}
		replyToParentWorkflow = replyToParentWorkflow && !IsTerminatedByResetter(completionEvent)
	}
	parentNamespaceID := executionInfo.ParentNamespaceId
	parentWorkflowID := executionInfo.ParentWorkflowId
	parentRunID := executionInfo.ParentRunId
	parentInitiatedID := executionInfo.ParentInitiatedId
	parentInitiatedVersion := executionInfo.ParentInitiatedVersion
	var parentClock *clockspb.VectorClock
	if executionInfo.ParentClock != nil {
		parentClock = vclock.NewVectorClock(
			executionInfo.ParentClock.ClusterId,
			executionInfo.ParentClock.ShardId,
			executionInfo.ParentClock.Clock,
		)
	}

	workflowTypeName := executionInfo.WorkflowTypeName
	workflowCloseTime, err := mutableState.GetWorkflowCloseTime(ctx)
	if err != nil {
		return err
	}

	workflowStatus := executionState.Status
	workflowHistoryLength := mutableState.GetNextEventID() - 1

	workflowStartTime := timestamp.TimeValue(mutableState.GetExecutionInfo().GetStartTime())
	workflowExecutionTime := timestamp.TimeValue(mutableState.GetExecutionInfo().GetExecutionTime())
	visibilityMemo := getWorkflowMemo(copyMemo(executionInfo.Memo))
	searchAttr := getSearchAttributes(copySearchAttributes(executionInfo.SearchAttributes))
	namespaceName := mutableState.GetNamespaceEntry().Name()
	children := copyChildWorkflowInfos(mutableState.GetPendingChildExecutionInfos())

	// NOTE: do not access anything related mutable state after this lock release.
	// Release lock immediately since mutable state is not needed
	// and the rest of logic is RPC calls, which can take time.
	release(nil)

	err = t.archiveVisibility(
		ctx,
		namespace.ID(task.NamespaceID),
		task.WorkflowID,
		task.RunID,
		workflowTypeName,
		workflowStartTime,
		workflowExecutionTime,
		*workflowCloseTime,
		workflowStatus,
		workflowHistoryLength,
		visibilityMemo,
		searchAttr,
	)
	if err != nil {
		return err
	}

	// Communicate the result to parent execution if this is Child Workflow execution
	if replyToParentWorkflow {
		_, err := t.historyClient.RecordChildExecutionCompleted(ctx, &historyservice.RecordChildExecutionCompletedRequest{
			NamespaceId: parentNamespaceID,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: parentWorkflowID,
				RunId:      parentRunID,
			},
			ParentInitiatedId:      parentInitiatedID,
			ParentInitiatedVersion: parentInitiatedVersion,
			CompletedExecution:     &workflowExecution,
			Clock:                  parentClock,
			CompletionEvent:        completionEvent,
		})
		switch err.(type) {
		case nil:
			// noop
		case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
			// parent gone, noop
		default:
			return err
		}
	}

	err = t.processParentClosePolicy(
		ctx,
		namespaceName.String(),
		workflowExecution,
		children,
	)

	if err != nil {
		// This is some retryable error, not NotFound or NamespaceNotFound.
		return err
	}

	if task.DeleteAfterClose {
		err = t.deleteExecution(
			ctx,
			task,
			// Visibility is not updated (to avoid race condition for visibility tasks) and workflow execution is
			//still open there.
			true,
			false,
		)
	}
	return err
}

func (t *transferQueueActiveTaskExecutor) processCancelExecution(
	ctx context.Context,
	task *tasks.CancelExecutionTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, weContext, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	requestCancelInfo, ok := mutableState.GetRequestCancelInfo(task.InitiatedEventID)
	if !ok {
		return nil
	}
	ok = VerifyTaskVersion(t.shard, t.logger, mutableState.GetNamespaceEntry(), requestCancelInfo.Version, task.Version, task)
	if !ok {
		return nil
	}

	initiatedEvent, err := mutableState.GetRequesteCancelExternalInitiatedEvent(ctx, task.InitiatedEventID)
	if err != nil {
		return err
	}
	attributes := initiatedEvent.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()

	targetNamespaceEntry, err := t.registry.GetNamespaceByID(namespace.ID(task.TargetNamespaceID))
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NamespaceNotFound); !isNotFound {
			return err
		}
		// It is possible that target namespace got deleted. Record failure.
		t.logger.Debug("Target namespace is not found.", tag.WorkflowNamespaceID(task.TargetNamespaceID))
		err = t.requestCancelExternalExecutionFailed(
			ctx,
			task,
			weContext,
			namespace.Name(task.TargetNamespaceID), // Use ID as namespace name because namespace is already deleted and name is used only for history.
			namespace.ID(task.TargetNamespaceID),
			task.TargetWorkflowID,
			task.TargetRunID,
			enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND)
		return err
	}
	targetNamespaceName := targetNamespaceEntry.Name()

	// handle workflow cancel itself
	if task.NamespaceID == task.TargetNamespaceID && task.WorkflowID == task.TargetWorkflowID {
		// it does not matter if the run ID is a mismatch
		err = t.requestCancelExternalExecutionFailed(
			ctx,
			task,
			weContext,
			targetNamespaceName,
			namespace.ID(task.TargetNamespaceID),
			task.TargetWorkflowID,
			task.TargetRunID,
			enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND)
		return err
	}

	if err = t.requestCancelExternalExecution(
		ctx,
		task,
		targetNamespaceName,
		requestCancelInfo,
		attributes,
	); err != nil {
		t.logger.Debug(fmt.Sprintf("Failed to cancel external workflow execution. Error: %v", err))

		// Check to see if the error is non-transient, in which case add RequestCancelFailed
		// event and complete transfer task by returning nil error.
		if common.IsServiceTransientError(err) || common.IsContextDeadlineExceededErr(err) {
			// for retryable error just return
			return err
		}
		var failedCause enumspb.CancelExternalWorkflowExecutionFailedCause
		switch err.(type) {
		case *serviceerror.NotFound:
			failedCause = enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND
		case *serviceerror.NamespaceNotFound:
			failedCause = enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND
		default:
			t.logger.Error("Unexpected error type returned from RequestCancelWorkflowExecution API call.", tag.ErrorType(err), tag.Error(err))
			return err
		}
		return t.requestCancelExternalExecutionFailed(
			ctx,
			task,
			weContext,
			targetNamespaceName,
			namespace.ID(task.TargetNamespaceID),
			task.TargetWorkflowID,
			task.TargetRunID,
			failedCause,
		)
	}

	// Record ExternalWorkflowExecutionCancelRequested in source execution
	return t.requestCancelExternalExecutionCompleted(
		ctx,
		task,
		weContext,
		targetNamespaceName,
		namespace.ID(task.TargetNamespaceID),
		task.TargetWorkflowID,
		task.TargetRunID,
	)
}

func (t *transferQueueActiveTaskExecutor) processSignalExecution(
	ctx context.Context,
	task *tasks.SignalExecutionTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, weContext, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	signalInfo, ok := mutableState.GetSignalInfo(task.InitiatedEventID)
	if !ok {
		// TODO: here we should also RemoveSignalMutableState from target workflow
		// Otherwise, target SignalRequestID still can leak if shard restart after signalExternalExecutionCompleted
		// To do that, probably need to add the SignalRequestID in transfer task.
		return nil
	}
	ok = VerifyTaskVersion(t.shard, t.logger, mutableState.GetNamespaceEntry(), signalInfo.Version, task.Version, task)
	if !ok {
		return nil
	}

	initiatedEvent, err := mutableState.GetSignalExternalInitiatedEvent(ctx, task.InitiatedEventID)
	if err != nil {
		return err
	}
	attributes := initiatedEvent.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()

	targetNamespaceEntry, err := t.registry.GetNamespaceByID(namespace.ID(task.TargetNamespaceID))
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NamespaceNotFound); !isNotFound {
			return err
		}
		// It is possible that target namespace got deleted. Record failure.
		t.logger.Debug("Target namespace is not found.", tag.WorkflowNamespaceID(task.TargetNamespaceID))
		return t.signalExternalExecutionFailed(
			ctx,
			task,
			weContext,
			namespace.Name(task.TargetNamespaceID), // Use ID as namespace name because namespace is already deleted and name is used only for history.
			namespace.ID(task.TargetNamespaceID),
			task.TargetWorkflowID,
			task.TargetRunID,
			attributes.Control,
			enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND,
		)
	}
	targetNamespaceName := targetNamespaceEntry.Name()

	// handle workflow signal itself
	if task.NamespaceID == task.TargetNamespaceID && task.WorkflowID == task.TargetWorkflowID {
		// it does not matter if the run ID is a mismatch
		return t.signalExternalExecutionFailed(
			ctx,
			task,
			weContext,
			targetNamespaceName,
			namespace.ID(task.TargetNamespaceID),
			task.TargetWorkflowID,
			task.TargetRunID,
			attributes.Control,
			enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
		)
	}

	if err = t.signalExternalExecution(
		ctx,
		task,
		targetNamespaceName,
		signalInfo,
		attributes,
	); err != nil {
		t.logger.Debug("Failed to signal external workflow execution", tag.Error(err))

		// Check to see if the error is non-transient, in which case add SignalFailed
		// event and complete transfer task by returning nil error.
		if common.IsServiceTransientError(err) || common.IsContextDeadlineExceededErr(err) {
			// for retryable error just return
			return err
		}
		var failedCause enumspb.SignalExternalWorkflowExecutionFailedCause
		switch err.(type) {
		case *serviceerror.NotFound:
			failedCause = enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND
		case *serviceerror.NamespaceNotFound:
			failedCause = enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND
		default:
			t.logger.Error("Unexpected error type returned from SignalWorkflowExecution API call.", tag.ErrorType(err), tag.Error(err))
			return err
		}
		return t.signalExternalExecutionFailed(
			ctx,
			task,
			weContext,
			targetNamespaceName,
			namespace.ID(task.TargetNamespaceID),
			task.TargetWorkflowID,
			task.TargetRunID,
			attributes.Control,
			failedCause,
		)
	}

	err = t.signalExternalExecutionCompleted(
		ctx,
		task,
		weContext,
		targetNamespaceName,
		namespace.ID(task.TargetNamespaceID),
		task.TargetWorkflowID,
		task.TargetRunID,
		attributes.Control,
	)
	if err != nil {
		return err
	}

	signalRequestID := signalInfo.GetRequestId()

	// release the weContext lock since we no longer need mutable state and
	// the rest of logic is making RPC call, which takes time.
	release(retError)
	// remove signalRequestedID from target workflow, after Signal detail is removed from source workflow
	_, err = t.historyClient.RemoveSignalMutableState(ctx, &historyservice.RemoveSignalMutableStateRequest{
		NamespaceId: task.TargetNamespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: task.TargetWorkflowID,
			RunId:      task.TargetRunID,
		},
		RequestId: signalRequestID,
	})
	return err
}

func (t *transferQueueActiveTaskExecutor) processStartChildExecution(
	ctx context.Context,
	task *tasks.StartChildExecutionTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, weContext, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil {
		return nil
	}

	childInfo, ok := mutableState.GetChildExecutionInfo(task.InitiatedEventID)
	if !ok {
		return nil
	}
	ok = VerifyTaskVersion(t.shard, t.logger, mutableState.GetNamespaceEntry(), childInfo.Version, task.Version, task)
	if !ok {
		return nil
	}

	// workflow running or not, child started or not, parent close policy is abandon or not
	// 8 cases in total
	workflowRunning := mutableState.IsWorkflowExecutionRunning()
	childStarted := childInfo.StartedEventId != common.EmptyEventID
	abandonChild := childInfo.ParentClosePolicy == enumspb.PARENT_CLOSE_POLICY_ABANDON
	if !workflowRunning && !abandonChild {
		// case 1: workflow not running, child started, parent close policy is not abandon (rely on parent close policy to cancel/terminate wf)
		// case 2: workflow not running, child not started, parent close policy is not abandon (not started child at all)
		return nil
	}
	if !workflowRunning && !childStarted && abandonChild {
		// case 3: workflow not running, child not started, parent close policy is abandon

		if len(childInfo.RequestRunId) == 0 {
			// for backward-compatibility, if childInfo is created without RequestRunID
			// then we can't use runID to dedup start execution call, so skip starting the child
			return nil
		}

		initiatedEvent, err := mutableState.GetChildExecutionInitiatedEvent(ctx, task.InitiatedEventID)
		if err != nil {
			return err
		}

		startedRunID, childClock, err := t.startWorkflow(
			ctx,
			task,
			childInfo,
			initiatedEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes(),
		)
		switch err.(type) {
		case nil:
			// NOTE: during upgrade, if this call fails, it's possible that created child workflow runs and completes,
			// and another run reuses the workflowID. Then when this task retries, child workflow maybe created again.
			return t.createFirstWorkflowTask(ctx, task.TargetNamespaceID, childInfo, startedRunID, childClock)
		case *serviceerror.WorkflowExecutionAlreadyStarted,
			*serviceerror.NamespaceNotFound:
			// workflow already closed, don't record start failed
			t.logger.Info("Failed to start abandoned child workflow after parent close", tag.ErrorType(err), tag.Error(err))
			return nil
		default:
			return err
		}
	}

	if childStarted {
		// case 4, 5: workflow running, child started, parent close policy is or is not abandon
		// case 6: workflow closed, child started, parent close policy is abandon

		// NOTE: do not access anything related mutable state after this lock release
		// release the context lock since we no longer need mutable state and
		// the rest of logic is making RPC call, which takes time.
		release(nil)
		return t.createFirstWorkflowTask(ctx, task.TargetNamespaceID, childInfo, childInfo.StartedRunId, childInfo.Clock)
	}

	// remaining 2 cases:
	// case 7, 8: workflow running, child not started, parent close policy is or is not abandon
	initiatedEvent, err := mutableState.GetChildExecutionInitiatedEvent(ctx, task.InitiatedEventID)
	if err != nil {
		return err
	}
	attributes := initiatedEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes()

	startedRunID, childClock, err := t.startWorkflow(
		ctx,
		task,
		childInfo,
		attributes,
	)
	if err != nil {
		t.logger.Debug("Failed to start child workflow execution", tag.Error(err))
		if common.IsServiceTransientError(err) || common.IsContextDeadlineExceededErr(err) {
			// for retryable error just return
			return err
		}
		var failedCause enumspb.StartChildWorkflowExecutionFailedCause
		switch err.(type) {
		case *serviceerror.WorkflowExecutionAlreadyStarted:
			failedCause = enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS
		case *serviceerror.NamespaceNotFound:
			failedCause = enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND
		default:
			t.logger.Error("Unexpected error type for starting child workflow.", tag.ErrorType(err), tag.Error(err))
			return err
		}

		return t.recordStartChildExecutionFailed(
			ctx,
			task,
			weContext,
			attributes,
			failedCause,
		)
	}

	t.logger.Debug("Child Execution started successfully",
		tag.WorkflowID(attributes.WorkflowId), tag.WorkflowRunID(startedRunID))

	// Child execution is successfully started, record ChildExecutionStartedEvent in parent execution
	err = t.recordChildExecutionStarted(ctx, task, weContext, attributes, startedRunID, childClock)
	if err != nil {
		return err
	}

	// NOTE: do not access anything related mutable state after this lock is released.
	// Release the context lock since we no longer need mutable state and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	return t.createFirstWorkflowTask(ctx, task.TargetNamespaceID, childInfo, startedRunID, childClock)
}

func (t *transferQueueActiveTaskExecutor) processResetWorkflow(
	ctx context.Context,
	task *tasks.ResetWorkflowTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	currentContext, currentRelease, err := getWorkflowExecutionContextForTask(ctx, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { currentRelease(retError) }()

	currentMutableState, err := loadMutableStateForTransferTask(ctx, currentContext, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if currentMutableState == nil {
		return nil
	}

	logger := log.With(
		t.logger,
		tag.WorkflowNamespaceID(task.NamespaceID),
		tag.WorkflowID(task.WorkflowID),
		tag.WorkflowRunID(task.RunID),
	)

	if !currentMutableState.IsWorkflowExecutionRunning() {
		// it means this this might not be current anymore, we need to check
		var resp *persistence.GetCurrentExecutionResponse
		resp, err = t.shard.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
			ShardID:     t.shard.GetShardID(),
			NamespaceID: task.NamespaceID,
			WorkflowID:  task.WorkflowID,
		})
		if err != nil {
			return err
		}
		if resp.RunID != task.RunID {
			logger.Warn("Auto-Reset is skipped, because current run is stale.")
			return nil
		}
	}
	// TODO: current reset doesn't allow childWFs, in the future we will release this restriction
	if len(currentMutableState.GetPendingChildExecutionInfos()) > 0 {
		logger.Warn("Auto-Reset is skipped, because current run has pending child executions.")
		return nil
	}

	currentStartVersion, err := currentMutableState.GetStartVersion()
	if err != nil {
		return err
	}
	ok := VerifyTaskVersion(t.shard, t.logger, currentMutableState.GetNamespaceEntry(), currentStartVersion, task.Version, task)
	if !ok {
		return nil
	}

	executionInfo := currentMutableState.GetExecutionInfo()
	executionState := currentMutableState.GetExecutionState()
	namespaceEntry, err := t.registry.GetNamespaceByID(namespace.ID(executionInfo.NamespaceId))
	if err != nil {
		return err
	}
	logger = log.With(logger, tag.WorkflowNamespace(namespaceEntry.Name().String()))

	reason, resetPoint := workflow.FindAutoResetPoint(t.shard.GetTimeSource(), namespaceEntry.VerifyBinaryChecksum, executionInfo.AutoResetPoints)
	if resetPoint == nil {
		logger.Warn("Auto-Reset is skipped, because reset point is not found.")
		return nil
	}
	logger = log.With(
		logger,
		tag.WorkflowResetBaseRunID(resetPoint.GetRunId()),
		tag.WorkflowBinaryChecksum(resetPoint.GetBinaryChecksum()),
		tag.WorkflowEventID(resetPoint.GetFirstWorkflowTaskCompletedId()),
	)

	var baseContext workflow.Context
	var baseMutableState workflow.MutableState
	var baseRelease workflow.ReleaseCacheFunc
	if resetPoint.GetRunId() == executionState.RunId {
		baseContext = currentContext
		baseMutableState = currentMutableState
		baseRelease = currentRelease
	} else {
		baseContext, baseRelease, err = getWorkflowExecutionContext(
			ctx,
			t.cache,
			namespace.ID(task.NamespaceID),
			commonpb.WorkflowExecution{
				WorkflowId: task.WorkflowID,
				RunId:      resetPoint.GetRunId(),
			},
		)
		if err != nil {
			return err
		}
		defer func() { baseRelease(retError) }()
		baseMutableState, err = loadMutableStateForTransferTask(ctx, baseContext, task, t.metricsClient, t.logger)
		if err != nil {
			return err
		}
		if baseMutableState == nil {
			return nil
		}
	}

	// NOTE: reset need to go through history which may take a longer time,
	// so it's using its own timeout
	if err := t.resetWorkflow(
		task,
		reason,
		resetPoint,
		baseMutableState,
		currentContext,
		currentMutableState,
		logger,
	); err != nil {
		return err
	}
	return nil
}

func (t *transferQueueActiveTaskExecutor) recordChildExecutionStarted(
	ctx context.Context,
	task *tasks.StartChildExecutionTask,
	context workflow.Context,
	initiatedAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
	runID string,
	clock *clockspb.VectorClock,
) error {
	return t.updateWorkflowExecution(ctx, context, true,
		func(mutableState workflow.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return serviceerror.NewNotFound("Workflow execution already completed.")
			}

			ci, ok := mutableState.GetChildExecutionInfo(task.InitiatedEventID)
			if !ok || ci.StartedEventId != common.EmptyEventID {
				return serviceerror.NewNotFound("Pending child execution not found.")
			}

			_, err := mutableState.AddChildWorkflowExecutionStartedEvent(
				&commonpb.WorkflowExecution{
					WorkflowId: task.TargetWorkflowID,
					RunId:      runID,
				},
				initiatedAttributes.WorkflowType,
				task.InitiatedEventID,
				initiatedAttributes.Header,
				clock,
			)

			return err
		})
}

func (t *transferQueueActiveTaskExecutor) recordStartChildExecutionFailed(
	ctx context.Context,
	task *tasks.StartChildExecutionTask,
	context workflow.Context,
	initiatedAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
	failedCause enumspb.StartChildWorkflowExecutionFailedCause,
) error {
	return t.updateWorkflowExecution(ctx, context, true,
		func(mutableState workflow.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return serviceerror.NewNotFound("Workflow execution already completed.")
			}

			ci, ok := mutableState.GetChildExecutionInfo(task.InitiatedEventID)
			if !ok || ci.StartedEventId != common.EmptyEventID {
				return serviceerror.NewNotFound("Pending child execution not found.")
			}

			_, err := mutableState.AddStartChildWorkflowExecutionFailedEvent(
				task.InitiatedEventID,
				failedCause,
				initiatedAttributes,
			)
			return err
		})
}

// createFirstWorkflowTask is used by StartChildExecution transfer task to create the first workflow task for
// child execution.
func (t *transferQueueActiveTaskExecutor) createFirstWorkflowTask(
	ctx context.Context,
	namespaceID string,
	childInfo *persistencespb.ChildExecutionInfo,
	startedRunID string,
	clock *clockspb.VectorClock,
) error {
	if startedRunID == childInfo.RequestRunId {
		// create first workflow task can be skipped if
		// since we know both source and target shard are running new code path
		return nil
	}

	_, err := t.historyClient.ScheduleWorkflowTask(ctx, &historyservice.ScheduleWorkflowTaskRequest{
		NamespaceId: namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: childInfo.StartedWorkflowId,
			RunId:      startedRunID,
		},
		IsFirstWorkflowTask: true,
		Clock:               clock,
	})

	return err
}

func (t *transferQueueActiveTaskExecutor) requestCancelExternalExecutionCompleted(
	ctx context.Context,
	task *tasks.CancelExecutionTask,
	context workflow.Context,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	targetWorkflowID string,
	targetRunID string,
) error {
	err := t.updateWorkflowExecution(ctx, context, true,
		func(mutableState workflow.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return serviceerror.NewNotFound("Workflow execution already completed.")
			}

			_, ok := mutableState.GetRequestCancelInfo(task.InitiatedEventID)
			if !ok {
				return workflow.ErrMissingRequestCancelInfo
			}

			_, err := mutableState.AddExternalWorkflowExecutionCancelRequested(
				task.InitiatedEventID,
				targetNamespace,
				targetNamespaceID,
				targetWorkflowID,
				targetRunID,
			)
			return err
		})

	return err
}

func (t *transferQueueActiveTaskExecutor) signalExternalExecutionCompleted(
	ctx context.Context,
	task *tasks.SignalExecutionTask,
	context workflow.Context,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	targetWorkflowID string,
	targetRunID string,
	control string,
) error {
	err := t.updateWorkflowExecution(ctx, context, true,
		func(mutableState workflow.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return serviceerror.NewNotFound("Workflow execution already completed.")
			}

			_, ok := mutableState.GetSignalInfo(task.InitiatedEventID)
			if !ok {
				return workflow.ErrMissingSignalInfo
			}

			_, err := mutableState.AddExternalWorkflowExecutionSignaled(
				task.InitiatedEventID,
				targetNamespace,
				targetNamespaceID,
				targetWorkflowID,
				targetRunID,
				control,
			)
			return err
		})
	return err
}

func (t *transferQueueActiveTaskExecutor) requestCancelExternalExecutionFailed(
	ctx context.Context,
	task *tasks.CancelExecutionTask,
	context workflow.Context,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	targetWorkflowID string,
	targetRunID string,
	failedCause enumspb.CancelExternalWorkflowExecutionFailedCause,
) error {
	err := t.updateWorkflowExecution(ctx, context, true,
		func(mutableState workflow.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return serviceerror.NewNotFound("Workflow execution already completed.")
			}

			_, ok := mutableState.GetRequestCancelInfo(task.InitiatedEventID)
			if !ok {
				return workflow.ErrMissingRequestCancelInfo
			}

			_, err := mutableState.AddRequestCancelExternalWorkflowExecutionFailedEvent(
				task.InitiatedEventID,
				targetNamespace,
				targetNamespaceID,
				targetWorkflowID,
				targetRunID,
				failedCause,
			)
			return err
		})

	return err
}

func (t *transferQueueActiveTaskExecutor) signalExternalExecutionFailed(
	ctx context.Context,
	task *tasks.SignalExecutionTask,
	context workflow.Context,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	targetWorkflowID string,
	targetRunID string,
	control string,
	failedCause enumspb.SignalExternalWorkflowExecutionFailedCause,
) error {
	err := t.updateWorkflowExecution(ctx, context, true,
		func(mutableState workflow.MutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return serviceerror.NewNotFound("Workflow is not running.")
			}

			_, ok := mutableState.GetSignalInfo(task.InitiatedEventID)
			if !ok {
				return workflow.ErrMissingSignalInfo
			}

			_, err := mutableState.AddSignalExternalWorkflowExecutionFailedEvent(
				task.InitiatedEventID,
				targetNamespace,
				targetNamespaceID,
				targetWorkflowID,
				targetRunID,
				control,
				failedCause,
			)
			return err
		})

	return err
}

func (t *transferQueueActiveTaskExecutor) updateWorkflowExecution(
	ctx context.Context,
	context workflow.Context,
	createWorkflowTask bool,
	action func(workflow.MutableState) error,
) error {
	mutableState, err := context.LoadMutableState(ctx)
	if err != nil {
		return err
	}

	if err := action(mutableState); err != nil {
		return err
	}

	if createWorkflowTask {
		// Create a transfer task to schedule a workflow task
		err := workflow.ScheduleWorkflowTask(mutableState)
		if err != nil {
			return err
		}
	}

	return context.UpdateWorkflowExecutionAsActive(ctx, t.shard.GetTimeSource().Now())
}

func (t *transferQueueActiveTaskExecutor) requestCancelExternalExecution(
	ctx context.Context,
	task *tasks.CancelExecutionTask,
	targetNamespace namespace.Name,
	requestCancelInfo *persistencespb.RequestCancelInfo,
	attributes *historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes,
) error {
	request := &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: task.TargetNamespaceID,
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace: targetNamespace.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: task.TargetWorkflowID,
				RunId:      task.TargetRunID,
			},
			Identity: consts.IdentityHistoryService,
			// Use the same request ID to dedupe RequestCancelWorkflowExecution calls
			RequestId: requestCancelInfo.GetCancelRequestId(),
			Reason:    attributes.Reason,
		},
		ExternalInitiatedEventId: task.InitiatedEventID,
		ExternalWorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		ChildWorkflowOnly: task.TargetChildWorkflowOnly,
	}

	_, err := t.historyClient.RequestCancelWorkflowExecution(ctx, request)
	return err
}

func (t *transferQueueActiveTaskExecutor) signalExternalExecution(
	ctx context.Context,
	task *tasks.SignalExecutionTask,
	targetNamespace namespace.Name,
	signalInfo *persistencespb.SignalInfo,
	attributes *historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes,
) error {
	request := &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: task.TargetNamespaceID,
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace: targetNamespace.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: task.TargetWorkflowID,
				RunId:      task.TargetRunID,
			},
			Identity:   consts.IdentityHistoryService,
			SignalName: attributes.SignalName,
			Input:      attributes.Input,
			// Use same request ID to deduplicate SignalWorkflowExecution calls
			RequestId: signalInfo.GetRequestId(),
			Control:   attributes.Control,
			Header:    attributes.Header,
		},
		ExternalWorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		ChildWorkflowOnly: task.TargetChildWorkflowOnly,
	}

	_, err := t.historyClient.SignalWorkflowExecution(ctx, request)
	return err
}

func (t *transferQueueActiveTaskExecutor) startWorkflow(
	ctx context.Context,
	task *tasks.StartChildExecutionTask,
	childInfo *persistencespb.ChildExecutionInfo,
	attributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
) (string, *clockspb.VectorClock, error) {
	var parentNamespaceName string
	if namespaceEntry, err := t.registry.GetNamespaceByID(namespace.ID(task.NamespaceID)); err != nil {
		if _, isNotFound := err.(*serviceerror.NamespaceNotFound); !isNotFound {
			return "", nil, err
		}
		// It is possible that the parent namespace got deleted. Use namespaceID instead as this is only needed for the history event.
		parentNamespaceName = task.NamespaceID
	} else {
		parentNamespaceName = namespaceEntry.Name().String()
	}

	namespaceEntry, err := t.registry.GetNamespaceByID(namespace.ID(task.TargetNamespaceID))
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NamespaceNotFound); !isNotFound {
			// It is possible that target namespace got deleted
			t.logger.Debug("Target namespace is not found.", tag.WorkflowNamespaceID(task.TargetNamespaceID))
		}
		return "", nil, err

	}
	targetNamespaceName := namespaceEntry.Name().String()

	request := common.CreateHistoryStartWorkflowRequest(
		task.TargetNamespaceID,
		&workflowservice.StartWorkflowExecutionRequest{
			Namespace:                targetNamespaceName,
			WorkflowId:               attributes.WorkflowId,
			WorkflowType:             attributes.WorkflowType,
			TaskQueue:                attributes.TaskQueue,
			Input:                    attributes.Input,
			Header:                   attributes.Header,
			WorkflowExecutionTimeout: attributes.WorkflowExecutionTimeout,
			WorkflowRunTimeout:       attributes.WorkflowRunTimeout,
			WorkflowTaskTimeout:      attributes.WorkflowTaskTimeout,

			// Use the same request ID to dedupe StartWorkflowExecution calls
			RequestId:             childInfo.CreateRequestId,
			WorkflowIdReusePolicy: attributes.WorkflowIdReusePolicy,
			RetryPolicy:           attributes.RetryPolicy,
			CronSchedule:          attributes.CronSchedule,
			Memo:                  attributes.Memo,
			SearchAttributes:      attributes.SearchAttributes,
		},
		childInfo.RequestRunId,
		&workflowspb.ParentExecutionInfo{
			NamespaceId: task.NamespaceID,
			Namespace:   parentNamespaceName,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: task.WorkflowID,
				RunId:      task.RunID,
			},
			InitiatedId:      task.InitiatedEventID,
			InitiatedVersion: task.Version,
			Clock:            vclock.NewVectorClock(t.shard.GetClusterMetadata().GetClusterID(), t.shard.GetShardID(), task.TaskID),
		},
		t.shard.GetTimeSource().Now(),
	)

	response, err := t.historyClient.StartWorkflowExecution(ctx, request)
	if err != nil {
		return "", nil, err
	}
	return response.GetRunId(), response.GetClock(), nil
}

func (t *transferQueueActiveTaskExecutor) resetWorkflow(
	task *tasks.ResetWorkflowTask,
	reason string,
	resetPoint *workflowpb.ResetPointInfo,
	baseMutableState workflow.MutableState,
	currentContext workflow.Context,
	currentMutableState workflow.MutableState,
	logger log.Logger,
) error {
	var err error
	ctx, cancel := context.WithTimeout(context.Background(), taskHistoryOpTimeout)
	defer cancel()

	namespaceID := namespace.ID(task.NamespaceID)
	workflowID := task.WorkflowID
	baseRunID := baseMutableState.GetExecutionState().GetRunId()

	resetRunID := uuid.New()
	baseRebuildLastEventID := resetPoint.GetFirstWorkflowTaskCompletedId() - 1
	baseVersionHistories := baseMutableState.GetExecutionInfo().GetVersionHistories()
	baseCurrentVersionHistory, err := versionhistory.GetCurrentVersionHistory(baseVersionHistories)
	if err != nil {
		return err
	}
	baseRebuildLastEventVersion, err := versionhistory.GetVersionHistoryEventVersion(baseCurrentVersionHistory, baseRebuildLastEventID)
	if err != nil {
		return err
	}
	baseCurrentBranchToken := baseCurrentVersionHistory.GetBranchToken()
	baseNextEventID := baseMutableState.GetNextEventID()

	err = t.workflowResetter.resetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		baseRunID,
		baseCurrentBranchToken,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		baseNextEventID,
		resetRunID,
		uuid.New(),
		newNDCWorkflow(
			ctx,
			t.registry,
			t.shard.GetClusterMetadata(),
			currentContext,
			currentMutableState,
			workflow.NoopReleaseFn, // this is fine since caller will defer on release
		),
		reason,
		nil,
		enumspb.RESET_REAPPLY_TYPE_SIGNAL,
	)

	switch err.(type) {
	case nil:
		return nil

	case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
		// This means the reset point is corrupted and not retry able.
		// There must be a bug in our system that we must fix.(for example, history is not the same in active/passive)
		t.metricsClient.IncCounter(metrics.TransferQueueProcessorScope, metrics.AutoResetPointCorruptionCounter)
		logger.Error("Auto-Reset workflow failed and not retryable. The reset point is corrupted.", tag.Error(err))
		return nil

	default:
		// log this error and retry
		logger.Error("Auto-Reset workflow failed", tag.Error(err))
		return err
	}
}

func (t *transferQueueActiveTaskExecutor) processParentClosePolicy(
	ctx context.Context,
	parentNamespaceName string,
	parentExecution commonpb.WorkflowExecution,
	childInfos map[int64]*persistencespb.ChildExecutionInfo,
) error {
	if len(childInfos) == 0 {
		return nil
	}

	executions := make([]parentclosepolicy.RequestDetail, 0, len(childInfos))
	for _, childInfo := range childInfos {
		if childInfo.ParentClosePolicy == enumspb.PARENT_CLOSE_POLICY_ABANDON {
			continue
		}

		childNamespaceID := namespace.ID(childInfo.GetNamespaceId())
		if childNamespaceID.IsEmpty() {
			// TODO (alex): Remove after childInfo.NamespaceId is back filled. Backward compatibility: old childInfo doesn't have NamespaceId set.
			// TODO (alex): consider reverse lookup of namespace name from ID but namespace name is not actually used.
			var err error
			childNamespaceID, err = t.registry.GetNamespaceID(namespace.Name(childInfo.GetNamespace()))
			switch err.(type) {
			case nil:
			case *serviceerror.NamespaceNotFound:
				// If child namespace is deleted there is nothing to close.
				continue
			default:
				return err
			}
		}

		childRunID := childInfo.StartedRunId
		// NOTE: child may be started but start not recorded even if parent close policy is not abandon.
		// This is because the start child call may fail, and before retrying the task,
		// parent workflow can get closed. But the start child call actually succeeds.
		// NOTE: this also means there's a chance that the start child call can succeeds after we try to
		// apply parent close policy, leaving child workflow not terminated/cancelled even after parent
		// workflow is closed. But chance is very small and unlike to happen, so we're not handing it here.

		// TODO: uncomment the following code in 1.20
		// Ideally when StartedRunID is empty, we should first fallback to RequestRunID.
		// However, we have no idea if target/child shard will respect RequestRunID or not
		// so we can't not use RequestRunID to terminate or cancel
		//
		// if childRunID == "" {
		// 	childRunID = childInfo.RequestRunId
		// }

		// NOTE: childRunID can still be empty here, in which case we simply apply parent close policy
		// to the current run of the child workflowID and rely on the ChildWorkflowOnly flag to do the
		// right thing
		executions = append(executions, parentclosepolicy.RequestDetail{
			Namespace:   childInfo.Namespace,
			NamespaceID: childNamespaceID.String(),
			WorkflowID:  childInfo.StartedWorkflowId,
			RunID:       childRunID,
			Policy:      childInfo.ParentClosePolicy,
		})
	}
	if len(executions) == 0 {
		return nil
	}

	if t.shard.GetConfig().EnableParentClosePolicyWorker() &&
		len(executions) >= t.shard.GetConfig().ParentClosePolicyThreshold(parentNamespaceName) {
		return t.parentClosePolicyClient.SendParentClosePolicyRequest(ctx, parentclosepolicy.Request{
			ParentExecution: parentExecution,
			Executions:      executions,
		})
	}

	scope := t.metricsClient.Scope(metrics.TransferActiveTaskCloseExecutionScope)
	for _, childExecution := range executions {
		err := t.applyParentClosePolicy(ctx, &parentExecution, &childExecution)
		switch err.(type) {
		case nil:
			scope.IncCounter(metrics.ParentClosePolicyProcessorSuccess)
		case *serviceerror.NotFound:
			// If child execution is deleted there is nothing to close.
		case *serviceerror.NamespaceNotFound:
			// If child namespace is deleted there is nothing to close.
		default:
			scope.IncCounter(metrics.ParentClosePolicyProcessorFailures)
			return err
		}
	}
	return nil
}

func (t *transferQueueActiveTaskExecutor) applyParentClosePolicy(
	ctx context.Context,
	parentExecution *commonpb.WorkflowExecution,
	childExecution *parentclosepolicy.RequestDetail,
) error {
	switch childExecution.Policy {
	case enumspb.PARENT_CLOSE_POLICY_ABANDON:
		// noop
		return nil

	case enumspb.PARENT_CLOSE_POLICY_TERMINATE:
		_, err := t.historyClient.TerminateWorkflowExecution(ctx, &historyservice.TerminateWorkflowExecutionRequest{
			NamespaceId: childExecution.NamespaceID,
			TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{
				Namespace: childExecution.Namespace,
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: childExecution.WorkflowID,
				},
				// Include runID as FirstExecutionRunID on the request to allow child to be terminated across runs.
				// If the child does continue as new it still propagates the RunID of first execution.
				// use childInfo.CreateRunID is started runID is empty as child may be started by not recorded
				// even if parent close policy is not abandon.
				FirstExecutionRunId: childExecution.RunID,
				Reason:              "by parent close policy",
				Identity:            consts.IdentityHistoryService,
			},
			ExternalWorkflowExecution: parentExecution,
			ChildWorkflowOnly:         true,
		})
		return err

	case enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL:
		_, err := t.historyClient.RequestCancelWorkflowExecution(ctx, &historyservice.RequestCancelWorkflowExecutionRequest{
			NamespaceId: childExecution.NamespaceID,
			CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
				Namespace: childExecution.Namespace,
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: childExecution.WorkflowID,
				},
				// Include StartedRunID as FirstExecutionRunID on the request to allow child to be canceled across runs.
				// If the child does continue as new it still propagates the RunID of first execution.
				// use childInfo.CreateRunID is started runID is empty as child may be started by not recorded
				// even if parent close policy is not abandon.
				FirstExecutionRunId: childExecution.RunID,
				Identity:            consts.IdentityHistoryService,
			},
			ExternalWorkflowExecution: parentExecution,
			ChildWorkflowOnly:         true,
		})
		return err

	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown parent close policy: %v", childExecution.Policy))
	}
}

func copyChildWorkflowInfos(
	input map[int64]*persistencespb.ChildExecutionInfo,
) map[int64]*persistencespb.ChildExecutionInfo {
	result := make(map[int64]*persistencespb.ChildExecutionInfo)
	if input == nil {
		return result
	}

	for k, v := range input {
		result[k] = common.CloneProto(v)
	}
	return result
}
