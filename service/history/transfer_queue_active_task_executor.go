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

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/sdk"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/worker/parentclosepolicy"
)

type (
	transferQueueActiveTaskExecutor struct {
		*transferQueueTaskExecutorBase

		workflowResetter        ndc.WorkflowResetter
		parentClosePolicyClient parentclosepolicy.Client
	}
)

func newTransferQueueActiveTaskExecutor(
	shard shard.Context,
	workflowCache wcache.Cache,
	sdkClientFactory sdk.ClientFactory,
	logger log.Logger,
	metricProvider metrics.Handler,
	config *configs.Config,
	historyRawClient resource.HistoryRawClient,
	matchingRawClient resource.MatchingRawClient,
	visibilityManager manager.VisibilityManager,
) queues.Executor {
	return &transferQueueActiveTaskExecutor{
		transferQueueTaskExecutorBase: newTransferQueueTaskExecutorBase(
			shard,
			workflowCache,
			logger,
			metricProvider,
			historyRawClient,
			matchingRawClient,
			visibilityManager,
		),
		workflowResetter: ndc.NewWorkflowResetter(
			shard,
			workflowCache,
			logger,
		),
		parentClosePolicyClient: parentclosepolicy.NewClient(
			shard.GetMetricsHandler(),
			shard.GetLogger(),
			sdkClientFactory,
			config.NumParentClosePolicySystemWorkflows(),
		),
	}
}

func (t *transferQueueActiveTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) queues.ExecuteResponse {
	task := executable.GetTask()
	taskType := queues.GetActiveTransferTaskTypeTagValue(task)
	namespaceTag, replicationState := getNamespaceTagAndReplicationStateByID(
		t.shardContext.GetNamespaceRegistry(),
		task.GetNamespaceID(),
	)
	metricsTags := []metrics.Tag{
		namespaceTag,
		metrics.TaskTypeTag(taskType),
		metrics.OperationTag(taskType), // for backward compatibility
	}

	if replicationState == enumspb.REPLICATION_STATE_HANDOVER {
		// TODO: exclude task types here if we believe it's safe & necessary to execute
		// them during namespace handover.
		// TODO: move this logic to queues.Executable when metrics tag doesn't need to
		// be returned from task executor
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutedAsActive:    true,
			ExecutionErr:        consts.ErrNamespaceHandover,
		}
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

	return queues.ExecuteResponse{
		ExecutionMetricTags: metricsTags,
		ExecutedAsActive:    true,
		ExecutionErr:        err,
	}
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

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, task, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrWorkflowExecutionNotFound
	}

	ai, ok := mutableState.GetActivityInfo(task.ScheduledEventID)
	if !ok {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrActivityTaskNotFound
	}

	err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), ai.Version, task.Version, task)
	if err != nil {
		return err
	}

	if !mutableState.IsWorkflowExecutionRunning() {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrWorkflowCompleted
	}

	timeout := timestamp.DurationValue(ai.ScheduleToStartTimeout)
	directive := MakeDirectiveForActivityTask(mutableState, ai)

	// NOTE: do not access anything related mutable state after this lock release
	// release the context lock since we no longer need mutable state and
	// the rest of logic is making RPC call, which takes time.
	release(nil)

	return t.pushActivity(ctx, task, timeout, directive, workflow.TransactionPolicyActive)
}

func (t *transferQueueActiveTaskExecutor) processWorkflowTask(
	ctx context.Context,
	transferTask *tasks.WorkflowTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, transferTask)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, transferTask, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	workflowTask := mutableState.GetWorkflowTaskByID(transferTask.ScheduledEventID)
	if workflowTask == nil {
		return nil
	}
	err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), workflowTask.Version, transferTask.Version, transferTask)
	if err != nil {
		return err
	}

	// Task queue from transfer task (not current one from mutable state) must be used here.
	// If current task queue becomes sticky since this transfer task was created,
	// it can't be used here, because timeout timer was not created for it,
	// because it used to be non-sticky when this transfer task was created .
	taskQueue, scheduleToStartTimeout := mutableState.TaskQueueScheduleToStartTimeout(transferTask.TaskQueue)

	normalTaskQueueName := mutableState.GetExecutionInfo().TaskQueue

	directive := MakeDirectiveForWorkflowTask(mutableState)

	// NOTE: Do not access mutableState after this lock is released.
	// It is important to release the workflow lock here, because pushWorkflowTask will call matching,
	// which will call history back (with RecordWorkflowTaskStarted), and it will try to get workflow lock again.
	release(nil)

	err = t.pushWorkflowTask(
		ctx,
		transferTask,
		taskQueue,
		scheduleToStartTimeout.AsDuration(),
		directive,
		workflow.TransactionPolicyActive,
	)

	if _, ok := err.(*serviceerrors.StickyWorkerUnavailable); ok {
		// sticky worker is unavailable, switch to original normal task queue
		taskQueue = &taskqueuepb.TaskQueue{
			// do not use task.TaskQueue which is sticky, use original normal task queue from mutable state
			Name: normalTaskQueueName,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}

		// Continue to use sticky schedule_to_start timeout as TTL for the matching task. Because the schedule_to_start
		// timeout timer task is already created which will timeout this task if no worker pick it up in 5s anyway.
		// There is no need to reset sticky, because if this task is picked by new worker, the new worker will reset
		// the sticky queue to a new one. However, if worker is completely down, that schedule_to_start timeout task
		// will re-create a new non-sticky task and reset sticky.
		err = t.pushWorkflowTask(
			ctx,
			transferTask,
			taskQueue,
			scheduleToStartTimeout.AsDuration(),
			directive,
			workflow.TransactionPolicyActive,
		)
	}

	return err
}

func (t *transferQueueActiveTaskExecutor) processCloseExecution(
	ctx context.Context,
	task *tasks.CloseExecutionTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, task, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	// DeleteAfterClose is set to true when this close execution task was generated as part of delete open workflow execution procedure.
	// Delete workflow execution is started by user API call and should be done regardless of current workflow version.
	if !task.DeleteAfterClose {
		closeVersion, err := mutableState.GetCloseVersion()
		if err != nil {
			return err
		}
		err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), closeVersion, task.Version, task)
		if err != nil {
			return err
		}
	}

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowID(),
		RunId:      task.GetRunID(),
	}
	executionInfo := mutableState.GetExecutionInfo()
	var completionEvent *historypb.HistoryEvent // needed to report close event to parent workflow
	replyToParentWorkflow := mutableState.HasParentExecution() && executionInfo.NewExecutionRunId == ""
	if replyToParentWorkflow {
		// only load close event if needed.
		completionEvent, err = mutableState.GetCompletionEvent(ctx)
		if err != nil {
			return err
		}
		replyToParentWorkflow = replyToParentWorkflow && !ndc.IsTerminatedByResetter(completionEvent)
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

	namespaceName := mutableState.GetNamespaceEntry().Name()
	children := copyChildWorkflowInfos(mutableState.GetPendingChildExecutionInfos())

	// NOTE: do not access anything related mutable state after this lock release.
	// Release lock immediately since mutable state is not needed
	// and the rest of logic is RPC calls, which can take time.
	release(nil)

	// Communicate the result to parent execution if this is Child Workflow execution
	if replyToParentWorkflow {
		_, err := t.historyRawClient.RecordChildExecutionCompleted(ctx, &historyservice.RecordChildExecutionCompletedRequest{
			NamespaceId: parentNamespaceID,
			ParentExecution: &commonpb.WorkflowExecution{
				WorkflowId: parentWorkflowID,
				RunId:      parentRunID,
			},
			ParentInitiatedId:      parentInitiatedID,
			ParentInitiatedVersion: parentInitiatedVersion,
			ChildExecution:         &workflowExecution,
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
		&workflowExecution,
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
			// still open there.
			true,
			false,
			&task.DeleteProcessStage,
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

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, task, t.metricHandler, t.logger)
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
	err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), requestCancelInfo.Version, task.Version, task)
	if err != nil {
		return err
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
			t.logger.Error("Unexpected error type returned from RequestCancelWorkflowExecution API call.", tag.ServiceErrorType(err), tag.Error(err))
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

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, task, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrWorkflowExecutionNotFound
	}

	signalInfo, ok := mutableState.GetSignalInfo(task.InitiatedEventID)
	if !ok {
		// TODO: here we should also RemoveSignalMutableState from target workflow
		// Otherwise, target SignalRequestID still can leak if shard restart after signalExternalExecutionCompleted
		// To do that, probably need to add the SignalRequestID in transfer task.
		return nil
	}
	err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), signalInfo.Version, task.Version, task)
	if err != nil {
		return err
	}

	if !mutableState.IsWorkflowExecutionRunning() {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrWorkflowCompleted
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
		case *serviceerror.InvalidArgument:
			failedCause = enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_SIGNAL_COUNT_LIMIT_EXCEEDED
		default:
			t.logger.Error("Unexpected error type returned from SignalWorkflowExecution API call.", tag.ServiceErrorType(err), tag.Error(err))
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
	_, err = t.historyRawClient.RemoveSignalMutableState(ctx, &historyservice.RemoveSignalMutableStateRequest{
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

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, weContext, task, t.metricHandler, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrWorkflowExecutionNotFound
	}

	childInfo, ok := mutableState.GetChildExecutionInfo(task.InitiatedEventID)
	if !ok {
		release(nil) // release(nil) so that the mutable state is not unloaded from cache
		return consts.ErrChildExecutionNotFound
	}
	err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), childInfo.Version, task.Version, task)
	if err != nil {
		return err
	}

	// workflow running or not, child started or not, parent close policy is abandon or not
	// 8 cases in total
	workflowRunning := mutableState.IsWorkflowExecutionRunning()
	childStarted := childInfo.StartedEventId != common.EmptyEventID
	if !workflowRunning && (!childStarted || childInfo.ParentClosePolicy != enumspb.PARENT_CLOSE_POLICY_ABANDON) {
		// three cases here:
		// case 1: workflow not running, child started, parent close policy is not abandon
		// case 2: workflow not running, child not started, parent close policy is not abandon
		// case 3: workflow not running, child not started, parent close policy is abandon
		//
		// NOTE: ideally for case 3, we should continue to start child. However, with current start child
		// and standby start child verification logic, we can't do that because:
		// 1. Once workflow is closed, we can't update mutable state or record child started event.
		// If the RPC call for scheduling first workflow task times out but the call actually succeeds on child workflow.
		// Then the child workflow can run, complete and another unrelated workflow can reuse this workflowID.
		// Now when the start child task retries, we can't rely on requestID to dedupe the start child call. (We can use runID instead of requestID to dedupe)
		// 2. No update to mutable state and child started event means we are not able to replicate the information
		// to the standby cluster, so standby start child logic won't be able to verify the child has started.
		// To resolve the issue above, we need to
		// 1. Start child workflow and schedule the first workflow task in one transaction. Use runID to perform deduplication
		// 2. Standby start child logic need to verify if child workflow actually started instead of relying on the information
		// in parent mutable state.
		return nil
	}

	// ChildExecution already started, just create WorkflowTask and complete transfer task
	// If parent already closed, since child workflow started event already written to history,
	// still schedule the workflowTask if the parent close policy is Abandon.
	// If parent close policy cancel or terminate, parent close policy will be applied in another
	// transfer task.
	// case 4, 5: workflow started, child started, parent close policy is or is not abandon
	// case 6: workflow closed, child started, parent close policy is abandon
	if childStarted {
		childExecution := &commonpb.WorkflowExecution{
			WorkflowId: childInfo.StartedWorkflowId,
			RunId:      childInfo.StartedRunId,
		}
		childClock := childInfo.Clock
		// NOTE: do not access anything related mutable state after this lock release
		// release the context lock since we no longer need mutable state and
		// the rest of logic is making RPC call, which takes time.
		release(nil)

		parentClock, err := t.shardContext.NewVectorClock()
		if err != nil {
			return err
		}
		return t.createFirstWorkflowTask(ctx, task.TargetNamespaceID, childExecution, parentClock, childClock)
	}

	// remaining 2 cases:
	// case 7, 8: workflow running, child not started, parent close policy is or is not abandon

	initiatedEvent, err := mutableState.GetChildExecutionInitiatedEvent(ctx, task.InitiatedEventID)
	if err != nil {
		return err
	}
	attributes := initiatedEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes()

	var parentNamespaceName namespace.Name
	if namespaceEntry, err := t.registry.GetNamespaceByID(namespace.ID(task.NamespaceID)); err != nil {
		if _, isNotFound := err.(*serviceerror.NamespaceNotFound); !isNotFound {
			return err
		}
		// It is possible that the parent namespace got deleted. Use namespaceID instead as this is only needed for the history event.
		parentNamespaceName = namespace.Name(task.NamespaceID)
	} else {
		parentNamespaceName = namespaceEntry.Name()
	}

	var targetNamespaceName namespace.Name
	if namespaceEntry, err := t.registry.GetNamespaceByID(namespace.ID(task.TargetNamespaceID)); err != nil {
		if _, isNotFound := err.(*serviceerror.NamespaceNotFound); !isNotFound {
			return err
		}
		// It is possible that target namespace got deleted. Record failure.
		t.logger.Debug("Target namespace is not found.", tag.WorkflowNamespaceID(task.TargetNamespaceID))
		err = t.recordStartChildExecutionFailed(
			ctx,
			task,
			weContext,
			attributes,
			enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_NAMESPACE_NOT_FOUND,
		)
		return err
	} else {
		targetNamespaceName = namespaceEntry.Name()
	}

	var sourceVersionStamp *commonpb.WorkerVersionStamp
	var inheritedBuildId string
	if attributes.InheritBuildId {
		// setting inheritedBuildId of the child wf to the assignedBuildId of the parent
		inheritedBuildId = mutableState.GetAssignedBuildId()
		if inheritedBuildId == "" {
			// TODO: this is only needed for old versioning. get rid of StartWorkflowExecutionRequest.SourceVersionStamp
			// [cleanup-old-wv]
			// Copy version stamp to new workflow only if:
			// - command says to use compatible version
			// - using versioning
			sourceVersionStamp = worker_versioning.StampIfUsingVersioning(mutableState.GetMostRecentWorkerVersionStamp())
		}
	}

	executionInfo := mutableState.GetExecutionInfo()
	rootExecutionInfo := &workflowspb.RootExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: executionInfo.RootWorkflowId,
			RunId:      executionInfo.RootRunId,
		},
	}

	childRunID, childClock, err := t.startWorkflow(
		ctx,
		task,
		parentNamespaceName,
		targetNamespaceName,
		childInfo.CreateRequestId,
		attributes,
		sourceVersionStamp,
		rootExecutionInfo,
		inheritedBuildId,
		initiatedEvent.GetUserMetadata(),
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
			t.logger.Error("Unexpected error type returned from StartWorkflowExecution API call for child workflow.", tag.ServiceErrorType(err), tag.Error(err))
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
		tag.WorkflowID(attributes.WorkflowId), tag.WorkflowRunID(childRunID))

	// Child execution is successfully started, record ChildExecutionStartedEvent in parent execution
	err = t.recordChildExecutionStarted(ctx, task, weContext, attributes, childRunID, childClock)
	if err != nil {
		return err
	}

	// NOTE: do not access anything related mutable state after this lock is released.
	// Release the context lock since we no longer need mutable state and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	parentClock, err := t.shardContext.NewVectorClock()
	if err != nil {
		return err
	}
	return t.createFirstWorkflowTask(ctx, task.TargetNamespaceID, &commonpb.WorkflowExecution{
		WorkflowId: task.TargetWorkflowID,
		RunId:      childRunID,
	}, parentClock, childClock)
}

func (t *transferQueueActiveTaskExecutor) processResetWorkflow(
	ctx context.Context,
	task *tasks.ResetWorkflowTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	currentContext, currentRelease, err := getWorkflowExecutionContextForTask(ctx, t.shardContext, t.cache, task)
	if err != nil {
		return err
	}
	defer func() { currentRelease(retError) }()

	currentMutableState, err := loadMutableStateForTransferTask(ctx, t.shardContext, currentContext, task, t.metricHandler, t.logger)
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
		resp, err = t.shardContext.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
			ShardID:     t.shardContext.GetShardID(),
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

	// TODO: why we are comparing task version to workflow start version here?
	// GenerateWorkflowResetTasks uses currentVersion
	currentStartVersion, err := currentMutableState.GetStartVersion()
	if err != nil {
		return err
	}

	err = CheckTaskVersion(t.shardContext, t.logger, currentMutableState.GetNamespaceEntry(), currentStartVersion, task.Version, task)
	if err != nil {
		return err
	}

	executionInfo := currentMutableState.GetExecutionInfo()
	executionState := currentMutableState.GetExecutionState()
	namespaceEntry, err := t.registry.GetNamespaceByID(namespace.ID(executionInfo.NamespaceId))
	if err != nil {
		return err
	}
	logger = log.With(logger, tag.WorkflowNamespace(namespaceEntry.Name().String()))

	reason, resetPoint := workflow.FindAutoResetPoint(t.shardContext.GetTimeSource(), namespaceEntry.VerifyBinaryChecksum, executionInfo.AutoResetPoints)
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
	var baseRelease wcache.ReleaseCacheFunc
	if resetPoint.GetRunId() == executionState.RunId {
		baseContext = currentContext
		baseMutableState = currentMutableState
		baseRelease = currentRelease
	} else {
		baseContext, baseRelease, err = getWorkflowExecutionContext(
			ctx,
			t.shardContext,
			t.cache,
			definition.NewWorkflowKey(task.NamespaceID, task.WorkflowID, resetPoint.GetRunId()),
			locks.PriorityLow,
		)
		if err != nil {
			return err
		}
		defer func() { baseRelease(retError) }()
		baseMutableState, err = loadMutableStateForTransferTask(ctx, t.shardContext, baseContext, task, t.metricHandler, t.logger)
		if err != nil {
			return err
		}
		if baseMutableState == nil {
			return nil
		}
	}

	// NOTE: reset need to go through history which may take a longer time,
	// so it's using its own timeout
	return t.resetWorkflow(
		ctx,
		task,
		reason,
		resetPoint,
		baseContext,
		baseMutableState,
		currentContext,
		currentMutableState,
		logger,
	)
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
	execution *commonpb.WorkflowExecution,
	parentClock *clockspb.VectorClock,
	childClock *clockspb.VectorClock,
) error {
	_, err := t.historyRawClient.ScheduleWorkflowTask(ctx, &historyservice.ScheduleWorkflowTaskRequest{
		NamespaceId:         namespaceID,
		WorkflowExecution:   execution,
		IsFirstWorkflowTask: true,
		ParentClock:         parentClock,
		ChildClock:          childClock,
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
	return t.updateWorkflowExecution(ctx, context, true,
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
	return t.updateWorkflowExecution(ctx, context, true,
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
	return t.updateWorkflowExecution(ctx, context, true,
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
	return t.updateWorkflowExecution(ctx, context, true,
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
}

func (t *transferQueueActiveTaskExecutor) updateWorkflowExecution(
	ctx context.Context,
	context workflow.Context,
	createWorkflowTask bool,
	action func(workflow.MutableState) error,
) error {
	mutableState, err := context.LoadMutableState(ctx, t.shardContext)
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

	return context.UpdateWorkflowExecutionAsActive(ctx, t.shardContext)
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

	_, err := t.historyRawClient.RequestCancelWorkflowExecution(ctx, request)
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

	_, err := t.historyRawClient.SignalWorkflowExecution(ctx, request)
	return err
}

func (t *transferQueueActiveTaskExecutor) startWorkflow(
	ctx context.Context,
	task *tasks.StartChildExecutionTask,
	namespace namespace.Name,
	targetNamespace namespace.Name,
	childRequestID string,
	attributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
	sourceVersionStamp *commonpb.WorkerVersionStamp,
	rootExecutionInfo *workflowspb.RootExecutionInfo,
	inheritedBuildId string,
	userMetadata *sdkpb.UserMetadata,
) (string, *clockspb.VectorClock, error) {
	request := common.CreateHistoryStartWorkflowRequest(
		task.TargetNamespaceID,
		&workflowservice.StartWorkflowExecutionRequest{
			Namespace:                targetNamespace.String(),
			WorkflowId:               attributes.WorkflowId,
			WorkflowType:             attributes.WorkflowType,
			TaskQueue:                attributes.TaskQueue,
			Input:                    attributes.Input,
			Header:                   attributes.Header,
			WorkflowExecutionTimeout: attributes.WorkflowExecutionTimeout,
			WorkflowRunTimeout:       attributes.WorkflowRunTimeout,
			WorkflowTaskTimeout:      attributes.WorkflowTaskTimeout,

			// Use the same request ID to dedupe StartWorkflowExecution calls
			RequestId:             childRequestID,
			WorkflowIdReusePolicy: attributes.WorkflowIdReusePolicy,
			RetryPolicy:           attributes.RetryPolicy,
			CronSchedule:          attributes.CronSchedule,
			Memo:                  attributes.Memo,
			SearchAttributes:      attributes.SearchAttributes,
			UserMetadata:          userMetadata,
		},
		&workflowspb.ParentExecutionInfo{
			NamespaceId: task.NamespaceID,
			Namespace:   namespace.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: task.WorkflowID,
				RunId:      task.RunID,
			},
			InitiatedId:      task.InitiatedEventID,
			InitiatedVersion: task.Version,
			Clock:            vclock.NewVectorClock(t.shardContext.GetClusterMetadata().GetClusterID(), t.shardContext.GetShardID(), task.TaskID),
		},
		rootExecutionInfo,
		t.shardContext.GetTimeSource().Now(),
	)

	request.SourceVersionStamp = sourceVersionStamp
	request.InheritedBuildId = inheritedBuildId

	response, err := t.historyRawClient.StartWorkflowExecution(ctx, request)
	if err != nil {
		return "", nil, err
	}
	return response.GetRunId(), response.GetClock(), nil
}

func (t *transferQueueActiveTaskExecutor) resetWorkflow(
	ctx context.Context,
	task *tasks.ResetWorkflowTask,
	reason string,
	resetPoint *workflowpb.ResetPointInfo,
	baseContext workflow.Context,
	baseMutableState workflow.MutableState,
	currentContext workflow.Context,
	currentMutableState workflow.MutableState,
	logger log.Logger,
) error {
	// the actual reset operation needs to read history and may not be able to completed within
	// the original context timeout.
	// create a new context with a longer timeout, but retain all existing context values.
	resetWorkflowCtx, cancel := rpc.ResetContextTimeout(ctx, taskHistoryOpTimeout)
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

	baseWorkflow := ndc.NewWorkflow(
		t.shardContext.GetClusterMetadata(),
		baseContext,
		baseMutableState,
		wcache.NoopReleaseFn, // this is fine since caller will defer on release
	)
	err = t.workflowResetter.ResetWorkflow(
		resetWorkflowCtx,
		namespaceID,
		workflowID,
		baseRunID,
		baseCurrentBranchToken,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		baseNextEventID,
		resetRunID,
		uuid.New(),
		baseWorkflow,
		ndc.NewWorkflow(
			t.shardContext.GetClusterMetadata(),
			currentContext,
			currentMutableState,
			wcache.NoopReleaseFn, // this is fine since caller will defer on release
		),
		reason,
		nil,
		nil,
	)

	switch err.(type) {
	case nil:
		return nil

	case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
		// This means the reset point is corrupted and not retry able.
		// There must be a bug in our system that we must fix.(for example, history is not the same in active/passive)
		metrics.AutoResetPointCorruptionCounter.With(t.metricHandler).Record(
			1,
			metrics.OperationTag(metrics.OperationTransferQueueProcessorScope),
		)
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
	parentExecution *commonpb.WorkflowExecution,
	childInfos map[int64]*persistencespb.ChildExecutionInfo,
) error {
	if len(childInfos) == 0 {
		return nil
	}

	scope := t.metricHandler.WithTags(metrics.OperationTag(metrics.TransferActiveTaskCloseExecutionScope))

	if t.shardContext.GetConfig().EnableParentClosePolicyWorker() &&
		len(childInfos) >= t.shardContext.GetConfig().ParentClosePolicyThreshold(parentNamespaceName) {

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

			executions = append(executions, parentclosepolicy.RequestDetail{
				Namespace:   childInfo.Namespace,
				NamespaceID: childNamespaceID.String(),
				WorkflowID:  childInfo.StartedWorkflowId,
				RunID:       childInfo.StartedRunId,
				Policy:      childInfo.ParentClosePolicy,
			})
		}

		if len(executions) == 0 {
			return nil
		}

		request := parentclosepolicy.Request{
			ParentExecution: parentExecution,
			Executions:      executions,
		}
		return t.parentClosePolicyClient.SendParentClosePolicyRequest(ctx, request)
	}

	for _, childInfo := range childInfos {
		err := t.applyParentClosePolicy(ctx, parentExecution, childInfo)
		switch err.(type) {
		case nil:
			metrics.ParentClosePolicyProcessorSuccess.With(scope).Record(1)
		case *serviceerror.NotFound:
			// If child execution is deleted there is nothing to close.
		case *serviceerror.NamespaceNotFound:
			// If child namespace is deleted there is nothing to close.
		default:
			metrics.ParentClosePolicyProcessorFailures.With(scope).Record(1)
			return err
		}
	}
	return nil
}

func (t *transferQueueActiveTaskExecutor) applyParentClosePolicy(
	ctx context.Context,
	parentExecution *commonpb.WorkflowExecution,
	childInfo *persistencespb.ChildExecutionInfo,
) error {
	switch childInfo.ParentClosePolicy {
	case enumspb.PARENT_CLOSE_POLICY_ABANDON:
		// noop
		return nil

	case enumspb.PARENT_CLOSE_POLICY_TERMINATE:
		childNamespaceID := namespace.ID(childInfo.GetNamespaceId())
		if childNamespaceID.IsEmpty() {
			// TODO (alex): Remove after childInfo.NamespaceId is back filled. Backward compatibility: old childInfo doesn't have NamespaceId set.
			// TODO (alex): consider reverse lookup of namespace name from ID but namespace name is not actually used.
			var err error
			childNamespaceID, err = t.registry.GetNamespaceID(namespace.Name(childInfo.GetNamespace()))
			if err != nil {
				return err
			}
		}
		_, err := t.historyRawClient.TerminateWorkflowExecution(ctx, &historyservice.TerminateWorkflowExecutionRequest{
			NamespaceId: childNamespaceID.String(),
			TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{
				Namespace: childInfo.GetNamespace(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: childInfo.GetStartedWorkflowId(),
				},
				// Include StartedRunID as FirstExecutionRunID on the request to allow child to be terminated across runs.
				// If the child does continue as new it still propagates the RunID of first execution.
				FirstExecutionRunId: childInfo.GetStartedRunId(),
				Reason:              "by parent close policy",
				Identity:            consts.IdentityHistoryService,
			},
			ExternalWorkflowExecution: parentExecution,
			ChildWorkflowOnly:         true,
		})
		return err

	case enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL:
		childNamespaceID := namespace.ID(childInfo.GetNamespaceId())
		if childNamespaceID.IsEmpty() {
			// TODO (alex): Remove after childInfo.NamespaceId is back filled. Backward compatibility: old childInfo doesn't have NamespaceId set.
			// TODO (alex): consider reverse lookup of namespace name from ID but namespace name is not actually used.
			var err error
			childNamespaceID, err = t.registry.GetNamespaceID(namespace.Name(childInfo.GetNamespace()))
			if err != nil {
				return err
			}
		}

		_, err := t.historyRawClient.RequestCancelWorkflowExecution(ctx, &historyservice.RequestCancelWorkflowExecutionRequest{
			NamespaceId: childNamespaceID.String(),
			CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
				Namespace: childInfo.GetNamespace(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: childInfo.GetStartedWorkflowId(),
				},
				// Include StartedRunID as FirstExecutionRunID on the request to allow child to be canceled across runs.
				// If the child does continue as new it still propagates the RunID of first execution.
				FirstExecutionRunId: childInfo.GetStartedRunId(),
				Identity:            consts.IdentityHistoryService,
			},
			ExternalWorkflowExecution: parentExecution,
			ChildWorkflowOnly:         true,
		})
		return err

	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown parent close policy: %v", childInfo.ParentClosePolicy))
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
