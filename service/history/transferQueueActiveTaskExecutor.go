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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/worker/parentclosepolicy"
)

type (
	transferQueueActiveTaskExecutor struct {
		*transferQueueTaskExecutorBase

		historyClient           history.Client
		parentClosePolicyClient parentclosepolicy.Client
	}
)

func newTransferQueueActiveTaskExecutor(
	shard ShardContext,
	historyService *historyEngineImpl,
	logger log.Logger,
	metricsClient metrics.Client,
	config *Config,
) queueTaskExecutor {
	return &transferQueueActiveTaskExecutor{
		transferQueueTaskExecutorBase: newTransferQueueTaskExecutorBase(
			shard,
			historyService,
			logger,
			metricsClient,
			config,
		),
		historyClient: shard.GetService().GetHistoryClient(),
		parentClosePolicyClient: parentclosepolicy.NewClient(
			shard.GetMetricsClient(),
			shard.GetLogger(),
			historyService.publicClient,
			config.NumParentClosePolicySystemWorkflows(),
		),
	}
}

func (t *transferQueueActiveTaskExecutor) execute(
	taskInfo queueTaskInfo,
	shouldProcessTask bool,
) error {

	task, ok := taskInfo.(*persistenceblobs.TransferTaskInfo)
	if !ok {
		return errUnexpectedQueueTask
	}

	if !shouldProcessTask {
		return nil
	}

	switch task.TaskType {
	case enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK:
		return t.processActivityTask(task)
	case enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK:
		return t.processWorkflowTask(task)
	case enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION:
		return t.processCloseExecution(task)
	case enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION:
		return t.processCancelExecution(task)
	case enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION:
		return t.processSignalExecution(task)
	case enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION:
		return t.processStartChildExecution(task)
	case enumsspb.TASK_TYPE_TRANSFER_RECORD_WORKFLOW_STARTED:
		return t.processRecordWorkflowStarted(task)
	case enumsspb.TASK_TYPE_TRANSFER_RESET_WORKFLOW:
		return t.processResetWorkflow(task)
	case enumsspb.TASK_TYPE_TRANSFER_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
		return t.processUpsertWorkflowSearchAttributes(task)
	default:
		return errUnknownTransferTask
	}
}

func (t *transferQueueActiveTaskExecutor) processActivityTask(
	task *persistenceblobs.TransferTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getNamespaceIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	ai, ok := mutableState.GetActivityInfo(task.GetScheduleId())
	if !ok {
		t.logger.Debug("Potentially duplicate task.", tag.TaskID(task.GetTaskId()), tag.WorkflowScheduleID(task.GetScheduleId()), tag.TaskType(enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK))
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), ai.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	timeout := common.MinInt32(ai.ScheduleToStartTimeout, common.MaxTaskTimeout)
	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	return t.pushActivity(task, timeout)
}

func (t *transferQueueActiveTaskExecutor) processWorkflowTask(
	task *persistenceblobs.TransferTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getNamespaceIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	workflowTask, found := mutableState.GetWorkflowTaskInfo(task.GetScheduleId())
	if !found {
		t.logger.Debug("Potentially duplicate task.", tag.TaskID(task.GetTaskId()), tag.WorkflowScheduleID(task.GetScheduleId()), tag.TaskType(enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK))
		return nil
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), workflowTask.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	executionInfo := mutableState.GetExecutionInfo()
	runTimeout := executionInfo.WorkflowRunTimeout
	taskTimeout := common.MinInt32(runTimeout, common.MaxTaskTimeout)

	// NOTE: previously this section check whether mutable state has enabled
	// sticky workflowTask, if so convert the workflowTask to a sticky workflowTask.
	// that logic has a bug which timer task for that sticky workflowTask is not generated
	// the correct logic should check whether the workflow task is a sticky workflowTask
	// task or not.
	taskQueue := &taskqueuepb.TaskQueue{
		Name: task.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	if mutableState.GetExecutionInfo().TaskQueue != task.TaskQueue {
		// this workflowTask is an sticky workflowTask
		// there shall already be an timer set
		taskQueue.Kind = enumspb.TASK_QUEUE_KIND_STICKY
		taskTimeout = executionInfo.StickyScheduleToStartTimeout
	}

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	return t.pushWorkflowTask(task, taskQueue, taskTimeout)
}

func (t *transferQueueActiveTaskExecutor) processCloseExecution(
	task *persistenceblobs.TransferTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getNamespaceIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(weContext, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	lastWriteVersion, err := mutableState.GetLastWriteVersion()
	if err != nil {
		return err
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), lastWriteVersion, task.Version, task)
	if err != nil || !ok {
		return err
	}

	executionInfo := mutableState.GetExecutionInfo()
	replyToParentWorkflow := mutableState.HasParentExecution() && executionInfo.Status != enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
	completionEvent, err := mutableState.GetCompletionEvent()
	if err != nil {
		return err
	}
	wfCloseTime := completionEvent.GetTimestamp()

	parentNamespaceID := executionInfo.ParentNamespaceID
	parentWorkflowID := executionInfo.ParentWorkflowID
	parentRunID := executionInfo.ParentRunID
	initiatedID := executionInfo.InitiatedID

	workflowTypeName := executionInfo.WorkflowTypeName
	workflowCloseTimestamp := wfCloseTime
	workflowStatus := executionInfo.Status
	workflowHistoryLength := mutableState.GetNextEventID() - 1

	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return err
	}
	workflowStartTimestamp := startEvent.GetTimestamp()
	workflowExecutionTimestamp := getWorkflowExecutionTimestamp(mutableState, startEvent)
	visibilityMemo := getWorkflowMemo(executionInfo.Memo)
	searchAttr := executionInfo.SearchAttributes
	namespace := mutableState.GetNamespaceEntry().GetInfo().Name
	children := mutableState.GetPendingChildExecutionInfos()

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	err = t.recordWorkflowClosed(
		task.GetNamespaceId(),
		task.GetWorkflowId(),
		task.GetRunId(),
		workflowTypeName,
		workflowStartTimestamp,
		workflowExecutionTimestamp.UnixNano(),
		workflowCloseTimestamp,
		workflowStatus,
		workflowHistoryLength,
		task.GetTaskId(),
		visibilityMemo,
		executionInfo.TaskQueue,
		searchAttr,
	)
	if err != nil {
		return err
	}

	// Communicate the result to parent execution if this is Child Workflow execution
	if replyToParentWorkflow {
		ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
		defer cancel()
		_, err = t.historyClient.RecordChildExecutionCompleted(ctx, &historyservice.RecordChildExecutionCompletedRequest{
			NamespaceId: parentNamespaceID,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: parentWorkflowID,
				RunId:      parentRunID,
			},
			InitiatedId: initiatedID,
			CompletedExecution: &commonpb.WorkflowExecution{
				WorkflowId: task.GetWorkflowId(),
				RunId:      task.GetRunId(),
			},
			CompletionEvent: completionEvent,
		})

		// Check to see if the error is non-transient, in which case reset the error and continue with processing
		if _, ok := err.(*serviceerror.NotFound); ok {
			err = nil
		}
	}

	if err != nil {
		return err
	}

	return t.processParentClosePolicy(task.GetNamespaceId(), namespace, children)
}

func (t *transferQueueActiveTaskExecutor) processCancelExecution(
	task *persistenceblobs.TransferTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getNamespaceIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	initiatedEventID := task.GetScheduleId()
	requestCancelInfo, ok := mutableState.GetRequestCancelInfo(initiatedEventID)
	if !ok {
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), requestCancelInfo.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	targetNamespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(task.GetTargetNamespaceId())
	if err != nil {
		return err
	}
	targetNamespace := targetNamespaceEntry.GetInfo().Name

	// handle workflow cancel itself
	if task.GetNamespaceId() == task.GetTargetNamespaceId() && task.GetWorkflowId() == task.GetTargetWorkflowId() {
		// it does not matter if the run ID is a mismatch
		err = t.requestCancelExternalExecutionFailed(task, context, targetNamespace, task.GetTargetWorkflowId(), task.GetTargetRunId())
		if _, ok := err.(*serviceerror.NotFound); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
		return err
	}

	if err = t.requestCancelExternalExecutionWithRetry(
		task,
		targetNamespace,
		requestCancelInfo,
	); err != nil {
		t.logger.Debug(fmt.Sprintf("Failed to cancel external workflow execution. Error: %v", err))

		// Check to see if the error is non-transient, in which case add RequestCancelFailed
		// event and complete transfer task by setting the err = nil
		if !common.IsServiceNonRetryableError(err) {
			// for retryable error just return
			return err
		}
		return t.requestCancelExternalExecutionFailed(
			task,
			context,
			targetNamespace,
			task.GetTargetWorkflowId(),
			task.GetTargetRunId(),
		)
	}

	t.logger.Debug("RequestCancel successfully recorded to external workflow execution",
		tag.WorkflowID(task.GetTargetWorkflowId()),
		tag.WorkflowRunID(task.GetTargetRunId()),
	)

	// Record ExternalWorkflowExecutionCancelRequested in source execution
	return t.requestCancelExternalExecutionCompleted(
		task,
		context,
		targetNamespace,
		task.GetTargetWorkflowId(),
		task.GetTargetRunId(),
	)
}

func (t *transferQueueActiveTaskExecutor) processSignalExecution(
	task *persistenceblobs.TransferTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getNamespaceIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(weContext, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	initiatedEventID := task.GetScheduleId()
	signalInfo, ok := mutableState.GetSignalInfo(initiatedEventID)
	if !ok {
		// TODO: here we should also RemoveSignalMutableState from target workflow
		// Otherwise, target SignalRequestID still can leak if shard restart after signalExternalExecutionCompleted
		// To do that, probably need to add the SignalRequestID in transfer task.
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), signalInfo.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	targetNamespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(task.GetTargetNamespaceId())
	if err != nil {
		return err
	}
	targetNamespace := targetNamespaceEntry.GetInfo().Name

	// handle workflow signal itself
	if task.GetNamespaceId() == task.GetTargetNamespaceId() && task.GetWorkflowId() == task.GetTargetWorkflowId() {
		// it does not matter if the run ID is a mismatch
		return t.signalExternalExecutionFailed(
			task,
			weContext,
			targetNamespace,
			task.GetTargetWorkflowId(),
			task.GetTargetRunId(),
			signalInfo.Control,
		)
	}

	if err = t.signalExternalExecutionWithRetry(
		task,
		targetNamespace,
		signalInfo,
	); err != nil {
		t.logger.Debug("Failed to signal external workflow execution", tag.Error(err))

		// Check to see if the error is non-transient, in which case add SignalFailed
		// event and complete transfer task by setting the err = nil
		if !common.IsServiceNonRetryableError(err) {
			// for retryable error just return
			return err
		}
		return t.signalExternalExecutionFailed(
			task,
			weContext,
			targetNamespace,
			task.GetTargetWorkflowId(),
			task.GetTargetRunId(),
			signalInfo.Control,
		)
	}

	t.logger.Debug("Signal successfully recorded to external workflow execution",
		tag.WorkflowID(task.GetTargetWorkflowId()),
		tag.WorkflowRunID(task.GetTargetRunId()),
	)

	err = t.signalExternalExecutionCompleted(
		task,
		weContext,
		targetNamespace,
		task.GetTargetWorkflowId(),
		task.GetTargetRunId(),
		signalInfo.Control,
	)
	if err != nil {
		return err
	}

	// release the weContext lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(retError)
	// remove signalRequestedID from target workflow, after Signal detail is removed from source workflow
	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()
	_, err = t.historyClient.RemoveSignalMutableState(ctx, &historyservice.RemoveSignalMutableStateRequest{
		NamespaceId: task.GetTargetNamespaceId(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: task.GetTargetWorkflowId(),
			RunId:      task.GetTargetRunId(),
		},
		RequestId: signalInfo.GetRequestId(),
	})
	return err
}

func (t *transferQueueActiveTaskExecutor) processStartChildExecution(
	task *persistenceblobs.TransferTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getNamespaceIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	// Get parent namespace name
	var namespace string
	if namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(task.GetNamespaceId()); err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}
		// it is possible that the namespace got deleted. Use namespaceID instead as this is only needed for the history event
		namespace = task.GetNamespaceId()
	} else {
		namespace = namespaceEntry.GetInfo().Name
	}

	// Get target namespace name
	var targetNamespace string
	if namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(task.GetTargetNamespaceId()); err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}
		// it is possible that the namespace got deleted. Use namespaceID instead as this is only needed for the history event
		targetNamespace = task.GetNamespaceId()
	} else {
		targetNamespace = namespaceEntry.GetInfo().Name
	}

	initiatedEventID := task.GetScheduleId()
	childInfo, ok := mutableState.GetChildExecutionInfo(initiatedEventID)
	if !ok {
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), childInfo.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	initiatedEvent, err := mutableState.GetChildExecutionInitiatedEvent(initiatedEventID)
	if err != nil {
		return err
	}

	// ChildExecution already started, just create WorkflowTask and complete transfer task
	if childInfo.StartedID != common.EmptyEventID {
		childExecution := &commonpb.WorkflowExecution{
			WorkflowId: childInfo.StartedWorkflowID,
			RunId:      childInfo.StartedRunID,
		}
		return t.createFirstWorkflowTask(task.GetTargetNamespaceId(), childExecution)
	}

	attributes := initiatedEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	childRunID, err := t.startWorkflowWithRetry(
		task,
		namespace,
		targetNamespace,
		childInfo,
		attributes,
	)
	if err != nil {
		t.logger.Debug("Failed to start child workflow execution", tag.Error(err))

		// Check to see if the error is non-transient, in which case add StartChildWorkflowExecutionFailed
		// event and complete transfer task by setting the err = nil
		if _, ok := err.(*serviceerror.WorkflowExecutionAlreadyStarted); ok {
			err = t.recordStartChildExecutionFailed(task, context, attributes)
		}

		return err
	}

	t.logger.Debug("Child Execution started successfully",
		tag.WorkflowID(attributes.WorkflowId), tag.WorkflowRunID(childRunID))

	// Child execution is successfully started, record ChildExecutionStartedEvent in parent execution
	err = t.recordChildExecutionStarted(task, context, attributes, childRunID)

	if err != nil {
		return err
	}
	// Finally create first workflow task for Child execution so it is really started
	return t.createFirstWorkflowTask(task.GetTargetNamespaceId(), &commonpb.WorkflowExecution{
		WorkflowId: task.GetTargetWorkflowId(),
		RunId:      childRunID,
	})
}

func (t *transferQueueActiveTaskExecutor) processRecordWorkflowStarted(
	task *persistenceblobs.TransferTaskInfo,
) (retError error) {

	return t.processRecordWorkflowStartedOrUpsertHelper(task, true)
}

func (t *transferQueueActiveTaskExecutor) processUpsertWorkflowSearchAttributes(
	task *persistenceblobs.TransferTaskInfo,
) (retError error) {

	return t.processRecordWorkflowStartedOrUpsertHelper(task, false)
}

func (t *transferQueueActiveTaskExecutor) processRecordWorkflowStartedOrUpsertHelper(
	task *persistenceblobs.TransferTaskInfo,
	recordStart bool,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getNamespaceIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || !mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	// verify task version for RecordWorkflowStarted.
	// upsert doesn't require verifyTask, because it is just a sync of mutableState.
	if recordStart {
		startVersion, err := mutableState.GetStartVersion()
		if err != nil {
			return err
		}
		ok, err := verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), startVersion, task.Version, task)
		if err != nil || !ok {
			return err
		}
	}

	executionInfo := mutableState.GetExecutionInfo()
	runTimeout := executionInfo.WorkflowRunTimeout
	wfTypeName := executionInfo.WorkflowTypeName
	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return err
	}
	startTimestamp := startEvent.GetTimestamp()
	executionTimestamp := getWorkflowExecutionTimestamp(mutableState, startEvent)
	visibilityMemo := getWorkflowMemo(executionInfo.Memo)
	searchAttr := copySearchAttributes(executionInfo.SearchAttributes)

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)

	if recordStart {
		return t.recordWorkflowStarted(
			task.GetNamespaceId(),
			task.GetWorkflowId(),
			task.GetRunId(),
			wfTypeName,
			startTimestamp,
			executionTimestamp.UnixNano(),
			runTimeout,
			task.GetTaskId(),
			executionInfo.TaskQueue,
			visibilityMemo,
			searchAttr,
		)
	}
	return t.upsertWorkflowExecution(
		task.GetNamespaceId(),
		task.GetWorkflowId(),
		task.GetRunId(),
		wfTypeName,
		startTimestamp,
		executionTimestamp.UnixNano(),
		runTimeout,
		task.GetTaskId(),
		executionInfo.TaskQueue,
		visibilityMemo,
		searchAttr,
	)
}

func (t *transferQueueActiveTaskExecutor) processResetWorkflow(
	task *persistenceblobs.TransferTaskInfo,
) (retError error) {

	currentContext, currentRelease, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getNamespaceIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { currentRelease(retError) }()

	currentMutableState, err := loadMutableStateForTransferTask(currentContext, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if currentMutableState == nil {
		return nil
	}

	logger := t.logger.WithTags(
		tag.WorkflowNamespaceID(task.GetNamespaceId()),
		tag.WorkflowID(task.GetWorkflowId()),
		tag.WorkflowRunID(task.GetRunId()),
	)

	if !currentMutableState.IsWorkflowExecutionRunning() {
		// it means this this might not be current anymore, we need to check
		var resp *persistence.GetCurrentExecutionResponse
		resp, err = t.shard.GetExecutionManager().GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
			NamespaceID: task.GetNamespaceId(),
			WorkflowID:  task.GetWorkflowId(),
		})
		if err != nil {
			return err
		}
		if resp.RunID != task.GetRunId() {
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
	ok, err := verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), currentStartVersion, task.Version, task)
	if err != nil || !ok {
		return err
	}

	executionInfo := currentMutableState.GetExecutionInfo()
	namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(executionInfo.NamespaceID)
	if err != nil {
		return err
	}
	logger = logger.WithTags(tag.WorkflowNamespace(namespaceEntry.GetInfo().Name))

	reason, resetPoint := FindAutoResetPoint(t.shard.GetTimeSource(), namespaceEntry.GetConfig().BadBinaries, executionInfo.AutoResetPoints)
	if resetPoint == nil {
		logger.Warn("Auto-Reset is skipped, because reset point is not found.")
		return nil
	}
	logger = logger.WithTags(
		tag.WorkflowResetBaseRunID(resetPoint.GetRunId()),
		tag.WorkflowBinaryChecksum(resetPoint.GetBinaryChecksum()),
		tag.WorkflowEventID(resetPoint.GetFirstWorkflowTaskCompletedId()),
	)

	var baseContext workflowExecutionContext
	var baseMutableState mutableState
	var baseRelease releaseWorkflowExecutionFunc
	if resetPoint.GetRunId() == executionInfo.RunID {
		baseContext = currentContext
		baseMutableState = currentMutableState
		baseRelease = currentRelease
	} else {
		baseExecution := &commonpb.WorkflowExecution{
			WorkflowId: task.GetWorkflowId(),
			RunId:      resetPoint.GetRunId(),
		}
		baseContext, baseRelease, err = t.cache.getOrCreateWorkflowExecutionForBackground(task.GetNamespaceId(), *baseExecution)
		if err != nil {
			return err
		}
		defer func() { baseRelease(retError) }()
		baseMutableState, err = loadMutableStateForTransferTask(baseContext, task, t.metricsClient, t.logger)
		if err != nil {
			return err
		}
		if baseMutableState == nil {
			return nil
		}
	}

	if err := t.resetWorkflow(
		task,
		namespaceEntry.GetInfo().Name,
		reason,
		resetPoint,
		baseContext,
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
	task *persistenceblobs.TransferTaskInfo,
	context workflowExecutionContext,
	initiatedAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
	runID string,
) error {

	return t.updateWorkflowExecution(context, true,
		func(mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return serviceerror.NewNotFound("Workflow execution already completed.")
			}

			namespace := initiatedAttributes.Namespace
			initiatedEventID := task.GetScheduleId()
			ci, ok := mutableState.GetChildExecutionInfo(initiatedEventID)
			if !ok || ci.StartedID != common.EmptyEventID {
				return serviceerror.NewNotFound("Pending child execution not found.")
			}

			_, err := mutableState.AddChildWorkflowExecutionStartedEvent(
				namespace,
				&commonpb.WorkflowExecution{
					WorkflowId: task.GetTargetWorkflowId(),
					RunId:      runID,
				},
				initiatedAttributes.WorkflowType,
				initiatedEventID,
				initiatedAttributes.Header,
			)

			return err
		})
}

func (t *transferQueueActiveTaskExecutor) recordStartChildExecutionFailed(
	task *persistenceblobs.TransferTaskInfo,
	context workflowExecutionContext,
	initiatedAttributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
) error {

	return t.updateWorkflowExecution(context, true,
		func(mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return serviceerror.NewNotFound("Workflow execution already completed.")
			}

			initiatedEventID := task.GetScheduleId()
			ci, ok := mutableState.GetChildExecutionInfo(initiatedEventID)
			if !ok || ci.StartedID != common.EmptyEventID {
				return serviceerror.NewNotFound("Pending child execution not found.")
			}

			_, err := mutableState.AddStartChildWorkflowExecutionFailedEvent(initiatedEventID,
				enumspb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS, initiatedAttributes)

			return err
		})
}

// createFirstWorkflowTask is used by StartChildExecution transfer task to create the first workflow task for
// child execution.
func (t *transferQueueActiveTaskExecutor) createFirstWorkflowTask(
	namespaceID string,
	execution *commonpb.WorkflowExecution,
) error {

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()
	_, err := t.historyClient.ScheduleWorkflowTask(ctx, &historyservice.ScheduleWorkflowTaskRequest{
		NamespaceId:         namespaceID,
		WorkflowExecution:   execution,
		IsFirstWorkflowTask: true,
	})

	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			// Maybe child workflow execution already timed out or terminated
			// Safe to discard the error and complete this transfer task
			return nil
		}
	}

	return err
}

func (t *transferQueueActiveTaskExecutor) requestCancelExternalExecutionCompleted(
	task *persistenceblobs.TransferTaskInfo,
	context workflowExecutionContext,
	targetNamespace string,
	targetWorkflowID string,
	targetRunID string,
) error {

	err := t.updateWorkflowExecution(context, true,
		func(mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return &serviceerror.NotFound{Message: "Workflow execution already completed."}
			}

			initiatedEventID := task.GetScheduleId()
			_, ok := mutableState.GetRequestCancelInfo(initiatedEventID)
			if !ok {
				return ErrMissingRequestCancelInfo
			}

			_, err := mutableState.AddExternalWorkflowExecutionCancelRequested(
				initiatedEventID,
				targetNamespace,
				targetWorkflowID,
				targetRunID,
			)
			return err
		})

	if _, ok := err.(*serviceerror.NotFound); ok {
		// this could happen if this is a duplicate processing of the task,
		// or the execution has already completed.
		return nil
	}
	return err
}

func (t *transferQueueActiveTaskExecutor) signalExternalExecutionCompleted(
	task *persistenceblobs.TransferTaskInfo,
	context workflowExecutionContext,
	targetNamespace string,
	targetWorkflowID string,
	targetRunID string,
	control string,
) error {

	err := t.updateWorkflowExecution(context, true,
		func(mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return &serviceerror.NotFound{Message: "Workflow execution already completed."}
			}

			initiatedEventID := task.GetScheduleId()
			_, ok := mutableState.GetSignalInfo(initiatedEventID)
			if !ok {
				return ErrMissingSignalInfo
			}

			_, err := mutableState.AddExternalWorkflowExecutionSignaled(
				initiatedEventID,
				targetNamespace,
				targetWorkflowID,
				targetRunID,
				control,
			)
			return err
		})

	if _, ok := err.(*serviceerror.NotFound); ok {
		// this could happen if this is a duplicate processing of the task,
		// or the execution has already completed.
		return nil
	}
	return err
}

func (t *transferQueueActiveTaskExecutor) requestCancelExternalExecutionFailed(
	task *persistenceblobs.TransferTaskInfo,
	context workflowExecutionContext,
	targetNamespace string,
	targetWorkflowID string,
	targetRunID string,
) error {

	err := t.updateWorkflowExecution(context, true,
		func(mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return &serviceerror.NotFound{Message: "Workflow execution already completed."}
			}

			initiatedEventID := task.GetScheduleId()
			_, ok := mutableState.GetRequestCancelInfo(initiatedEventID)
			if !ok {
				return ErrMissingRequestCancelInfo
			}

			_, err := mutableState.AddRequestCancelExternalWorkflowExecutionFailedEvent(
				common.EmptyEventID,
				initiatedEventID,
				targetNamespace,
				targetWorkflowID,
				targetRunID,
				enumspb.CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
			)
			return err
		})

	if _, ok := err.(*serviceerror.NotFound); ok {
		// this could happen if this is a duplicate processing of the task,
		// or the execution has already completed.
		return nil
	}
	return err
}

func (t *transferQueueActiveTaskExecutor) signalExternalExecutionFailed(
	task *persistenceblobs.TransferTaskInfo,
	context workflowExecutionContext,
	targetNamespace string,
	targetWorkflowID string,
	targetRunID string,
	control string,
) error {

	err := t.updateWorkflowExecution(context, true,
		func(mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return &serviceerror.NotFound{Message: "Workflow is not running."}
			}

			initiatedEventID := task.GetScheduleId()
			_, ok := mutableState.GetSignalInfo(initiatedEventID)
			if !ok {
				return ErrMissingSignalInfo
			}

			_, err := mutableState.AddSignalExternalWorkflowExecutionFailedEvent(
				common.EmptyEventID,
				initiatedEventID,
				targetNamespace,
				targetWorkflowID,
				targetRunID,
				control,
				enumspb.SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED_CAUSE_EXTERNAL_WORKFLOW_EXECUTION_NOT_FOUND,
			)
			return err
		})

	if _, ok := err.(*serviceerror.NotFound); ok {
		// this could happen if this is a duplicate processing of the task,
		// or the execution has already completed.
		return nil
	}
	return err
}

func (t *transferQueueActiveTaskExecutor) updateWorkflowExecution(
	context workflowExecutionContext,
	createWorkflowTask bool,
	action func(builder mutableState) error,
) error {

	mutableState, err := context.loadWorkflowExecution()
	if err != nil {
		return err
	}

	if err := action(mutableState); err != nil {
		return err
	}

	if createWorkflowTask {
		// Create a transfer task to schedule a workflow task
		err := scheduleWorkflowTask(mutableState)
		if err != nil {
			return err
		}
	}

	return context.updateWorkflowExecutionAsActive(t.shard.GetTimeSource().Now())
}

func (t *transferQueueActiveTaskExecutor) requestCancelExternalExecutionWithRetry(
	task *persistenceblobs.TransferTaskInfo,
	targetNamespace string,
	requestCancelInfo *persistenceblobs.RequestCancelInfo,
) error {

	request := &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: task.GetTargetNamespaceId(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace: targetNamespace,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: task.GetTargetWorkflowId(),
				RunId:      task.GetTargetRunId(),
			},
			Identity: identityHistoryService,
			// Use the same request ID to dedupe RequestCancelWorkflowExecution calls
			RequestId: requestCancelInfo.GetCancelRequestId(),
		},
		ExternalInitiatedEventId: task.GetScheduleId(),
		ExternalWorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: task.GetWorkflowId(),
			RunId:      task.GetRunId(),
		},
		ChildWorkflowOnly: task.TargetChildWorkflowOnly,
	}

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()
	op := func() error {
		_, err := t.historyClient.RequestCancelWorkflowExecution(ctx, request)
		return err
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)

	if _, ok := err.(*serviceerror.CancellationAlreadyRequested); ok {
		// err is CancellationAlreadyRequested
		// this could happen if target workflow cancellation is already requested
		// mark as success
		return nil
	}
	return err
}

func (t *transferQueueActiveTaskExecutor) signalExternalExecutionWithRetry(
	task *persistenceblobs.TransferTaskInfo,
	targetNamespace string,
	signalInfo *persistenceblobs.SignalInfo,
) error {

	request := &historyservice.SignalWorkflowExecutionRequest{
		NamespaceId: task.GetTargetNamespaceId(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace: targetNamespace,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: task.GetTargetWorkflowId(),
				RunId:      task.GetTargetRunId(),
			},
			Identity:   identityHistoryService,
			SignalName: signalInfo.Name,
			Input:      signalInfo.Input,
			// Use same request ID to deduplicate SignalWorkflowExecution calls
			RequestId: signalInfo.GetRequestId(),
			Control:   signalInfo.Control,
		},
		ExternalWorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: task.GetWorkflowId(),
			RunId:      task.GetRunId(),
		},
		ChildWorkflowOnly: task.TargetChildWorkflowOnly,
	}

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()
	op := func() error {
		_, err := t.historyClient.SignalWorkflowExecution(ctx, request)
		return err
	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *transferQueueActiveTaskExecutor) startWorkflowWithRetry(
	task *persistenceblobs.TransferTaskInfo,
	namespace string,
	targetNamespace string,
	childInfo *persistence.ChildExecutionInfo,
	attributes *historypb.StartChildWorkflowExecutionInitiatedEventAttributes,
) (string, error) {

	now := t.shard.GetTimeSource().Now()
	request := &historyservice.StartWorkflowExecutionRequest{
		NamespaceId: task.GetTargetNamespaceId(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                       targetNamespace,
			WorkflowId:                      attributes.WorkflowId,
			WorkflowType:                    attributes.WorkflowType,
			TaskQueue:                       attributes.TaskQueue,
			Input:                           attributes.Input,
			Header:                          attributes.Header,
			WorkflowExecutionTimeoutSeconds: attributes.WorkflowExecutionTimeoutSeconds,
			WorkflowRunTimeoutSeconds:       attributes.WorkflowRunTimeoutSeconds,
			WorkflowTaskTimeoutSeconds:      attributes.WorkflowTaskTimeoutSeconds,
			// Use the same request ID to dedupe StartWorkflowExecution calls
			RequestId:             childInfo.CreateRequestID,
			WorkflowIdReusePolicy: attributes.WorkflowIdReusePolicy,
			RetryPolicy:           attributes.RetryPolicy,
			CronSchedule:          attributes.CronSchedule,
			Memo:                  attributes.Memo,
			SearchAttributes:      attributes.SearchAttributes,
		},
		ParentExecutionInfo: &workflowspb.ParentExecutionInfo{
			NamespaceId: task.GetNamespaceId(),
			Namespace:   namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: task.GetWorkflowId(),
				RunId:      task.GetRunId(),
			},
			InitiatedId: task.GetScheduleId(),
		},
		FirstWorkflowTaskBackoffSeconds: backoff.GetBackoffForNextScheduleInSeconds(
			attributes.GetCronSchedule(),
			now,
			now,
		),
		ContinueAsNewInitiator: enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW,
		Attempt:                1,
	}

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()
	var response *historyservice.StartWorkflowExecutionResponse
	var err error
	op := func() error {
		response, err = t.historyClient.StartWorkflowExecution(ctx, request)
		return err
	}

	err = backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return "", err
	}
	return response.GetRunId(), nil
}

func (t *transferQueueActiveTaskExecutor) resetWorkflow(
	task *persistenceblobs.TransferTaskInfo,
	namespace string,
	reason string,
	resetPoint *workflowpb.ResetPointInfo,
	baseContext workflowExecutionContext,
	baseMutableState mutableState,
	currentContext workflowExecutionContext,
	currentMutableState mutableState,
	logger log.Logger,
) error {

	var err error
	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()

	namespaceID := task.GetNamespaceId()
	workflowID := task.GetWorkflowId()
	baseRunID := baseMutableState.GetExecutionInfo().RunID

	// TODO when NDC is rolled out, remove this block
	if baseMutableState.GetVersionHistories() == nil {
		_, err = t.historyService.resetor.ResetWorkflowExecution(
			ctx,
			&workflowservice.ResetWorkflowExecutionRequest{
				Namespace: namespace,
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      baseRunID,
				},
				Reason:                    fmt.Sprintf("auto-reset reason:%v, binaryChecksum:%v ", reason, resetPoint.GetBinaryChecksum()),
				WorkflowTaskFinishEventId: resetPoint.GetFirstWorkflowTaskCompletedId(),
				RequestId:                 uuid.New(),
			},
			baseContext,
			baseMutableState,
			currentContext,
			currentMutableState,
		)
	} else {
		resetRunID := uuid.New()
		baseRunID := baseMutableState.GetExecutionInfo().RunID
		baseRebuildLastEventID := resetPoint.GetFirstWorkflowTaskCompletedId() - 1
		baseVersionHistories := baseMutableState.GetVersionHistories()
		baseCurrentVersionHistory, err := baseVersionHistories.GetCurrentVersionHistory()
		if err != nil {
			return err
		}
		baseRebuildLastEventVersion, err := baseCurrentVersionHistory.GetEventVersion(baseRebuildLastEventID)
		if err != nil {
			return err
		}
		baseCurrentBranchToken := baseCurrentVersionHistory.GetBranchToken()
		baseNextEventID := baseMutableState.GetNextEventID()

		err = t.historyService.workflowResetter.resetWorkflow(
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
				t.shard.GetNamespaceCache(),
				t.shard.GetClusterMetadata(),
				currentContext,
				currentMutableState,
				noopReleaseFn, // this is fine since caller will defer on release
			),
			reason,
			nil,
		)
	}

	switch err.(type) {
	case nil:
		return nil

	case *serviceerror.NotFound:
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
	namespaceID string,
	namespace string,
	childInfos map[int64]*persistence.ChildExecutionInfo,
) error {

	if len(childInfos) == 0 {
		return nil
	}

	scope := t.metricsClient.Scope(metrics.TransferActiveTaskCloseExecutionScope)

	if t.shard.GetConfig().EnableParentClosePolicyWorker() &&
		len(childInfos) >= t.shard.GetConfig().ParentClosePolicyThreshold(namespace) {

		executions := make([]parentclosepolicy.RequestDetail, 0, len(childInfos))
		for _, childInfo := range childInfos {
			if childInfo.ParentClosePolicy == enumspb.PARENT_CLOSE_POLICY_ABANDON {
				continue
			}

			executions = append(executions, parentclosepolicy.RequestDetail{
				WorkflowID: childInfo.StartedWorkflowID,
				RunID:      childInfo.StartedRunID,
				Policy:     childInfo.ParentClosePolicy,
			})
		}

		if len(executions) == 0 {
			return nil
		}

		request := parentclosepolicy.Request{
			NamespaceID: namespaceID,
			Namespace:   namespace,
			Executions:  executions,
		}
		return t.parentClosePolicyClient.SendParentClosePolicyRequest(request)
	}

	for _, childInfo := range childInfos {
		if err := t.applyParentClosePolicy(
			namespaceID,
			namespace,
			childInfo,
		); err != nil {
			if _, ok := err.(*serviceerror.NotFound); !ok {
				scope.IncCounter(metrics.ParentClosePolicyProcessorFailures)
				return err
			}
		}
		scope.IncCounter(metrics.ParentClosePolicyProcessorSuccess)
	}
	return nil
}

func (t *transferQueueActiveTaskExecutor) applyParentClosePolicy(
	namespaceID string,
	namespace string,
	childInfo *persistence.ChildExecutionInfo,
) error {

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()

	switch childInfo.ParentClosePolicy {
	case enumspb.PARENT_CLOSE_POLICY_ABANDON:
		// noop
		return nil

	case enumspb.PARENT_CLOSE_POLICY_TERMINATE:
		_, err := t.historyClient.TerminateWorkflowExecution(ctx, &historyservice.TerminateWorkflowExecutionRequest{
			NamespaceId: namespaceID,
			TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{
				Namespace: namespace,
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: childInfo.StartedWorkflowID,
					RunId:      childInfo.StartedRunID,
				},
				Reason:   "by parent close policy",
				Identity: identityHistoryService,
			},
		})
		return err

	case enumspb.PARENT_CLOSE_POLICY_REQUEST_CANCEL:
		_, err := t.historyClient.RequestCancelWorkflowExecution(ctx, &historyservice.RequestCancelWorkflowExecutionRequest{
			NamespaceId: namespaceID,
			CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
				Namespace: namespace,
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: childInfo.StartedWorkflowID,
					RunId:      childInfo.StartedRunID,
				},
				Identity: identityHistoryService,
			},
		})
		return err

	default:
		return &serviceerror.Internal{
			Message: fmt.Sprintf("unknown parent close policy: %v", childInfo.ParentClosePolicy),
		}
	}
}
