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
	"bytes"
	"context"
	"fmt"

	"github.com/pborman/uuid"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"

	commonproto "go.temporal.io/temporal-proto/common"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/client/history"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/service/worker/parentclosepolicy"
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
	case persistence.TransferTaskTypeActivityTask:
		return t.processActivityTask(task)
	case persistence.TransferTaskTypeDecisionTask:
		return t.processDecisionTask(task)
	case persistence.TransferTaskTypeCloseExecution:
		return t.processCloseExecution(task)
	case persistence.TransferTaskTypeCancelExecution:
		return t.processCancelExecution(task)
	case persistence.TransferTaskTypeSignalExecution:
		return t.processSignalExecution(task)
	case persistence.TransferTaskTypeStartChildExecution:
		return t.processStartChildExecution(task)
	case persistence.TransferTaskTypeRecordWorkflowStarted:
		return t.processRecordWorkflowStarted(task)
	case persistence.TransferTaskTypeResetWorkflow:
		return t.processResetWorkflow(task)
	case persistence.TransferTaskTypeUpsertWorkflowSearchAttributes:
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
		t.logger.Debug("Potentially duplicate task.", tag.TaskID(task.GetTaskId()), tag.WorkflowScheduleID(task.GetScheduleId()), tag.TaskType(persistence.TransferTaskTypeActivityTask))
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

func (t *transferQueueActiveTaskExecutor) processDecisionTask(
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

	decision, found := mutableState.GetDecisionInfo(task.GetScheduleId())
	if !found {
		t.logger.Debug("Potentially duplicate task.", tag.TaskID(task.GetTaskId()), tag.WorkflowScheduleID(task.GetScheduleId()), tag.TaskType(persistence.TransferTaskTypeDecisionTask))
		return nil
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), decision.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	executionInfo := mutableState.GetExecutionInfo()
	workflowTimeout := executionInfo.WorkflowTimeout
	decisionTimeout := common.MinInt32(workflowTimeout, common.MaxTaskTimeout)

	// NOTE: previously this section check whether mutable state has enabled
	// sticky decision, if so convert the decision to a sticky decision.
	// that logic has a bug which timer task for that sticky decision is not generated
	// the correct logic should check whether the decision task is a sticky decision
	// task or not.
	taskList := &commonproto.TaskList{
		Name: task.TaskList,
	}
	if mutableState.GetExecutionInfo().TaskList != task.TaskList {
		// this decision is an sticky decision
		// there shall already be an timer set
		taskList.Kind = enums.TaskListKindSticky
		decisionTimeout = executionInfo.StickyScheduleToStartTimeout
	}

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	return t.pushDecision(task, taskList, decisionTimeout)
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
	replyToParentWorkflow := mutableState.HasParentExecution() && executionInfo.Status != enums.WorkflowExecutionStatusContinuedAsNew
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
	workflowCloseStatus := executionInfo.Status
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
		primitives.UUIDString(task.GetNamespaceId()),
		task.GetWorkflowId(),
		primitives.UUIDString(task.GetRunId()),
		workflowTypeName,
		workflowStartTimestamp,
		workflowExecutionTimestamp.UnixNano(),
		workflowCloseTimestamp,
		workflowCloseStatus,
		workflowHistoryLength,
		task.GetTaskId(),
		visibilityMemo,
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
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: parentWorkflowID,
				RunId:      parentRunID,
			},
			InitiatedId: initiatedID,
			CompletedExecution: &commonproto.WorkflowExecution{
				WorkflowId: task.GetWorkflowId(),
				RunId:      primitives.UUID(task.GetRunId()).String(),
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

	return t.processParentClosePolicy(primitives.UUID(task.GetNamespaceId()).String(), namespace, children)
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

	targetNamespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(primitives.UUIDString(task.GetTargetNamespaceId()))
	if err != nil {
		return err
	}
	targetNamespace := targetNamespaceEntry.GetInfo().Name

	// handle workflow cancel itself
	if bytes.Compare(task.GetNamespaceId(), task.GetTargetNamespaceId()) == 0 && task.GetWorkflowId() == task.GetTargetWorkflowId() {
		// it does not matter if the run ID is a mismatch
		err = t.requestCancelExternalExecutionFailed(task, context, targetNamespace, task.GetTargetWorkflowId(), primitives.UUID(task.GetTargetRunId()).String())
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
		t.logger.Debug(fmt.Sprintf("Failed to cancel external commonproto execution. Error: %v", err))

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
			primitives.UUID(task.GetTargetRunId()).String(),
		)
	}

	t.logger.Debug("RequestCancel successfully recorded to external workflow execution",
		tag.WorkflowID(task.GetTargetWorkflowId()),
		tag.WorkflowRunIDBytes(task.GetTargetRunId()),
	)

	// Record ExternalWorkflowExecutionCancelRequested in source execution
	return t.requestCancelExternalExecutionCompleted(
		task,
		context,
		targetNamespace,
		task.GetTargetWorkflowId(),
		primitives.UUID(task.GetTargetRunId()).String(),
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
		// TODO: here we should also RemoveSignalMutableState from target commonproto
		// Otherwise, target SignalRequestID still can leak if shard restart after signalExternalExecutionCompleted
		// To do that, probably need to add the SignalRequestID in transfer task.
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), signalInfo.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	targetNamespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(primitives.UUIDString(task.GetTargetNamespaceId()))
	if err != nil {
		return err
	}
	targetNamespace := targetNamespaceEntry.GetInfo().Name

	// handle workflow signal itself
	if bytes.Compare(task.GetNamespaceId(), task.GetTargetNamespaceId()) == 0 && task.GetWorkflowId() == task.GetTargetWorkflowId() {
		// it does not matter if the run ID is a mismatch
		return t.signalExternalExecutionFailed(
			task,
			weContext,
			targetNamespace,
			task.GetTargetWorkflowId(),
			primitives.UUID(task.GetTargetRunId()).String(),
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
			primitives.UUID(task.GetTargetRunId()).String(),
			signalInfo.Control,
		)
	}

	t.logger.Debug("Signal successfully recorded to external workflow execution",
		tag.WorkflowID(task.GetTargetWorkflowId()),
		tag.WorkflowRunIDBytes(task.GetTargetRunId()),
	)

	err = t.signalExternalExecutionCompleted(
		task,
		weContext,
		targetNamespace,
		task.GetTargetWorkflowId(),
		primitives.UUID(task.GetTargetRunId()).String(),
		signalInfo.Control,
	)
	if err != nil {
		return err
	}

	// release the weContext lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(retError)
	// remove signalRequestedID from target commonproto, after Signal detail is removed from source commonproto
	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()
	_, err = t.historyClient.RemoveSignalMutableState(ctx, &historyservice.RemoveSignalMutableStateRequest{
		NamespaceId: primitives.UUID(task.GetTargetNamespaceId()).String(),
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: task.GetTargetWorkflowId(),
			RunId:      primitives.UUID(task.GetTargetRunId()).String(),
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
	if namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(primitives.UUID(task.GetNamespaceId()).String()); err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}
		// it is possible that the namespace got deleted. Use namespaceID instead as this is only needed for the history event
		namespace = primitives.UUID(task.GetNamespaceId()).String()
	} else {
		namespace = namespaceEntry.GetInfo().Name
	}

	// Get target namespace name
	var targetNamespace string
	if namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(primitives.UUID(task.GetTargetNamespaceId()).String()); err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}
		// it is possible that the namespace got deleted. Use namespaceID instead as this is only needed for the history event
		targetNamespace = primitives.UUID(task.GetNamespaceId()).String()
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

	// ChildExecution already started, just create DecisionTask and complete transfer task
	if childInfo.StartedID != common.EmptyEventID {
		childExecution := &commonproto.WorkflowExecution{
			WorkflowId: childInfo.StartedWorkflowID,
			RunId:      childInfo.StartedRunID,
		}
		return t.createFirstDecisionTask(primitives.UUID(task.GetTargetNamespaceId()).String(), childExecution)
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
	// Finally create first decision task for Child execution so it is really started
	return t.createFirstDecisionTask(primitives.UUID(task.GetTargetNamespaceId()).String(), &commonproto.WorkflowExecution{
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
	workflowTimeout := executionInfo.WorkflowTimeout
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
			primitives.UUID(task.GetNamespaceId()).String(),
			task.GetWorkflowId(),
			primitives.UUID(task.GetRunId()).String(),
			wfTypeName,
			startTimestamp,
			executionTimestamp.UnixNano(),
			workflowTimeout,
			task.GetTaskId(),
			visibilityMemo,
			searchAttr,
		)
	}
	return t.upsertWorkflowExecution(
		primitives.UUID(task.GetNamespaceId()).String(),
		task.GetWorkflowId(),
		primitives.UUID(task.GetRunId()).String(),
		wfTypeName,
		startTimestamp,
		executionTimestamp.UnixNano(),
		workflowTimeout,
		task.GetTaskId(),
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
		tag.WorkflowNamespaceIDBytes(task.GetNamespaceId()),
		tag.WorkflowID(task.GetWorkflowId()),
		tag.WorkflowRunIDBytes(task.GetRunId()),
	)

	if !currentMutableState.IsWorkflowExecutionRunning() {
		// it means this this might not be current anymore, we need to check
		var resp *persistence.GetCurrentExecutionResponse
		resp, err = t.shard.GetExecutionManager().GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
			NamespaceID: primitives.UUIDString(task.GetNamespaceId()),
			WorkflowID:  task.GetWorkflowId(),
		})
		if err != nil {
			return err
		}
		if resp.RunID != primitives.UUIDString(task.GetRunId()) {
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

	reason, resetPoint := FindAutoResetPoint(t.shard.GetTimeSource(), &namespaceEntry.GetConfig().BadBinaries, executionInfo.AutoResetPoints)
	if resetPoint == nil {
		logger.Warn("Auto-Reset is skipped, because reset point is not found.")
		return nil
	}
	logger = logger.WithTags(
		tag.WorkflowResetBaseRunID(resetPoint.GetRunId()),
		tag.WorkflowBinaryChecksum(resetPoint.GetBinaryChecksum()),
		tag.WorkflowEventID(resetPoint.GetFirstDecisionCompletedId()),
	)

	var baseContext workflowExecutionContext
	var baseMutableState mutableState
	var baseRelease releaseWorkflowExecutionFunc
	if resetPoint.GetRunId() == executionInfo.RunID {
		baseContext = currentContext
		baseMutableState = currentMutableState
		baseRelease = currentRelease
	} else {
		baseExecution := &commonproto.WorkflowExecution{
			WorkflowId: task.GetWorkflowId(),
			RunId:      resetPoint.GetRunId(),
		}
		baseContext, baseRelease, err = t.cache.getOrCreateWorkflowExecutionForBackground(primitives.UUIDString(task.GetNamespaceId()), *baseExecution)
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
	initiatedAttributes *commonproto.StartChildWorkflowExecutionInitiatedEventAttributes,
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
				&commonproto.WorkflowExecution{
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
	initiatedAttributes *commonproto.StartChildWorkflowExecutionInitiatedEventAttributes,
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
				enums.ChildWorkflowExecutionFailedCauseWorkflowAlreadyRunning, initiatedAttributes)

			return err
		})
}

// createFirstDecisionTask is used by StartChildExecution transfer task to create the first decision task for
// child execution.
func (t *transferQueueActiveTaskExecutor) createFirstDecisionTask(
	namespaceID string,
	execution *commonproto.WorkflowExecution,
) error {

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()
	_, err := t.historyClient.ScheduleDecisionTask(ctx, &historyservice.ScheduleDecisionTaskRequest{
		NamespaceId:       namespaceID,
		WorkflowExecution: execution,
		IsFirstDecision:   true,
	})

	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			// Maybe child commonproto execution already timedout or terminated
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
	control []byte,
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
				enums.CancelExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution,
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
	control []byte,
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
				enums.SignalExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution,
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
	createDecisionTask bool,
	action func(builder mutableState) error,
) error {

	mutableState, err := context.loadWorkflowExecution()
	if err != nil {
		return err
	}

	if err := action(mutableState); err != nil {
		return err
	}

	if createDecisionTask {
		// Create a transfer task to schedule a decision task
		err := scheduleDecision(mutableState)
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
		NamespaceId: primitives.UUID(task.GetTargetNamespaceId()).String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace: targetNamespace,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: task.GetTargetWorkflowId(),
				RunId:      primitives.UUID(task.GetTargetRunId()).String(),
			},
			Identity: identityHistoryService,
			// Use the same request ID to dedupe RequestCancelWorkflowExecution calls
			RequestId: requestCancelInfo.GetCancelRequestId(),
		},
		ExternalInitiatedEventId: task.GetScheduleId(),
		ExternalWorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: task.GetWorkflowId(),
			RunId:      primitives.UUID(task.GetRunId()).String(),
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
		NamespaceId: primitives.UUID(task.GetTargetNamespaceId()).String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Namespace: targetNamespace,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: task.GetTargetWorkflowId(),
				RunId:      primitives.UUID(task.GetTargetRunId()).String(),
			},
			Identity:   identityHistoryService,
			SignalName: signalInfo.Name,
			Input:      signalInfo.Input,
			// Use same request ID to deduplicate SignalWorkflowExecution calls
			RequestId: signalInfo.GetRequestId(),
			Control:   signalInfo.Control,
		},
		ExternalWorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: task.GetWorkflowId(),
			RunId:      primitives.UUID(task.GetRunId()).String(),
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
	attributes *commonproto.StartChildWorkflowExecutionInitiatedEventAttributes,
) (string, error) {

	now := t.shard.GetTimeSource().Now()
	request := &historyservice.StartWorkflowExecutionRequest{
		NamespaceId: primitives.UUID(task.GetTargetNamespaceId()).String(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                           targetNamespace,
			WorkflowId:                          attributes.WorkflowId,
			WorkflowType:                        attributes.WorkflowType,
			TaskList:                            attributes.TaskList,
			Input:                               attributes.Input,
			Header:                              attributes.Header,
			ExecutionStartToCloseTimeoutSeconds: attributes.ExecutionStartToCloseTimeoutSeconds,
			TaskStartToCloseTimeoutSeconds:      attributes.TaskStartToCloseTimeoutSeconds,
			// Use the same request ID to dedupe StartWorkflowExecution calls
			RequestId:             childInfo.CreateRequestID,
			WorkflowIdReusePolicy: attributes.WorkflowIdReusePolicy,
			RetryPolicy:           attributes.RetryPolicy,
			CronSchedule:          attributes.CronSchedule,
			Memo:                  attributes.Memo,
			SearchAttributes:      attributes.SearchAttributes,
		},
		ParentExecutionInfo: &commonproto.ParentExecutionInfo{
			NamespaceId: primitives.UUID(task.GetNamespaceId()).String(),
			Namespace:   namespace,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: task.GetWorkflowId(),
				RunId:      primitives.UUID(task.GetRunId()).String(),
			},
			InitiatedId: task.GetScheduleId(),
		},
		FirstDecisionTaskBackoffSeconds: backoff.GetBackoffForNextScheduleInSeconds(
			attributes.GetCronSchedule(),
			now,
			now,
		),
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
	resetPoint *commonproto.ResetPointInfo,
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
				WorkflowExecution: &commonproto.WorkflowExecution{
					WorkflowId: workflowID,
					RunId:      baseRunID,
				},
				Reason:                fmt.Sprintf("auto-reset reason:%v, binaryChecksum:%v ", reason, resetPoint.GetBinaryChecksum()),
				DecisionFinishEventId: resetPoint.GetFirstDecisionCompletedId(),
				RequestId:             uuid.New(),
			},
			baseContext,
			baseMutableState,
			currentContext,
			currentMutableState,
		)
	} else {
		resetRunID := uuid.New()
		baseRunID := baseMutableState.GetExecutionInfo().RunID
		baseRebuildLastEventID := resetPoint.GetFirstDecisionCompletedId() - 1
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
			primitives.UUIDString(namespaceID),
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
		logger.Error("Auto-Reset commonproto failed and not retryable. The reset point is corrupted.", tag.Error(err))
		return nil

	default:
		// log this error and retry
		logger.Error("Auto-Reset commonproto failed", tag.Error(err))
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
			if childInfo.ParentClosePolicy == enums.ParentClosePolicyAbandon {
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
	case enums.ParentClosePolicyAbandon:
		// noop
		return nil

	case enums.ParentClosePolicyTerminate:
		_, err := t.historyClient.TerminateWorkflowExecution(ctx, &historyservice.TerminateWorkflowExecutionRequest{
			NamespaceId: namespaceID,
			TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{
				Namespace: namespace,
				WorkflowExecution: &commonproto.WorkflowExecution{
					WorkflowId: childInfo.StartedWorkflowID,
					RunId:      childInfo.StartedRunID,
				},
				Reason:   "by parent close policy",
				Identity: identityHistoryService,
			},
		})
		return err

	case enums.ParentClosePolicyRequestCancel:
		_, err := t.historyClient.RequestCancelWorkflowExecution(ctx, &historyservice.RequestCancelWorkflowExecutionRequest{
			NamespaceId: namespaceID,
			CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
				Namespace: namespace,
				WorkflowExecution: &commonproto.WorkflowExecution{
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
