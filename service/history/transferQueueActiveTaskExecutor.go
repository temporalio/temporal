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
	ctx "context"
	"fmt"

	"github.com/pborman/uuid"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice"
	"golang.org/x/net/context"

	commonproto "go.temporal.io/temporal-proto/common"

	h "github.com/temporalio/temporal/.gen/proto/historyservice"
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
		t.getDomainIDAndWorkflowExecution(task),
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

	ai, ok := mutableState.GetActivityInfo(task.ScheduleID)
	if !ok {
		t.logger.Debug("Potentially duplicate task.", tag.TaskID(task.TaskID), tag.WorkflowScheduleID(task.ScheduleID), tag.TaskType(persistence.TransferTaskTypeActivityTask))
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, task.DomainID, ai.Version, task.Version, task)
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
		t.getDomainIDAndWorkflowExecution(task),
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

	decision, found := mutableState.GetDecisionInfo(task.ScheduleID)
	if !found {
		t.logger.Debug("Potentially duplicate task.", tag.TaskID(task.TaskID), tag.WorkflowScheduleID(task.ScheduleID), tag.TaskType(persistence.TransferTaskTypeDecisionTask))
		return nil
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, decision.Version, task.Version, task)
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

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
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
	ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, lastWriteVersion, task.Version, task)
	if err != nil || !ok {
		return err
	}

	executionInfo := mutableState.GetExecutionInfo()
	replyToParentWorkflow := mutableState.HasParentExecution() && executionInfo.CloseStatus != persistence.WorkflowCloseStatusContinuedAsNew
	completionEvent, err := mutableState.GetCompletionEvent()
	if err != nil {
		return err
	}
	wfCloseTime := completionEvent.GetTimestamp()

	parentDomainID := executionInfo.ParentDomainID
	parentWorkflowID := executionInfo.ParentWorkflowID
	parentRunID := executionInfo.ParentRunID
	initiatedID := executionInfo.InitiatedID

	workflowTypeName := executionInfo.WorkflowTypeName
	workflowCloseTimestamp := wfCloseTime
	workflowCloseStatus := executionInfo.CloseStatus
	workflowHistoryLength := mutableState.GetNextEventID() - 1

	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return err
	}
	workflowStartTimestamp := startEvent.GetTimestamp()
	workflowExecutionTimestamp := getWorkflowExecutionTimestamp(mutableState, startEvent)
	visibilityMemo := getWorkflowMemo(executionInfo.Memo)
	searchAttr := executionInfo.SearchAttributes
	domainName := mutableState.GetDomainEntry().GetInfo().Name
	children := mutableState.GetPendingChildExecutionInfos()

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	err = t.recordWorkflowClosed(
		primitives.UUIDString(task.DomainID),
		task.WorkflowID,
		primitives.UUIDString(task.RunID),
		workflowTypeName,
		workflowStartTimestamp,
		workflowExecutionTimestamp.UnixNano(),
		workflowCloseTimestamp,
		workflowCloseStatus,
		workflowHistoryLength,
		task.GetTaskID(),
		visibilityMemo,
		searchAttr,
	)
	if err != nil {
		return err
	}

	// Communicate the result to parent execution if this is Child Workflow execution
	if replyToParentWorkflow {
		ctx, cancel := ctx.WithTimeout(ctx.Background(), transferActiveTaskDefaultTimeout)
		defer cancel()
		_, err = t.historyClient.RecordChildExecutionCompleted(ctx, &h.RecordChildExecutionCompletedRequest{
			DomainUUID: parentDomainID,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: parentWorkflowID,
				RunId:      parentRunID,
			},
			InitiatedId: initiatedID,
			CompletedExecution: &commonproto.WorkflowExecution{
				WorkflowId: task.WorkflowID,
				RunId:      primitives.UUID(task.RunID).String(),
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

	return t.processParentClosePolicy(primitives.UUID(task.DomainID).String(), domainName, children)
}

func (t *transferQueueActiveTaskExecutor) processCancelExecution(
	task *persistenceblobs.TransferTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
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

	initiatedEventID := task.ScheduleID
	requestCancelInfo, ok := mutableState.GetRequestCancelInfo(initiatedEventID)
	if !ok {
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, task.DomainID, requestCancelInfo.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	targetDomainEntry, err := t.shard.GetDomainCache().GetDomainByID(primitives.UUIDString(task.TargetDomainID))
	if err != nil {
		return err
	}
	targetDomain := targetDomainEntry.GetInfo().Name

	// handle workflow cancel itself
	if bytes.Compare(task.DomainID, task.TargetDomainID) == 0 && task.WorkflowID == task.TargetWorkflowID {
		// it does not matter if the run ID is a mismatch
		err = t.requestCancelExternalExecutionFailed(task, context, targetDomain, task.TargetWorkflowID, primitives.UUID(task.TargetRunID).String())
		if _, ok := err.(*serviceerror.NotFound); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
		return err
	}

	if err = t.requestCancelExternalExecutionWithRetry(
		task,
		targetDomain,
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
			targetDomain,
			task.TargetWorkflowID,
			primitives.UUID(task.TargetRunID).String(),
		)
	}

	t.logger.Debug("RequestCancel successfully recorded to external workflow execution",
		tag.WorkflowID(task.TargetWorkflowID),
		tag.WorkflowRunIDBytes(task.TargetRunID),
	)

	// Record ExternalWorkflowExecutionCancelRequested in source execution
	return t.requestCancelExternalExecutionCompleted(
		task,
		context,
		targetDomain,
		task.TargetWorkflowID,
		primitives.UUID(task.TargetRunID).String(),
	)
}

func (t *transferQueueActiveTaskExecutor) processSignalExecution(
	task *persistenceblobs.TransferTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
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

	initiatedEventID := task.ScheduleID
	signalInfo, ok := mutableState.GetSignalInfo(initiatedEventID)
	if !ok {
		// TODO: here we should also RemoveSignalMutableState from target commonproto
		// Otherwise, target SignalRequestID still can leak if shard restart after signalExternalExecutionCompleted
		// To do that, probably need to add the SignalRequestID in transfer task.
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, task.DomainID, signalInfo.Version, task.Version, task)
	if err != nil || !ok {
		return err
	}

	targetDomainEntry, err := t.shard.GetDomainCache().GetDomainByID(primitives.UUIDString(task.TargetDomainID))
	if err != nil {
		return err
	}
	targetDomain := targetDomainEntry.GetInfo().Name

	// handle workflow signal itself
	if bytes.Compare(task.DomainID, task.TargetDomainID) == 0 && task.WorkflowID == task.TargetWorkflowID {
		// it does not matter if the run ID is a mismatch
		return t.signalExternalExecutionFailed(
			task,
			weContext,
			targetDomain,
			task.TargetWorkflowID,
			primitives.UUID(task.TargetRunID).String(),
			signalInfo.Control,
		)
	}

	if err = t.signalExternalExecutionWithRetry(
		task,
		targetDomain,
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
			targetDomain,
			task.TargetWorkflowID,
			primitives.UUID(task.TargetRunID).String(),
			signalInfo.Control,
		)
	}

	t.logger.Debug("Signal successfully recorded to external workflow execution",
		tag.WorkflowID(task.TargetWorkflowID),
		tag.WorkflowRunIDBytes(task.TargetRunID),
	)

	err = t.signalExternalExecutionCompleted(
		task,
		weContext,
		targetDomain,
		task.TargetWorkflowID,
		primitives.UUID(task.TargetRunID).String(),
		signalInfo.Control,
	)
	if err != nil {
		return err
	}

	// release the weContext lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(retError)
	// remove signalRequestedID from target commonproto, after Signal detail is removed from source commonproto
	ctx, cancel := ctx.WithTimeout(ctx.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()
	_, err = t.historyClient.RemoveSignalMutableState(ctx, &h.RemoveSignalMutableStateRequest{
		DomainUUID: primitives.UUID(task.TargetDomainID).String(),
		WorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: task.TargetWorkflowID,
			RunId:      primitives.UUID(task.TargetRunID).String(),
		},
		RequestId: signalInfo.RequestID,
	})
	return err
}

func (t *transferQueueActiveTaskExecutor) processStartChildExecution(
	task *persistenceblobs.TransferTaskInfo,
) (retError error) {

	context, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
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

	// Get parent domain name
	var domain string
	if domainEntry, err := t.shard.GetDomainCache().GetDomainByID(primitives.UUID(task.DomainID).String()); err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}
		// it is possible that the domain got deleted. Use domainID instead as this is only needed for the history event
		domain = primitives.UUID(task.DomainID).String()
	} else {
		domain = domainEntry.GetInfo().Name
	}

	// Get target domain name
	var targetDomain string
	if domainEntry, err := t.shard.GetDomainCache().GetDomainByID(primitives.UUID(task.TargetDomainID).String()); err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}
		// it is possible that the domain got deleted. Use domainID instead as this is only needed for the history event
		targetDomain = primitives.UUID(task.DomainID).String()
	} else {
		targetDomain = domainEntry.GetInfo().Name
	}

	initiatedEventID := task.ScheduleID
	childInfo, ok := mutableState.GetChildExecutionInfo(initiatedEventID)
	if !ok {
		return nil
	}
	ok, err = verifyTaskVersion(t.shard, t.logger, task.DomainID, childInfo.Version, task.Version, task)
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
		return t.createFirstDecisionTask(primitives.UUID(task.TargetDomainID).String(), childExecution)
	}

	attributes := initiatedEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	childRunID, err := t.startWorkflowWithRetry(
		task,
		domain,
		targetDomain,
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
	return t.createFirstDecisionTask(primitives.UUID(task.TargetDomainID).String(), &commonproto.WorkflowExecution{
		WorkflowId: task.TargetWorkflowID,
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
		t.getDomainIDAndWorkflowExecution(task),
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
		ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, startVersion, task.Version, task)
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
			primitives.UUID(task.DomainID).String(),
			task.WorkflowID,
			primitives.UUID(task.RunID).String(),
			wfTypeName,
			startTimestamp,
			executionTimestamp.UnixNano(),
			workflowTimeout,
			task.GetTaskID(),
			visibilityMemo,
			searchAttr,
		)
	}
	return t.upsertWorkflowExecution(
		primitives.UUID(task.DomainID).String(),
		task.WorkflowID,
		primitives.UUID(task.RunID).String(),
		wfTypeName,
		startTimestamp,
		executionTimestamp.UnixNano(),
		workflowTimeout,
		task.GetTaskID(),
		visibilityMemo,
		searchAttr,
	)
}

func (t *transferQueueActiveTaskExecutor) processResetWorkflow(
	task *persistenceblobs.TransferTaskInfo,
) (retError error) {

	currentContext, currentRelease, err := t.cache.getOrCreateWorkflowExecutionForBackground(
		t.getDomainIDAndWorkflowExecution(task),
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
		tag.WorkflowDomainIDBytes(task.DomainID),
		tag.WorkflowID(task.WorkflowID),
		tag.WorkflowRunIDBytes(task.RunID),
	)

	if !currentMutableState.IsWorkflowExecutionRunning() {
		// it means this this might not be current anymore, we need to check
		var resp *persistence.GetCurrentExecutionResponse
		resp, err = t.shard.GetExecutionManager().GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
			DomainID:   primitives.UUIDString(task.DomainID),
			WorkflowID: task.WorkflowID,
		})
		if err != nil {
			return err
		}
		if resp.RunID != primitives.UUIDString(task.RunID) {
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
	ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, currentStartVersion, task.Version, task)
	if err != nil || !ok {
		return err
	}

	executionInfo := currentMutableState.GetExecutionInfo()
	domainEntry, err := t.shard.GetDomainCache().GetDomainByID(executionInfo.DomainID)
	if err != nil {
		return err
	}
	logger = logger.WithTags(tag.WorkflowDomainName(domainEntry.GetInfo().Name))

	reason, resetPoint := FindAutoResetPoint(t.shard.GetTimeSource(), &domainEntry.GetConfig().BadBinaries, executionInfo.AutoResetPoints)
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
			WorkflowId: task.WorkflowID,
			RunId:      resetPoint.GetRunId(),
		}
		baseContext, baseRelease, err = t.cache.getOrCreateWorkflowExecutionForBackground(primitives.UUIDString(task.DomainID), *baseExecution)
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
		domainEntry.GetInfo().Name,
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

			domain := initiatedAttributes.Domain
			initiatedEventID := task.ScheduleID
			ci, ok := mutableState.GetChildExecutionInfo(initiatedEventID)
			if !ok || ci.StartedID != common.EmptyEventID {
				return serviceerror.NewNotFound("Pending child execution not found.")
			}

			_, err := mutableState.AddChildWorkflowExecutionStartedEvent(
				domain,
				&commonproto.WorkflowExecution{
					WorkflowId: task.TargetWorkflowID,
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

			initiatedEventID := task.ScheduleID
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
	domainID string,
	execution *commonproto.WorkflowExecution,
) error {

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()
	_, err := t.historyClient.ScheduleDecisionTask(ctx, &h.ScheduleDecisionTaskRequest{
		DomainUUID:        domainID,
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
	targetDomain string,
	targetWorkflowID string,
	targetRunID string,
) error {

	err := t.updateWorkflowExecution(context, true,
		func(mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return &serviceerror.NotFound{Message: "Workflow execution already completed."}
			}

			initiatedEventID := task.ScheduleID
			_, ok := mutableState.GetRequestCancelInfo(initiatedEventID)
			if !ok {
				return ErrMissingRequestCancelInfo
			}

			_, err := mutableState.AddExternalWorkflowExecutionCancelRequested(
				initiatedEventID,
				targetDomain,
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
	targetDomain string,
	targetWorkflowID string,
	targetRunID string,
	control []byte,
) error {

	err := t.updateWorkflowExecution(context, true,
		func(mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return &serviceerror.NotFound{Message: "Workflow execution already completed."}
			}

			initiatedEventID := task.ScheduleID
			_, ok := mutableState.GetSignalInfo(initiatedEventID)
			if !ok {
				return ErrMissingSignalInfo
			}

			_, err := mutableState.AddExternalWorkflowExecutionSignaled(
				initiatedEventID,
				targetDomain,
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
	targetDomain string,
	targetWorkflowID string,
	targetRunID string,
) error {

	err := t.updateWorkflowExecution(context, true,
		func(mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return &serviceerror.NotFound{Message: "Workflow execution already completed."}
			}

			initiatedEventID := task.ScheduleID
			_, ok := mutableState.GetRequestCancelInfo(initiatedEventID)
			if !ok {
				return ErrMissingRequestCancelInfo
			}

			_, err := mutableState.AddRequestCancelExternalWorkflowExecutionFailedEvent(
				common.EmptyEventID,
				initiatedEventID,
				targetDomain,
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
	targetDomain string,
	targetWorkflowID string,
	targetRunID string,
	control []byte,
) error {

	err := t.updateWorkflowExecution(context, true,
		func(mutableState mutableState) error {
			if !mutableState.IsWorkflowExecutionRunning() {
				return &serviceerror.NotFound{Message: "Workflow is not running."}
			}

			initiatedEventID := task.ScheduleID
			_, ok := mutableState.GetSignalInfo(initiatedEventID)
			if !ok {
				return ErrMissingSignalInfo
			}

			_, err := mutableState.AddSignalExternalWorkflowExecutionFailedEvent(
				common.EmptyEventID,
				initiatedEventID,
				targetDomain,
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
	targetDomain string,
	requestCancelInfo *persistenceblobs.RequestCancelInfo,
) error {

	request := &h.RequestCancelWorkflowExecutionRequest{
		DomainUUID: primitives.UUID(task.TargetDomainID).String(),
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Domain: targetDomain,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: task.TargetWorkflowID,
				RunId:      primitives.UUID(task.TargetRunID).String(),
			},
			Identity: identityHistoryService,
			// Use the same request ID to dedupe RequestCancelWorkflowExecution calls
			RequestId: requestCancelInfo.CancelRequestID,
		},
		ExternalInitiatedEventId: task.ScheduleID,
		ExternalWorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      primitives.UUID(task.RunID).String(),
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
	targetDomain string,
	signalInfo *persistenceblobs.SignalInfo,
) error {

	request := &h.SignalWorkflowExecutionRequest{
		DomainUUID: primitives.UUID(task.TargetDomainID).String(),
		SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
			Domain: targetDomain,
			WorkflowExecution: &commonproto.WorkflowExecution{
				WorkflowId: task.TargetWorkflowID,
				RunId:      primitives.UUID(task.TargetRunID).String(),
			},
			Identity:   identityHistoryService,
			SignalName: signalInfo.Name,
			Input:      signalInfo.Input,
			// Use same request ID to deduplicate SignalWorkflowExecution calls
			RequestId: signalInfo.RequestID,
			Control:   signalInfo.Control,
		},
		ExternalWorkflowExecution: &commonproto.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      primitives.UUID(task.RunID).String(),
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
	domain string,
	targetDomain string,
	childInfo *persistence.ChildExecutionInfo,
	attributes *commonproto.StartChildWorkflowExecutionInitiatedEventAttributes,
) (string, error) {

	now := t.shard.GetTimeSource().Now()
	request := &h.StartWorkflowExecutionRequest{
		DomainUUID: primitives.UUID(task.TargetDomainID).String(),
		StartRequest: &workflowservice.StartWorkflowExecutionRequest{
			Domain:                              targetDomain,
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
			DomainUUID: primitives.UUID(task.DomainID).String(),
			Domain:     domain,
			Execution: &commonproto.WorkflowExecution{
				WorkflowId: task.WorkflowID,
				RunId:      primitives.UUID(task.RunID).String(),
			},
			InitiatedId: task.ScheduleID,
		},
		FirstDecisionTaskBackoffSeconds: backoff.GetBackoffForNextScheduleInSeconds(
			attributes.GetCronSchedule(),
			now,
			now,
		),
	}

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()
	var response *h.StartWorkflowExecutionResponse
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
	domain string,
	reason string,
	resetPoint *commonproto.ResetPointInfo,
	baseContext workflowExecutionContext,
	baseMutableState mutableState,
	currentContext workflowExecutionContext,
	currentMutableState mutableState,
	logger log.Logger,
) error {

	var err error
	ctx, cancel := ctx.WithTimeout(ctx.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()

	domainID := task.DomainID
	workflowID := task.WorkflowID
	baseRunID := baseMutableState.GetExecutionInfo().RunID

	// TODO when NDC is rolled out, remove this block
	if baseMutableState.GetVersionHistories() == nil {
		_, err = t.historyService.resetor.ResetWorkflowExecution(
			ctx,
			&workflowservice.ResetWorkflowExecutionRequest{
				Domain: domain,
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
			primitives.UUIDString(domainID),
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
				t.shard.GetDomainCache(),
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
	domainID string,
	domainName string,
	childInfos map[int64]*persistence.ChildExecutionInfo,
) error {

	if len(childInfos) == 0 {
		return nil
	}

	scope := t.metricsClient.Scope(metrics.TransferActiveTaskCloseExecutionScope)

	if t.shard.GetConfig().EnableParentClosePolicyWorker() &&
		len(childInfos) >= t.shard.GetConfig().ParentClosePolicyThreshold(domainName) {

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
			DomainUUID: domainID,
			DomainName: domainName,
			Executions: executions,
		}
		return t.parentClosePolicyClient.SendParentClosePolicyRequest(request)
	}

	for _, childInfo := range childInfos {
		if err := t.applyParentClosePolicy(
			domainID,
			domainName,
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
	domainID string,
	domainName string,
	childInfo *persistence.ChildExecutionInfo,
) error {

	ctx, cancel := ctx.WithTimeout(ctx.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()

	switch childInfo.ParentClosePolicy {
	case enums.ParentClosePolicyAbandon:
		// noop
		return nil

	case enums.ParentClosePolicyTerminate:
		_, err := t.historyClient.TerminateWorkflowExecution(ctx, &h.TerminateWorkflowExecutionRequest{
			DomainUUID: domainID,
			TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{
				Domain: domainName,
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
		_, err := t.historyClient.RequestCancelWorkflowExecution(ctx, &h.RequestCancelWorkflowExecutionRequest{
			DomainUUID: domainID,
			CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
				Domain: domainName,
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
