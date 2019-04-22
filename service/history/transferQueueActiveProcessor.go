// Copyright (c) 2017 Uber Technologies, Inc.
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
	"fmt"
	"time"

	"github.com/pborman/uuid"
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cron"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

const identityHistoryService = "history-service"

type (
	transferQueueActiveProcessorImpl struct {
		currentClusterName string
		shard              ShardContext
		historyService     *historyEngineImpl
		options            *QueueProcessorOptions
		historyClient      history.Client
		cache              *historyCache
		transferTaskFilter queueTaskFilter
		logger             log.Logger
		metricsClient      metrics.Client
		maxReadAckLevel    maxReadAckLevel
		*transferQueueProcessorBase
		*queueProcessorBase
		queueAckMgr
	}
)

func newTransferQueueActiveProcessor(shard ShardContext, historyService *historyEngineImpl, visibilityMgr persistence.VisibilityManager,
	matchingClient matching.Client, historyClient history.Client, taskAllocator taskAllocator, logger log.Logger) *transferQueueActiveProcessorImpl {
	config := shard.GetConfig()
	options := &QueueProcessorOptions{
		StartDelay:                         config.TransferProcessorStartDelay,
		BatchSize:                          config.TransferTaskBatchSize,
		WorkerCount:                        config.TransferTaskWorkerCount,
		MaxPollRPS:                         config.TransferProcessorMaxPollRPS,
		MaxPollInterval:                    config.TransferProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:   config.TransferProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                  config.TransferProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient: config.TransferProcessorUpdateAckIntervalJitterCoefficient,
		MaxRetryCount:                      config.TransferTaskMaxRetryCount,
		MetricScope:                        metrics.TransferActiveQueueProcessorScope,
	}
	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	logger = logger.WithTags(tag.ClusterName(currentClusterName))
	transferTaskFilter := func(qTask queueTaskInfo) (bool, error) {
		task, ok := qTask.(*persistence.TransferTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.verifyActiveTask(task.DomainID, task)
	}
	maxReadAckLevel := func() int64 {
		return shard.GetTransferMaxReadLevel()
	}
	updateTransferAckLevel := func(ackLevel int64) error {
		return shard.UpdateTransferClusterAckLevel(currentClusterName, ackLevel)
	}

	transferQueueShutdown := func() error {
		return nil
	}

	processor := &transferQueueActiveProcessorImpl{
		currentClusterName: currentClusterName,
		shard:              shard,
		historyService:     historyService,
		options:            options,
		historyClient:      historyClient,
		logger:             logger,
		metricsClient:      historyService.metricsClient,
		cache:              historyService.historyCache,
		transferTaskFilter: transferTaskFilter,
		transferQueueProcessorBase: newTransferQueueProcessorBase(
			shard, options, visibilityMgr, matchingClient, maxReadAckLevel, updateTransferAckLevel, transferQueueShutdown, logger,
		),
	}

	queueAckMgr := newQueueAckMgr(shard, options, processor, shard.GetTransferClusterAckLevel(currentClusterName), logger)
	queueProcessorBase := newQueueProcessorBase(currentClusterName, shard, options, processor, queueAckMgr, logger)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase

	return processor
}

func newTransferQueueFailoverProcessor(shard ShardContext, historyService *historyEngineImpl,
	visibilityMgr persistence.VisibilityManager, matchingClient matching.Client,
	historyClient history.Client, domainIDs map[string]struct{}, standbyClusterName string,
	minLevel int64, maxLevel int64, taskAllocator taskAllocator, logger log.Logger) (func(ackLevel int64) error, *transferQueueActiveProcessorImpl) {
	config := shard.GetConfig()
	options := &QueueProcessorOptions{
		StartDelay:                         config.TransferProcessorFailoverStartDelay,
		BatchSize:                          config.TransferTaskBatchSize,
		WorkerCount:                        config.TransferTaskWorkerCount,
		MaxPollRPS:                         config.TransferProcessorFailoverMaxPollRPS,
		MaxPollInterval:                    config.TransferProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:   config.TransferProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                  config.TransferProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient: config.TransferProcessorUpdateAckIntervalJitterCoefficient,
		MaxRetryCount:                      config.TransferTaskMaxRetryCount,
		MetricScope:                        metrics.TransferActiveQueueProcessorScope,
	}
	currentClusterName := shard.GetService().GetClusterMetadata().GetCurrentClusterName()
	failoverUUID := uuid.New()
	logger = logger.WithTags(
		tag.ClusterName(currentClusterName),
		tag.WorkflowDomainIDs(domainIDs),
		tag.FailoverMsg("from: "+standbyClusterName),
	)

	transferTaskFilter := func(qTask queueTaskInfo) (bool, error) {
		task, ok := qTask.(*persistence.TransferTaskInfo)
		if !ok {
			return false, errUnexpectedQueueTask
		}
		return taskAllocator.verifyFailoverActiveTask(domainIDs, task.DomainID, task)
	}
	maxReadAckLevel := func() int64 {
		return maxLevel // this is a const
	}
	failoverStartTime := time.Now()
	updateTransferAckLevel := func(ackLevel int64) error {
		return shard.UpdateTransferFailoverLevel(
			failoverUUID,
			persistence.TransferFailoverLevel{
				StartTime:    failoverStartTime,
				MinLevel:     minLevel,
				CurrentLevel: ackLevel,
				MaxLevel:     maxLevel,
				DomainIDs:    domainIDs,
			},
		)
	}
	transferQueueShutdown := func() error {
		return shard.DeleteTransferFailoverLevel(failoverUUID)
	}

	processor := &transferQueueActiveProcessorImpl{
		currentClusterName: currentClusterName,
		shard:              shard,
		historyService:     historyService,
		options:            options,
		historyClient:      historyClient,
		logger:             logger,
		metricsClient:      historyService.metricsClient,
		cache:              historyService.historyCache,
		transferTaskFilter: transferTaskFilter,
		transferQueueProcessorBase: newTransferQueueProcessorBase(
			shard, options, visibilityMgr, matchingClient,
			maxReadAckLevel, updateTransferAckLevel, transferQueueShutdown, logger,
		),
	}

	queueAckMgr := newQueueFailoverAckMgr(shard, options, processor, minLevel, logger)
	queueProcessorBase := newQueueProcessorBase(currentClusterName, shard, options, processor, queueAckMgr, logger)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase
	return updateTransferAckLevel, processor
}

func (t *transferQueueActiveProcessorImpl) getTaskFilter() queueTaskFilter {
	return t.transferTaskFilter
}

func (t *transferQueueActiveProcessorImpl) notifyNewTask() {
	t.queueProcessorBase.notifyNewTask()
}

func (t *transferQueueActiveProcessorImpl) process(qTask queueTaskInfo, shouldProcessTask bool) (int, error) {
	task, ok := qTask.(*persistence.TransferTaskInfo)
	if !ok {
		return metrics.TransferActiveQueueProcessorScope, errUnexpectedQueueTask
	}

	var err error
	switch task.TaskType {
	case persistence.TransferTaskTypeActivityTask:
		if shouldProcessTask {
			err = t.processActivityTask(task)
		}
		return metrics.TransferActiveTaskActivityScope, err

	case persistence.TransferTaskTypeDecisionTask:
		if shouldProcessTask {
			err = t.processDecisionTask(task)
		}
		return metrics.TransferActiveTaskDecisionScope, err

	case persistence.TransferTaskTypeCloseExecution:
		if shouldProcessTask {
			err = t.processCloseExecution(task)
		}
		return metrics.TransferActiveTaskCloseExecutionScope, err

	case persistence.TransferTaskTypeCancelExecution:
		if shouldProcessTask {
			err = t.processCancelExecution(task)
		}
		return metrics.TransferActiveTaskCancelExecutionScope, err

	case persistence.TransferTaskTypeSignalExecution:
		if shouldProcessTask {
			err = t.processSignalExecution(task)
		}
		return metrics.TransferActiveTaskSignalExecutionScope, err

	case persistence.TransferTaskTypeStartChildExecution:
		if shouldProcessTask {
			err = t.processStartChildExecution(task)
		}
		return metrics.TransferActiveTaskStartChildExecutionScope, err

	case persistence.TransferTaskTypeRecordWorkflowStarted:
		if shouldProcessTask {
			err = t.processRecordWorkflowStarted(task)
		}
		return metrics.TransferActiveTaskRecordWorkflowStartedScope, err

	default:
		return metrics.TransferActiveQueueProcessorScope, errUnknownTransferTask
	}
}

func (t *transferQueueActiveProcessorImpl) processActivityTask(task *persistence.TransferTaskInfo) (retError error) {

	var err error
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID)}

	context, release, err := t.cache.getOrCreateWorkflowExecution(task.DomainID, execution)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	var msBuilder mutableState
	msBuilder, err = loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
		return nil
	}

	ai, found := msBuilder.GetActivityInfo(task.ScheduleID)
	if !found {
		t.logger.Debug("Potentially duplicate task.", tag.TaskID(task.TaskID), tag.WorkflowScheduleID(task.ScheduleID), tag.TaskType(persistence.TransferTaskTypeActivityTask))
		return
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, ai.Version, task.Version, task)
	if err != nil {
		return err
	} else if !ok {
		return nil
	}

	timeout := common.MinInt32(ai.ScheduleToStartTimeout, common.MaxTaskTimeout)
	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	return t.pushActivity(task, timeout)
}

func (t *transferQueueActiveProcessorImpl) processDecisionTask(task *persistence.TransferTaskInfo) (retError error) {
	var err error
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
	tasklist := &workflow.TaskList{
		Name: &task.TaskList,
	}

	// get workflow timeout
	context, release, err := t.cache.getOrCreateWorkflowExecution(task.DomainID, execution)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	var msBuilder mutableState
	msBuilder, err = loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
		return nil
	}

	di, found := msBuilder.GetPendingDecision(task.ScheduleID)
	if !found {
		t.logger.Debug("Potentially duplicate task.", tag.TaskID(task.TaskID), tag.WorkflowScheduleID(task.ScheduleID), tag.TaskType(persistence.TransferTaskTypeDecisionTask))
		return nil
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, di.Version, task.Version, task)
	if err != nil {
		return err
	} else if !ok {
		return nil
	}

	executionInfo := msBuilder.GetExecutionInfo()
	workflowTimeout := executionInfo.WorkflowTimeout
	decisionTimeout := common.MinInt32(workflowTimeout, common.MaxTaskTimeout)

	// NOTE: previously this section check whether mutable state has enabled
	// sticky decision, if so convert the decision to a sticky decision.
	// that logic has a bug which timer task for that sticky decision is not generated
	// the correct logic should check whether the decision task is a sticky decision
	// task or not.
	if msBuilder.GetExecutionInfo().TaskList != task.TaskList {
		// this decision is an sticky decision
		// there shall already be an timer set
		tasklist.Kind = common.TaskListKindPtr(workflow.TaskListKindSticky)
		decisionTimeout = executionInfo.StickyScheduleToStartTimeout
	}

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	return t.pushDecision(task, tasklist, decisionTimeout)
}

func (t *transferQueueActiveProcessorImpl) processCloseExecution(task *persistence.TransferTaskInfo) (retError error) {

	var err error
	domainID := task.DomainID
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}

	context, release, err := t.cache.getOrCreateWorkflowExecution(domainID, execution)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	var msBuilder mutableState
	msBuilder, err = loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil || msBuilder.IsWorkflowExecutionRunning() {
		// this can happen if workflow is reset.
		return nil
	}

	ok, err := verifyTaskVersion(t.shard, t.logger, domainID, msBuilder.GetLastWriteVersion(), task.Version, task)
	if err != nil {
		return err
	} else if !ok {
		return nil
	}

	executionInfo := msBuilder.GetExecutionInfo()
	replyToParentWorkflow := msBuilder.HasParentExecution() && executionInfo.CloseStatus != persistence.WorkflowCloseStatusContinuedAsNew
	completionEvent, ok := msBuilder.GetCompletionEvent()
	var wfCloseTime int64
	if !ok {
		if replyToParentWorkflow {
			return &workflow.InternalServiceError{Message: "Unable to get workflow completion event."}
		}

		// This is need for backwards compatibility
		// TODO: remove usage of getLastUpdatedTimestamp after release 0.5.4, only use completionEvent timestamp
		wfCloseTime = getLastUpdatedTimestamp(msBuilder)
	} else {
		wfCloseTime = completionEvent.GetTimestamp()
	}
	parentDomainID := executionInfo.ParentDomainID
	parentWorkflowID := executionInfo.ParentWorkflowID
	parentRunID := executionInfo.ParentRunID
	initiatedID := executionInfo.InitiatedID

	workflowTypeName := executionInfo.WorkflowTypeName
	workflowStartTimestamp := executionInfo.StartTimestamp.UnixNano()
	workflowCloseTimestamp := wfCloseTime
	workflowCloseStatus := getWorkflowExecutionCloseStatus(executionInfo.CloseStatus)
	workflowHistoryLength := msBuilder.GetNextEventID() - 1

	startEvent, ok := msBuilder.GetStartEvent()
	if !ok && replyToParentWorkflow {
		return &workflow.InternalServiceError{Message: "Unable to get workflow start event."}
	}
	workflowExecutionTimestamp := getWorkflowExecutionTimestamp(msBuilder, startEvent)
	visibilityMemo := getVisibilityMemo(startEvent)

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	err = t.recordWorkflowClosed(
		domainID, execution, workflowTypeName, workflowStartTimestamp, workflowExecutionTimestamp.UnixNano(),
		workflowCloseTimestamp, workflowCloseStatus, workflowHistoryLength, task.GetTaskID(), visibilityMemo,
	)
	if err != nil {
		return err
	}

	// Communicate the result to parent execution if this is Child Workflow execution
	if replyToParentWorkflow {
		err = t.historyClient.RecordChildExecutionCompleted(nil, &h.RecordChildExecutionCompletedRequest{
			DomainUUID: common.StringPtr(parentDomainID),
			WorkflowExecution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(parentWorkflowID),
				RunId:      common.StringPtr(parentRunID),
			},
			InitiatedId: common.Int64Ptr(initiatedID),
			CompletedExecution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(task.WorkflowID),
				RunId:      common.StringPtr(task.RunID),
			},
			CompletionEvent: completionEvent,
		})

		// Check to see if the error is non-transient, in which case reset the error and continue with processing
		switch err.(type) {
		case *workflow.EntityNotExistsError:
			err = nil
		}
	}
	return err
}

func (t *transferQueueActiveProcessorImpl) processCancelExecution(task *persistence.TransferTaskInfo) (retError error) {

	var err error
	domainID := task.DomainID
	targetDomainID := task.TargetDomainID
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}

	var context workflowExecutionContext
	var release releaseWorkflowExecutionFunc
	context, release, err = t.cache.getOrCreateWorkflowExecution(domainID, execution)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	// First load the execution to validate if there is pending request cancellation for this transfer task
	var msBuilder mutableState
	msBuilder, err = loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
		return nil
	}

	initiatedEventID := task.ScheduleID
	ri, found := msBuilder.GetRequestCancelInfo(initiatedEventID)
	if !found {
		t.logger.Debug("Potentially duplicate task.", tag.TaskID(task.TaskID), tag.WorkflowScheduleID(task.ScheduleID), tag.TaskType(persistence.TransferTaskTypeCancelExecution))
		return nil
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, domainID, ri.Version, task.Version, task)
	if err != nil {
		return err
	} else if !ok {
		return nil
	}

	// handle workflow cancel itself
	if domainID == targetDomainID && task.WorkflowID == task.TargetWorkflowID {
		// it does not matter if the run ID is a mismatch
		cancelRequest := &h.RequestCancelWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(domainID),
			CancelRequest: &workflow.RequestCancelWorkflowExecutionRequest{
				Domain: common.StringPtr(targetDomainID),
				WorkflowExecution: &workflow.WorkflowExecution{
					WorkflowId: common.StringPtr(task.TargetWorkflowID),
					RunId:      common.StringPtr(task.TargetRunID),
				},
				Identity: common.StringPtr(identityHistoryService),
			},
		}
		err = t.requestCancelFailed(task, context, cancelRequest)
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
		return err
	}

	cancelRequest := &h.RequestCancelWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(targetDomainID),
		CancelRequest: &workflow.RequestCancelWorkflowExecutionRequest{
			Domain: common.StringPtr(targetDomainID),
			WorkflowExecution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(task.TargetWorkflowID),
				RunId:      common.StringPtr(task.TargetRunID),
			},
			Identity: common.StringPtr(identityHistoryService),
			// Use the same request ID to dedupe RequestCancelWorkflowExecution calls
			RequestId: common.StringPtr(ri.CancelRequestID),
		},
		ExternalInitiatedEventId: common.Int64Ptr(task.ScheduleID),
		ExternalWorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(task.WorkflowID),
			RunId:      common.StringPtr(task.RunID),
		},
		ChildWorkflowOnly: common.BoolPtr(task.TargetChildWorkflowOnly),
	}

	op := func() error {
		return t.historyClient.RequestCancelWorkflowExecution(nil, cancelRequest)
	}

	err = backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		if _, ok := err.(*workflow.CancellationAlreadyRequestedError); ok {
			// this could happen if target workflow cancellation is alreay requested
			// to make workflow cancellation idempotent, we should clear this error.
			err = nil
		} else {
			t.logger.Debug(fmt.Sprintf("Failed to cancel external workflow execution. Error: %v", err))
			// Check to see if the error is non-transient, in which case add RequestCancelFailed
			// event and complete transfer task by setting the err = nil
			if common.IsServiceNonRetryableError(err) {
				err = t.requestCancelFailed(task, context, cancelRequest)
				if _, ok := err.(*workflow.EntityNotExistsError); ok {
					// this could happen if this is a duplicate processing of the task, and the execution has already completed.
					return nil
				}
			}
			return err
		}
	}

	t.logger.Debug(fmt.Sprintf("RequestCancel successfully recorded to external workflow execution.  WorkflowID: %v, RunID: %v",
		task.TargetWorkflowID, task.TargetRunID))

	// Record ExternalWorkflowExecutionCancelRequested in source execution
	err = t.requestCancelCompleted(task, context, cancelRequest)
	if _, ok := err.(*workflow.EntityNotExistsError); ok {
		// this could happen if this is a duplicate processing of the task, and the execution has already completed.
		return nil
	}

	return err
}

func (t *transferQueueActiveProcessorImpl) processSignalExecution(task *persistence.TransferTaskInfo) (retError error) {

	var err error
	domainID := task.DomainID
	targetDomainID := task.TargetDomainID
	execution := workflow.WorkflowExecution{WorkflowId: common.StringPtr(task.WorkflowID),
		RunId: common.StringPtr(task.RunID)}

	var context workflowExecutionContext
	var release releaseWorkflowExecutionFunc
	context, release, err = t.cache.getOrCreateWorkflowExecution(domainID, execution)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	var msBuilder mutableState
	msBuilder, err = loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
		return nil
	}

	initiatedEventID := task.ScheduleID
	si, found := msBuilder.GetSignalInfo(initiatedEventID)
	if !found {
		// TODO: here we should also RemoveSignalMutableState from target workflow
		// Otherwise, target SignalRequestID still can leak if shard restart after requestSignalCompleted
		// To do that, probably need to add the SignalRequestID in transfer task.
		t.logger.Debug("Potentially duplicate task.", tag.TaskID(task.TaskID), tag.WorkflowScheduleID(task.ScheduleID), tag.TaskType(persistence.TransferTaskTypeSignalExecution))
		return nil
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, domainID, si.Version, task.Version, task)
	if err != nil {
		return err
	} else if !ok {
		return nil
	}

	// handle workflow signal itself
	if domainID == targetDomainID && task.WorkflowID == task.TargetWorkflowID {
		// it does not matter if the run ID is a mismatch
		signalRequest := &h.SignalWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(targetDomainID),
			SignalRequest: &workflow.SignalWorkflowExecutionRequest{
				Domain: common.StringPtr(targetDomainID),
				WorkflowExecution: &workflow.WorkflowExecution{
					WorkflowId: common.StringPtr(task.TargetWorkflowID),
					RunId:      common.StringPtr(task.TargetRunID),
				},
				Identity: common.StringPtr(identityHistoryService),
				Control:  si.Control,
			},
		}
		err = t.requestSignalFailed(task, context, signalRequest)
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
		return err
	}

	signalRequest := &h.SignalWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(targetDomainID),
		SignalRequest: &workflow.SignalWorkflowExecutionRequest{
			Domain: common.StringPtr(targetDomainID),
			WorkflowExecution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(task.TargetWorkflowID),
				RunId:      common.StringPtr(task.TargetRunID),
			},
			Identity:   common.StringPtr(identityHistoryService),
			SignalName: common.StringPtr(si.SignalName),
			Input:      si.Input,
			// Use same request ID to deduplicate SignalWorkflowExecution calls
			RequestId: common.StringPtr(si.SignalRequestID),
			Control:   si.Control,
		},
		ExternalWorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(task.WorkflowID),
			RunId:      common.StringPtr(task.RunID),
		},
		ChildWorkflowOnly: common.BoolPtr(task.TargetChildWorkflowOnly),
	}

	err = t.SignalExecutionWithRetry(signalRequest)

	if err != nil {
		t.logger.Debug(fmt.Sprintf("Failed to signal external workflow execution. Error: %v", err))

		// Check to see if the error is non-transient, in which case add SignalFailed
		// event and complete transfer task by setting the err = nil
		if common.IsServiceNonRetryableError(err) {
			err = t.requestSignalFailed(task, context, signalRequest)
			if _, ok := err.(*workflow.EntityNotExistsError); ok {
				// this could happen if this is a duplicate processing of the task, and the execution has already completed.
				return nil
			}
		}
		return err
	}

	t.logger.Debug(fmt.Sprintf("Signal successfully recorded to external workflow execution.  WorkflowID: %v, RunID: %v",
		task.TargetWorkflowID, task.TargetRunID))

	err = t.requestSignalCompleted(task, context, signalRequest)
	if _, ok := err.(*workflow.EntityNotExistsError); ok {
		// this could happen if this is a duplicate processing of the task, and the execution has already completed.
		return nil
	}

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(retError)
	// remove signalRequestedID from target workflow, after Signal detail is removed from source workflow
	removeRequest := &h.RemoveSignalMutableStateRequest{
		DomainUUID: common.StringPtr(targetDomainID),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(task.TargetWorkflowID),
			RunId:      common.StringPtr(task.TargetRunID),
		},
		RequestId: common.StringPtr(si.SignalRequestID),
	}

	t.historyClient.RemoveSignalMutableState(nil, removeRequest)

	return err
}

func (t *transferQueueActiveProcessorImpl) processStartChildExecution(task *persistence.TransferTaskInfo) (retError error) {

	var err error
	domainID := task.DomainID
	targetDomainID := task.TargetDomainID
	execution := workflow.WorkflowExecution{WorkflowId: common.StringPtr(task.WorkflowID),
		RunId: common.StringPtr(task.RunID)}

	var context workflowExecutionContext
	var release releaseWorkflowExecutionFunc
	context, release, err = t.cache.getOrCreateWorkflowExecution(domainID, execution)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	// First step is to load workflow execution so we can retrieve the initiated event
	var msBuilder mutableState
	msBuilder, err = loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
		return nil
	}

	// Get parent domain name
	var domain string
	if domainEntry, err := t.shard.GetDomainCache().GetDomainByID(domainID); err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			return err
		}
		// it is possible that the domain got deleted. Use domainID instead as this is only needed for the history event
		domain = domainID
	} else {
		domain = domainEntry.GetInfo().Name
	}

	// Get target domain name
	var targetDomain string
	if domainEntry, err := t.shard.GetDomainCache().GetDomainByID(targetDomainID); err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			return err
		}
		// it is possible that the domain got deleted. Use domainID instead as this is only needed for the history event
		targetDomain = targetDomainID
	} else {
		targetDomain = domainEntry.GetInfo().Name
	}

	initiatedEventID := task.ScheduleID
	ci, isRunning := msBuilder.GetChildExecutionInfo(initiatedEventID)
	if !isRunning {
		t.logger.Debug("Potentially duplicate task.", tag.TaskID(task.TaskID), tag.WorkflowScheduleID(task.ScheduleID), tag.TaskType(persistence.TransferTaskTypeStartChildExecution))
		return nil
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, domainID, ci.Version, task.Version, task)
	if err != nil {
		return err
	} else if !ok {
		return nil
	}

	initiatedEvent, ok := msBuilder.GetChildExecutionInitiatedEvent(initiatedEventID)
	if ok && ci.StartedID == common.EmptyEventID {
		attributes := initiatedEvent.StartChildWorkflowExecutionInitiatedEventAttributes
		// Found pending child execution and it is not marked as started
		// Let's try and start the child execution
		startRequest := &h.StartWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(targetDomainID),
			StartRequest: &workflow.StartWorkflowExecutionRequest{
				Domain:                              common.StringPtr(targetDomain),
				WorkflowId:                          attributes.WorkflowId,
				WorkflowType:                        attributes.WorkflowType,
				TaskList:                            attributes.TaskList,
				Input:                               attributes.Input,
				ExecutionStartToCloseTimeoutSeconds: attributes.ExecutionStartToCloseTimeoutSeconds,
				TaskStartToCloseTimeoutSeconds:      attributes.TaskStartToCloseTimeoutSeconds,
				// Use the same request ID to dedupe StartWorkflowExecution calls
				RequestId:             common.StringPtr(ci.CreateRequestID),
				WorkflowIdReusePolicy: attributes.WorkflowIdReusePolicy,
				ChildPolicy:           attributes.ChildPolicy,
				RetryPolicy:           attributes.RetryPolicy,
				CronSchedule:          attributes.CronSchedule,
			},
			ParentExecutionInfo: &h.ParentExecutionInfo{
				DomainUUID: common.StringPtr(domainID),
				Domain:     common.StringPtr(domain),
				Execution: &workflow.WorkflowExecution{
					WorkflowId: common.StringPtr(task.WorkflowID),
					RunId:      common.StringPtr(task.RunID),
				},
				InitiatedId: common.Int64Ptr(initiatedEventID),
			},
			FirstDecisionTaskBackoffSeconds: common.Int32Ptr(cron.GetBackoffForNextScheduleInSeconds(attributes.GetCronSchedule(), time.Now())),
		}

		var startResponse *workflow.StartWorkflowExecutionResponse
		startResponse, err = t.historyClient.StartWorkflowExecution(nil, startRequest)
		if err != nil {
			t.logger.Debug(fmt.Sprintf("Failed to start child workflow execution. Error: %v", err))

			// Check to see if the error is non-transient, in which case add StartChildWorkflowExecutionFailed
			// event and complete transfer task by setting the err = nil
			switch err.(type) {
			case *workflow.WorkflowExecutionAlreadyStartedError:
				err = t.recordStartChildExecutionFailed(task, context, attributes)
			}
			return err
		}

		t.logger.Debug(fmt.Sprintf("Child Execution started successfully.  WorkflowID: %v, RunID: %v",
			*attributes.WorkflowId, *startResponse.RunId))

		// Child execution is successfully started, record ChildExecutionStartedEvent in parent execution
		err = t.recordChildExecutionStarted(task, context, attributes, *startResponse.RunId)

		if err != nil {
			return err
		}
		// Finally create first decision task for Child execution so it is really started
		err = t.createFirstDecisionTask(targetDomainID, &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(task.TargetWorkflowID),
			RunId:      common.StringPtr(*startResponse.RunId),
		})
	} else {
		// ChildExecution already started, just create DecisionTask and complete transfer task
		childExecution := &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(ci.StartedWorkflowID),
			RunId:      common.StringPtr(ci.StartedRunID),
		}
		err = t.createFirstDecisionTask(targetDomainID, childExecution)
	}

	return err
}

func (t *transferQueueActiveProcessorImpl) processRecordWorkflowStarted(task *persistence.TransferTaskInfo) (retError error) {
	var err error
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}

	// get workflow timeout
	context, release, err := t.cache.getOrCreateWorkflowExecution(task.DomainID, execution)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	var msBuilder mutableState
	msBuilder, err = loadMutableStateForTransferTask(context, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	} else if msBuilder == nil || !msBuilder.IsWorkflowExecutionRunning() {
		return nil
	}

	ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, msBuilder.GetStartVersion(), task.Version, task)
	if err != nil {
		return err
	} else if !ok {
		return nil
	}

	executionInfo := msBuilder.GetExecutionInfo()
	workflowTimeout := executionInfo.WorkflowTimeout
	wfTypeName := executionInfo.WorkflowTypeName
	startTimestamp := executionInfo.StartTimestamp.UnixNano()
	startEvent, _ := msBuilder.GetStartEvent()
	executionTimestamp := getWorkflowExecutionTimestamp(msBuilder, startEvent)
	visibilityMemo := getVisibilityMemo(startEvent)

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release(nil)
	return t.recordWorkflowStarted(task.DomainID, execution, wfTypeName, startTimestamp, executionTimestamp.UnixNano(), workflowTimeout, task.GetTaskID(), visibilityMemo)
}

func (t *transferQueueActiveProcessorImpl) recordChildExecutionStarted(task *persistence.TransferTaskInfo,
	context workflowExecutionContext, initiatedAttributes *workflow.StartChildWorkflowExecutionInitiatedEventAttributes,
	runID string) error {

	return t.updateWorkflowExecution(task.DomainID, context, true,
		func(msBuilder mutableState) error {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return &workflow.EntityNotExistsError{Message: "Workflow execution already completed."}
			}

			domain := initiatedAttributes.Domain
			initiatedEventID := task.ScheduleID
			ci, isRunning := msBuilder.GetChildExecutionInfo(initiatedEventID)
			if !isRunning || ci.StartedID != common.EmptyEventID {
				return &workflow.EntityNotExistsError{Message: "Pending child execution not found."}
			}

			msBuilder.AddChildWorkflowExecutionStartedEvent(domain,
				&workflow.WorkflowExecution{
					WorkflowId: common.StringPtr(task.TargetWorkflowID),
					RunId:      common.StringPtr(runID),
				}, initiatedAttributes.WorkflowType, initiatedEventID)

			return nil
		})
}

func (t *transferQueueActiveProcessorImpl) recordStartChildExecutionFailed(task *persistence.TransferTaskInfo,
	context workflowExecutionContext,
	initiatedAttributes *workflow.StartChildWorkflowExecutionInitiatedEventAttributes) error {

	return t.updateWorkflowExecution(task.DomainID, context, true,
		func(msBuilder mutableState) error {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return &workflow.EntityNotExistsError{Message: "Workflow execution already completed."}
			}

			initiatedEventID := task.ScheduleID
			ci, isRunning := msBuilder.GetChildExecutionInfo(initiatedEventID)
			if !isRunning || ci.StartedID != common.EmptyEventID {
				return &workflow.EntityNotExistsError{Message: "Pending child execution not found."}
			}

			msBuilder.AddStartChildWorkflowExecutionFailedEvent(initiatedEventID,
				workflow.ChildWorkflowExecutionFailedCauseWorkflowAlreadyRunning, initiatedAttributes)

			return nil
		})
}

// createFirstDecisionTask is used by StartChildExecution transfer task to create the first decision task for
// child execution.
func (t *transferQueueActiveProcessorImpl) createFirstDecisionTask(domainID string,
	execution *workflow.WorkflowExecution) error {
	err := t.historyClient.ScheduleDecisionTask(nil, &h.ScheduleDecisionTaskRequest{
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: execution,
		IsFirstDecision:   common.BoolPtr(true),
	})

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// Maybe child workflow execution already timedout or terminated
			// Safe to discard the error and complete this transfer task
			return nil
		}
	}

	return err
}

func (t *transferQueueActiveProcessorImpl) requestCancelCompleted(task *persistence.TransferTaskInfo,
	context workflowExecutionContext, request *h.RequestCancelWorkflowExecutionRequest) error {

	return t.updateWorkflowExecution(task.DomainID, context, true,
		func(msBuilder mutableState) error {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return &workflow.EntityNotExistsError{Message: "Workflow execution already completed."}
			}

			initiatedEventID := task.ScheduleID
			_, isPending := msBuilder.GetRequestCancelInfo(initiatedEventID)
			if !isPending {
				return &workflow.EntityNotExistsError{Message: "Pending request cancellation not found."}
			}

			msBuilder.AddExternalWorkflowExecutionCancelRequested(
				initiatedEventID,
				request.GetDomainUUID(),
				request.CancelRequest.WorkflowExecution.GetWorkflowId(),
				request.CancelRequest.WorkflowExecution.GetRunId(),
			)

			return nil
		})
}

func (t *transferQueueActiveProcessorImpl) requestSignalCompleted(task *persistence.TransferTaskInfo,
	context workflowExecutionContext,
	request *h.SignalWorkflowExecutionRequest) error {

	return t.updateWorkflowExecution(task.DomainID, context, true,
		func(msBuilder mutableState) error {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return &workflow.EntityNotExistsError{Message: "Workflow execution already completed."}
			}

			initiatedEventID := task.ScheduleID
			_, isPending := msBuilder.GetSignalInfo(initiatedEventID)
			if !isPending {
				return &workflow.EntityNotExistsError{Message: "Pending signal request not found."}
			}

			msBuilder.AddExternalWorkflowExecutionSignaled(
				initiatedEventID,
				request.GetDomainUUID(),
				request.SignalRequest.WorkflowExecution.GetWorkflowId(),
				request.SignalRequest.WorkflowExecution.GetRunId(),
				request.SignalRequest.Control)

			return nil
		})
}

func (t *transferQueueActiveProcessorImpl) requestCancelFailed(task *persistence.TransferTaskInfo,
	context workflowExecutionContext, request *h.RequestCancelWorkflowExecutionRequest) error {

	return t.updateWorkflowExecution(task.DomainID, context, true,
		func(msBuilder mutableState) error {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return &workflow.EntityNotExistsError{Message: "Workflow execution already completed."}
			}

			initiatedEventID := task.ScheduleID
			_, isPending := msBuilder.GetRequestCancelInfo(initiatedEventID)
			if !isPending {
				return &workflow.EntityNotExistsError{Message: "Pending request cancellation not found."}
			}

			msBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(
				common.EmptyEventID,
				initiatedEventID,
				request.GetDomainUUID(),
				request.CancelRequest.WorkflowExecution.GetWorkflowId(),
				request.CancelRequest.WorkflowExecution.GetRunId(),
				workflow.CancelExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution)

			return nil
		})
}

func (t *transferQueueActiveProcessorImpl) requestSignalFailed(task *persistence.TransferTaskInfo,
	context workflowExecutionContext,
	request *h.SignalWorkflowExecutionRequest) error {

	return t.updateWorkflowExecution(task.DomainID, context, true,
		func(msBuilder mutableState) error {
			if !msBuilder.IsWorkflowExecutionRunning() {
				return &workflow.EntityNotExistsError{Message: "Workflow is not running."}
			}

			initiatedEventID := task.ScheduleID
			_, isPending := msBuilder.GetSignalInfo(initiatedEventID)
			if !isPending {
				return &workflow.EntityNotExistsError{Message: "Pending signal request not found."}
			}

			msBuilder.AddSignalExternalWorkflowExecutionFailedEvent(
				common.EmptyEventID,
				initiatedEventID,
				request.GetDomainUUID(),
				request.SignalRequest.WorkflowExecution.GetWorkflowId(),
				request.SignalRequest.WorkflowExecution.GetRunId(),
				request.SignalRequest.Control,
				workflow.SignalExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution)

			return nil
		})
}

func (t *transferQueueActiveProcessorImpl) updateWorkflowExecution(domainID string, context workflowExecutionContext,
	createDecisionTask bool, action func(builder mutableState) error) error {
Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		msBuilder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}

		var transferTasks []persistence.Task
		var timerTasks []persistence.Task
		if err := action(msBuilder); err != nil {
			return err
		}

		if createDecisionTask {
			// Create a transfer task to schedule a decision task
			var err error
			transferTasks, timerTasks, err = context.scheduleNewDecision(transferTasks, timerTasks)
			if err != nil {
				return err
			}
		}

		// Generate a transaction ID for appending events to history
		transactionID, err2 := t.shard.GetNextTransferTaskID()
		if err2 != nil {
			return err2
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict then reload
		// the history and try the operation again.
		if err := context.updateWorkflowExecution(transferTasks, timerTasks, transactionID); err != nil {
			if err == ErrConflict {
				continue Update_History_Loop
			}
			return err
		}

		t.historyService.timerProcessor.NotifyNewTimers(t.currentClusterName, t.shard.GetCurrentTime(t.currentClusterName), timerTasks)

		return nil
	}

	return ErrMaxAttemptsExceeded
}

func (t *transferQueueActiveProcessorImpl) SignalExecutionWithRetry(signalRequest *h.SignalWorkflowExecutionRequest) error {
	op := func() error {
		return t.historyClient.SignalWorkflowExecution(nil, signalRequest)
	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func getWorkflowExecutionCloseStatus(status int) workflow.WorkflowExecutionCloseStatus {
	switch status {
	case persistence.WorkflowCloseStatusCompleted:
		return workflow.WorkflowExecutionCloseStatusCompleted
	case persistence.WorkflowCloseStatusFailed:
		return workflow.WorkflowExecutionCloseStatusFailed
	case persistence.WorkflowCloseStatusCanceled:
		return workflow.WorkflowExecutionCloseStatusCanceled
	case persistence.WorkflowCloseStatusTerminated:
		return workflow.WorkflowExecutionCloseStatusTerminated
	case persistence.WorkflowCloseStatusContinuedAsNew:
		return workflow.WorkflowExecutionCloseStatusContinuedAsNew
	case persistence.WorkflowCloseStatusTimedOut:
		return workflow.WorkflowExecutionCloseStatusTimedOut
	default:
		panic("Invalid value for enum WorkflowExecutionCloseStatus")
	}
}

// TODO: remove this after release 0.5.4
func getLastUpdatedTimestamp(msBuilder mutableState) int64 {
	executionInfo := msBuilder.GetExecutionInfo()

	lastUpdated := executionInfo.LastUpdatedTimestamp.UnixNano()
	if executionInfo.StartTimestamp.UnixNano() >= lastUpdated {
		// This could happen due to clock skews
		// ensure that the lastUpdatedTimestamp is always greater than the StartTimestamp
		lastUpdated = executionInfo.StartTimestamp.UnixNano() + 1
	}

	return lastUpdated
}
