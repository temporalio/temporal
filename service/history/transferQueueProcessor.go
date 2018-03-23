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
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"

	"fmt"

	"github.com/uber/cadence/.gen/go/history"
	m "github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
	hc "github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

const identityHistoryService = "history-service"

type (
	transferQueueProcessorImpl struct {
		shard             ShardContext
		ackMgr            *ackManager
		executionManager  persistence.ExecutionManager
		visibilityManager persistence.VisibilityManager
		matchingClient    matching.Client
		historyClient     hc.Client
		cache             *historyCache
		domainCache       cache.DomainCache
		rateLimiter       common.TokenBucket // Read rate limiter
		appendCh          chan struct{}
		isStarted         int32
		isStopped         int32
		shutdownWG        sync.WaitGroup
		shutdownCh        chan struct{}
		config            *Config
		logger            bark.Logger
		metricsClient     metrics.Client
		historyService    *historyEngineImpl
	}

	// ackManager is created by transferQueueProcessor to keep track of the transfer queue ackLevel for the shard.
	// It keeps track of read level when dispatching transfer tasks to processor and maintains a map of outstanding tasks.
	// Outstanding tasks map uses the task id sequencer as the key, which is used by updateAckLevel to move the ack level
	// for the shard when all preceding tasks are acknowledged.
	ackManager struct {
		processor     *transferQueueProcessorImpl
		shard         ShardContext
		executionMgr  persistence.ExecutionManager
		logger        bark.Logger
		metricsClient metrics.Client
		lastUpdated   time.Time
		config        *Config

		sync.RWMutex
		outstandingTasks map[int64]bool
		readLevel        int64
		maxReadLevel     int64
		ackLevel         int64
	}
)

func newTransferQueueProcessor(shard ShardContext, historyService *historyEngineImpl,
	visibilityMgr persistence.VisibilityManager, matching matching.Client, historyClient hc.Client) transferQueueProcessor {
	executionManager := shard.GetExecutionManager()
	logger := shard.GetLogger()
	config := shard.GetConfig()
	processor := &transferQueueProcessorImpl{
		historyService:    historyService,
		shard:             shard,
		executionManager:  executionManager,
		matchingClient:    matching,
		historyClient:     historyClient,
		visibilityManager: visibilityMgr,
		cache:             historyService.historyCache,
		domainCache:       historyService.domainCache,
		rateLimiter:       common.NewTokenBucket(config.TransferProcessorMaxPollRPS, common.NewRealTimeSource()),
		appendCh:          make(chan struct{}, 1),
		shutdownCh:        make(chan struct{}),
		config:            config,
		logger: logger.WithFields(bark.Fields{
			logging.TagWorkflowComponent: logging.TagValueTransferQueueComponent,
		}),
		metricsClient: shard.GetMetricsClient(),
	}
	processor.ackMgr = newAckManager(processor, shard, executionManager, logger, shard.GetMetricsClient())

	return processor
}

func newAckManager(processor *transferQueueProcessorImpl, shard ShardContext, executionMgr persistence.ExecutionManager,
	logger bark.Logger, metricsClient metrics.Client) *ackManager {
	ackLevel := shard.GetTransferAckLevel()
	config := shard.GetConfig()
	return &ackManager{
		processor:        processor,
		shard:            shard,
		executionMgr:     executionMgr,
		outstandingTasks: make(map[int64]bool),
		readLevel:        ackLevel,
		ackLevel:         ackLevel,
		logger:           logger,
		metricsClient:    metricsClient,
		lastUpdated:      time.Now(),
		config:           config,
	}
}

func (t *transferQueueProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&t.isStarted, 0, 1) {
		return
	}

	logging.LogTransferQueueProcesorStartingEvent(t.logger)
	defer logging.LogTransferQueueProcesorStartedEvent(t.logger)

	t.shutdownWG.Add(1)
	t.NotifyNewTask()
	go t.processorPump()
}

func (t *transferQueueProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&t.isStopped, 0, 1) {
		return
	}

	logging.LogTransferQueueProcesorShuttingDownEvent(t.logger)
	defer logging.LogTransferQueueProcesorShutdownEvent(t.logger)

	if atomic.LoadInt32(&t.isStarted) == 1 {
		close(t.shutdownCh)
	}

	if success := common.AwaitWaitGroup(&t.shutdownWG, time.Minute); !success {
		logging.LogTransferQueueProcesorShutdownTimedoutEvent(t.logger)
	}
}

func (t *transferQueueProcessorImpl) NotifyNewTask() {
	var event struct{}
	select {
	case t.appendCh <- event:
	default: // channel already has an event, don't block
	}
}

func (t *transferQueueProcessorImpl) processorPump() {
	defer t.shutdownWG.Done()
	tasksCh := make(chan *persistence.TransferTaskInfo, t.config.TransferTaskBatchSize)

	var workerWG sync.WaitGroup
	for i := 0; i < t.config.TransferTaskWorkerCount; i++ {
		workerWG.Add(1)
		go t.taskWorker(tasksCh, &workerWG)
	}

	pollTimer := time.NewTimer(t.config.TransferProcessorMaxPollInterval)
	updateAckTimer := time.NewTimer(t.config.TransferProcessorUpdateAckInterval)

processorPumpLoop:
	for {
		select {
		case <-t.shutdownCh:
			break processorPumpLoop
		case <-t.appendCh:
			t.processTransferTasks(tasksCh)
		case <-pollTimer.C:
			t.processTransferTasks(tasksCh)
			pollTimer = time.NewTimer(t.config.TransferProcessorMaxPollInterval)
		case <-updateAckTimer.C:
			t.ackMgr.updateAckLevel()
			updateAckTimer = time.NewTimer(t.config.TransferProcessorUpdateAckInterval)
		}
	}

	t.logger.Info("Transfer queue processor pump shutting down.")
	// This is the only pump which writes to tasksCh, so it is safe to close channel here
	close(tasksCh)
	if success := common.AwaitWaitGroup(&workerWG, 10*time.Second); !success {
		t.logger.Warn("Transfer queue processor timed out on worker shutdown.")
	}
	updateAckTimer.Stop()
	pollTimer.Stop()
}

func (t *transferQueueProcessorImpl) processTransferTasks(tasksCh chan<- *persistence.TransferTaskInfo) {

	if !t.rateLimiter.Consume(1, t.config.TransferProcessorMaxPollInterval) {
		t.NotifyNewTask() // re-enqueue the event
		return
	}

	tasks, err := t.ackMgr.readTransferTasks()

	if err != nil {
		t.logger.Warnf("Processor unable to retrieve transfer tasks: %v", err)
		t.NotifyNewTask() // re-enqueue the event
		return
	}

	if len(tasks) == 0 {
		return
	}

	// the transfer tasks are not guaranteed to be executed in serious.
	// since there are multiple workers polling from this task channel
	// so if one workflow, reports a series of decitions, one depend on another,
	// e.g. 1. start child workflow; 2. signal this workflow, the execution order
	// is not guatanteed.
	for _, tsk := range tasks {
		tasksCh <- tsk
	}

	if len(tasks) == t.config.TransferTaskBatchSize {
		// There might be more task
		// We return now to yield, but enqueue an event to poll later
		t.NotifyNewTask()
	}
	return
}

func (t *transferQueueProcessorImpl) taskWorker(tasksCh <-chan *persistence.TransferTaskInfo, workerWG *sync.WaitGroup) {
	defer workerWG.Done()
	for {
		select {
		case task, ok := <-tasksCh:
			if !ok {
				return
			}

			t.processTransferTask(task)
		}
	}
}

func (t *transferQueueProcessorImpl) processTransferTask(task *persistence.TransferTaskInfo) {
	t.logger.Debugf("Processing transfer task: %v, type: %v", task.TaskID, task.TaskType)
ProcessRetryLoop:
	for retryCount := 1; retryCount <= 100; retryCount++ {
		select {
		case <-t.shutdownCh:
			return
		default:
			var err error
			scope := metrics.TransferQueueProcessorScope
			switch task.TaskType {
			case persistence.TransferTaskTypeActivityTask:
				scope = metrics.TransferTaskActivityScope
				err = t.processActivityTask(task)
			case persistence.TransferTaskTypeDecisionTask:
				scope = metrics.TransferTaskDecisionScope
				err = t.processDecisionTask(task)
			case persistence.TransferTaskTypeCloseExecution:
				scope = metrics.TransferTaskCloseExecutionScope
				err = t.processCloseExecution(task)
			case persistence.TransferTaskTypeCancelExecution:
				scope = metrics.TransferTaskCancelExecutionScope
				err = t.processCancelExecution(task)
			case persistence.TransferTaskTypeSignalExecution:
				scope = metrics.TransferTaskSignalExecutionScope
				err = t.processSignalExecution(task)
			case persistence.TransferTaskTypeStartChildExecution:
				scope = metrics.TransferTaskStartChildExecutionScope
				err = t.processStartChildExecution(task)
			}

			if err != nil {
				logging.LogTransferTaskProcessingFailedEvent(t.logger, task.TaskID, task.TaskType, err)
				t.metricsClient.IncCounter(scope, metrics.TaskFailures)
				backoff := time.Duration(retryCount * 100)
				time.Sleep(backoff * time.Millisecond)
				continue ProcessRetryLoop
			}

			t.ackMgr.completeTask(task.TaskID)
			return
		}
	}

	// All attempts to process transfer task failed.  We won't be able to move the ackLevel so panic
	logging.LogOperationPanicEvent(t.logger,
		fmt.Sprintf("Retry count exceeded for transfer taskID: %v", task.TaskID), nil)
}

func (t *transferQueueProcessorImpl) processActivityTask(task *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskActivityScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskActivityScope, metrics.TaskLatency)
	defer sw.Stop()

	var err error
	domainID := task.DomainID
	targetDomainID := task.TargetDomainID
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID)}
	taskList := &workflow.TaskList{
		Name: &task.TaskList,
	}

	context, release, err := t.cache.getOrCreateWorkflowExecution(domainID, execution)
	if err != nil {
		return err
	}
	defer release()

	var msBuilder *mutableStateBuilder
	msBuilder, err = context.loadWorkflowExecution()
	timeout := int32(0)
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
		return err
	}

	if ai, found := msBuilder.GetActivityInfo(task.ScheduleID); found {
		timeout = ai.ScheduleToStartTimeout
	} else {
		logging.LogDuplicateTransferTaskEvent(t.logger, persistence.TransferTaskTypeActivityTask, task.TaskID, task.ScheduleID)
	}

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release()
	if timeout != 0 {
		err = t.matchingClient.AddActivityTask(nil, &m.AddActivityTaskRequest{
			DomainUUID:                    common.StringPtr(targetDomainID),
			SourceDomainUUID:              common.StringPtr(domainID),
			Execution:                     &execution,
			TaskList:                      taskList,
			ScheduleId:                    &task.ScheduleID,
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(timeout),
		})
	}

	return err
}

func (t *transferQueueProcessorImpl) processDecisionTask(task *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskDecisionScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskDecisionScope, metrics.TaskLatency)
	defer sw.Stop()

	var err error
	domainID := task.DomainID
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
	taskList := &workflow.TaskList{
		Name: &task.TaskList,
	}

	// get workflow timeout
	context, release, err := t.cache.getOrCreateWorkflowExecution(domainID, execution)
	if err != nil {
		return err
	}
	defer release()

	var msBuilder *mutableStateBuilder
	msBuilder, err = context.loadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
		return err
	}

	timeout := msBuilder.executionInfo.WorkflowTimeout
	wfTypeName := msBuilder.executionInfo.WorkflowTypeName
	startTimestamp := msBuilder.executionInfo.StartTimestamp
	if msBuilder.isStickyTaskListEnabled() {
		taskList.Name = common.StringPtr(msBuilder.executionInfo.StickyTaskList)
		taskList.Kind = common.TaskListKindPtr(workflow.TaskListKindSticky)
		timeout = msBuilder.executionInfo.StickyScheduleToStartTimeout
	}

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release()
	err = t.matchingClient.AddDecisionTask(nil, &m.AddDecisionTaskRequest{
		DomainUUID:                    common.StringPtr(domainID),
		Execution:                     &execution,
		TaskList:                      taskList,
		ScheduleId:                    &task.ScheduleID,
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(timeout),
	})

	if err != nil {
		return err
	}

	if task.ScheduleID == firstEventID+1 {
		err = t.recordWorkflowExecutionStarted(execution, task, wfTypeName, startTimestamp, timeout)
	}

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
	}
	return err
}

func (t *transferQueueProcessorImpl) processCloseExecution(task *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskCloseExecutionScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskCloseExecutionScope, metrics.TaskLatency)
	defer sw.Stop()

	var err error
	domainID := task.DomainID
	execution := workflow.WorkflowExecution{WorkflowId: common.StringPtr(task.WorkflowID),
		RunId: common.StringPtr(task.RunID)}

	context, release, err := t.cache.getOrCreateWorkflowExecution(domainID, execution)
	if err != nil {
		return err
	}
	defer release()

	var msBuilder *mutableStateBuilder
	msBuilder, err = context.loadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, but the mutable state was
			// already deleted on a previous attempt to process the task.
			return nil
		}
		return err
	}

	replyToParentWorkflow := msBuilder.hasParentExecution() && msBuilder.executionInfo.CloseStatus != persistence.WorkflowCloseStatusContinuedAsNew
	var completionEvent *workflow.HistoryEvent
	if replyToParentWorkflow {
		completionEvent, _ = msBuilder.GetCompletionEvent()
	}
	parentDomainID := msBuilder.executionInfo.ParentDomainID
	parentWorkflowID := msBuilder.executionInfo.ParentWorkflowID
	parentRunID := msBuilder.executionInfo.ParentRunID
	initiatedID := msBuilder.executionInfo.InitiatedID

	workflowTypeName := msBuilder.executionInfo.WorkflowTypeName
	workflowStartTimestamp := msBuilder.executionInfo.StartTimestamp.UnixNano()
	workflowCloseTimestamp := msBuilder.getLastUpdatedTimestamp()
	workflowCloseStatus := getWorkflowExecutionCloseStatus(msBuilder.executionInfo.CloseStatus)
	workflowHistoryLength := msBuilder.GetNextEventID()

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release()
	// Communicate the result to parent execution if this is Child Workflow execution
	if replyToParentWorkflow {
		err = t.historyClient.RecordChildExecutionCompleted(nil, &history.RecordChildExecutionCompletedRequest{
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
	if err != nil {
		return err
	}

	// Record closing in visibility store
	retentionSeconds := int64(0)
	domainEntry, err := t.domainCache.GetDomainByID(task.DomainID)
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			return err
		}
		// it is possible that the domain got deleted. Use default retention.
	} else {
		// retention in domain config is in days, convert to seconds
		retentionSeconds = int64(domainEntry.GetConfig().Retention) * 24 * 60 * 60
	}

	return t.visibilityManager.RecordWorkflowExecutionClosed(&persistence.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       task.DomainID,
		Execution:        execution,
		WorkflowTypeName: workflowTypeName,
		StartTimestamp:   workflowStartTimestamp,
		CloseTimestamp:   workflowCloseTimestamp,
		Status:           workflowCloseStatus,
		HistoryLength:    workflowHistoryLength,
		RetentionSeconds: retentionSeconds,
	})
}

func (t *transferQueueProcessorImpl) processCancelExecution(task *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskCancelExecutionScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskCancelExecutionScope, metrics.TaskLatency)
	defer sw.Stop()

	var err error
	domainID := task.DomainID
	targetDomainID := task.TargetDomainID
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}

	var context *workflowExecutionContext
	var release releaseWorkflowExecutionFunc
	context, release, err = t.cache.getOrCreateWorkflowExecution(domainID, execution)
	if err != nil {
		return err
	}
	defer release()

	// First load the execution to validate if there is pending request cancellation for this transfer task
	var msBuilder *mutableStateBuilder
	msBuilder, err = context.loadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
		return err
	}

	initiatedEventID := task.ScheduleID
	ri, isRunning := msBuilder.GetRequestCancelInfo(initiatedEventID)
	if !isRunning {
		// No pending request cancellation for this initiatedID, complete this transfer task
		return nil
	}

	// handle workflow cancel itself
	if domainID == targetDomainID && task.WorkflowID == task.TargetWorkflowID {
		// it does not matter if the run ID is a mismatch
		cancelRequest := &history.RequestCancelWorkflowExecutionRequest{
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

	cancelRequest := &history.RequestCancelWorkflowExecutionRequest{
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
			t.logger.Debugf("Failed to cancel external workflow execution. Error: %v", err)
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

	t.logger.Debugf("RequestCancel successfully recorded to external workflow execution.  WorkflowID: %v, RunID: %v",
		task.TargetWorkflowID, task.TargetRunID)

	// Record ExternalWorkflowExecutionCancelRequested in source execution
	err = t.requestCancelCompleted(task, context, cancelRequest)
	if _, ok := err.(*workflow.EntityNotExistsError); ok {
		// this could happen if this is a duplicate processing of the task, and the execution has already completed.
		return nil
	}

	return err
}

func (t *transferQueueProcessorImpl) processSignalExecution(task *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskSignalExecutionScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskSignalExecutionScope, metrics.TaskLatency)
	defer sw.Stop()

	var err error
	domainID := task.DomainID
	targetDomainID := task.TargetDomainID
	execution := workflow.WorkflowExecution{WorkflowId: common.StringPtr(task.WorkflowID),
		RunId: common.StringPtr(task.RunID)}

	var context *workflowExecutionContext
	var release releaseWorkflowExecutionFunc
	context, release, err = t.cache.getOrCreateWorkflowExecution(domainID, execution)
	if err != nil {
		return err
	}
	defer release()

	var msBuilder *mutableStateBuilder
	msBuilder, err = context.loadWorkflowExecution()
	if err != nil || !msBuilder.isWorkflowExecutionRunning() {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
		return err
	}

	initiatedEventID := task.ScheduleID
	ri, isRunning := msBuilder.GetSignalInfo(initiatedEventID)
	if !isRunning {
		// TODO: here we should also RemoveSignalMutableState from target workflow
		// Otherwise, target SignalRequestID still can leak if shard restart after requestSignalCompleted
		// To do that, probably need to add the SignalRequestID in transfer task.
		return nil
	}

	// handle workflow signal itself
	if domainID == targetDomainID && task.WorkflowID == task.TargetWorkflowID {
		// it does not matter if the run ID is a mismatch
		signalRequest := &history.SignalWorkflowExecutionRequest{
			DomainUUID: common.StringPtr(targetDomainID),
			SignalRequest: &workflow.SignalWorkflowExecutionRequest{
				Domain: common.StringPtr(targetDomainID),
				WorkflowExecution: &workflow.WorkflowExecution{
					WorkflowId: common.StringPtr(task.TargetWorkflowID),
					RunId:      common.StringPtr(task.TargetRunID),
				},
				Identity: common.StringPtr(identityHistoryService),
				Control:  ri.Control,
			},
		}
		err = t.requestSignalFailed(task, context, signalRequest)
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
		return err
	}

	signalRequest := &history.SignalWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(targetDomainID),
		SignalRequest: &workflow.SignalWorkflowExecutionRequest{
			Domain: common.StringPtr(targetDomainID),
			WorkflowExecution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(task.TargetWorkflowID),
				RunId:      common.StringPtr(task.TargetRunID),
			},
			Identity:   common.StringPtr(identityHistoryService),
			SignalName: common.StringPtr(ri.SignalName),
			Input:      ri.Input,
			// Use same request ID to deduplicate SignalWorkflowExecution calls
			RequestId: common.StringPtr(ri.SignalRequestID),
			Control:   ri.Control,
		},
		ExternalWorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(task.WorkflowID),
			RunId:      common.StringPtr(task.RunID),
		},
		ChildWorkflowOnly: common.BoolPtr(task.TargetChildWorkflowOnly),
	}

	err = t.SignalExecutionWithRetry(signalRequest)

	if err != nil {
		t.logger.Debugf("Failed to signal external workflow execution. Error: %v", err)

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

	t.logger.Debugf("Signal successfully recorded to external workflow execution.  WorkflowID: %v, RunID: %v",
		task.TargetWorkflowID, task.TargetRunID)

	err = t.requestSignalCompleted(task, context, signalRequest)
	if _, ok := err.(*workflow.EntityNotExistsError); ok {
		// this could happen if this is a duplicate processing of the task, and the execution has already completed.
		return nil
	}

	// release the context lock since we no longer need mutable state builder and
	// the rest of logic is making RPC call, which takes time.
	release()
	// remove signalRequestedID from target workflow, after Signal detail is removed from source workflow
	removeRequest := &history.RemoveSignalMutableStateRequest{
		DomainUUID: common.StringPtr(targetDomainID),
		WorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(task.TargetWorkflowID),
			RunId:      common.StringPtr(task.TargetRunID),
		},
		RequestId: common.StringPtr(ri.SignalRequestID),
	}

	t.historyClient.RemoveSignalMutableState(nil, removeRequest)

	return err
}

func (t *transferQueueProcessorImpl) processStartChildExecution(task *persistence.TransferTaskInfo) error {
	t.metricsClient.IncCounter(metrics.TransferTaskStartChildExecutionScope, metrics.TaskRequests)
	sw := t.metricsClient.StartTimer(metrics.TransferTaskStartChildExecutionScope, metrics.TaskLatency)
	defer sw.Stop()

	var err error
	domainID := task.DomainID
	targetDomainID := task.TargetDomainID
	execution := workflow.WorkflowExecution{WorkflowId: common.StringPtr(task.WorkflowID),
		RunId: common.StringPtr(task.RunID)}

	var context *workflowExecutionContext
	var release releaseWorkflowExecutionFunc
	context, release, err = t.cache.getOrCreateWorkflowExecution(domainID, execution)
	if err != nil {
		return err
	}
	defer release()

	// First step is to load workflow execution so we can retrieve the initiated event
	var msBuilder *mutableStateBuilder
	msBuilder, err = context.loadWorkflowExecution()
	if err != nil || !msBuilder.isWorkflowExecutionRunning() {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
		return err
	}

	initiatedEventID := task.ScheduleID
	ci, isRunning := msBuilder.GetChildExecutionInfo(initiatedEventID)
	if isRunning {
		initiatedEvent, ok := msBuilder.GetChildExecutionInitiatedEvent(initiatedEventID)
		attributes := initiatedEvent.StartChildWorkflowExecutionInitiatedEventAttributes
		if ok && ci.StartedID == emptyEventID {
			// Found pending child execution and it is not marked as started
			// Let's try and start the child execution
			startRequest := &history.StartWorkflowExecutionRequest{
				DomainUUID: common.StringPtr(targetDomainID),
				StartRequest: &workflow.StartWorkflowExecutionRequest{
					Domain:       common.StringPtr(targetDomainID),
					WorkflowId:   common.StringPtr(*attributes.WorkflowId),
					WorkflowType: attributes.WorkflowType,
					TaskList:     attributes.TaskList,
					Input:        attributes.Input,
					ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(*attributes.ExecutionStartToCloseTimeoutSeconds),
					TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(*attributes.TaskStartToCloseTimeoutSeconds),
					// Use the same request ID to dedupe StartWorkflowExecution calls
					RequestId:             common.StringPtr(ci.CreateRequestID),
					WorkflowIdReusePolicy: attributes.WorkflowIdReusePolicy,
				},
				ParentExecutionInfo: &history.ParentExecutionInfo{
					DomainUUID: common.StringPtr(domainID),
					Execution: &workflow.WorkflowExecution{
						WorkflowId: common.StringPtr(task.WorkflowID),
						RunId:      common.StringPtr(task.RunID),
					},
					InitiatedId: common.Int64Ptr(initiatedEventID),
				},
			}

			var startResponse *workflow.StartWorkflowExecutionResponse
			startResponse, err = t.historyClient.StartWorkflowExecution(nil, startRequest)
			if err != nil {
				t.logger.Debugf("Failed to start child workflow execution. Error: %v", err)

				// Check to see if the error is non-transient, in which case add StartChildWorkflowExecutionFailed
				// event and complete transfer task by setting the err = nil
				switch err.(type) {
				case *workflow.WorkflowExecutionAlreadyStartedError:
					err = t.recordStartChildExecutionFailed(task, context, attributes)
				}
				return err
			}

			t.logger.Debugf("Child Execution started successfully.  WorkflowID: %v, RunID: %v",
				*attributes.WorkflowId, *startResponse.RunId)

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
			startedEvent, _ := msBuilder.GetChildExecutionStartedEvent(initiatedEventID)
			startedAttributes := startedEvent.ChildWorkflowExecutionStartedEventAttributes
			err = t.createFirstDecisionTask(targetDomainID, startedAttributes.WorkflowExecution)
		}
	}

	return err
}

func (t *transferQueueProcessorImpl) recordWorkflowExecutionStarted(
	execution workflow.WorkflowExecution, task *persistence.TransferTaskInfo, wfTypeName string,
	startTimestamp time.Time, timeout int32,
) error {
	err := t.visibilityManager.RecordWorkflowExecutionStarted(&persistence.RecordWorkflowExecutionStartedRequest{
		DomainUUID:       task.DomainID,
		Execution:        execution,
		WorkflowTypeName: wfTypeName,
		StartTimestamp:   startTimestamp.UnixNano(),
		WorkflowTimeout:  int64(timeout),
	})

	return err
}

func (t *transferQueueProcessorImpl) recordChildExecutionStarted(task *persistence.TransferTaskInfo,
	context *workflowExecutionContext, initiatedAttributes *workflow.StartChildWorkflowExecutionInitiatedEventAttributes,
	runID string) error {

	return t.updateWorkflowExecution(task.DomainID, context, true,
		func(msBuilder *mutableStateBuilder) error {
			if !msBuilder.isWorkflowExecutionRunning() {
				return &workflow.EntityNotExistsError{Message: "Workflow execution already completed."}
			}

			domain := initiatedAttributes.Domain
			initiatedEventID := task.ScheduleID
			ci, isRunning := msBuilder.GetChildExecutionInfo(initiatedEventID)
			if !isRunning || ci.StartedID != emptyEventID {
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

func (t *transferQueueProcessorImpl) recordStartChildExecutionFailed(task *persistence.TransferTaskInfo,
	context *workflowExecutionContext,
	initiatedAttributes *workflow.StartChildWorkflowExecutionInitiatedEventAttributes) error {

	return t.updateWorkflowExecution(task.DomainID, context, true,
		func(msBuilder *mutableStateBuilder) error {
			if !msBuilder.isWorkflowExecutionRunning() {
				return &workflow.EntityNotExistsError{Message: "Workflow execution already completed."}
			}

			initiatedEventID := task.ScheduleID
			ci, isRunning := msBuilder.GetChildExecutionInfo(initiatedEventID)
			if !isRunning || ci.StartedID != emptyEventID {
				return &workflow.EntityNotExistsError{Message: "Pending child execution not found."}
			}

			msBuilder.AddStartChildWorkflowExecutionFailedEvent(initiatedEventID,
				workflow.ChildWorkflowExecutionFailedCauseWorkflowAlreadyRunning, initiatedAttributes)

			return nil
		})
}

// createFirstDecisionTask is used by StartChildExecution transfer task to create the first decision task for
// child execution.
func (t *transferQueueProcessorImpl) createFirstDecisionTask(domainID string,
	execution *workflow.WorkflowExecution) error {
	err := t.historyClient.ScheduleDecisionTask(nil, &history.ScheduleDecisionTaskRequest{
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: execution,
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

func (t *transferQueueProcessorImpl) requestCancelCompleted(task *persistence.TransferTaskInfo,
	context *workflowExecutionContext, request *history.RequestCancelWorkflowExecutionRequest) error {

	return t.updateWorkflowExecution(task.DomainID, context, true,
		func(msBuilder *mutableStateBuilder) error {
			if !msBuilder.isWorkflowExecutionRunning() {
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

func (t *transferQueueProcessorImpl) requestSignalCompleted(task *persistence.TransferTaskInfo,
	context *workflowExecutionContext,
	request *history.SignalWorkflowExecutionRequest) error {

	return t.updateWorkflowExecution(task.DomainID, context, true,
		func(msBuilder *mutableStateBuilder) error {
			if !msBuilder.isWorkflowExecutionRunning() {
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

func (t *transferQueueProcessorImpl) requestCancelFailed(task *persistence.TransferTaskInfo,
	context *workflowExecutionContext, request *history.RequestCancelWorkflowExecutionRequest) error {

	return t.updateWorkflowExecution(task.DomainID, context, true,
		func(msBuilder *mutableStateBuilder) error {
			if !msBuilder.isWorkflowExecutionRunning() {
				return &workflow.EntityNotExistsError{Message: "Workflow execution already completed."}
			}

			initiatedEventID := task.ScheduleID
			_, isPending := msBuilder.GetRequestCancelInfo(initiatedEventID)
			if !isPending {
				return &workflow.EntityNotExistsError{Message: "Pending request cancellation not found."}
			}

			msBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(
				emptyEventID,
				initiatedEventID,
				request.GetDomainUUID(),
				request.CancelRequest.WorkflowExecution.GetWorkflowId(),
				request.CancelRequest.WorkflowExecution.GetRunId(),
				workflow.CancelExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution)

			return nil
		})
}

func (t *transferQueueProcessorImpl) requestSignalFailed(task *persistence.TransferTaskInfo,
	context *workflowExecutionContext,
	request *history.SignalWorkflowExecutionRequest) error {

	return t.updateWorkflowExecution(task.DomainID, context, true,
		func(msBuilder *mutableStateBuilder) error {
			if !msBuilder.isWorkflowExecutionRunning() {
				return &workflow.EntityNotExistsError{Message: "Workflow is not running."}
			}

			initiatedEventID := task.ScheduleID
			_, isPending := msBuilder.GetSignalInfo(initiatedEventID)
			if !isPending {
				return &workflow.EntityNotExistsError{Message: "Pending signal request not found."}
			}

			msBuilder.AddSignalExternalWorkflowExecutionFailedEvent(
				emptyEventID,
				initiatedEventID,
				request.GetDomainUUID(),
				request.SignalRequest.WorkflowExecution.GetWorkflowId(),
				request.SignalRequest.WorkflowExecution.GetRunId(),
				request.SignalRequest.Control,
				workflow.SignalExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution)

			return nil
		})
}

func (t *transferQueueProcessorImpl) updateWorkflowExecution(domainID string, context *workflowExecutionContext,
	createDecisionTask bool, action func(builder *mutableStateBuilder) error) error {
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
			if !msBuilder.HasPendingDecisionTask() {
				di := msBuilder.AddDecisionTaskScheduledEvent()
				transferTasks = append(transferTasks, &persistence.DecisionTask{
					DomainID:   domainID,
					TaskList:   di.Tasklist,
					ScheduleID: di.ScheduleID,
				})
				if msBuilder.isStickyTaskListEnabled() {
					lg := t.logger.WithFields(bark.Fields{
						logging.TagWorkflowExecutionID: context.workflowExecution.WorkflowId,
						logging.TagWorkflowRunID:       context.workflowExecution.RunId,
					})
					tBuilder := newTimerBuilder(t.shard.GetConfig(), lg, common.NewRealTimeSource())
					stickyTaskTimeoutTimer := tBuilder.AddScheduleToStartDecisionTimoutTask(di.ScheduleID, di.Attempt,
						msBuilder.executionInfo.StickyScheduleToStartTimeout)
					timerTasks = []persistence.Task{stickyTaskTimeoutTimer}
				}
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

		t.historyService.timerProcessor.NotifyNewTimers(timerTasks)

		return nil
	}

	return ErrMaxAttemptsExceeded
}

func (t *transferQueueProcessorImpl) SignalExecutionWithRetry(signalRequest *history.SignalWorkflowExecutionRequest) error {
	op := func() error {
		return t.historyClient.SignalWorkflowExecution(nil, signalRequest)
	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (a *ackManager) readTransferTasks() ([]*persistence.TransferTaskInfo, error) {
	a.RLock()
	rLevel := a.readLevel
	a.RUnlock()

	var response *persistence.GetTransferTasksResponse
	op := func() error {
		var err error
		response, err = a.executionMgr.GetTransferTasks(&persistence.GetTransferTasksRequest{
			ReadLevel:    rLevel,
			MaxReadLevel: a.shard.GetTransferMaxReadLevel(),
			BatchSize:    a.processor.config.TransferTaskBatchSize,
		})
		return err
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}

	tasks := response.Tasks
	if len(tasks) == 0 {
		return tasks, nil
	}

	a.Lock()
	for _, task := range tasks {
		if a.readLevel >= task.TaskID {
			a.logger.Fatalf("Next task ID is less than current read level.  TaskID: %v, ReadLevel: %v", task.TaskID,
				a.readLevel)
		}
		a.logger.Debugf("Moving read level: %v", task.TaskID)
		a.readLevel = task.TaskID
		a.outstandingTasks[a.readLevel] = false
	}
	a.Unlock()

	return tasks, nil
}

func (a *ackManager) completeTask(taskID int64) {
	a.Lock()
	if _, ok := a.outstandingTasks[taskID]; ok {
		a.outstandingTasks[taskID] = true
	}
	a.Unlock()
}

func (a *ackManager) updateAckLevel() {
	a.metricsClient.IncCounter(metrics.TransferQueueProcessorScope, metrics.AckLevelUpdateCounter)
	initialAckLevel := a.ackLevel
	updatedAckLevel := a.ackLevel
	a.Lock()
MoveAckLevelLoop:
	for current := a.ackLevel + 1; current <= a.readLevel; current++ {
		if acked, ok := a.outstandingTasks[current]; ok {
			if acked {
				err := a.executionMgr.CompleteTransferTask(&persistence.CompleteTransferTaskRequest{TaskID: current})

				if err != nil {
					a.logger.Warnf("Processor unable to complete transfer task '%v': %v", current, err)
					break MoveAckLevelLoop
				}
				a.logger.Debugf("Updating ack level: %v", current)
				a.ackLevel = current
				updatedAckLevel = current
				delete(a.outstandingTasks, current)
			} else {
				break MoveAckLevelLoop
			}
		}
	}
	a.Unlock()

	// Do not update Acklevel if nothing changed upto force update interval
	if initialAckLevel == updatedAckLevel && time.Since(a.lastUpdated) < a.config.TransferProcessorForceUpdateInterval {
		return
	}

	// Always update ackLevel to detect if the shared is stolen
	if err := a.shard.UpdateTransferAckLevel(updatedAckLevel); err != nil {
		a.metricsClient.IncCounter(metrics.TransferQueueProcessorScope, metrics.AckLevelUpdateFailedCounter)
		logging.LogOperationFailedEvent(a.logger, "Error updating ack level for shard", err)
	} else {
		a.lastUpdated = time.Now()
	}
}

func minDuration(x, y time.Duration) time.Duration {
	if x < y {
		return x
	}

	return y
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
