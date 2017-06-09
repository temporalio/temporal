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

	"github.com/uber/cadence/.gen/go/history"
	m "github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
	hc "github.com/uber/cadence/client/history"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

const (
	transferTaskBatchSize              = 10
	transferProcessorMaxPollRPS        = 100
	transferProcessorMaxPollInterval   = 10 * time.Second
	transferProcessorUpdateAckInterval = 10 * time.Second
	taskWorkerCount                    = 10
)

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
		logger            bark.Logger
		metricsClient     metrics.Client
	}

	// ackManager is created by transferQueueProcessor to keep track of the transfer queue ackLevel for the shard.
	// It keeps track of read level when dispatching transfer tasks to processor and maintains a map of outstanding tasks.
	// Outstanding tasks map uses the task id sequencer as the key, which is used by updateAckLevel to move the ack level
	// for the shard when all preceding tasks are acknowledged.
	ackManager struct {
		processor    transferQueueProcessor
		shard        ShardContext
		executionMgr persistence.ExecutionManager
		logger       bark.Logger

		sync.RWMutex
		outstandingTasks map[int64]bool
		readLevel        int64
		maxReadLevel     int64
		ackLevel         int64
	}
)

func newTransferQueueProcessor(shard ShardContext, visibilityMgr persistence.VisibilityManager, matching matching.Client,
	historyClient hc.Client, cache *historyCache, domainCache cache.DomainCache) transferQueueProcessor {
	executionManager := shard.GetExecutionManager()
	logger := shard.GetLogger()
	processor := &transferQueueProcessorImpl{
		shard:             shard,
		executionManager:  executionManager,
		matchingClient:    matching,
		historyClient:     historyClient,
		visibilityManager: visibilityMgr,
		cache:             cache,
		domainCache:       domainCache,
		rateLimiter:       common.NewTokenBucket(transferProcessorMaxPollRPS, common.NewRealTimeSource()),
		appendCh:          make(chan struct{}, 1),
		shutdownCh:        make(chan struct{}),
		logger: logger.WithFields(bark.Fields{
			logging.TagWorkflowComponent: logging.TagValueTransferQueueComponent,
		}),
		metricsClient: shard.GetMetricsClient(),
	}
	processor.ackMgr = newAckManager(processor, shard, executionManager, logger)

	return processor
}

func newAckManager(processor transferQueueProcessor, shard ShardContext, executionMgr persistence.ExecutionManager,
	logger bark.Logger) *ackManager {
	ackLevel := shard.GetTransferAckLevel()
	return &ackManager{
		processor:        processor,
		shard:            shard,
		executionMgr:     executionMgr,
		outstandingTasks: make(map[int64]bool),
		readLevel:        ackLevel,
		ackLevel:         ackLevel,
		logger:           logger,
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
	tasksCh := make(chan *persistence.TransferTaskInfo, transferTaskBatchSize)

	var workerWG sync.WaitGroup
	for i := 0; i < taskWorkerCount; i++ {
		workerWG.Add(1)
		go t.taskWorker(tasksCh, &workerWG)
	}

	pollTimer := time.NewTimer(transferProcessorMaxPollInterval)
	updateAckTimer := time.NewTimer(transferProcessorUpdateAckInterval)

processorPumpLoop:
	for {
		select {
		case <-t.shutdownCh:
			break processorPumpLoop
		case <-t.appendCh:
			t.processTransferTasks(tasksCh)
		case <-pollTimer.C:
			t.processTransferTasks(tasksCh)
			pollTimer = time.NewTimer(transferProcessorMaxPollInterval)
		case <-updateAckTimer.C:
			t.ackMgr.updateAckLevel()
			updateAckTimer = time.NewTimer(transferProcessorUpdateAckInterval)
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

	if !t.rateLimiter.Consume(1, transferProcessorMaxPollInterval) {
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

	for _, tsk := range tasks {
		tasksCh <- tsk
	}

	if len(tasks) == transferTaskBatchSize {
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
	t.metricsClient.AddCounter(metrics.HistoryProcessTransferTasksScope, metrics.TransferTasksProcessedCounter, 1)
ProcessRetryLoop:
	for retryCount := 1; retryCount <= 100; retryCount++ {
		select {
		case <-t.shutdownCh:
			return
		default:
			var err error
			switch task.TaskType {
			case persistence.TransferTaskTypeActivityTask:
				err = t.processActivityTask(task)
			case persistence.TransferTaskTypeDecisionTask:
				err = t.processDecisionTask(task)
			case persistence.TransferTaskTypeDeleteExecution:
				err = t.processDeleteExecution(task)
			case persistence.TransferTaskTypeCancelExecution:
				err = t.processCancelExecution(task)
			case persistence.TransferTaskTypeStartChildExecution:
				err = t.processStartChildExecution(task)
			}

			if err != nil {
				t.logger.WithField("error", err).Warn("Processor failed to create task")
				backoff := time.Duration(retryCount * 100)
				time.Sleep(backoff * time.Millisecond)
				continue ProcessRetryLoop
			}

			t.ackMgr.completeTask(task.TaskID)
			return
		}
	}

	// All attempts to process transfer task failed.  We won't be able to move the ackLevel so panic
	t.logger.Fatalf("Retry count exceeded for transfer taskID: %v", task.TaskID)
}

func (t *transferQueueProcessorImpl) processActivityTask(task *persistence.TransferTaskInfo) error {
	var err error
	domainID := task.DomainID
	targetDomainID := task.TargetDomainID
	execution := workflow.WorkflowExecution{WorkflowId: common.StringPtr(task.WorkflowID),
		RunId: common.StringPtr(task.RunID)}
	taskList := &workflow.TaskList{
		Name: &task.TaskList,
	}

	context, release, err := t.cache.getOrCreateWorkflowExecution(domainID, execution)
	if err != nil {
		return err
	}

	var mb *mutableStateBuilder
	mb, err = context.loadWorkflowExecution()
	timeout := int32(0)
	if err != nil {
		release()
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
		return err
	}

	if ai, found := mb.GetActivityInfo(task.ScheduleID); found {
		timeout = ai.ScheduleToStartTimeout
	} else {
		logging.LogDuplicateTransferTaskEvent(t.logger, persistence.TransferTaskTypeActivityTask, task.TaskID, task.ScheduleID)
	}
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
	var err error
	domainID := task.DomainID
	execution := workflow.WorkflowExecution{WorkflowId: common.StringPtr(task.WorkflowID),
		RunId: common.StringPtr(task.RunID)}

	if task.ScheduleID == firstEventID+1 {
		err = t.recordWorkflowExecutionStarted(execution, task)
	}

	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
		return err
	}

	taskList := &workflow.TaskList{
		Name: &task.TaskList,
	}
	err = t.matchingClient.AddDecisionTask(nil, &m.AddDecisionTaskRequest{
		DomainUUID: common.StringPtr(domainID),
		Execution:  &execution,
		TaskList:   taskList,
		ScheduleId: &task.ScheduleID,
	})

	return err
}

func (t *transferQueueProcessorImpl) processDeleteExecution(task *persistence.TransferTaskInfo) error {
	var err error
	domainID := task.DomainID
	execution := workflow.WorkflowExecution{WorkflowId: common.StringPtr(task.WorkflowID),
		RunId: common.StringPtr(task.RunID)}

	context, release, err := t.cache.getOrCreateWorkflowExecution(domainID, execution)
	defer release()
	if err != nil {
		return err
	}

	// TODO: We need to keep completed executions for auditing purpose.  Need a design for keeping them around
	// for visibility purpose.
	var mb *mutableStateBuilder
	mb, err = context.loadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, but the mutable state was
			// already deleted on a previous attempt to process the task.
			return nil
		}
		return err
	}

	// Communicate the result to parent execution if this is Child Workflow execution
	if mb.hasParentExecution() && mb.executionInfo.CloseStatus != persistence.WorkflowCloseStatusContinuedAsNew {
		completionEvent, _ := mb.GetCompletionEvent()
		err = t.historyClient.RecordChildExecutionCompleted(nil, &history.RecordChildExecutionCompletedRequest{
			DomainUUID: common.StringPtr(mb.executionInfo.ParentDomainID),
			WorkflowExecution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(mb.executionInfo.ParentWorkflowID),
				RunId:      common.StringPtr(mb.executionInfo.ParentRunID),
			},
			InitiatedId: common.Int64Ptr(mb.executionInfo.InitiatedID),
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
	_, domainConfig, err := t.domainCache.GetDomainByID(task.DomainID)
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			return err
		}
		// it is possible that the domain got deleted. Use default retention.
	} else {
		// retention in domain config is in days, convert to seconds
		retentionSeconds = int64(domainConfig.Retention) * 24 * 60 * 60
	}

	err = t.visibilityManager.RecordWorkflowExecutionClosed(&persistence.RecordWorkflowExecutionClosedRequest{
		DomainUUID:       task.DomainID,
		Execution:        execution,
		WorkflowTypeName: mb.executionInfo.WorkflowTypeName,
		StartTimestamp:   mb.executionInfo.StartTimestamp.UnixNano(),
		CloseTimestamp:   mb.executionInfo.LastUpdatedTimestamp.UnixNano(),
		Status:           getWorkflowExecutionCloseStatus(mb.executionInfo.CloseStatus),
		HistoryLength:    mb.GetNextEventID(),
		RetentionSeconds: retentionSeconds,
	})
	if err != nil {
		return err
	}

	err = context.deleteWorkflowExecution()

	return err
}

func (t *transferQueueProcessorImpl) processCancelExecution(task *persistence.TransferTaskInfo) error {
	var err error
	domainID := task.DomainID
	targetDomainID := task.TargetDomainID
	execution := workflow.WorkflowExecution{WorkflowId: common.StringPtr(task.WorkflowID),
		RunId: common.StringPtr(task.RunID)}

	var context *workflowExecutionContext
	var release releaseWorkflowExecutionFunc
	context, release, err = t.cache.getOrCreateWorkflowExecution(domainID, execution)
	defer release()
	if err != nil {
		return err
	}
	// Load workflow execution.
	_, err = context.loadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); ok {
			// this could happen if this is a duplicate processing of the task, and the execution has already completed.
			return nil
		}
		return err
	}

	cancelRequest := &history.RequestCancelWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(targetDomainID),
		CancelRequest: &workflow.RequestCancelWorkflowExecutionRequest{
			WorkflowExecution: &workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(task.TargetWorkflowID),
				RunId:      common.StringPtr(task.TargetRunID),
			},
			Identity: common.StringPtr("history-service"),
		},
		ExternalInitiatedEventId: common.Int64Ptr(task.ScheduleID),
		ExternalWorkflowExecution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(task.WorkflowID),
			RunId:      common.StringPtr(task.RunID),
		},
	}

	err = context.requestExternalCancelWorkflowExecutionWithRetry(
		t.historyClient,
		cancelRequest,
		task.ScheduleID)

	return err
}

func (t *transferQueueProcessorImpl) processStartChildExecution(task *persistence.TransferTaskInfo) error {
	var err error
	domainID := task.DomainID
	targetDomainID := task.TargetDomainID
	execution := workflow.WorkflowExecution{WorkflowId: common.StringPtr(task.WorkflowID),
		RunId: common.StringPtr(task.RunID)}

	var context *workflowExecutionContext
	var release releaseWorkflowExecutionFunc
	context, release, err = t.cache.getOrCreateWorkflowExecution(domainID, execution)
	defer release()
	if err != nil {
		return err
	}

	// First step is to load workflow execution so we can retrieve the initiated event
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
	ci, isRunning := msBuilder.GetChildExecutionInfo(initiatedEventID)
	if isRunning {
		initiatedEvent, ok := msBuilder.GetChildExecutionInitiatedEvent(initiatedEventID)
		attributes := initiatedEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes()
		if ok && ci.StartedID == emptyEventID {
			// Found pending child execution and it is not marked as started
			// Let's try and start the child execution
			startRequest := &history.StartWorkflowExecutionRequest{
				DomainUUID: common.StringPtr(targetDomainID),
				StartRequest: &workflow.StartWorkflowExecutionRequest{
					WorkflowId:   common.StringPtr(attributes.GetWorkflowId()),
					WorkflowType: attributes.GetWorkflowType(),
					TaskList:     attributes.GetTaskList(),
					Input:        attributes.GetInput(),
					ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(attributes.GetExecutionStartToCloseTimeoutSeconds()),
					TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(attributes.GetTaskStartToCloseTimeoutSeconds()),
					// Use the same request ID to dedupe StartWorkflowExecution calls
					RequestId: common.StringPtr(ci.CreateRequestID),
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
				attributes.GetWorkflowId(), startResponse.GetRunId())

			// Child execution is successfully started, record ChildExecutionStartedEvent in parent execution
			err = t.recordChildExecutionStarted(task, context, attributes, startResponse.GetRunId())

			if err != nil {
				return err
			}
			// Finally create first decision task for Child execution so it is really started
			err = t.historyClient.ScheduleDecisionTask(nil, &history.ScheduleDecisionTaskRequest{
				DomainUUID: common.StringPtr(targetDomainID),
				WorkflowExecution: &workflow.WorkflowExecution{
					WorkflowId: common.StringPtr(task.TargetWorkflowID),
					RunId:      common.StringPtr(startResponse.GetRunId()),
				},
			})
		} else {
			// ChildExecution already started, just create DecisionTask and complete transfer task
			startedEvent, _ := msBuilder.GetChildExecutionStartedEvent(initiatedEventID)
			startedAttributes := startedEvent.GetChildWorkflowExecutionStartedEventAttributes()
			err = t.historyClient.ScheduleDecisionTask(nil, &history.ScheduleDecisionTaskRequest{
				DomainUUID:        common.StringPtr(targetDomainID),
				WorkflowExecution: startedAttributes.GetWorkflowExecution(),
			})
		}
	}

	return err
}

func (t *transferQueueProcessorImpl) recordWorkflowExecutionStarted(
	execution workflow.WorkflowExecution, task *persistence.TransferTaskInfo) error {
	context, release, err := t.cache.getOrCreateWorkflowExecution(task.DomainID, execution)
	if err != nil {
		return err
	}
	defer release()
	mb, err := context.loadWorkflowExecution()
	if err != nil {
		return err
	}

	err = t.visibilityManager.RecordWorkflowExecutionStarted(&persistence.RecordWorkflowExecutionStartedRequest{
		DomainUUID:       task.DomainID,
		Execution:        execution,
		WorkflowTypeName: mb.executionInfo.WorkflowTypeName,
		StartTimestamp:   mb.executionInfo.StartTimestamp.UnixNano(),
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

			domain := initiatedAttributes.GetDomain()
			initiatedEventID := task.ScheduleID
			ci, isRunning := msBuilder.GetChildExecutionInfo(initiatedEventID)
			if !isRunning || ci.StartedID != emptyEventID {
				return &workflow.EntityNotExistsError{Message: "Pending child execution not found."}
			}

			msBuilder.AddChildWorkflowExecutionStartedEvent(domain,
				&workflow.WorkflowExecution{
					WorkflowId: common.StringPtr(task.TargetWorkflowID),
					RunId:      common.StringPtr(runID),
				}, initiatedAttributes.GetWorkflowType(), initiatedEventID)

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
				workflow.ChildWorkflowExecutionFailedCause_WORKFLOW_ALREADY_RUNNING, initiatedAttributes)

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
		if err := action(msBuilder); err != nil {
			return err
		}

		if createDecisionTask {
			// Create a transfer task to schedule a decision task
			if !msBuilder.HasPendingDecisionTask() {
				newDecisionEvent, _ := msBuilder.AddDecisionTaskScheduledEvent()
				transferTasks = append(transferTasks, &persistence.DecisionTask{
					DomainID:   domainID,
					TaskList:   newDecisionEvent.GetDecisionTaskScheduledEventAttributes().GetTaskList().GetName(),
					ScheduleID: newDecisionEvent.GetEventId(),
				})
			}
		}

		// Generate a transaction ID for appending events to history
		transactionID, err2 := t.shard.GetNextTransferTaskID()
		if err2 != nil {
			return err2
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict then reload
		// the history and try the operation again.
		if err := context.updateWorkflowExecution(transferTasks, nil, transactionID); err != nil {
			if err == ErrConflict {
				continue Update_History_Loop
			}
			return err
		}

		return nil
	}

	return ErrMaxAttemptsExceeded
}

func (a *ackManager) readTransferTasks() ([]*persistence.TransferTaskInfo, error) {
	a.RLock()
	rLevel := a.readLevel
	a.RUnlock()
	response, err := a.executionMgr.GetTransferTasks(&persistence.GetTransferTasksRequest{
		ReadLevel:    rLevel,
		MaxReadLevel: a.shard.GetTransferMaxReadLevel(),
		BatchSize:    transferTaskBatchSize,
	})

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

	// Always update ackLevel to detect if the shared is stolen
	if err := a.shard.UpdateAckLevel(updatedAckLevel); err != nil {
		logging.LogOperationFailedEvent(a.logger, "Error updating ack level for shard", err)
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
		return workflow.WorkflowExecutionCloseStatus_COMPLETED
	case persistence.WorkflowCloseStatusFailed:
		return workflow.WorkflowExecutionCloseStatus_FAILED
	case persistence.WorkflowCloseStatusCanceled:
		return workflow.WorkflowExecutionCloseStatus_CANCELED
	case persistence.WorkflowCloseStatusTerminated:
		return workflow.WorkflowExecutionCloseStatus_TERMINATED
	case persistence.WorkflowCloseStatusContinuedAsNew:
		return workflow.WorkflowExecutionCloseStatus_CONTINUED_AS_NEW
	case persistence.WorkflowCloseStatusTimedOut:
		return workflow.WorkflowExecutionCloseStatus_TIMED_OUT
	default:
		panic("Invalid value for enum WorkflowExecutionCloseStatus")
	}
}
