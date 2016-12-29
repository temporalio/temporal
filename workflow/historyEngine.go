package workflow

import (
	"fmt"

	h "code.uber.internal/devexp/minions/.gen/go/history"
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/backoff"
	"code.uber.internal/devexp/minions/persistence"
	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
)

type (
	historyEngineImpl struct {
		shard            *shardContext
		executionManager persistence.ExecutionManager
		txProcessor      transferQueueProcessor
		tokenSerializer  taskTokenSerializer
		logger           bark.Logger
	}

	workflowExecutionContext struct {
		workflowExecution workflow.WorkflowExecution
		builder           *historyBuilder
		executionInfo     *persistence.WorkflowExecutionInfo
		historyService    *historyEngineImpl
		updateCondition   int64
		logger            bark.Logger
	}
)

// NewHistoryEngine creates an instance of history engine
func NewHistoryEngine(shardID int, executionManager persistence.ExecutionManager,
	taskManager persistence.TaskManager, logger bark.Logger) HistoryEngine {
	shard, err := acquireShard(shardID, executionManager)
	if err != nil {
		return nil
	}
	return &historyEngineImpl{
		shard:            shard,
		executionManager: executionManager,
		txProcessor:      newTransferQueueProcessor(executionManager, taskManager, logger),
		tokenSerializer:  newJSONTaskTokenSerializer(),
		logger: logger.WithFields(bark.Fields{
			tagWorkflowComponent: tagValueWorkflowEngineComponent,
		}),
	}
}

// Start the service.
func (e *historyEngineImpl) Start() {
	e.txProcessor.Start()
}

// Stop the service.
func (e *historyEngineImpl) Stop() {
	e.txProcessor.Stop()
}

// StartWorkflowExecution starts a workflow execution
func (e *historyEngineImpl) StartWorkflowExecution(request *workflow.StartWorkflowExecutionRequest) (
	workflow.WorkflowExecution, error) {
	executionID := request.GetWorkflowId()
	runID := uuid.New()
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(executionID),
		RunId:      common.StringPtr(runID),
	}

	taskList := request.GetTaskList().GetName()
	builder := newHistoryBuilder(e.logger)
	builder.AddWorkflowExecutionStartedEvent(request)
	dt := builder.AddDecisionTaskScheduledEvent(taskList, request.GetTaskStartToCloseTimeoutSeconds())
	h, serializedError := builder.Serialize()
	if serializedError != nil {
		logHistorySerializationErrorEvent(e.logger, serializedError, fmt.Sprintf(
			"History serialization error on start workflow.  WorkflowID: %v, RunID: %v", executionID, runID))
		return nilWorkflowExecution, serializedError
	}

	_, err := e.executionManager.CreateWorkflowExecution(&persistence.CreateWorkflowExecutionRequest{
		Execution:          workflowExecution,
		TaskList:           request.GetTaskList().GetName(),
		History:            h,
		ExecutionContext:   nil,
		NextEventID:        builder.nextEventID,
		LastProcessedEvent: 0,
		TransferTasks: []persistence.Task{&persistence.DecisionTask{
			TaskID:   e.shard.GetTransferTaskID(),
			TaskList: taskList, ScheduleID: dt.GetEventId(),
		}},
		RangeID: e.shard.GetRangeID(),
	})

	if err != nil {
		logPersistantStoreErrorEvent(e.logger, tagValueStoreOperationCreateWorkflowExecution, err,
			fmt.Sprintf("{WorkflowID: %v, RunID: %v}", executionID, runID))
		return nilWorkflowExecution, err
	}

	return workflowExecution, nil
}

// GetWorkflowExecutionHistory retrieves the history for given workflow execution
func (e *historyEngineImpl) GetWorkflowExecutionHistory(
	request *workflow.GetWorkflowExecutionHistoryRequest) (*workflow.GetWorkflowExecutionHistoryResponse, error) {
	r := &persistence.GetWorkflowExecutionRequest{
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(request.GetExecution().GetWorkflowId()),
			RunId:      common.StringPtr(request.GetExecution().GetRunId()),
		},
	}

	response, err := e.getWorkflowExecutionWithRetry(r)
	if err != nil {
		return nil, err
	}

	builder := newHistoryBuilder(e.logger)
	if err := builder.loadExecutionInfo(response.ExecutionInfo); err != nil {
		return nil, err
	}

	result := workflow.NewGetWorkflowExecutionHistoryResponse()
	result.History = builder.getHistory()

	return result, nil
}

func (e *historyEngineImpl) RecordDecisionTaskStarted(
	request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error) {
	context := newWorkflowExecutionContext(e, *request.WorkflowExecution)
	scheduleID := *request.ScheduleId

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		builder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return nil, err1
		}

		// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
		// task is not outstanding than it is most probably a duplicate and complete the task.
		if isRunning, startedID := builder.isDecisionTaskRunning(scheduleID); !isRunning || startedID != emptyEventID {
			logDuplicateTaskEvent(context.logger, persistence.TaskTypeDecision, *request.TaskId, scheduleID, startedID,
				isRunning)
			return nil, errDuplicate
		}

		event := builder.AddDecisionTaskStartedEvent(scheduleID, request.PollRequest)
		if event == nil {
			return nil, errCreateEvent
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		if err2 := context.updateWorkflowExecution(nil); err2 != nil {
			if err2 == errConflict {
				continue Update_History_Loop
			}

			return nil, err2
		}

		return e.createRecordDecisionTaskStartedResponse(context, event), nil
	}

	return nil, errMaxAttemptsExceeded
}

func (e *historyEngineImpl) RecordActivityTaskStarted(
	request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error) {
	context := newWorkflowExecutionContext(e, *request.WorkflowExecution)
	scheduleID := *request.ScheduleId

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		builder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return nil, err1
		}

		// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
		// task is not outstanding than it is most probably a duplicate and complete the task.
		if isRunning, startedID := builder.isActivityTaskRunning(scheduleID); !isRunning || startedID != emptyEventID {
			logDuplicateTaskEvent(context.logger, persistence.TaskTypeActivity, request.GetTaskId(), scheduleID, startedID,
				isRunning)
			return nil, errDuplicate
		}

		event := builder.AddActivityTaskStartedEvent(scheduleID, request.PollRequest)
		if event == nil {
			return nil, errCreateEvent
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operationi again.
		if err2 := context.updateWorkflowExecution(nil); err2 != nil {
			if err2 == errConflict {
				continue Update_History_Loop
			}

			return nil, err2
		}

		response := h.NewRecordActivityTaskStartedResponse()
		response.StartedEvent = event
		response.ScheduledEvent = builder.GetEvent(scheduleID)
		return response, nil
	}

	return nil, errMaxAttemptsExceeded
}

// RespondDecisionTaskCompleted completes a decision task
func (e *historyEngineImpl) RespondDecisionTaskCompleted(request *workflow.RespondDecisionTaskCompletedRequest) error {
	token, err0 := e.tokenSerializer.Deserialize(request.GetTaskToken())
	if err0 != nil {
		return &workflow.BadRequestError{Message: "Error deserializing task token."}
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	context := newWorkflowExecutionContext(e, workflowExecution)

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		builder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}

		scheduleID := token.ScheduleID
		isRunning, startedID := builder.isDecisionTaskRunning(scheduleID)
		if !isRunning || startedID == emptyEventID {
			return &workflow.EntityNotExistsError{Message: "Decision task not found."}
		}

		completedEvent := builder.AddDecisionTaskCompletedEvent(scheduleID, startedID, request)
		completedID := completedEvent.GetEventId()
		isComplete := false
		transferTasks := []persistence.Task{}
	Process_Decision_Loop:
		for _, d := range request.Decisions {
			switch d.GetDecisionType() {
			case workflow.DecisionType_ScheduleActivityTask:
				attributes := d.GetScheduleActivityTaskDecisionAttributes()
				scheduleEvent := builder.AddActivityTaskScheduledEvent(completedID, attributes)
				transferTasks = append(transferTasks, &persistence.ActivityTask{
					TaskList:   attributes.GetTaskList().GetName(),
					ScheduleID: scheduleEvent.GetEventId(),
				})
			case workflow.DecisionType_CompleteWorkflowExecution:
				if isComplete || builder.hasPendingTasks() {
					builder.AddCompleteWorkflowExecutionFailedEvent(completedID,
						workflow.WorkflowCompleteFailedCause_UNHANDLED_DECISION)
					continue Process_Decision_Loop
				}
				attributes := d.GetCompleteWorkflowExecutionDecisionAttributes()
				builder.AddCompletedWorkflowEvent(completedID, attributes)
				isComplete = true
			case workflow.DecisionType_FailWorkflowExecution:
				if isComplete || builder.hasPendingTasks() {
					builder.AddCompleteWorkflowExecutionFailedEvent(completedID,
						workflow.WorkflowCompleteFailedCause_UNHANDLED_DECISION)
					continue Process_Decision_Loop
				}
				attributes := d.GetFailWorkflowExecutionDecisionAttributes()
				builder.AddFailWorkflowEvent(completedID, attributes)
				isComplete = true
			default:
				return &workflow.BadRequestError{Message: fmt.Sprintf("Unknown decision type: %v", d.GetDecisionType())}
			}
		}

		// Schedule another decision task if new events came in during this decision
		if (completedID - startedID) > 1 {
			startWorkflowExecutionEvent := builder.GetEvent(firstEventID)
			startAttributes := startWorkflowExecutionEvent.GetWorkflowExecutionStartedEventAttributes()
			newDecisionEvent := builder.AddDecisionTaskScheduledEvent(startAttributes.GetTaskList().GetName(),
				startAttributes.GetTaskStartToCloseTimeoutSeconds())
			transferTasks = append(transferTasks, &persistence.DecisionTask{
				TaskList:   startAttributes.GetTaskList().GetName(),
				ScheduleID: newDecisionEvent.GetEventId(),
			})
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict then reload
		// the history and try the operation again.
		if err := context.updateWorkflowExecutionWithContext(request.GetExecutionContext(), transferTasks); err != nil {
			if err == errConflict {
				continue Update_History_Loop
			}

			return err
		}

		if isComplete {
			// TODO: We need to keep completed executions for auditing purpose.  Need a design for keeping them around
			// for visibility purpose.
			context.deleteWorkflowExecution()
		}

		return nil
	}

	return errMaxAttemptsExceeded
}

// RespondActivityTaskCompleted completes an activity task.
func (e *historyEngineImpl) RespondActivityTaskCompleted(request *workflow.RespondActivityTaskCompletedRequest) error {
	token, err0 := e.tokenSerializer.Deserialize(request.GetTaskToken())
	if err0 != nil {
		return &workflow.BadRequestError{Message: "Error deserializing task token."}
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	context := newWorkflowExecutionContext(e, workflowExecution)

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		builder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}

		scheduleID := token.ScheduleID
		isRunning, startedID := builder.isActivityTaskRunning(scheduleID)
		if !isRunning || startedID == emptyEventID {
			return &workflow.EntityNotExistsError{Message: "Activity task not found."}
		}

		if builder.AddActivityTaskCompletedEvent(scheduleID, startedID, request) == nil {
			return &workflow.InternalServiceError{Message: "Unable to add completed event to history"}
		}

		var transferTasks []persistence.Task
		if !builder.hasPendingDecisionTask() {
			startWorkflowExecutionEvent := builder.GetEvent(firstEventID)
			startAttributes := startWorkflowExecutionEvent.GetWorkflowExecutionStartedEventAttributes()
			newDecisionEvent := builder.AddDecisionTaskScheduledEvent(startAttributes.GetTaskList().GetName(),
				startAttributes.GetTaskStartToCloseTimeoutSeconds())
			transferTasks = []persistence.Task{&persistence.DecisionTask{
				TaskList:   startAttributes.GetTaskList().GetName(),
				ScheduleID: newDecisionEvent.GetEventId(),
			}}
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		if err := context.updateWorkflowExecution(transferTasks); err != nil {
			if err == errConflict {
				continue Update_History_Loop
			}

			return err
		}

		return nil
	}

	return errMaxAttemptsExceeded
}

// RespondActivityTaskFailed completes an activity task failure.
func (e *historyEngineImpl) RespondActivityTaskFailed(request *workflow.RespondActivityTaskFailedRequest) error {
	token, err0 := e.tokenSerializer.Deserialize(request.GetTaskToken())
	if err0 != nil {
		return &workflow.BadRequestError{Message: "Error deserializing task token."}
	}

	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(token.WorkflowID),
		RunId:      common.StringPtr(token.RunID),
	}

	context := newWorkflowExecutionContext(e, workflowExecution)

Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		builder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return err1
		}

		scheduleID := token.ScheduleID
		isRunning, startedID := builder.isActivityTaskRunning(scheduleID)
		if !isRunning || startedID == emptyEventID {
			return &workflow.EntityNotExistsError{Message: "Activity task not found."}
		}

		if builder.AddActivityTaskFailedEvent(scheduleID, startedID, request) == nil {
			return &workflow.InternalServiceError{Message: "Unable to add failed event to history"}
		}

		var transferTasks []persistence.Task
		if !builder.hasPendingDecisionTask() {
			startWorkflowExecutionEvent := builder.GetEvent(firstEventID)
			startAttributes := startWorkflowExecutionEvent.GetWorkflowExecutionStartedEventAttributes()
			newDecisionEvent := builder.AddDecisionTaskScheduledEvent(startAttributes.GetTaskList().GetName(),
				startAttributes.GetTaskStartToCloseTimeoutSeconds())
			transferTasks = []persistence.Task{&persistence.DecisionTask{
				TaskList:   startAttributes.GetTaskList().GetName(),
				ScheduleID: newDecisionEvent.GetEventId(),
			}}
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		if err := context.updateWorkflowExecution(transferTasks); err != nil {
			if err == errConflict {
				continue Update_History_Loop
			}

			return err
		}

		return nil
	}

	return errMaxAttemptsExceeded
}

func (e *historyEngineImpl) getWorkflowExecutionWithRetry(
	request *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
	var response *persistence.GetWorkflowExecutionResponse
	op := func() error {
		var err error
		response, err = e.executionManager.GetWorkflowExecution(request)

		return err
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, isPersistenceTransientError)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (e *historyEngineImpl) deleteWorkflowExecutionWithRetry(
	request *persistence.DeleteWorkflowExecutionRequest) error {
	op := func() error {
		return e.executionManager.DeleteWorkflowExecution(request)
	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, isPersistenceTransientError)
}

func (e *historyEngineImpl) updateWorkflowExecutionWithRetry(
	request *persistence.UpdateWorkflowExecutionRequest) error {
	op := func() error {
		return e.executionManager.UpdateWorkflowExecution(request)

	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, isPersistenceTransientError)
}

func (e *historyEngineImpl) createRecordDecisionTaskStartedResponse(context *workflowExecutionContext,
	startedEvent *workflow.HistoryEvent) *h.RecordDecisionTaskStartedResponse {
	builder := context.builder

	response := h.NewRecordDecisionTaskStartedResponse()
	response.WorkflowType = builder.getWorkflowType()
	if builder.previousDecisionStartedEvent() != emptyEventID {
		response.PreviousStartedEventId = common.Int64Ptr(builder.previousDecisionStartedEvent())
	}
	response.StartedEventId = common.Int64Ptr(startedEvent.GetEventId())
	response.History = builder.getHistory()

	return response
}

func newWorkflowExecutionContext(historyService *historyEngineImpl,
	execution workflow.WorkflowExecution) *workflowExecutionContext {
	return &workflowExecutionContext{
		workflowExecution: execution,
		historyService:    historyService,
		logger: historyService.logger.WithFields(bark.Fields{
			tagWorkflowExecutionID: execution.GetWorkflowId(),
			tagWorkflowRunID:       execution.GetRunId(),
		}),
	}
}

// Used to either create or update the execution context for the task context.
// Update can happen when conditional write fails.
func (c *workflowExecutionContext) loadWorkflowExecution() (*historyBuilder, error) {
	response, err := c.historyService.getWorkflowExecutionWithRetry(&persistence.GetWorkflowExecutionRequest{
		Execution: c.workflowExecution})
	if err != nil {
		logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationGetWorkflowExecution, err, "")
		return nil, err
	}

	c.executionInfo = response.ExecutionInfo
	c.updateCondition = response.ExecutionInfo.NextEventID
	builder := newHistoryBuilder(c.logger)
	if err := builder.loadExecutionInfo(response.ExecutionInfo); err != nil {
		return nil, err
	}
	c.builder = builder
	return builder, nil
}

func (c *workflowExecutionContext) updateWorkflowExecutionWithContext(context []byte, transferTasks []persistence.Task) error {
	c.executionInfo.ExecutionContext = context

	return c.updateWorkflowExecution(transferTasks)
}

func (c *workflowExecutionContext) updateWorkflowExecution(transferTasks []persistence.Task) error {
	updatedHistory, err := c.builder.Serialize()
	if err != nil {
		logHistorySerializationErrorEvent(c.logger, err, "Unable to serialize execution history for update.")
		return err
	}

	c.executionInfo.NextEventID = c.builder.nextEventID
	c.executionInfo.LastProcessedEvent = c.builder.previousDecisionStartedEvent()
	c.executionInfo.History = updatedHistory
	c.executionInfo.DecisionPending = c.builder.hasPendingDecisionTask()
	c.executionInfo.State = c.builder.getWorklowState()
	if err1 := c.historyService.updateWorkflowExecutionWithRetry(&persistence.UpdateWorkflowExecutionRequest{
		ExecutionInfo: c.executionInfo,
		TransferTasks: transferTasks,
		Condition:     c.updateCondition,
		RangeID:       c.historyService.shard.GetRangeID(),
	}); err1 != nil {
		switch err1.(type) {
		case *persistence.ConditionFailedError:
			return errConflict
		}

		logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationUpdateWorkflowExecution, err,
			fmt.Sprintf("{updateCondition: %v}", c.updateCondition))
		return err1
	}

	// Update went through so update the condition for new updates
	c.updateCondition = c.builder.nextEventID
	return nil
}

func (c *workflowExecutionContext) deleteWorkflowExecution() error {
	err := c.historyService.deleteWorkflowExecutionWithRetry(&persistence.DeleteWorkflowExecutionRequest{
		ExecutionInfo: c.executionInfo,
	})
	if err != nil {
		// TODO: We will be needing a background job to delete all leaking workflow executions due to failed delete
		// We cannot return an error back to client at this stage.  For now just log and move on.
		logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationDeleteWorkflowExecution, err,
			fmt.Sprintf("{updateCondition: %v}", c.updateCondition))
	}

	return err
}
