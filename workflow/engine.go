package workflow

import (
	"errors"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"

	workflow "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/backoff"
)

const (
	taskLockDuration      = 10 * time.Second
	conditionalRetryCount = 5

	retryPersistenceOperationInitialInterval    = 50 * time.Millisecond
	retryPersistenceOperationMaxInterval        = time.Second
	retryPersistenceOperationExpirationInterval = 10 * time.Second

	retryLongPollInitialInterval    = 10 * time.Millisecond
	retryLongPollMaxInterval        = 10 * time.Second
	retryLongPollExpirationInterval = 2 * time.Minute
)

type (
	engineImpl struct {
		executionManager workflowExecutionPersistence
		taskManager      taskPersistence
		txProcessor      transferQueueProcessor
		tokenSerializer  taskTokenSerializer
		logger           bark.Logger
	}

	workflowExecutionContext struct {
		workflowExecution workflow.WorkflowExecution
		builder           *historyBuilder
		executionInfo     *workflowExecutionInfo
		engine            *engineImpl
		updateCondition   int64
		logger            bark.Logger
	}

	// Contains information needed for current task transition from Activity queue to Workflow execution history.
	taskContext struct {
		info *taskInfo
		*workflowExecutionContext
	}
)

var (
	emptyPollForDecisionTaskResponse = workflow.NewPollForDecisionTaskResponse()
	emptyPollForActivityTaskResponse = workflow.NewPollForActivityTaskResponse()
	persistenceOperationRetryPolicy  = createPersistanceRetryPolicy()
	longPollRetryPolicy              = createLongPollRetryPolicy()

	errDuplicate           = errors.New("Duplicate task, completing it")
	errCreateEvent         = errors.New("Can't create activity task started event")
	errNoTasks             = errors.New("No tasks")
	errConflict            = errors.New("Conditional update failed")
	errMaxAttemptsExceeded = errors.New("Maximum attempts exceeded to update history")
)

func newWorkflowEngine(executionManager workflowExecutionPersistence, taskManager taskPersistence,
	logger bark.Logger) workflowEngine {
	return &engineImpl{
		executionManager: executionManager,
		taskManager:      taskManager,
		txProcessor:      newTransferQueueProcessor(executionManager, taskManager, logger),
		tokenSerializer:  newJSONTaskTokenSerializer(),
		logger: logger.WithFields(bark.Fields{
			tagWorkflowComponent: tagValueWorkflowEngineComponent,
		}),
	}
}

func (e *engineImpl) Start() {
	e.txProcessor.Start()
}

func (e *engineImpl) Stop() {
	e.txProcessor.Stop()
}

func (e *engineImpl) StartWorkflowExecution(request *workflow.StartWorkflowExecutionRequest) (
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

	_, err := e.executionManager.CreateWorkflowExecution(&createWorkflowExecutionRequest{
		execution:          workflowExecution,
		taskList:           request.GetTaskList().GetName(),
		history:            h,
		executionContext:   nil,
		nextEventID:        builder.nextEventID,
		lastProcessedEvent: 0,
		transferTasks:      []task{&decisionTask{taskList: taskList, scheduleID: dt.GetEventId()}},
	})

	if err != nil {
		logPersistantStoreErrorEvent(e.logger, tagValueStoreOperationCreateWorkflowExecution, err,
			fmt.Sprintf("{WorkflowID: %v, RunID: %v}", executionID, runID))
		return nilWorkflowExecution, err
	}

	return workflowExecution, nil
}

// PollForDecisionTask tries to get the decision task using exponential backoff.
func (e *engineImpl) PollForDecisionTask(request *workflow.PollForDecisionTaskRequest) (
	*workflow.PollForDecisionTaskResponse, error) {
	var response *workflow.PollForDecisionTaskResponse
	err := backoff.Retry(
		func() error {
			var er error
			response, er = e.pollForDecisionTaskOperation(request)
			return er
		}, longPollRetryPolicy, isLongPollRetryableError)

	if err != nil && err == errNoTasks {
		return emptyPollForDecisionTaskResponse, nil
	}

	return response, err
}

func (e *engineImpl) RespondDecisionTaskCompleted(request *workflow.RespondDecisionTaskCompletedRequest) error {
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
		transferTasks := []task{}
	Process_Decision_Loop:
		for _, d := range request.Decisions {
			switch d.GetDecisionType() {
			case workflow.DecisionType_ScheduleActivityTask:
				attributes := d.GetScheduleActivityTaskDecisionAttributes()
				scheduleEvent := builder.AddActivityTaskScheduledEvent(completedID, attributes)
				transferTasks = append(transferTasks, &activityTask{
					taskList:   attributes.GetTaskList().GetName(),
					scheduleID: scheduleEvent.GetEventId(),
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
			transferTasks = append(transferTasks, &decisionTask{
				taskList:   startAttributes.GetTaskList().GetName(),
				scheduleID: newDecisionEvent.GetEventId(),
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

// PollForActivityTask tries to get the activity task using exponential backoff.
func (e *engineImpl) PollForActivityTask(request *workflow.PollForActivityTaskRequest) (
	*workflow.PollForActivityTaskResponse, error) {
	var response *workflow.PollForActivityTaskResponse
	err := backoff.Retry(
		func() error {
			var er error
			response, er = e.pollForActivityTaskOperation(request)
			return er
		}, longPollRetryPolicy, isLongPollRetryableError)

	if err != nil && err == errNoTasks {
		return emptyPollForActivityTaskResponse, nil
	}

	return response, err
}

func (e *engineImpl) RespondActivityTaskCompleted(request *workflow.RespondActivityTaskCompletedRequest) error {
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

		var transferTasks []task
		if !builder.hasPendingDecisionTask() {
			startWorkflowExecutionEvent := builder.GetEvent(firstEventID)
			startAttributes := startWorkflowExecutionEvent.GetWorkflowExecutionStartedEventAttributes()
			newDecisionEvent := builder.AddDecisionTaskScheduledEvent(startAttributes.GetTaskList().GetName(),
				startAttributes.GetTaskStartToCloseTimeoutSeconds())
			transferTasks = []task{&decisionTask{
				taskList:   startAttributes.GetTaskList().GetName(),
				scheduleID: newDecisionEvent.GetEventId(),
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

func (e *engineImpl) RespondActivityTaskFailed(request *workflow.RespondActivityTaskFailedRequest) error {
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

		var transferTasks []task
		if !builder.hasPendingDecisionTask() {
			startWorkflowExecutionEvent := builder.GetEvent(firstEventID)
			startAttributes := startWorkflowExecutionEvent.GetWorkflowExecutionStartedEventAttributes()
			newDecisionEvent := builder.AddDecisionTaskScheduledEvent(startAttributes.GetTaskList().GetName(),
				startAttributes.GetTaskStartToCloseTimeoutSeconds())
			transferTasks = []task{&decisionTask{
				taskList:   startAttributes.GetTaskList().GetName(),
				scheduleID: newDecisionEvent.GetEventId(),
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

func (e *engineImpl) pollForDecisionTaskOperation(request *workflow.PollForDecisionTaskRequest) (
	*workflow.PollForDecisionTaskResponse, error) {
	context, err := e.buildTaskContext(request.TaskList.GetName(), taskTypeDecision)
	if err != nil {
		return nil, err
	}
	defer context.completeTask()

	scheduleID := context.info.scheduleID
Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		builder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return nil, err1
		}

		// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
		// task is not outstanding than it is most probably a duplicate and complete the task.
		if isRunning, startedID := builder.isDecisionTaskRunning(scheduleID); !isRunning || startedID != emptyEventID {
			logDuplicateTaskEvent(context.logger, taskTypeDecision, context.info.taskID, scheduleID, startedID, isRunning)
			return nil, errDuplicate
		}

		event := builder.AddDecisionTaskStartedEvent(scheduleID, request)
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

		return e.createPollForDecisionTaskResponse(context, event), nil
	}

	return nil, errMaxAttemptsExceeded
}

// pollForActivityTaskOperation takes one task from the task manager, update workflow execution history, mark task as
// completed and return it to user. If a task from task manager is already started, return an empty response, without
// error. Timeouts handled by the timer queue.
func (e *engineImpl) pollForActivityTaskOperation(request *workflow.PollForActivityTaskRequest) (
	*workflow.PollForActivityTaskResponse, error) {
	context, err := e.buildTaskContext(request.TaskList.GetName(), taskTypeActivity)
	if err != nil {
		return nil, err
	}
	defer context.completeTask()

	scheduleID := context.info.scheduleID
Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		builder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return nil, err1
		}

		// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
		// task is not outstanding than it is most probably a duplicate and complete the task.
		if isRunning, startedID := builder.isActivityTaskRunning(scheduleID); !isRunning || startedID != emptyEventID {
			logDuplicateTaskEvent(context.logger, taskTypeActivity, context.info.taskID, scheduleID, startedID, isRunning)
			return nil, errDuplicate
		}

		event := builder.AddActivityTaskStartedEvent(scheduleID, request)
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

		return e.createPollForActivityTaskResponse(context, event), nil
	}

	return nil, errMaxAttemptsExceeded
}

// Creates a task context for a given task list and type.
func (e *engineImpl) buildTaskContext(taskList string, taskType int) (*taskContext, error) {
	getTaskResponse, err := e.getTasksWithRetry(&getTasksRequest{
		taskList:    taskList,
		taskType:    taskType,
		lockTimeout: taskLockDuration,
		batchSize:   1,
	})

	if err != nil {
		logPersistantStoreErrorEvent(e.logger, tagValueStoreOperationGetTasks, err,
			fmt.Sprintf("{taskType: %v, taskList: %v}", taskType, taskList))
		return nil, err
	}

	if len(getTaskResponse.tasks) == 0 {
		return nil, errNoTasks
	}

	t := getTaskResponse.tasks[0]
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(t.workflowID),
		RunId:      common.StringPtr(t.runID),
	}

	context := &taskContext{
		info: t,
		workflowExecutionContext: newWorkflowExecutionContext(e, workflowExecution),
	}

	return context, nil
}

func (e *engineImpl) getTasksWithRetry(request *getTasksRequest) (*getTasksResponse, error) {
	var response *getTasksResponse
	op := func() error {
		var err error
		response, err = e.taskManager.GetTasks(request)

		return err
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, isPersistenceTransientError)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (e *engineImpl) completeTaskWithRetry(request *completeTaskRequest) error {
	op := func() error {
		return e.taskManager.CompleteTask(request)
	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, isPersistenceTransientError)
}

func (e *engineImpl) getWorkflowExecutionWithRetry(request *getWorkflowExecutionRequest) (*getWorkflowExecutionResponse,
	error) {
	var response *getWorkflowExecutionResponse
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

func (e *engineImpl) deleteWorkflowExecutionWithRetry(request *deleteWorkflowExecutionRequest) error {
	op := func() error {
		return e.executionManager.DeleteWorkflowExecution(request)
	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, isPersistenceTransientError)
}

func (e *engineImpl) updateWorkflowExecutionWithRetry(request *updateWorkflowExecutionRequest) error {
	op := func() error {
		return e.executionManager.UpdateWorkflowExecution(request)

	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, isPersistenceTransientError)
}

func (e *engineImpl) createPollForDecisionTaskResponse(context *taskContext,
	startedEvent *workflow.HistoryEvent) *workflow.PollForDecisionTaskResponse {
	task := context.info
	builder := context.builder

	response := workflow.NewPollForDecisionTaskResponse()
	response.WorkflowExecution = workflowExecutionPtr(context.workflowExecution)
	token := &taskToken{
		WorkflowID: task.workflowID,
		RunID:      task.runID,
		ScheduleID: task.scheduleID,
	}
	response.TaskToken, _ = e.tokenSerializer.Serialize(token)
	response.WorkflowType = builder.getWorkflowType()
	if builder.previousDecisionStartedEvent() != emptyEventID {
		response.PreviousStartedEventId = common.Int64Ptr(builder.previousDecisionStartedEvent())
	}
	response.StartedEventId = common.Int64Ptr(startedEvent.GetEventId())
	response.History = builder.getHistory()

	return response
}

// Populate the activity task response based on context and scheduled/started events.
func (e *engineImpl) createPollForActivityTaskResponse(context *taskContext,
	startedEvent *workflow.HistoryEvent) *workflow.PollForActivityTaskResponse {
	task := context.info
	builder := context.builder

	scheduledEvent := builder.GetEvent(task.scheduleID)
	attributes := scheduledEvent.GetActivityTaskScheduledEventAttributes()

	response := workflow.NewPollForActivityTaskResponse()
	response.ActivityId = common.StringPtr(attributes.GetActivityId())
	response.ActivityType = attributes.GetActivityType()
	response.Input = attributes.GetInput()
	response.StartedEventId = common.Int64Ptr(startedEvent.GetEventId())
	response.WorkflowExecution = workflowExecutionPtr(context.workflowExecution)

	token := &taskToken{
		WorkflowID: task.workflowID,
		RunID:      task.runID,
		ScheduleID: task.scheduleID,
	}

	response.TaskToken, _ = e.tokenSerializer.Serialize(token)
	return response
}

func newWorkflowExecutionContext(engine *engineImpl, execution workflow.WorkflowExecution) *workflowExecutionContext {
	return &workflowExecutionContext{
		workflowExecution: execution,
		engine:            engine,
		logger: engine.logger.WithFields(bark.Fields{
			tagWorkflowExecutionID: execution.GetWorkflowId(),
			tagWorkflowRunID:       execution.GetRunId(),
		}),
	}
}

// Used to either create or update the execution context for the task context.
// Update can happen when conditional write fails.
func (c *workflowExecutionContext) loadWorkflowExecution() (*historyBuilder, error) {
	response, err := c.engine.getWorkflowExecutionWithRetry(&getWorkflowExecutionRequest{execution: c.workflowExecution})
	if err != nil {
		logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationGetWorkflowExecution, err, "")
		return nil, err
	}

	c.executionInfo = response.executionInfo
	c.updateCondition = response.executionInfo.nextEventID
	builder := newHistoryBuilder(c.logger)
	if err := builder.loadExecutionInfo(response.executionInfo); err != nil {
		return nil, err
	}
	c.builder = builder
	return builder, nil
}

func (c *workflowExecutionContext) updateWorkflowExecutionWithContext(context []byte, transferTasks []task) error {
	c.executionInfo.executionContext = context

	return c.updateWorkflowExecution(transferTasks)
}

func (c *workflowExecutionContext) updateWorkflowExecution(transferTasks []task) error {
	updatedHistory, err := c.builder.Serialize()
	if err != nil {
		logHistorySerializationErrorEvent(c.logger, err, "Unable to serialize execution history for update.")
		return err
	}

	c.executionInfo.nextEventID = c.builder.nextEventID
	c.executionInfo.lastProcessedEvent = c.builder.previousDecisionStartedEvent()
	c.executionInfo.history = updatedHistory
	c.executionInfo.decisionPending = c.builder.hasPendingDecisionTask()
	c.executionInfo.state = c.builder.getWorklowState()
	if err1 := c.engine.updateWorkflowExecutionWithRetry(&updateWorkflowExecutionRequest{
		executionInfo: c.executionInfo,
		transferTasks: transferTasks,
		condition:     c.updateCondition,
	}); err1 != nil {
		switch err1.(type) {
		case *conditionFailedError:
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
	err := c.engine.deleteWorkflowExecutionWithRetry(&deleteWorkflowExecutionRequest{
		execution: c.workflowExecution,
		condition: c.updateCondition,
	})
	if err != nil {
		// TODO: We will be needing a background job to delete all leaking workflow executions due to failed delete
		// We cannot return an error back to client at this stage.  For now just log and move on.
		logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationDeleteWorkflowExecution, err,
			fmt.Sprintf("{updateCondition: %v}", c.updateCondition))
	}

	return err
}

func (c *taskContext) completeTask() error {
	completeReq := &completeTaskRequest{
		execution: c.workflowExecution,
		taskList:  c.info.taskList,
		taskType:  c.info.taskType,
		taskID:    c.info.taskID,
		lockToken: c.info.lockToken,
	}

	err := c.engine.completeTaskWithRetry(completeReq)
	if err != nil {
		logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationCompleteTask, err,
			fmt.Sprintf("{taskID: %v, taskType: %v, taskList: %v}", c.info.taskID, c.info.taskType, c.info.taskList))
	}

	return err
}

func createPersistanceRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(retryPersistenceOperationInitialInterval)
	policy.SetMaximumInterval(retryPersistenceOperationMaxInterval)
	policy.SetExpirationInterval(retryPersistenceOperationExpirationInterval)

	return policy
}

func createLongPollRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(retryLongPollInitialInterval)
	policy.SetMaximumInterval(retryLongPollMaxInterval)
	policy.SetExpirationInterval(retryLongPollExpirationInterval)

	return policy
}

func isPersistenceTransientError(err error) bool {
	switch err.(type) {
	case *workflow.InternalServiceError:
		return true
	}

	return false
}

func isLongPollRetryableError(err error) bool {
	if err == errNoTasks {
		return true
	}

	return false
}

func workflowExecutionPtr(execution workflow.WorkflowExecution) *workflow.WorkflowExecution {
	return &execution
}
