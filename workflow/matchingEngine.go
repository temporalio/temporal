package workflow

import (
	"fmt"

	workflow "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/backoff"
	"code.uber.internal/devexp/minions/persistence"
	"github.com/uber-common/bark"
)

type matchingEngineImpl struct {
	taskManager     persistence.TaskManager
	historyService  HistoryEngine
	tokenSerializer taskTokenSerializer
	logger          bark.Logger
}

// Contains information needed for current task transition from Activity queue to Workflow execution history.
type taskContext struct {
	info              *persistence.TaskInfo
	workflowExecution workflow.WorkflowExecution
	executionInfo     *persistence.WorkflowExecutionInfo
	builder           *historyBuilder
	matchingEngine    *matchingEngineImpl
	logger            bark.Logger
}

func newMatchingEngine(taskManager persistence.TaskManager, historyService HistoryEngine, logger bark.Logger) MatchingEngine {
	return &matchingEngineImpl{
		taskManager:     taskManager,
		historyService:  historyService,
		tokenSerializer: newJSONTaskTokenSerializer(),
		logger: logger.WithFields(bark.Fields{
			tagWorkflowComponent: tagValueWorkflowEngineComponent,
		}),
	}
}

// PollForDecisionTask tries to get the decision task using exponential backoff.
func (e *matchingEngineImpl) PollForDecisionTask(request *workflow.PollForDecisionTaskRequest) (
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

// PollForActivityTask tries to get the activity task using exponential backoff.
func (e *matchingEngineImpl) PollForActivityTask(request *workflow.PollForActivityTaskRequest) (
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

func (e *matchingEngineImpl) pollForDecisionTaskOperation(request *workflow.PollForDecisionTaskRequest) (
	*workflow.PollForDecisionTaskResponse, error) {
	context, err := e.buildTaskContext(request.TaskList.GetName(), persistence.TaskTypeDecision)
	if err != nil {
		return nil, err
	}
	defer context.completeTask()

	scheduleID := context.info.ScheduleID
Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		builder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return nil, err1
		}

		// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
		// task is not outstanding than it is most probably a duplicate and complete the task.
		if isRunning, startedID := builder.isDecisionTaskRunning(scheduleID); !isRunning || startedID != emptyEventID {
			logDuplicateTaskEvent(context.logger, persistence.TaskTypeDecision, context.info.TaskID, scheduleID, startedID, isRunning)
			return nil, errDuplicate
		}

		event := builder.AddDecisionTaskStartedEvent(scheduleID, request)
		if event == nil {
			return nil, errCreateEvent
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operation again.
		if err2 := context.updateWorkflowExecution(); err2 != nil {
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
func (e *matchingEngineImpl) pollForActivityTaskOperation(request *workflow.PollForActivityTaskRequest) (
	*workflow.PollForActivityTaskResponse, error) {
	context, err := e.buildTaskContext(request.TaskList.GetName(), persistence.TaskTypeActivity)
	if err != nil {
		return nil, err
	}
	defer context.completeTask()

	scheduleID := context.info.ScheduleID
Update_History_Loop:
	for attempt := 0; attempt < conditionalRetryCount; attempt++ {
		builder, err1 := context.loadWorkflowExecution()
		if err1 != nil {
			return nil, err1
		}

		// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
		// task is not outstanding than it is most probably a duplicate and complete the task.
		if isRunning, startedID := builder.isActivityTaskRunning(scheduleID); !isRunning || startedID != emptyEventID {
			logDuplicateTaskEvent(context.logger, persistence.TaskTypeActivity, context.info.TaskID, scheduleID, startedID, isRunning)
			return nil, errDuplicate
		}

		event := builder.AddActivityTaskStartedEvent(scheduleID, request)
		if event == nil {
			return nil, errCreateEvent
		}

		// We apply the update to execution using optimistic concurrency.  If it fails due to a conflict than reload
		// the history and try the operationi again.
		if err2 := context.updateWorkflowExecution(); err2 != nil {
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
func (e *matchingEngineImpl) buildTaskContext(taskList string, taskType int) (*taskContext, error) {
	getTaskResponse, err := e.getTasksWithRetry(&persistence.GetTasksRequest{
		TaskList:    taskList,
		TaskType:    taskType,
		LockTimeout: taskLockDuration,
		BatchSize:   1,
	})

	if err != nil {
		logPersistantStoreErrorEvent(e.logger, tagValueStoreOperationGetTasks, err,
			fmt.Sprintf("{taskType: %v, taskList: %v}", taskType, taskList))
		return nil, err
	}

	if len(getTaskResponse.Tasks) == 0 {
		return nil, errNoTasks
	}

	t := getTaskResponse.Tasks[0]
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(t.WorkflowID),
		RunId:      common.StringPtr(t.RunID),
	}

	context := newTaskContext(e, t, workflowExecution, e.logger)

	return context, nil
}

func (e *matchingEngineImpl) getTasksWithRetry(request *persistence.GetTasksRequest) (*persistence.GetTasksResponse, error) {
	var response *persistence.GetTasksResponse
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

func (e *matchingEngineImpl) completeTaskWithRetry(request *persistence.CompleteTaskRequest) error {
	op := func() error {
		return e.taskManager.CompleteTask(request)
	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, isPersistenceTransientError)
}

func (e *matchingEngineImpl) createPollForDecisionTaskResponse(context *taskContext,
	startedEvent *workflow.HistoryEvent) *workflow.PollForDecisionTaskResponse {
	task := context.info
	builder := context.builder

	response := workflow.NewPollForDecisionTaskResponse()
	response.WorkflowExecution = workflowExecutionPtr(context.workflowExecution)
	token := &taskToken{
		WorkflowID: task.WorkflowID,
		RunID:      task.RunID,
		ScheduleID: task.ScheduleID,
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
func (e *matchingEngineImpl) createPollForActivityTaskResponse(context *taskContext,
	startedEvent *workflow.HistoryEvent) *workflow.PollForActivityTaskResponse {
	task := context.info
	builder := context.builder

	scheduledEvent := builder.GetEvent(task.ScheduleID)
	attributes := scheduledEvent.GetActivityTaskScheduledEventAttributes()

	response := workflow.NewPollForActivityTaskResponse()
	response.ActivityId = common.StringPtr(attributes.GetActivityId())
	response.ActivityType = attributes.GetActivityType()
	response.Input = attributes.GetInput()
	response.StartedEventId = common.Int64Ptr(startedEvent.GetEventId())
	response.WorkflowExecution = workflowExecutionPtr(context.workflowExecution)

	token := &taskToken{
		WorkflowID: task.WorkflowID,
		RunID:      task.RunID,
		ScheduleID: task.ScheduleID,
	}

	response.TaskToken, _ = e.tokenSerializer.Serialize(token)
	return response
}

func (c *taskContext) completeTask() error {
	completeReq := &persistence.CompleteTaskRequest{
		Execution: c.workflowExecution,
		TaskList:  c.info.TaskList,
		TaskType:  c.info.TaskType,
		TaskID:    c.info.TaskID,
		LockToken: c.info.LockToken,
	}

	err := c.matchingEngine.completeTaskWithRetry(completeReq)
	if err != nil {
		logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationCompleteTask, err,
			fmt.Sprintf("{taskID: %v, taskType: %v, taskList: %v}", c.info.TaskID, c.info.TaskType, c.info.TaskList))
	}

	return err
}

func (c *taskContext) loadWorkflowExecution() (*historyBuilder, error) {
	resp, err := c.matchingEngine.historyService.GetWorkflowExecution(&persistence.GetWorkflowExecutionRequest{Execution: c.workflowExecution})
	if err != nil {
		return nil, err
	}
	builder := newHistoryBuilder(c.logger)
	if err := builder.loadExecutionInfo(resp.ExecutionInfo); err != nil {
		return nil, err
	}
	c.builder = builder
	c.executionInfo = resp.ExecutionInfo
	return builder, nil
}

func (c *taskContext) updateWorkflowExecution() error {
	updateCondition := c.executionInfo.NextEventID

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

	err1 := c.matchingEngine.historyService.UpdateWorkflowExecution(&persistence.UpdateWorkflowExecutionRequest{
		ExecutionInfo: c.executionInfo,
		Condition:     updateCondition,
	})
	if err1 != nil {
		switch err1.(type) {
		case *persistence.ConditionFailedError:
			return errConflict
		}

		return err1
	}

	return nil
}

func newTaskContext(matchingEngine *matchingEngineImpl, info *persistence.TaskInfo, execution workflow.WorkflowExecution, logger bark.Logger) *taskContext {
	return &taskContext{
		info:              info,
		matchingEngine:    matchingEngine,
		workflowExecution: execution,
		logger:            logger,
	}
}
