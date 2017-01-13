package matching

import (
	"errors"
	"fmt"
	"time"

	h "code.uber.internal/devexp/minions/.gen/go/history"
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/client/history"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/backoff"
	"code.uber.internal/devexp/minions/common/persistence"
	"code.uber.internal/devexp/minions/common/util"
	"github.com/uber-common/bark"
)

type matchingEngineImpl struct {
	taskManager     persistence.TaskManager
	historyService  history.Client
	tokenSerializer common.TaskTokenSerializer
	logger          bark.Logger
}

// Contains information needed for current task transition from Activity queue to Workflow execution history.
type taskContext struct {
	taskUUID          string
	info              *persistence.TaskInfo
	workflowExecution workflow.WorkflowExecution
	matchingEngine    *matchingEngineImpl
	logger            bark.Logger
}

const (
	taskLockDuration = 10 * time.Second

	retryLongPollInitialInterval    = 10 * time.Millisecond
	retryLongPollMaxInterval        = 10 * time.Millisecond
	retryLongPollExpirationInterval = 2 * time.Minute
)

var (
	// EmptyPollForDecisionTaskResponse is the response when there are no decision tasks to hand out
	EmptyPollForDecisionTaskResponse = workflow.NewPollForDecisionTaskResponse()
	// EmptyPollForActivityTaskResponse is the response when there are no activity tasks to hand out
	EmptyPollForActivityTaskResponse = workflow.NewPollForActivityTaskResponse()
	persistenceOperationRetryPolicy  = util.CreatePersistanceRetryPolicy()
	longPollRetryPolicy              = createLongPollRetryPolicy()

	// ErrNoTasks is exported temporarily for integration test
	ErrNoTasks = errors.New("No tasks")
)

// NewEngine creates an instance of matching engine
func NewEngine(taskManager persistence.TaskManager, historyService history.Client, logger bark.Logger) Engine {
	return &matchingEngineImpl{
		taskManager:     taskManager,
		historyService:  historyService,
		tokenSerializer: common.NewJSONTaskTokenSerializer(),
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

	if err != nil && err == ErrNoTasks {
		return EmptyPollForDecisionTaskResponse, nil
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

	if err != nil && err == ErrNoTasks {
		return EmptyPollForActivityTaskResponse, nil
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

	resp, err := e.historyService.RecordDecisionTaskStarted(&h.RecordDecisionTaskStartedRequest{
		WorkflowExecution: &context.workflowExecution,
		ScheduleId:        &context.info.ScheduleID,
		TaskId:            &context.info.TaskID,
		PollRequest:       request,
	})

	if err != nil {
		return nil, err
	}

	return e.createPollForDecisionTaskResponse(context, resp), nil
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

	resp, err := e.historyService.RecordActivityTaskStarted(&h.RecordActivityTaskStartedRequest{
		WorkflowExecution: &context.workflowExecution,
		ScheduleId:        &context.info.ScheduleID,
		TaskId:            &context.info.TaskID,
		PollRequest:       request,
	})

	if err != nil {
		return nil, err
	}

	return e.createPollForActivityTaskResponse(context, resp), nil

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
		return nil, ErrNoTasks
	}

	tWrapped := getTaskResponse.Tasks[0]
	t := tWrapped.Info
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(t.WorkflowID),
		RunId:      common.StringPtr(t.RunID),
	}

	context := newTaskContext(e, tWrapped.TaskUUID, t, workflowExecution, e.logger)

	return context, nil
}

func (e *matchingEngineImpl) getTasksWithRetry(request *persistence.GetTasksRequest) (*persistence.GetTasksResponse, error) {
	var response *persistence.GetTasksResponse
	op := func() error {
		var err error
		response, err = e.taskManager.GetTasks(request)

		return err
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, util.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (e *matchingEngineImpl) completeTaskWithRetry(request *persistence.CompleteTaskRequest) error {
	op := func() error {
		return e.taskManager.CompleteTask(request)
	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, util.IsPersistenceTransientError)
}

func (e *matchingEngineImpl) createPollForDecisionTaskResponse(context *taskContext,
	historyResponse *h.RecordDecisionTaskStartedResponse) *workflow.PollForDecisionTaskResponse {
	task := context.info

	response := workflow.NewPollForDecisionTaskResponse()
	response.WorkflowExecution = workflowExecutionPtr(context.workflowExecution)
	token := &common.TaskToken{
		WorkflowID: task.WorkflowID,
		RunID:      task.RunID,
		ScheduleID: task.ScheduleID,
	}
	response.TaskToken, _ = e.tokenSerializer.Serialize(token)
	response.WorkflowType = historyResponse.GetWorkflowType()
	if historyResponse.GetPreviousStartedEventId() != common.EmptyEventID {
		response.PreviousStartedEventId = historyResponse.PreviousStartedEventId
	}
	response.StartedEventId = historyResponse.StartedEventId
	response.History = historyResponse.History

	return response
}

// Populate the activity task response based on context and scheduled/started events.
func (e *matchingEngineImpl) createPollForActivityTaskResponse(context *taskContext,
	historyResponse *h.RecordActivityTaskStartedResponse) *workflow.PollForActivityTaskResponse {
	task := context.info

	startedEvent := historyResponse.StartedEvent
	scheduledEvent := historyResponse.ScheduledEvent
	attributes := scheduledEvent.GetActivityTaskScheduledEventAttributes()

	response := workflow.NewPollForActivityTaskResponse()
	response.ActivityId = common.StringPtr(attributes.GetActivityId())
	response.ActivityType = attributes.GetActivityType()
	response.Input = attributes.GetInput()
	response.StartedEventId = common.Int64Ptr(startedEvent.GetEventId())
	response.WorkflowExecution = workflowExecutionPtr(context.workflowExecution)

	token := &common.TaskToken{
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
		TaskID:    c.taskUUID,
		LockToken: c.info.LockToken,
	}

	err := c.matchingEngine.completeTaskWithRetry(completeReq)
	if err != nil {
		logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationCompleteTask, err,
			fmt.Sprintf("{taskID: %v, taskType: %v, taskList: %v}", c.info.TaskID, c.info.TaskType, c.info.TaskList))
	}

	return err
}

func newTaskContext(matchingEngine *matchingEngineImpl, taskUUID string, info *persistence.TaskInfo,
	execution workflow.WorkflowExecution, logger bark.Logger) *taskContext {
	return &taskContext{
		taskUUID:          taskUUID,
		info:              info,
		matchingEngine:    matchingEngine,
		workflowExecution: execution,
		logger:            logger,
	}
}

func createLongPollRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(retryLongPollInitialInterval)
	policy.SetMaximumInterval(retryLongPollMaxInterval)
	policy.SetExpirationInterval(retryLongPollExpirationInterval)

	return policy
}

func isLongPollRetryableError(err error) bool {
	if err == ErrNoTasks {
		return true
	}

	return false
}

func workflowExecutionPtr(execution workflow.WorkflowExecution) *workflow.WorkflowExecution {
	return &execution
}
