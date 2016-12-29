package workflow

import (
	"errors"
	"time"

	"github.com/uber-common/bark"

	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common/backoff"
	"code.uber.internal/devexp/minions/persistence"
)

const (
	taskLockDuration      = 10 * time.Second
	conditionalRetryCount = 5

	retryPersistenceOperationInitialInterval = 50 * time.Millisecond
	// TODO: Just minimizing for demo from 10 * time.second to time.second
	retryPersistenceOperationMaxInterval        = time.Second
	retryPersistenceOperationExpirationInterval = 10 * time.Second

	retryLongPollInitialInterval    = 10 * time.Millisecond
	retryLongPollMaxInterval        = 10 * time.Millisecond
	retryLongPollExpirationInterval = 2 * time.Minute
)

type (
	// EngineImpl wraps up implementation for engine layer.
	EngineImpl struct {
		historyService HistoryEngine
		matchingEngine MatchingEngine
		logger         bark.Logger
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

// NewWorkflowEngine creates an instannce of engine.
func NewWorkflowEngine(executionManager persistence.ExecutionManager, taskManager persistence.TaskManager,
	logger bark.Logger) Engine {
	shard, err := acquireShard(1, executionManager)
	if err != nil {
		return nil
	}

	history := newHistoryEngine(shard, executionManager, taskManager, logger)
	return &EngineImpl{
		historyService: history,
		matchingEngine: newMatchingEngine(taskManager, history, logger),
		logger:         logger,
	}
}

// Start the engine.
func (e *EngineImpl) Start() {
	e.historyService.Start()
}

// Stop the engine.
func (e *EngineImpl) Stop() {
	e.historyService.Stop()
}

// StartWorkflowExecution starts a workflow.
func (e *EngineImpl) StartWorkflowExecution(request *workflow.StartWorkflowExecutionRequest) (workflow.WorkflowExecution, error) {
	return e.historyService.StartWorkflowExecution(request)
}

// PollForDecisionTask polls for decision task.
func (e *EngineImpl) PollForDecisionTask(request *workflow.PollForDecisionTaskRequest) (*workflow.PollForDecisionTaskResponse, error) {
	return e.matchingEngine.PollForDecisionTask(request)
}

// PollForActivityTask polls for activity task.
func (e *EngineImpl) PollForActivityTask(request *workflow.PollForActivityTaskRequest) (*workflow.PollForActivityTaskResponse, error) {
	return e.matchingEngine.PollForActivityTask(request)
}

// RespondDecisionTaskCompleted responds to an decision completion.
func (e *EngineImpl) RespondDecisionTaskCompleted(request *workflow.RespondDecisionTaskCompletedRequest) error {
	return e.historyService.RespondDecisionTaskCompleted(request)
}

// RespondActivityTaskCompleted responds to an activity completion.
func (e *EngineImpl) RespondActivityTaskCompleted(request *workflow.RespondActivityTaskCompletedRequest) error {
	return e.historyService.RespondActivityTaskCompleted(request)
}

// RespondActivityTaskFailed responds to an activity failure.
func (e *EngineImpl) RespondActivityTaskFailed(request *workflow.RespondActivityTaskFailedRequest) error {
	return e.historyService.RespondActivityTaskFailed(request)
}

// GetWorkflowExecutionHistory retrieves the history for given workflow execution
func (e *EngineImpl) GetWorkflowExecutionHistory(
	request *workflow.GetWorkflowExecutionHistoryRequest) (*workflow.GetWorkflowExecutionHistoryResponse, error) {
	return e.historyService.GetWorkflowExecutionHistory(request)
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
