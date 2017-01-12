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
	// EmptyPollForDecisionTaskResponse is the response when there are no decision tasks to hand out
	EmptyPollForDecisionTaskResponse = workflow.NewPollForDecisionTaskResponse()
	// EmptyPollForActivityTaskResponse is the response when there are no activity tasks to hand out
	EmptyPollForActivityTaskResponse = workflow.NewPollForActivityTaskResponse()
	persistenceOperationRetryPolicy  = createPersistanceRetryPolicy()
	longPollRetryPolicy              = createLongPollRetryPolicy()

	// ErrDuplicate is exported temporarily for integration test
	ErrDuplicate = errors.New("Duplicate task, completing it")
	// ErrCreateEvent is exported temporarily for integration test
	ErrCreateEvent = errors.New("Can't create activity task started event")
	// ErrNoTasks is exported temporarily for integration test
	ErrNoTasks = errors.New("No tasks")
	// ErrConflict is exported temporarily for integration test
	ErrConflict = errors.New("Conditional update failed")
	// ErrMaxAttemptsExceeded is exported temporarily for integration test
	ErrMaxAttemptsExceeded = errors.New("Maximum attempts exceeded to update history")
)

// NewWorkflowEngineWithShard creates an instannce of engine.
func NewWorkflowEngineWithShard(shard ShardContext, executionManager persistence.ExecutionManager, taskManager persistence.TaskManager,
	logger bark.Logger) Engine {

	history := NewHistoryEngineWithShardContext(shard, executionManager, taskManager, logger)
	return &EngineImpl{
		historyService: history,
		matchingEngine: NewMatchingEngine(taskManager, history, logger),
		logger:         logger,
	}
}

// NewWorkflowEngine creates an instannce of engine.
func NewWorkflowEngine(executionManager persistence.ExecutionManager, taskManager persistence.TaskManager,
	logger bark.Logger) Engine {

	history := NewHistoryEngine(1, executionManager, taskManager, logger)
	return &EngineImpl{
		historyService: history,
		matchingEngine: NewMatchingEngine(taskManager, history, logger),
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
func (e *EngineImpl) StartWorkflowExecution(request *workflow.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error) {
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
	if err == ErrNoTasks || err == ErrDuplicate {
		return true
	}

	return false
}

func workflowExecutionPtr(execution workflow.WorkflowExecution) *workflow.WorkflowExecution {
	return &execution
}

// PrintHistory prints history
func PrintHistory(history *workflow.History, logger bark.Logger) {
	serializer := newJSONHistorySerializer()
	data, err := serializer.Serialize(history.GetEvents())
	if err != nil {
		logger.Errorf("Error serializing history: %v\n", err)
	}

	logger.Info("******************************************")
	logger.Infof("History: %v", string(data))
	logger.Info("******************************************")
}
