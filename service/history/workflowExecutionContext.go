package history

import (
	"fmt"
	"sync"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/persistence"

	"github.com/uber-common/bark"
)

type (
	workflowExecutionContext struct {
		workflowExecution workflow.WorkflowExecution
		shard             ShardContext
		executionManager  persistence.ExecutionManager
		logger            bark.Logger

		sync.Mutex
		builder         *historyBuilder
		msBuilder       *mutableStateBuilder
		tBuilder        *timerBuilder
		executionInfo   *persistence.WorkflowExecutionInfo
		updateCondition int64
		deleteTimerTask persistence.Task
	}
)

var (
	persistenceOperationRetryPolicy = common.CreatePersistanceRetryPolicy()
)

func newWorkflowExecutionContext(execution workflow.WorkflowExecution, shard ShardContext,
	executionManager persistence.ExecutionManager, logger bark.Logger) *workflowExecutionContext {
	lg := logger.WithFields(bark.Fields{
		tagWorkflowExecutionID: execution.GetWorkflowId(),
		tagWorkflowRunID:       execution.GetRunId(),
	})
	tBuilder := newTimerBuilder(&shardSeqNumGenerator{context: shard}, lg)

	return &workflowExecutionContext{
		workflowExecution: execution,
		shard:             shard,
		executionManager:  executionManager,
		tBuilder:          tBuilder,
		logger:            lg,
	}
}

func (c *workflowExecutionContext) loadWorkflowExecution() (*historyBuilder, error) {
	if c.builder != nil {
		return c.builder, nil
	}

	response, err := c.getWorkflowExecutionWithRetry(&persistence.GetWorkflowExecutionRequest{
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

func (c *workflowExecutionContext) loadWorkflowMutableState() (*mutableStateBuilder, error) {
	// TODO: Disable caching for mutable state until it supports life-cycle
	/*if c.msBuilder != nil {
		return c.msBuilder, nil
	}*/
	response, err := c.getWorkflowMutableStateWithRetry(&persistence.GetWorkflowMutableStateRequest{
		WorkflowID: c.workflowExecution.GetWorkflowId(),
		RunID:      c.workflowExecution.GetRunId()})

	if err != nil {
		logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationGetWorkflowMutableState, err, "")
		return nil, err
	}

	msBuilder := newMutableStateBuilder(c.logger)
	if response != nil && response.State != nil {
		msBuilder.Load(response.State.ActivitInfos, response.State.TimerInfos)
	}

	c.msBuilder = msBuilder
	return msBuilder, nil
}

func (c *workflowExecutionContext) updateWorkflowExecutionWithContext(context []byte, transferTasks []persistence.Task,
	timerTasks []persistence.Task) error {
	c.executionInfo.ExecutionContext = context

	return c.updateWorkflowExecution(transferTasks, timerTasks)
}

func (c *workflowExecutionContext) updateWorkflowExecutionWithDeleteTask(transferTasks []persistence.Task,
	timerTasks []persistence.Task, deleteTimerTask persistence.Task) error {
	c.deleteTimerTask = deleteTimerTask

	return c.updateWorkflowExecution(transferTasks, timerTasks)
}

func (c *workflowExecutionContext) updateWorkflowExecution(transferTasks []persistence.Task,
	timerTasks []persistence.Task) error {
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
	var upsertActivityInfos []*persistence.ActivityInfo
	var deleteActivityInfo *int64
	var upsertTimerInfos []*persistence.TimerInfo
	var deleteTimerInfos []string
	if c.msBuilder != nil {
		upsertActivityInfos = c.msBuilder.updateActivityInfos
		deleteActivityInfo = c.msBuilder.deleteActivityInfo
		upsertTimerInfos = c.msBuilder.updateTimerInfos
		deleteTimerInfos = c.msBuilder.deleteTimerInfos
	}

	if err1 := c.updateWorkflowExecutionWithRetry(&persistence.UpdateWorkflowExecutionRequest{
		ExecutionInfo:       c.executionInfo,
		TransferTasks:       transferTasks,
		TimerTasks:          timerTasks,
		Condition:           c.updateCondition,
		DeleteTimerTask:     c.deleteTimerTask,
		UpsertActivityInfos: upsertActivityInfos,
		DeleteActivityInfo:  deleteActivityInfo,
		UpserTimerInfos:     upsertTimerInfos,
		DeleteTimerInfos:    deleteTimerInfos,
	}); err1 != nil {
		// Clear all cached state in case of error
		c.clear()

		switch err1.(type) {
		case *persistence.ConditionFailedError:
			return ErrConflict
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
	err := c.deleteWorkflowExecutionWithRetry(&persistence.DeleteWorkflowExecutionRequest{
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

func (c *workflowExecutionContext) getWorkflowExecutionWithRetry(
	request *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
	var response *persistence.GetWorkflowExecutionResponse
	op := func() error {
		var err error
		response, err = c.executionManager.GetWorkflowExecution(request)

		return err
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (c *workflowExecutionContext) getWorkflowMutableStateWithRetry(
	request *persistence.GetWorkflowMutableStateRequest) (*persistence.GetWorkflowMutableStateResponse, error) {
	var response *persistence.GetWorkflowMutableStateResponse
	op := func() error {
		var err error
		response, err = c.executionManager.GetWorkflowMutableState(request)

		return err
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (c *workflowExecutionContext) updateWorkflowExecutionWithRetry(
	request *persistence.UpdateWorkflowExecutionRequest) error {
	op := func() error {
		return c.shard.UpdateWorkflowExecution(request)

	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (c *workflowExecutionContext) deleteWorkflowExecutionWithRetry(
	request *persistence.DeleteWorkflowExecutionRequest) error {
	op := func() error {
		return c.executionManager.DeleteWorkflowExecution(request)
	}

	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (c *workflowExecutionContext) clear() {
	c.builder = nil
	c.msBuilder = nil
	c.tBuilder = newTimerBuilder(&shardSeqNumGenerator{context: c.shard}, c.logger)
}
