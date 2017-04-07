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
		domainID          string
		workflowExecution workflow.WorkflowExecution
		shard             ShardContext
		executionManager  persistence.ExecutionManager
		logger            bark.Logger

		sync.Mutex
		msBuilder       *mutableStateBuilder
		tBuilder        *timerBuilder
		updateCondition int64
		deleteTimerTask persistence.Task
	}
)

var (
	persistenceOperationRetryPolicy = common.CreatePersistanceRetryPolicy()
)

func newWorkflowExecutionContext(domainID string, execution workflow.WorkflowExecution, shard ShardContext,
	executionManager persistence.ExecutionManager, logger bark.Logger) *workflowExecutionContext {
	lg := logger.WithFields(bark.Fields{
		tagWorkflowExecutionID: execution.GetWorkflowId(),
		tagWorkflowRunID:       execution.GetRunId(),
	})
	tBuilder := newTimerBuilder(&shardSeqNumGenerator{context: shard}, lg)

	return &workflowExecutionContext{
		domainID:          domainID,
		workflowExecution: execution,
		shard:             shard,
		executionManager:  executionManager,
		tBuilder:          tBuilder,
		logger:            lg,
	}
}

func (c *workflowExecutionContext) loadWorkflowExecution() (*mutableStateBuilder, error) {
	if c.msBuilder != nil {
		return c.msBuilder, nil
	}

	response, err := c.getWorkflowExecutionWithRetry(&persistence.GetWorkflowExecutionRequest{
		DomainID:  c.domainID,
		Execution: c.workflowExecution,
	})
	if err != nil {
		logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationGetWorkflowExecution, err, "")
		return nil, err
	}

	msBuilder := newMutableStateBuilder(c.logger)
	if response != nil && response.State != nil {
		state := response.State
		msBuilder.Load(state)
		info := state.ExecutionInfo
		c.updateCondition = info.NextEventID
	}

	c.msBuilder = msBuilder
	return msBuilder, nil
}

func (c *workflowExecutionContext) updateWorkflowExecutionWithContext(context []byte, transferTasks []persistence.Task,
	timerTasks []persistence.Task, transactionID int64) error {
	c.msBuilder.executionInfo.ExecutionContext = context

	return c.updateWorkflowExecution(transferTasks, timerTasks, transactionID)
}

func (c *workflowExecutionContext) updateWorkflowExecutionWithDeleteTask(transferTasks []persistence.Task,
	timerTasks []persistence.Task, deleteTimerTask persistence.Task, transactionID int64) error {
	c.deleteTimerTask = deleteTimerTask

	return c.updateWorkflowExecution(transferTasks, timerTasks, transactionID)
}

func (c *workflowExecutionContext) updateWorkflowExecution(transferTasks []persistence.Task,
	timerTasks []persistence.Task, transactionID int64) error {
	// Take a snapshot of all updates we have accumulated for this execution
	updates := c.msBuilder.CloseUpdateSession()

	builder := updates.newEventsBuilder
	if builder.history != nil && len(builder.history) > 0 {
		// Some operations only update the mutable state. For example RecordActivityTaskHeartbeat.
		firstEvent := builder.history[0]
		newEvents, err := builder.Serialize()
		if err != nil {
			logHistorySerializationErrorEvent(c.logger, err, "Unable to serialize execution history for update.")
			return err
		}

		if err0 := c.shard.AppendHistoryEvents(&persistence.AppendHistoryEventsRequest{
			Execution:     c.workflowExecution,
			TransactionID: transactionID,
			FirstEventID:  firstEvent.GetEventId(),
			Events:        newEvents,
		}); err0 != nil {
			// Clear all cached state in case of error
			c.clear()

			switch err0.(type) {
			case *persistence.ConditionFailedError:
				return ErrConflict
			}

			logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationUpdateWorkflowExecution, err0,
				fmt.Sprintf("{updateCondition: %v}", c.updateCondition))
			return err0
		}
	}

	if err1 := c.updateWorkflowExecutionWithRetry(&persistence.UpdateWorkflowExecutionRequest{
		ExecutionInfo:       c.msBuilder.executionInfo,
		TransferTasks:       transferTasks,
		TimerTasks:          timerTasks,
		Condition:           c.updateCondition,
		DeleteTimerTask:     c.deleteTimerTask,
		UpsertActivityInfos: updates.updateActivityInfos,
		DeleteActivityInfo:  updates.deleteActivityInfo,
		UpserTimerInfos:     updates.updateTimerInfos,
		DeleteTimerInfos:    updates.deleteTimerInfos,
	}); err1 != nil {
		// Clear all cached state in case of error
		c.clear()

		switch err1.(type) {
		case *persistence.ConditionFailedError:
			return ErrConflict
		}

		logPersistantStoreErrorEvent(c.logger, tagValueStoreOperationUpdateWorkflowExecution, err1,
			fmt.Sprintf("{updateCondition: %v}", c.updateCondition))
		return err1
	}

	// Update went through so update the condition for new updates
	c.updateCondition = c.msBuilder.GetNextEventID()
	return nil
}

func (c *workflowExecutionContext) deleteWorkflowExecution() error {
	err := c.deleteWorkflowExecutionWithRetry(&persistence.DeleteWorkflowExecutionRequest{
		ExecutionInfo: c.msBuilder.executionInfo,
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
	c.msBuilder = nil
	c.tBuilder = newTimerBuilder(&shardSeqNumGenerator{context: c.shard}, c.logger)
}
