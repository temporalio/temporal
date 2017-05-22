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
	"fmt"
	"sync"
	"time"

	"github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/persistence"

	"github.com/uber-common/bark"
	hc "github.com/uber/cadence/client/history"
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
		logging.TagWorkflowExecutionID: execution.GetWorkflowId(),
		logging.TagWorkflowRunID:       execution.GetRunId(),
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
		logging.LogPersistantStoreErrorEvent(c.logger, logging.TagValueStoreOperationGetWorkflowExecution, err, "")
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
		serializedHistory, err := builder.Serialize()
		if err != nil {
			logging.LogHistorySerializationErrorEvent(c.logger, err, "Unable to serialize execution history for update.")
			return err
		}

		if err0 := c.shard.AppendHistoryEvents(&persistence.AppendHistoryEventsRequest{
			DomainID:      c.domainID,
			Execution:     c.workflowExecution,
			TransactionID: transactionID,
			FirstEventID:  firstEvent.GetEventId(),
			Events:        serializedHistory,
		}); err0 != nil {
			// Clear all cached state in case of error
			c.clear()

			switch err0.(type) {
			case *persistence.ConditionFailedError:
				return ErrConflict
			}

			logging.LogPersistantStoreErrorEvent(c.logger, logging.TagValueStoreOperationUpdateWorkflowExecution, err0,
				fmt.Sprintf("{updateCondition: %v}", c.updateCondition))
			return err0
		}

	}

	continueAsNew := updates.continueAsNew
	deleteExecution := false
	if c.msBuilder.executionInfo.State == persistence.WorkflowStateCompleted {
		// Workflow execution completed as part of this transaction.
		// Also transactionally delete workflow execution representing current run for the execution
		deleteExecution = true
	}
	if err1 := c.updateWorkflowExecutionWithRetry(&persistence.UpdateWorkflowExecutionRequest{
		ExecutionInfo:             c.msBuilder.executionInfo,
		TransferTasks:             transferTasks,
		TimerTasks:                timerTasks,
		Condition:                 c.updateCondition,
		DeleteTimerTask:           c.deleteTimerTask,
		UpsertActivityInfos:       updates.updateActivityInfos,
		DeleteActivityInfo:        updates.deleteActivityInfo,
		UpserTimerInfos:           updates.updateTimerInfos,
		DeleteTimerInfos:          updates.deleteTimerInfos,
		UpsertChildExecutionInfos: updates.updateChildExecutionInfos,
		DeleteChildExecutionInfo:  updates.deleteChildExecutionInfo,
		ContinueAsNew:             continueAsNew,
		CloseExecution:            deleteExecution,
	}); err1 != nil {
		// Clear all cached state in case of error
		c.clear()

		switch err1.(type) {
		case *persistence.ConditionFailedError:
			return ErrConflict
		}

		logging.LogPersistantStoreErrorEvent(c.logger, logging.TagValueStoreOperationUpdateWorkflowExecution, err1,
			fmt.Sprintf("{updateCondition: %v}", c.updateCondition))
		return err1
	}

	// Update went through so update the condition for new updates
	c.updateCondition = c.msBuilder.GetNextEventID()
	c.msBuilder.executionInfo.LastUpdatedTimestamp = time.Now()
	return nil
}

func (c *workflowExecutionContext) continueAsNewWorkflowExecution(context []byte, newStateBuilder *mutableStateBuilder,
	transferTasks []persistence.Task, transactionID int64) error {

	domainID := newStateBuilder.executionInfo.DomainID
	newExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(newStateBuilder.executionInfo.WorkflowID),
		RunId:      common.StringPtr(newStateBuilder.executionInfo.RunID),
	}
	firstEvent := newStateBuilder.hBuilder.history[0]

	// Serialize the history
	serializedHistory, serializedError := newStateBuilder.hBuilder.Serialize()
	if serializedError != nil {
		logging.LogHistorySerializationErrorEvent(c.logger, serializedError, fmt.Sprintf(
			"HistoryEventBatch serialization error on start workflow.  WorkflowID: %v, RunID: %v", newExecution.GetWorkflowId(),
			newExecution.GetRunId()))
		return serializedError
	}

	err1 := c.shard.AppendHistoryEvents(&persistence.AppendHistoryEventsRequest{
		DomainID:  domainID,
		Execution: newExecution,
		// It is ok to use 0 for TransactionID because RunID is unique so there are
		// no potential duplicates to override.
		TransactionID: 0,
		FirstEventID:  firstEvent.GetEventId(),
		Events:        serializedHistory,
	})
	if err1 != nil {
		return err1
	}

	err2 := c.updateWorkflowExecutionWithContext(context, transferTasks, nil, transactionID)

	if err2 != nil {
		// TODO: Delete new execution if update fails due to conflict or shard being lost
	}

	return err2
}

func (c *workflowExecutionContext) deleteWorkflowExecution() error {
	err := c.deleteWorkflowExecutionWithRetry(&persistence.DeleteWorkflowExecutionRequest{
		ExecutionInfo: c.msBuilder.executionInfo,
	})
	if err != nil {
		// TODO: We will be needing a background job to delete all leaking workflow executions due to failed delete
		// We cannot return an error back to client at this stage.  For now just log and move on.
		logging.LogPersistantStoreErrorEvent(c.logger, logging.TagValueStoreOperationDeleteWorkflowExecution, err,
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

// Few problems with this approach.
//   https://github.com/uber/cadence/issues/145
//  (1) On the target workflow we can generate more than one cancel request if we end up retrying because of intermittent
//	errors. We might want to have deduping logic internally to avoid that.
//  (2) For single cancel transfer task we can generate more than one ExternalWorkflowExecutionCancelRequested event in the
//	history if we fail to delete transfer task and retry again. We need some logic to look back at the event
//      state in mutable state when we are processing this transfer task.
//      This means for one single ExternalWorkflowExecutionCancelInitiated we can see a
//      ExternalWorkflowExecutionCancelRequested and RequestCancelExternalWorkflowExecutionFailedEvent.
func (c *workflowExecutionContext) requestExternalCancelWorkflowExecutionWithRetry(
	historyClient hc.Client,
	request *history.RequestCancelWorkflowExecutionRequest,
	initiatedEventID int64) error {
	op := func() error {
		return historyClient.RequestCancelWorkflowExecution(nil, request)
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err == nil {
		// We succeeded in request to cancel workflow.
		if c.msBuilder.AddExternalWorkflowExecutionCancelRequested(
			initiatedEventID,
			request.GetDomainUUID(),
			request.GetCancelRequest().GetWorkflowExecution().GetWorkflowId(),
			request.GetCancelRequest().GetWorkflowExecution().GetRunId()) == nil {
			return &workflow.InternalServiceError{
				Message: "Unable to write event to complete request of external cancel workflow execution."}
		}

		// Generate a transaction ID for appending events to history
		transactionID, err := c.shard.GetNextTransferTaskID()
		if err != nil {
			return err
		}
		return c.updateWorkflowExecution(nil, nil, transactionID)

	} else if err != nil && common.IsServiceNonRetryableError(err) {
		// We failed in request to cancel workflow.
		if c.msBuilder.AddRequestCancelExternalWorkflowExecutionFailedEvent(
			emptyEventID,
			initiatedEventID,
			request.GetDomainUUID(),
			request.GetCancelRequest().GetWorkflowExecution().GetWorkflowId(),
			request.GetCancelRequest().GetWorkflowExecution().GetRunId(),
			workflow.CancelExternalWorkflowExecutionFailedCause_UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION) == nil {
			return &workflow.InternalServiceError{
				Message: "Unable to write failure event of external cancel workflow execution."}
		}

		// Generate a transaction ID for appending events to history
		transactionID, err := c.shard.GetNextTransferTaskID()
		if err != nil {
			return err
		}
		return c.updateWorkflowExecution(nil, nil, transactionID)
	}
	return err
}

func (c *workflowExecutionContext) clear() {
	c.msBuilder = nil
	c.tBuilder = newTimerBuilder(&shardSeqNumGenerator{context: c.shard}, c.logger)
}
