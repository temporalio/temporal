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
	"time"

	"github.com/uber-common/bark"
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/persistence"
)

const (
	secondsInDay = int32(24 * time.Hour / time.Second)
)

type (
	workflowExecutionContext struct {
		domainID          string
		workflowExecution workflow.WorkflowExecution
		shard             ShardContext
		clusterMetadata   cluster.Metadata
		executionManager  persistence.ExecutionManager
		logger            bark.Logger

		locker                common.Mutex
		msBuilder             mutableState
		updateCondition       int64
		deleteTimerTask       persistence.Task
		createReplicationTask bool
	}
)

var (
	persistenceOperationRetryPolicy = common.CreatePersistanceRetryPolicy()
)

func newWorkflowExecutionContext(domainID string, execution workflow.WorkflowExecution, shard ShardContext,
	executionManager persistence.ExecutionManager, logger bark.Logger) *workflowExecutionContext {
	lg := logger.WithFields(bark.Fields{
		logging.TagWorkflowExecutionID: *execution.WorkflowId,
		logging.TagWorkflowRunID:       *execution.RunId,
	})

	return &workflowExecutionContext{
		domainID:          domainID,
		workflowExecution: execution,
		shard:             shard,
		clusterMetadata:   shard.GetService().GetClusterMetadata(),
		executionManager:  executionManager,
		logger:            lg,
		locker:            common.NewMutex(),
	}
}

func (c *workflowExecutionContext) loadWorkflowExecution() (mutableState, error) {
	err := c.loadWorkflowExecutionInternal()
	if err != nil {
		return nil, err
	}
	err = c.updateVersion()
	if err != nil {
		return nil, err
	}
	return c.msBuilder, nil
}

func (c *workflowExecutionContext) loadWorkflowExecutionInternal() error {
	if c.msBuilder != nil {
		return nil
	}

	response, err := c.getWorkflowExecutionWithRetry(&persistence.GetWorkflowExecutionRequest{
		DomainID:  c.domainID,
		Execution: c.workflowExecution,
	})
	if err != nil {
		if common.IsPersistenceTransientError(err) {
			logging.LogPersistantStoreErrorEvent(c.logger, logging.TagValueStoreOperationGetWorkflowExecution, err, "")
		}
		return err
	}

	msBuilder := newMutableStateBuilder(c.clusterMetadata.GetCurrentClusterName(), c.shard.GetConfig(), c.logger)
	if response != nil && response.State != nil {
		state := response.State
		msBuilder.Load(state)
		info := state.ExecutionInfo
		c.updateCondition = info.NextEventID
	}

	c.msBuilder = msBuilder
	return nil
}

func (c *workflowExecutionContext) resetWorkflowExecution(prevRunID string, resetBuilder mutableState) (mutableState,
	error) {
	snapshotRequest := resetBuilder.ResetSnapshot(prevRunID)
	snapshotRequest.Condition = c.updateCondition

	err := c.shard.ResetMutableState(snapshotRequest)
	if err != nil {
		return nil, err
	}

	c.clear()
	return c.loadWorkflowExecution()
}

func (c *workflowExecutionContext) updateWorkflowExecutionWithContext(context []byte, transferTasks []persistence.Task,
	timerTasks []persistence.Task, transactionID int64) error {
	c.msBuilder.GetExecutionInfo().ExecutionContext = context

	return c.updateWorkflowExecution(transferTasks, timerTasks, transactionID)
}

func (c *workflowExecutionContext) updateWorkflowExecutionWithDeleteTask(transferTasks []persistence.Task,
	timerTasks []persistence.Task, deleteTimerTask persistence.Task, transactionID int64) error {
	c.deleteTimerTask = deleteTimerTask

	return c.updateWorkflowExecution(transferTasks, timerTasks, transactionID)
}

func (c *workflowExecutionContext) replicateWorkflowExecution(request *h.ReplicateEventsRequest,
	transferTasks []persistence.Task, timerTasks []persistence.Task, lastEventID, transactionID int64, now time.Time) error {
	nextEventID := lastEventID + 1
	c.msBuilder.GetExecutionInfo().NextEventID = nextEventID

	standbyHistoryBuilder := newHistoryBuilderFromEvents(request.History.Events, c.logger)
	return c.updateHelper(transferTasks, timerTasks, transactionID, now, false, standbyHistoryBuilder, request.GetSourceCluster())
}

func (c *workflowExecutionContext) updateVersion() error {
	if c.shard.GetService().GetClusterMetadata().IsGlobalDomainEnabled() && c.msBuilder.GetReplicationState() != nil {
		if !c.msBuilder.IsWorkflowExecutionRunning() {
			// we should not update the version on mutable state when the workflow is finished
			return nil
		}
		// Support for global domains is enabled and we are performing an update for global domain
		domainEntry, err := c.shard.GetDomainCache().GetDomainByID(c.domainID)
		if err != nil {
			return err
		}
		c.msBuilder.UpdateReplicationStateVersion(domainEntry.GetFailoverVersion(), false)

		// this is a hack, only create replication task if have # target cluster > 1, for more see #868
		c.createReplicationTask = domainEntry.CanReplicateEvent()
	}
	return nil
}

func (c *workflowExecutionContext) updateWorkflowExecution(transferTasks []persistence.Task,
	timerTasks []persistence.Task, transactionID int64) error {

	if c.msBuilder.GetReplicationState() != nil {
		currentVersion := c.msBuilder.GetCurrentVersion()

		activeCluster := c.clusterMetadata.ClusterNameForFailoverVersion(currentVersion)
		currentCluster := c.clusterMetadata.GetCurrentClusterName()
		if activeCluster != currentCluster {
			domainID := c.msBuilder.GetExecutionInfo().DomainID
			c.clear()
			return errors.NewDomainNotActiveError(domainID, currentCluster, activeCluster)
		}

		// Handling mutable state turn from standby to active, while having a decision on the fly
		if di, ok := c.msBuilder.GetInFlightDecisionTask(); ok && c.msBuilder.IsWorkflowExecutionRunning() {
			if di.Version < currentVersion {
				// we have a decision on the fly with a lower version, fail it
				c.msBuilder.AddDecisionTaskFailedEvent(di.ScheduleID, di.StartedID,
					workflow.DecisionTaskFailedCauseFailoverCloseDecision, nil, identityHistoryService)

				var transT, timerT []persistence.Task
				transT, timerT, err := c.scheduleNewDecision(transT, timerT)
				if err != nil {
					return err
				}
				transferTasks = append(transferTasks, transT...)
				timerTasks = append(timerTasks, timerT...)
			}
		}
	}

	if !c.createReplicationTask {
		c.logger.Debugf("Skipping replication task creation: %v, workflowID: %v, runID: %v, firstEventID: %v, nextEventID: %v.",
			c.domainID, c.workflowExecution.GetWorkflowId(), c.workflowExecution.GetRunId(),
			c.msBuilder.GetExecutionInfo().LastFirstEventID, c.msBuilder.GetExecutionInfo().NextEventID)
	}

	now := time.Now()
	return c.updateHelper(transferTasks, timerTasks, transactionID, now, c.createReplicationTask, nil, "")
}

func (c *workflowExecutionContext) updateHelper(transferTasks []persistence.Task, timerTasks []persistence.Task,
	transactionID int64, now time.Time,
	createReplicationTask bool, standbyHistoryBuilder *historyBuilder, sourceCluster string) (errRet error) {

	defer func() {
		if errRet != nil {
			// Clear all cached state in case of error
			c.clear()
		}
	}()

	// Take a snapshot of all updates we have accumulated for this execution
	updates, err := c.msBuilder.CloseUpdateSession()
	if err != nil {
		return err
	}

	executionInfo := c.msBuilder.GetExecutionInfo()

	// this builder has events generated locally
	hasNewStandbyHistoryEvents := standbyHistoryBuilder != nil && len(standbyHistoryBuilder.history) > 0
	activeHistoryBuilder := updates.newEventsBuilder
	hasNewActiveHistoryEvents := len(activeHistoryBuilder.history) > 0

	if hasNewStandbyHistoryEvents && hasNewActiveHistoryEvents {
		c.logger.WithFields(bark.Fields{
			logging.TagDomainID:            executionInfo.DomainID,
			logging.TagWorkflowExecutionID: executionInfo.WorkflowID,
			logging.TagWorkflowRunID:       executionInfo.RunID,
			logging.TagFirstEventID:        executionInfo.LastFirstEventID,
			logging.TagNextEventID:         executionInfo.NextEventID,
			logging.TagReplicationState:    c.msBuilder.GetReplicationState(),
		}).Fatal("Both standby and active history builder has events.")
	}

	// Replication state should only be updated after the UpdateSession is closed.  IDs for certain events are only
	// generated on CloseSession as they could be buffered events.  The value for NextEventID will be wrong on
	// mutable state if read before flushing the buffered events.
	crossDCEnabled := c.msBuilder.GetReplicationState() != nil
	if crossDCEnabled {
		// always standby history first
		if hasNewStandbyHistoryEvents {
			lastEvent := standbyHistoryBuilder.history[len(standbyHistoryBuilder.history)-1]
			c.msBuilder.UpdateReplicationStateLastEventID(
				sourceCluster,
				lastEvent.GetVersion(),
				lastEvent.GetEventId(),
			)
		}

		if hasNewActiveHistoryEvents {
			c.msBuilder.UpdateReplicationStateLastEventID(
				c.clusterMetadata.GetCurrentClusterName(),
				c.msBuilder.GetCurrentVersion(),
				executionInfo.NextEventID-1,
			)
		}
	}

	// always standby history first
	if hasNewStandbyHistoryEvents {
		firstEvent := standbyHistoryBuilder.GetFirstEvent()
		// Note: standby events has no transient decision events
		err = c.appendHistoryEvents(standbyHistoryBuilder, standbyHistoryBuilder.history, transactionID)
		if err != nil {
			return err
		}

		executionInfo.LastFirstEventID = firstEvent.GetEventId()
	}

	// Some operations only update the mutable state. For example RecordActivityTaskHeartbeat.
	if hasNewActiveHistoryEvents {
		firstEvent := activeHistoryBuilder.GetFirstEvent()
		// Transient decision events need to be written as a separate batch
		if activeHistoryBuilder.HasTransientEvents() {
			err = c.appendHistoryEvents(activeHistoryBuilder, activeHistoryBuilder.transientHistory, transactionID)
			if err != nil {
				return err
			}
		}

		err = c.appendHistoryEvents(activeHistoryBuilder, activeHistoryBuilder.history, transactionID)
		if err != nil {
			return err
		}

		executionInfo.LastFirstEventID = firstEvent.GetEventId()
	}

	continueAsNew := updates.continueAsNew
	finishExecution := false
	var finishExecutionTTL int32
	if executionInfo.State == persistence.WorkflowStateCompleted {
		// Workflow execution completed as part of this transaction.
		// Also transactionally delete workflow execution representing
		// current run for the execution using cassandra TTL
		finishExecution = true
		domainEntry, err := c.shard.GetDomainCache().GetDomainByID(executionInfo.DomainID)
		if err != nil {
			return err
		}
		// NOTE: domain retention is in days, so we need to do a conversion
		finishExecutionTTL = domainEntry.GetConfig().Retention * secondsInDay
	}

	var replicationTasks []persistence.Task
	// Check if the update resulted in new history events before generating replication task
	if hasNewActiveHistoryEvents && createReplicationTask {
		// Let's create a replication task as part of this update
		replicationTasks = append(replicationTasks, c.msBuilder.CreateReplicationTask())
	}

	setTaskInfo(c.msBuilder.GetCurrentVersion(), now, transferTasks, timerTasks)

	if err1 := c.updateWorkflowExecutionWithRetry(&persistence.UpdateWorkflowExecutionRequest{
		ExecutionInfo:                 executionInfo,
		ReplicationState:              c.msBuilder.GetReplicationState(),
		TransferTasks:                 transferTasks,
		ReplicationTasks:              replicationTasks,
		TimerTasks:                    timerTasks,
		Condition:                     c.updateCondition,
		DeleteTimerTask:               c.deleteTimerTask,
		UpsertActivityInfos:           updates.updateActivityInfos,
		DeleteActivityInfos:           updates.deleteActivityInfos,
		UpserTimerInfos:               updates.updateTimerInfos,
		DeleteTimerInfos:              updates.deleteTimerInfos,
		UpsertChildExecutionInfos:     updates.updateChildExecutionInfos,
		DeleteChildExecutionInfo:      updates.deleteChildExecutionInfo,
		UpsertRequestCancelInfos:      updates.updateCancelExecutionInfos,
		DeleteRequestCancelInfo:       updates.deleteCancelExecutionInfo,
		UpsertSignalInfos:             updates.updateSignalInfos,
		DeleteSignalInfo:              updates.deleteSignalInfo,
		UpsertSignalRequestedIDs:      updates.updateSignalRequestedIDs,
		DeleteSignalRequestedID:       updates.deleteSignalRequestedID,
		NewBufferedEvents:             updates.newBufferedEvents,
		ClearBufferedEvents:           updates.clearBufferedEvents,
		NewBufferedReplicationTask:    updates.newBufferedReplicationEventsInfo,
		DeleteBufferedReplicationTask: updates.deleteBufferedReplicationEvent,
		ContinueAsNew:                 continueAsNew,
		FinishExecution:               finishExecution,
		FinishedExecutionTTL:          finishExecutionTTL,
	}); err1 != nil {
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
	c.msBuilder.GetExecutionInfo().LastUpdatedTimestamp = time.Now()

	// for any change in the workflow, send a event
	c.shard.NotifyNewHistoryEvent(newHistoryEventNotification(
		c.domainID,
		&c.workflowExecution,
		c.msBuilder.GetLastFirstEventID(),
		c.msBuilder.GetNextEventID(),
		c.msBuilder.IsWorkflowExecutionRunning(),
	))

	return nil
}

func (c *workflowExecutionContext) appendHistoryEvents(builder *historyBuilder, history []*workflow.HistoryEvent,
	transactionID int64) error {

	firstEvent := history[0]
	serializedHistory, err := builder.SerializeEvents(history)
	if err != nil {
		logging.LogHistorySerializationErrorEvent(c.logger, err, "Unable to serialize execution history for update.")
		return err
	}

	if err0 := c.shard.AppendHistoryEvents(&persistence.AppendHistoryEventsRequest{
		DomainID:          c.domainID,
		Execution:         c.workflowExecution,
		TransactionID:     transactionID,
		FirstEventID:      firstEvent.GetEventId(),
		EventBatchVersion: firstEvent.GetVersion(),
		Events:            serializedHistory,
	}); err0 != nil {
		switch err0.(type) {
		case *persistence.ConditionFailedError:
			return ErrConflict
		}

		logging.LogPersistantStoreErrorEvent(c.logger, logging.TagValueStoreOperationUpdateWorkflowExecution, err0,
			fmt.Sprintf("{updateCondition: %v}", c.updateCondition))
		return err0
	}

	return nil
}

func (c *workflowExecutionContext) replicateContinueAsNewWorkflowExecution(newStateBuilder mutableState,
	transactionID int64) error {
	return c.continueAsNewWorkflowExecutionHelper(nil, newStateBuilder, transactionID)
}

func (c *workflowExecutionContext) continueAsNewWorkflowExecution(context []byte, newStateBuilder mutableState,
	transferTasks []persistence.Task, timerTasks []persistence.Task, transactionID int64) error {

	err1 := c.continueAsNewWorkflowExecutionHelper(context, newStateBuilder, transactionID)
	if err1 != nil {
		return err1
	}

	err2 := c.updateWorkflowExecutionWithContext(context, transferTasks, timerTasks, transactionID)

	if err2 != nil {
		// TODO: Delete new execution if update fails due to conflict or shard being lost
	}

	return err2
}

func (c *workflowExecutionContext) continueAsNewWorkflowExecutionHelper(context []byte, newStateBuilder mutableState, transactionID int64) error {
	executionInfo := newStateBuilder.GetExecutionInfo()
	domainID := executionInfo.DomainID
	newExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(executionInfo.WorkflowID),
		RunId:      common.StringPtr(executionInfo.RunID),
	}
	firstEvent := newStateBuilder.GetHistoryBuilder().history[0]

	// Serialize the history
	serializedHistory, serializedError := newStateBuilder.GetHistoryBuilder().Serialize()
	if serializedError != nil {
		logging.LogHistorySerializationErrorEvent(c.logger, serializedError, fmt.Sprintf(
			"HistoryEventBatch serialization error on start workflow.  WorkflowID: %v, RunID: %v", *newExecution.WorkflowId,
			*newExecution.RunId))
		return serializedError
	}

	return c.shard.AppendHistoryEvents(&persistence.AppendHistoryEventsRequest{
		DomainID:          domainID,
		Execution:         newExecution,
		TransactionID:     transactionID,
		FirstEventID:      firstEvent.GetEventId(),
		EventBatchVersion: firstEvent.GetVersion(),
		Events:            serializedHistory,
	})
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

func (c *workflowExecutionContext) clear() {
	c.msBuilder = nil
}

// scheduleNewDecision is helper method which has the logic for scheduling new decision for a workflow execution.
// This function takes in a slice of transferTasks and timerTasks already scheduled for the current transaction
// and may append more tasks to it.  It also returns back the slice with new tasks appended to it.  It is expected
// caller to assign returned slice to original passed in slices.  For this reason we return the original slices
// even if the method fails due to an error on loading workflow execution.
func (c *workflowExecutionContext) scheduleNewDecision(transferTasks []persistence.Task,
	timerTasks []persistence.Task) ([]persistence.Task, []persistence.Task, error) {
	msBuilder, err := c.loadWorkflowExecution()
	if err != nil {
		return transferTasks, timerTasks, err
	}

	executionInfo := msBuilder.GetExecutionInfo()
	if !msBuilder.HasPendingDecisionTask() {
		di := msBuilder.AddDecisionTaskScheduledEvent()
		transferTasks = append(transferTasks, &persistence.DecisionTask{
			DomainID:   executionInfo.DomainID,
			TaskList:   di.TaskList,
			ScheduleID: di.ScheduleID,
		})
		if msBuilder.IsStickyTaskListEnabled() {
			tBuilder := newTimerBuilder(c.shard.GetConfig(), c.logger, common.NewRealTimeSource())
			stickyTaskTimeoutTimer := tBuilder.AddScheduleToStartDecisionTimoutTask(di.ScheduleID, di.Attempt,
				executionInfo.StickyScheduleToStartTimeout)
			timerTasks = append(timerTasks, stickyTaskTimeoutTimer)
		}
	}

	return transferTasks, timerTasks, nil
}
