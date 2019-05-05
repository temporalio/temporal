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
	"context"
	"fmt"
	"time"

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/shared"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/locks"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

const (
	secondsInDay = int32(24 * time.Hour / time.Second)
)

type (
	workflowExecutionContext interface {
		appendFirstBatchHistoryForContinueAsNew(newStateBuilder mutableState, transactionID int64) error
		appendFirstBatchEventsForActive(msBuilder mutableState) (int, error)
		appendFirstBatchEventsForStandby(msBuilder mutableState, history []*workflow.HistoryEvent) (int, error)
		clear()
		continueAsNewWorkflowExecution(context []byte, newStateBuilder mutableState, transferTasks []persistence.Task, timerTasks []persistence.Task, transactionID int64) error
		createWorkflowExecution(
			msBuilder mutableState, sourceCluster string, createReplicationTask bool, now time.Time,
			transferTasks []persistence.Task, timerTasks []persistence.Task,
			createMode int, prevRunID string, prevLastWriteVersion int64) error
		getDomainID() string
		getExecution() *workflow.WorkflowExecution
		getLogger() log.Logger
		loadWorkflowExecution() (mutableState, error)
		lock(context.Context) error
		replicateWorkflowExecution(request *h.ReplicateEventsRequest, transferTasks []persistence.Task, timerTasks []persistence.Task, lastEventID, transactionID int64, now time.Time) error
		resetMutableState(prevRunID string, resetBuilder mutableState) (mutableState, error)
		resetWorkflowExecution(currMutableState mutableState, updateCurr bool, closeTask, cleanupTask persistence.Task, newMutableState mutableState, transferTasks, timerTasks, currReplicationTasks, insertReplicationTasks []persistence.Task, baseRunID string, forkRunNextEventID, prevRunVersion int64) (retError error)
		scheduleNewDecision(transferTasks []persistence.Task, timerTasks []persistence.Task) ([]persistence.Task, []persistence.Task, error)
		unlock()
		updateWorkflowExecutionForStandby(transferTasks []persistence.Task, timerTasks []persistence.Task, transactionID int64, now time.Time, createReplicationTask bool, standbyHistoryBuilder *historyBuilder, sourceCluster string) error
		updateWorkflowExecution(transferTasks []persistence.Task, timerTasks []persistence.Task, transactionID int64) error
		updateWorkflowExecutionWithContext(context []byte, transferTasks []persistence.Task, timerTasks []persistence.Task, transactionID int64) error
	}
)

type (
	workflowExecutionContextImpl struct {
		domainID          string
		workflowExecution workflow.WorkflowExecution
		shard             ShardContext
		clusterMetadata   cluster.Metadata
		executionManager  persistence.ExecutionManager
		logger            log.Logger
		metricsClient     metrics.Client

		locker                locks.Mutex
		msBuilder             mutableState
		updateCondition       int64
		createReplicationTask bool
	}
)

var _ workflowExecutionContext = (*workflowExecutionContextImpl)(nil)

var (
	persistenceOperationRetryPolicy = common.CreatePersistanceRetryPolicy()
	kafkaOperationRetryPolicy       = common.CreateKafkaOperationRetryPolicy()
)

func newWorkflowExecutionContext(
	domainID string,
	execution workflow.WorkflowExecution,
	shard ShardContext,
	executionManager persistence.ExecutionManager,
	logger log.Logger,
) *workflowExecutionContextImpl {
	lg := logger.WithTags(
		tag.WorkflowID(domainID),
		tag.WorkflowRunID(execution.GetWorkflowId()),
		tag.WorkflowDomainID(execution.GetRunId()))

	return &workflowExecutionContextImpl{
		domainID:          domainID,
		workflowExecution: execution,
		shard:             shard,
		clusterMetadata:   shard.GetService().GetClusterMetadata(),
		executionManager:  executionManager,
		logger:            lg,
		metricsClient:     shard.GetMetricsClient(),
		locker:            locks.NewMutex(),
	}
}

func (c *workflowExecutionContextImpl) lock(ctx context.Context) error {
	return c.locker.Lock(ctx)
}

func (c *workflowExecutionContextImpl) unlock() {
	c.locker.Unlock()
}

func (c *workflowExecutionContextImpl) getDomainID() string {
	return c.domainID
}

func (c *workflowExecutionContextImpl) getExecution() *workflow.WorkflowExecution {
	return &c.workflowExecution
}

func (c *workflowExecutionContextImpl) getLogger() log.Logger {
	return c.logger
}

func (c *workflowExecutionContextImpl) loadWorkflowExecution() (mutableState, error) {
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

func (c *workflowExecutionContextImpl) loadWorkflowExecutionInternal() error {
	if c.msBuilder != nil {
		return nil
	}

	response, err := c.getWorkflowExecutionWithRetry(&persistence.GetWorkflowExecutionRequest{
		DomainID:  c.domainID,
		Execution: c.workflowExecution,
	})
	if err != nil {
		if common.IsPersistenceTransientError(err) {
			c.logger.Error("Persistent store operation failure",
				tag.StoreOperationGetWorkflowExecution,
				tag.Error(err))
		}
		return err
	}

	msBuilder := newMutableStateBuilder(c.clusterMetadata.GetCurrentClusterName(), c.shard,
		c.shard.GetEventsCache(), c.logger)
	if response != nil && response.State != nil {
		state := response.State
		msBuilder.Load(state)
		info := state.ExecutionInfo
		c.updateCondition = info.NextEventID
	}

	c.msBuilder = msBuilder
	// finally emit execution and session stats
	c.emitWorkflowExecutionStats(response.MutableStateStats, c.msBuilder.GetHistorySize())
	return nil
}

func (c *workflowExecutionContextImpl) createWorkflowExecution(
	msBuilder mutableState, sourceCluster string, createReplicationTask bool, now time.Time,
	transferTasks []persistence.Task, timerTasks []persistence.Task,
	createMode int, prevRunID string, prevLastWriteVersion int64) error {

	if msBuilder.GetReplicationState() != nil {
		msBuilder.UpdateReplicationStateLastEventID(
			sourceCluster,
			msBuilder.GetCurrentVersion(),
			msBuilder.GetNextEventID()-1,
		)
	}

	executionInfo := msBuilder.GetExecutionInfo()
	replicationState := msBuilder.GetReplicationState()

	var replicationTasks []persistence.Task
	if createReplicationTask {
		// since this one is creating workflow at the very beginning, use 0, nil for continue as new
		replicationTasks = msBuilder.CreateReplicationTask(replicationTasks, 0, nil)
	}
	setTaskInfo(msBuilder.GetCurrentVersion(), now, transferTasks, timerTasks)

	createRequest := &persistence.CreateWorkflowExecutionRequest{
		RequestID: executionInfo.CreateRequestID,
		DomainID:  executionInfo.DomainID,
		Execution: workflow.WorkflowExecution{
			WorkflowId: &executionInfo.WorkflowID,
			RunId:      &executionInfo.RunID,
		},

		// parent execution
		ParentDomainID: executionInfo.ParentDomainID,
		ParentExecution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(executionInfo.ParentWorkflowID),
			RunId:      common.StringPtr(executionInfo.ParentRunID),
		},
		InitiatedID: executionInfo.InitiatedID,

		TaskList:                    executionInfo.TaskList,
		WorkflowTypeName:            executionInfo.WorkflowTypeName,
		WorkflowTimeout:             executionInfo.WorkflowTimeout,
		DecisionTimeoutValue:        executionInfo.DecisionTimeoutValue,
		ExecutionContext:            nil,
		LastEventTaskID:             executionInfo.LastEventTaskID,
		NextEventID:                 executionInfo.NextEventID,
		LastProcessedEvent:          common.EmptyEventID,
		HistorySize:                 executionInfo.HistorySize,
		TransferTasks:               transferTasks,
		ReplicationTasks:            replicationTasks,
		TimerTasks:                  timerTasks,
		DecisionVersion:             executionInfo.DecisionVersion,
		DecisionScheduleID:          executionInfo.DecisionScheduleID,
		DecisionStartedID:           executionInfo.DecisionStartedID,
		DecisionStartToCloseTimeout: executionInfo.DecisionTimeout,
		EventStoreVersion:           executionInfo.EventStoreVersion,
		BranchToken:                 executionInfo.BranchToken,
		CronSchedule:                executionInfo.CronSchedule,
		ReplicationState:            replicationState,

		// retry policy
		HasRetryPolicy:     executionInfo.HasRetryPolicy,
		BackoffCoefficient: executionInfo.BackoffCoefficient,
		ExpirationSeconds:  executionInfo.ExpirationSeconds,
		InitialInterval:    executionInfo.InitialInterval,
		MaximumAttempts:    executionInfo.MaximumAttempts,
		MaximumInterval:    executionInfo.MaximumInterval,
		NonRetriableErrors: executionInfo.NonRetriableErrors,
		ExpirationTime:     executionInfo.ExpirationTime,

		// workflow create mode & prev run ID & version
		CreateWorkflowMode:       createMode,
		PreviousRunID:            prevRunID,
		PreviousLastWriteVersion: prevLastWriteVersion,
	}

	_, err := c.shard.CreateWorkflowExecution(createRequest)
	return err
}

func (c *workflowExecutionContextImpl) resetMutableState(prevRunID string, resetBuilder mutableState) (mutableState,
	error) {
	// this only resets one mutableState for a workflow
	snapshotRequest := resetBuilder.ResetSnapshot(prevRunID)
	snapshotRequest.Condition = c.updateCondition

	err := c.shard.ResetMutableState(snapshotRequest)
	if err != nil {
		return nil, err
	}

	c.clear()
	return c.loadWorkflowExecution()
}

// this reset is more complex than "resetMutableState", it involes currentMutableState and newMutableState:
// 1. append history to new run
// 2. append history to current run if current run is not closed
// 3. update mutableState(terminate current run if not closed) and create new run
func (c *workflowExecutionContextImpl) resetWorkflowExecution(currMutableState mutableState, updateCurr bool, closeTask, cleanupTask persistence.Task,
	newMutableState mutableState, newTransferTasks, newTimerTasks, currReplicationTasks, insertReplicationTasks []persistence.Task, baseRunID string,
	baseRunNextEventID, prevRunVersion int64) (retError error) {

	now := time.Now()
	currTransferTasks := []persistence.Task{}
	currTimerTasks := []persistence.Task{}
	if closeTask != nil {
		currTransferTasks = append(currTransferTasks, closeTask)
	}
	if cleanupTask != nil {
		currTimerTasks = append(currTimerTasks, cleanupTask)
	}
	setTaskInfo(currMutableState.GetCurrentVersion(), now, currTransferTasks, currTimerTasks)
	setTaskInfo(newMutableState.GetCurrentVersion(), now, newTransferTasks, newTimerTasks)

	transactionID, retError := c.shard.GetNextTransferTaskID()
	if retError != nil {
		return retError
	}

	// Since we always reset to decision task, there shouldn't be any buffered events.
	// Therefore currently ResetWorkflowExecution persistence API doesn't implement setting buffered events.
	if newMutableState.HasBufferedEvents() {
		retError = &workflow.InternalServiceError{
			Message: fmt.Sprintf("reset workflow execution shouldn't have buffered events"),
		}
		return
	}

	// call FlushBufferedEvents to assign task id to event
	// as well as update last event task id in ms state builder
	retError = currMutableState.FlushBufferedEvents()
	if retError != nil {
		return retError
	}
	retError = newMutableState.FlushBufferedEvents()
	if retError != nil {
		return retError
	}

	if updateCurr {
		hBuilder := currMutableState.GetHistoryBuilder()
		var size int
		size, retError = c.appendHistoryEvents(hBuilder.GetHistory().GetEvents(), transactionID, true)
		if retError != nil {
			return
		}
		currMutableState.IncrementHistorySize(size)
	}

	// Note: we already made sure that newMutableState is using eventsV2
	hBuilder := newMutableState.GetHistoryBuilder()
	size, retError := c.shard.AppendHistoryV2Events(&persistence.AppendHistoryNodesRequest{
		IsNewBranch:   false,
		BranchToken:   newMutableState.GetCurrentBranch(),
		Events:        hBuilder.GetHistory().GetEvents(),
		TransactionID: transactionID,
	}, c.domainID, c.workflowExecution)
	if retError != nil {
		return
	}
	newMutableState.IncrementHistorySize(size)

	snapshotRequest := newMutableState.ResetSnapshot("")
	if len(snapshotRequest.InsertChildExecutionInfos) > 0 ||
		len(snapshotRequest.InsertSignalInfos) > 0 ||
		len(snapshotRequest.InsertSignalRequestedIDs) > 0 {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("something went wrong, we shouldn't see any pending childWF, sending Signal or signal requested"),
		}
	}

	// NOTE: workflow_state in current record is either completed(2) or running(1), there is no 0(created)
	prevRunState := persistence.WorkflowStateCompleted
	if updateCurr {
		prevRunState = persistence.WorkflowStateRunning
	}
	resetWFReq := &persistence.ResetWorkflowExecutionRequest{
		PrevRunVersion: prevRunVersion,
		PrevRunState:   prevRunState,

		Condition:  c.updateCondition,
		UpdateCurr: updateCurr,

		BaseRunID:          baseRunID,
		BaseRunNextEventID: baseRunNextEventID,

		CurrExecutionInfo:    currMutableState.GetExecutionInfo(),
		CurrReplicationState: currMutableState.GetReplicationState(),
		CurrReplicationTasks: currReplicationTasks,
		CurrTransferTasks:    currTransferTasks,
		CurrTimerTasks:       currTimerTasks,

		InsertExecutionInfo:    newMutableState.GetExecutionInfo(),
		InsertReplicationState: newMutableState.GetReplicationState(),
		InsertTransferTasks:    newTransferTasks,
		InsertTimerTasks:       newTimerTasks,
		InsertReplicationTasks: insertReplicationTasks,

		InsertTimerInfos:         snapshotRequest.InsertTimerInfos,
		InsertActivityInfos:      snapshotRequest.InsertActivityInfos,
		InsertRequestCancelInfos: snapshotRequest.InsertRequestCancelInfos,
	}

	return c.shard.ResetWorkflowExecution(resetWFReq)
}

func (c *workflowExecutionContextImpl) updateWorkflowExecutionWithContext(context []byte, transferTasks []persistence.Task,
	timerTasks []persistence.Task, transactionID int64) error {
	c.msBuilder.GetExecutionInfo().ExecutionContext = context

	return c.updateWorkflowExecution(transferTasks, timerTasks, transactionID)
}

func (c *workflowExecutionContextImpl) updateWorkflowExecutionWithNewRunAndContext(context []byte, transferTasks []persistence.Task,
	timerTasks []persistence.Task, transactionID int64, newStateBuilder mutableState) error {
	c.msBuilder.GetExecutionInfo().ExecutionContext = context

	return c.updateWorkflowExecutionForActive(transferTasks, timerTasks, transactionID, newStateBuilder)
}

func (c *workflowExecutionContextImpl) replicateWorkflowExecution(request *h.ReplicateEventsRequest,
	transferTasks []persistence.Task, timerTasks []persistence.Task, lastEventID, transactionID int64, now time.Time) error {
	nextEventID := lastEventID + 1
	c.msBuilder.GetExecutionInfo().SetNextEventID(nextEventID)

	standbyHistoryBuilder := newHistoryBuilderFromEvents(request.History.Events, c.logger)
	return c.updateWorkflowExecutionForStandby(transferTasks, timerTasks, transactionID, now, false, standbyHistoryBuilder, request.GetSourceCluster())
}

func (c *workflowExecutionContextImpl) updateVersion() error {
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

func (c *workflowExecutionContextImpl) updateWorkflowExecutionForActive(transferTasks []persistence.Task,
	timerTasks []persistence.Task, transactionID int64, newStateBuilder mutableState) error {
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
					workflow.DecisionTaskFailedCauseFailoverCloseDecision, nil, identityHistoryService, "", "", "", 0)

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
		c.logger.Debug(fmt.Sprintf("Skipping replication task creation: %v, workflowID: %v, runID: %v, firstEventID: %v, nextEventID: %v.",
			c.domainID, c.workflowExecution.GetWorkflowId(), c.workflowExecution.GetRunId(),
			c.msBuilder.GetExecutionInfo().LastFirstEventID, c.msBuilder.GetExecutionInfo().NextEventID))
	}

	// compare with bad binaries and schedule a reset task
	if len(c.msBuilder.GetPendingChildExecutionInfos()) == 0 {
		// only schedule reset task if current doesn't have childWFs.
		// TODO: This will be removed once our reset allows childWFs

		domainEntry, err := c.shard.GetDomainCache().GetDomainByID(c.domainID)
		if err != nil {
			return err
		}
		_, pt := FindAutoResetPoint(&domainEntry.GetConfig().BadBinaries, c.msBuilder.GetExecutionInfo().AutoResetPoints)
		if pt != nil {
			transferTasks = append(transferTasks, &persistence.ResetWorkflowTask{})
			c.logger.Info("Auto-Reset task is scheduled",
				tag.WorkflowDomainName(domainEntry.GetInfo().Name),
				tag.WorkflowID(c.msBuilder.GetExecutionInfo().WorkflowID),
				tag.WorkflowRunID(c.msBuilder.GetExecutionInfo().RunID),
				tag.WorkflowResetBaseRunID(pt.GetRunId()),
				tag.WorkflowEventID(pt.GetFirstDecisionCompletedId()),
				tag.WorkflowBinaryChecksum(pt.GetBinaryChecksum()))
		}
	}

	now := time.Now()
	return c.update(transferTasks, timerTasks, transactionID, now, c.createReplicationTask, nil, "", newStateBuilder)
}

func (c *workflowExecutionContextImpl) updateWorkflowExecution(transferTasks []persistence.Task,
	timerTasks []persistence.Task, transactionID int64) error {
	return c.updateWorkflowExecutionForActive(transferTasks, timerTasks, transactionID, nil)
}

func (c *workflowExecutionContextImpl) updateWorkflowExecutionForStandby(transferTasks []persistence.Task, timerTasks []persistence.Task,
	transactionID int64, now time.Time,
	createReplicationTask bool, standbyHistoryBuilder *historyBuilder, sourceCluster string) (errRet error) {
	return c.update(transferTasks, timerTasks, transactionID, now, createReplicationTask, standbyHistoryBuilder, sourceCluster, nil)
}

func (c *workflowExecutionContextImpl) update(transferTasks []persistence.Task, timerTasks []persistence.Task,
	transactionID int64, now time.Time,
	createReplicationTask bool, standbyHistoryBuilder *historyBuilder, sourceCluster string, newStateBuilder mutableState) (errRet error) {

	defer func() {
		if errRet != nil {
			// Clear all cached state in case of error
			c.clear()
		}
	}()

	// Take a snapshot of all updates we have accumulated for this execution
	updates, err := c.msBuilder.CloseUpdateSession()
	if err != nil {
		if err == ErrBufferedEventsLimitExceeded {
			if err1 := c.failInflightDecision(); err1 != nil {
				return err1
			}

			// Buffered events are flushed, we want upper layer to retry
			return ErrConflict
		}
		return err
	}

	executionInfo := c.msBuilder.GetExecutionInfo()

	// this builder has events generated locally
	hasNewStandbyHistoryEvents := standbyHistoryBuilder != nil && len(standbyHistoryBuilder.history) > 0
	activeHistoryBuilder := updates.newEventsBuilder
	hasNewActiveHistoryEvents := len(activeHistoryBuilder.history) > 0

	if hasNewStandbyHistoryEvents && hasNewActiveHistoryEvents {
		c.logger.Fatal("Both standby and active history builder has events.",
			tag.WorkflowID(executionInfo.WorkflowID),
			tag.WorkflowRunID(executionInfo.RunID),
			tag.WorkflowDomainID(executionInfo.DomainID),
			tag.WorkflowFirstEventID(executionInfo.LastFirstEventID),
			tag.WorkflowNextEventID(executionInfo.NextEventID),
			tag.ReplicationState(c.msBuilder.GetReplicationState()),
		)

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

	newHistorySize := 0
	// always standby history first
	if hasNewStandbyHistoryEvents {
		firstEvent := standbyHistoryBuilder.GetFirstEvent()
		// Note: standby events has no transient decision events
		newHistorySize, err = c.appendHistoryEvents(standbyHistoryBuilder.history, transactionID, true)
		if err != nil {
			return err
		}

		executionInfo.SetLastFirstEventID(firstEvent.GetEventId())
	}

	// Some operations only update the mutable state. For example RecordActivityTaskHeartbeat.
	if hasNewActiveHistoryEvents {
		firstEvent := activeHistoryBuilder.GetFirstEvent()
		// Transient decision events need to be written as a separate batch
		if activeHistoryBuilder.HasTransientEvents() {
			// transient decision events batch should not perform last event check
			newHistorySize, err = c.appendHistoryEvents(activeHistoryBuilder.transientHistory, transactionID, false)
			if err != nil {
				return err
			}
		}

		var size int
		size, err = c.appendHistoryEvents(activeHistoryBuilder.history, transactionID, true)
		if err != nil {
			return err
		}

		executionInfo.SetLastFirstEventID(firstEvent.GetEventId())
		newHistorySize += size

		// enforce history size/count limit (only on active side)
		config := c.shard.GetConfig()
		sizeLimitWarn := config.HistorySizeLimitWarn(executionInfo.DomainID)
		countLimitWarn := config.HistoryCountLimitWarn(executionInfo.DomainID)
		historyCount := int(c.msBuilder.GetNextEventID()) - 1
		historySize := int(c.msBuilder.GetHistorySize()) + newHistorySize

		// N.B. - Dual emit is required here so that we can see aggregate timer stats across all
		// domains along with the individual domains stats
		c.metricsClient.RecordTimer(metrics.PersistenceUpdateWorkflowExecutionScope, metrics.HistorySize, time.Duration(historySize))
		c.metricsClient.RecordTimer(metrics.PersistenceUpdateWorkflowExecutionScope, metrics.HistoryCount, time.Duration(historyCount))
		if entry, err := c.shard.GetDomainCache().GetDomainByID(executionInfo.DomainID); err == nil && entry != nil && entry.GetInfo() != nil {
			scope := c.metricsClient.Scope(metrics.PersistenceUpdateWorkflowExecutionScope, metrics.DomainTag(entry.GetInfo().Name))
			scope.RecordTimer(metrics.HistorySize, time.Duration(historySize))
			scope.RecordTimer(metrics.HistoryCount, time.Duration(historyCount))
		}

		if historySize > sizeLimitWarn || historyCount > countLimitWarn {
			// emit warning
			c.logger.Warn("history size exceeds limit.",
				tag.WorkflowID(executionInfo.WorkflowID),
				tag.WorkflowRunID(executionInfo.RunID),
				tag.WorkflowDomainID(executionInfo.DomainID),
				tag.WorkflowHistorySize(historySize),
				tag.WorkflowEventCount(historyCount))

			sizeLimitError := config.HistorySizeLimitError(executionInfo.DomainID)
			countLimitError := config.HistoryCountLimitError(executionInfo.DomainID)
			if (historySize > sizeLimitError || historyCount > countLimitError) && c.msBuilder.IsWorkflowExecutionRunning() {
				// hard terminate workflow if it is still running
				c.clear()                            // discard pending changes
				_, err1 := c.loadWorkflowExecution() // reload mutable state
				if err1 != nil {
					return err1
				}

				err = failInFlightDecisionToClearBufferedEvents(c.msBuilder)
				if err != nil {
					return err
				}

				c.msBuilder.AddWorkflowExecutionTerminatedEvent(&workflow.TerminateWorkflowExecutionRequest{
					Reason:   common.StringPtr(common.TerminateReasonSizeExceedsLimit),
					Identity: common.StringPtr("cadence-history-server"),
				})

				updates, err = c.msBuilder.CloseUpdateSession()
				if err != nil {
					return err
				}

				executionInfo = c.msBuilder.GetExecutionInfo()
				activeHistoryBuilder = updates.newEventsBuilder
				newStateBuilder = nil // since we terminate current workflow execution, so ignore new executions

				if crossDCEnabled {
					c.msBuilder.UpdateReplicationStateLastEventID(
						c.clusterMetadata.GetCurrentClusterName(),
						c.msBuilder.GetCurrentVersion(),
						executionInfo.NextEventID-1,
					)
				}

				firstEvent = activeHistoryBuilder.GetFirstEvent()
				terminateTransactionID, err1 := c.shard.GetNextTransferTaskID()
				if err1 != nil {
					return err1
				}
				newHistorySize, err = c.appendHistoryEvents(activeHistoryBuilder.history, terminateTransactionID, true)
				if err != nil {
					return err
				}
				executionInfo.SetLastFirstEventID(firstEvent.GetEventId())

				// add clean up tasks
				tranT, timerT, err := getWorkflowHistoryCleanupTasksFromShard(c.shard, executionInfo.DomainID, executionInfo.WorkflowID, nil)
				if err != nil {
					return err
				}
				transferTasks = append(transferTasks, tranT)
				timerTasks = append(timerTasks, timerT)
			} // end of hard terminate workflow
		} // end of enforce history size/count limit

	} // end of update history events for active builder

	continueAsNew := updates.continueAsNew
	if executionInfo.State == persistence.WorkflowStateCompleted {
		// clear stickness
		c.msBuilder.ClearStickyness()
	}

	var replicationTasks []persistence.Task
	// Check if the update resulted in new history events before generating replication task
	if createReplicationTask {
		// Let's create a replication task as part of this update
		if hasNewActiveHistoryEvents {
			if newStateBuilder != nil && newStateBuilder.GetEventStoreVersion() == persistence.EventStoreVersionV2 {
				replicationTasks = c.msBuilder.CreateReplicationTask(replicationTasks, persistence.EventStoreVersionV2, newStateBuilder.GetCurrentBranch())
			} else {
				replicationTasks = c.msBuilder.CreateReplicationTask(replicationTasks, 0, nil)
			}
		}
		replicationTasks = append(replicationTasks, updates.syncActivityTasks...)
	}

	setTaskInfo(c.msBuilder.GetCurrentVersion(), now, transferTasks, timerTasks)

	// Update history size on mutableState before calling UpdateWorkflowExecution
	c.msBuilder.IncrementHistorySize(newHistorySize)

	var resp *persistence.UpdateWorkflowExecutionResponse
	var err1 error
	if resp, err1 = c.updateWorkflowExecutionWithRetry(&persistence.UpdateWorkflowExecutionRequest{
		ExecutionInfo:                 executionInfo,
		ReplicationState:              c.msBuilder.GetReplicationState(),
		TransferTasks:                 transferTasks,
		ReplicationTasks:              replicationTasks,
		TimerTasks:                    timerTasks,
		Condition:                     c.updateCondition,
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
	}); err1 != nil {
		switch err1.(type) {
		case *persistence.ConditionFailedError:
			return ErrConflict
		}

		c.logger.Error("Persistent store operation failure",
			tag.StoreOperationUpdateWorkflowExecution,
			tag.Error(err), tag.Number(c.updateCondition))
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
		c.msBuilder.GetPreviousStartedEventID(),
		c.msBuilder.IsWorkflowExecutionRunning(),
	))

	// finally emit session stats
	if resp != nil {
		c.emitSessionUpdateStats(resp.MutableStateUpdateSessionStats)
	}

	// emit workflow completion stats if any
	if executionInfo.State == persistence.WorkflowStateCompleted {
		if event, ok := c.msBuilder.GetCompletionEvent(); ok {
			c.emitWorkflowCompletionStats(event)
		}
	}

	return nil
}

func (c *workflowExecutionContextImpl) appendFirstBatchEventsForActive(msBuilder mutableState) (int, error) {
	// call FlushBufferedEvents to assign task id to event
	// as well as update last event task id in mutable state builder
	err := msBuilder.FlushBufferedEvents()
	if err != nil {
		return 0, err
	}
	events := msBuilder.GetHistoryBuilder().GetHistory().Events
	return c.appendFirstBatchEventsForStandby(msBuilder, events)
}

func (c *workflowExecutionContextImpl) appendFirstBatchEventsForStandby(msBuilder mutableState, history []*workflow.HistoryEvent) (int, error) {

	firstEvent := history[0]
	var historySize int
	var err error
	if msBuilder.GetEventStoreVersion() == persistence.EventStoreVersionV2 {
		historySize, err = c.shard.AppendHistoryV2Events(&persistence.AppendHistoryNodesRequest{
			IsNewBranch: true,
			Info:        historyGarbageCleanupInfo(c.domainID, c.workflowExecution.GetWorkflowId(), c.workflowExecution.GetRunId()),
			BranchToken: msBuilder.GetCurrentBranch(),
			Events:      history,
			// It is ok to use 0 for TransactionID because RunID is unique so there are
			// no potential duplicates to override.
			TransactionID: 0,
		}, c.domainID, c.workflowExecution)
	} else {
		historySize, err = c.shard.AppendHistoryEvents(&persistence.AppendHistoryEventsRequest{
			DomainID:  c.domainID,
			Execution: c.workflowExecution,
			// It is ok to use 0 for TransactionID because RunID is unique so there are
			// no potential duplicates to override.
			TransactionID:     0,
			FirstEventID:      firstEvent.GetEventId(),
			EventBatchVersion: firstEvent.GetVersion(),
			Events:            history,
		})
	}
	if err == nil {
		msBuilder.IncrementHistorySize(historySize)
	}
	return historySize, err
}

func (c *workflowExecutionContextImpl) appendHistoryEvents(history []*workflow.HistoryEvent,
	transactionID int64, doLastEventValidation bool) (int, error) {

	if doLastEventValidation {
		if err := c.validateNoEventsAfterWorkflowFinish(history); err != nil {
			return 0, err
		}
	}

	firstEvent := history[0]
	var historySize int
	var err error

	if c.msBuilder.GetEventStoreVersion() == persistence.EventStoreVersionV2 {
		historySize, err = c.shard.AppendHistoryV2Events(&persistence.AppendHistoryNodesRequest{
			IsNewBranch:   false,
			BranchToken:   c.msBuilder.GetCurrentBranch(),
			Events:        history,
			TransactionID: transactionID,
		}, c.domainID, c.workflowExecution)
	} else {
		historySize, err = c.shard.AppendHistoryEvents(&persistence.AppendHistoryEventsRequest{
			DomainID:          c.domainID,
			Execution:         c.workflowExecution,
			TransactionID:     transactionID,
			FirstEventID:      firstEvent.GetEventId(),
			EventBatchVersion: firstEvent.GetVersion(),
			Events:            history,
		})
	}

	if err != nil {
		switch err.(type) {
		case *persistence.ConditionFailedError:
			return historySize, ErrConflict
		}

		c.logger.Error("Persistent store operation failure",
			tag.StoreOperationUpdateWorkflowExecution,
			tag.Error(err),
			tag.Number(c.updateCondition))
		return historySize, err
	}

	return historySize, nil
}

func (c *workflowExecutionContextImpl) continueAsNewWorkflowExecution(context []byte, newStateBuilder mutableState,
	transferTasks []persistence.Task, timerTasks []persistence.Task, transactionID int64) error {

	err1 := c.appendFirstBatchHistoryForContinueAsNew(newStateBuilder, transactionID)
	if err1 != nil {
		return err1
	}

	err2 := c.updateWorkflowExecutionWithNewRunAndContext(context, transferTasks, timerTasks, transactionID, newStateBuilder)
	if err2 != nil {
		// TODO: Delete new execution if update fails due to conflict or shard being lost
	}

	return err2
}

func (c *workflowExecutionContextImpl) appendFirstBatchHistoryForContinueAsNew(newStateBuilder mutableState,
	transactionID int64) error {
	executionInfo := newStateBuilder.GetExecutionInfo()
	domainID := executionInfo.DomainID
	newExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(executionInfo.WorkflowID),
		RunId:      common.StringPtr(executionInfo.RunID),
	}

	firstEvent := newStateBuilder.GetHistoryBuilder().history[0]
	history := newStateBuilder.GetHistoryBuilder().GetHistory()
	var historySize int
	var err error
	if newStateBuilder.GetEventStoreVersion() == persistence.EventStoreVersionV2 {
		historySize, err = c.shard.AppendHistoryV2Events(&persistence.AppendHistoryNodesRequest{
			IsNewBranch:   true,
			Info:          historyGarbageCleanupInfo(domainID, newExecution.GetWorkflowId(), newExecution.GetRunId()),
			BranchToken:   newStateBuilder.GetCurrentBranch(),
			Events:        history.Events,
			TransactionID: transactionID,
		}, newStateBuilder.GetExecutionInfo().DomainID, newExecution)
	} else {
		historySize, err = c.shard.AppendHistoryEvents(&persistence.AppendHistoryEventsRequest{
			DomainID:          domainID,
			Execution:         newExecution,
			TransactionID:     transactionID,
			FirstEventID:      firstEvent.GetEventId(),
			EventBatchVersion: firstEvent.GetVersion(),
			Events:            history.Events,
		})
	}

	if err == nil {
		// History update for new run succeeded, update the history size on both mutableState for current and new run
		c.msBuilder.SetNewRunSize(historySize)
		newStateBuilder.IncrementHistorySize(historySize)
	}

	return err
}

func (c *workflowExecutionContextImpl) getWorkflowExecutionWithRetry(
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

func (c *workflowExecutionContextImpl) updateWorkflowExecutionWithRetry(
	request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
	resp := &persistence.UpdateWorkflowExecutionResponse{}
	op := func() error {
		var err error
		resp, err = c.shard.UpdateWorkflowExecution(request)
		return err
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	return resp, err
}

func (c *workflowExecutionContextImpl) clear() {
	c.metricsClient.IncCounter(metrics.WorkflowContextScope, metrics.WorkflowContextCleared)
	c.msBuilder = nil
}

// scheduleNewDecision is helper method which has the logic for scheduling new decision for a workflow execution.
// This function takes in a slice of transferTasks and timerTasks already scheduled for the current transaction
// and may append more tasks to it.  It also returns back the slice with new tasks appended to it.  It is expected
// caller to assign returned slice to original passed in slices.  For this reason we return the original slices
// even if the method fails due to an error on loading workflow execution.
func (c *workflowExecutionContextImpl) scheduleNewDecision(transferTasks []persistence.Task,
	timerTasks []persistence.Task) ([]persistence.Task, []persistence.Task, error) {
	msBuilder, err := c.loadWorkflowExecution()
	if err != nil {
		return transferTasks, timerTasks, err
	}

	executionInfo := msBuilder.GetExecutionInfo()
	if !msBuilder.HasPendingDecisionTask() {
		di := msBuilder.AddDecisionTaskScheduledEvent()
		if di == nil {
			return nil, nil, &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
		}
		transferTasks = append(transferTasks, &persistence.DecisionTask{
			DomainID:   executionInfo.DomainID,
			TaskList:   di.TaskList,
			ScheduleID: di.ScheduleID,
		})
		if msBuilder.IsStickyTaskListEnabled() {
			tBuilder := newTimerBuilder(c.shard.GetConfig(), c.logger, clock.NewRealTimeSource())
			stickyTaskTimeoutTimer := tBuilder.AddScheduleToStartDecisionTimoutTask(di.ScheduleID, di.Attempt,
				executionInfo.StickyScheduleToStartTimeout)
			timerTasks = append(timerTasks, stickyTaskTimeoutTimer)
		}
	}

	return transferTasks, timerTasks, nil
}

func (c *workflowExecutionContextImpl) failInflightDecision() error {
	c.clear()

	// Reload workflow execution so we can apply the decision task failure event
	msBuilder, err1 := c.loadWorkflowExecution()
	if err1 != nil {
		return err1
	}

	if di, ok := msBuilder.GetInFlightDecisionTask(); ok {
		msBuilder.AddDecisionTaskFailedEvent(di.ScheduleID, di.StartedID,
			workflow.DecisionTaskFailedCauseForceCloseDecision, nil, identityHistoryService, "", "", "", 0)

		var transT, timerT []persistence.Task
		transT, timerT, err1 = c.scheduleNewDecision(transT, timerT)
		if err1 != nil {
			return err1
		}

		// Generate a transaction ID for appending events to history
		transactionID, err1 := c.shard.GetNextTransferTaskID()
		if err1 != nil {
			return err1
		}
		err1 = c.updateWorkflowExecution(transT, timerT, transactionID)
		if err1 != nil {
			return err1
		}
	}
	return nil
}

func (c *workflowExecutionContextImpl) emitWorkflowExecutionStats(stats *persistence.MutableStateStats, executionInfoHistorySize int64) {
	if stats == nil {
		return
	}

	domain := ""
	if entry, err := c.shard.GetDomainCache().GetDomainByID(c.domainID); err == nil && entry != nil && entry.GetInfo() != nil {
		domain = entry.GetInfo().Name
	}

	// emit domain tagged metrics if we can retrieve the domain
	if len(domain) > 0 {
		domainSizeScope := c.metricsClient.Scope(metrics.ExecutionSizeStatsScope, metrics.DomainTag(domain))
		domainCountScope := c.metricsClient.Scope(metrics.ExecutionCountStatsScope, metrics.DomainTag(domain))
		emitWorkflowExecutionStats(domainSizeScope, domainCountScope, stats, executionInfoHistorySize)
	}

	// N.B - always emit domain "all" metrics for aggregate information as we
	// need a way to look at aggregate stats across all domain
	allSizeScope := c.metricsClient.Scope(metrics.ExecutionSizeStatsScope, metrics.DomainAllTag())
	allCountScope := c.metricsClient.Scope(metrics.ExecutionCountStatsScope, metrics.DomainAllTag())
	emitWorkflowExecutionStats(allSizeScope, allCountScope, stats, executionInfoHistorySize)
}

func (c *workflowExecutionContextImpl) emitSessionUpdateStats(stats *persistence.MutableStateUpdateSessionStats) {
	if stats == nil {
		return
	}

	domain := ""
	if entry, err := c.shard.GetDomainCache().GetDomainByID(c.domainID); err == nil && entry != nil && entry.GetInfo() != nil {
		domain = entry.GetInfo().Name
	}

	// emit domain tagged metrics if we can retrieve the domain
	if len(domain) > 0 {
		domainSizeScope := c.metricsClient.Scope(metrics.SessionSizeStatsScope, metrics.DomainTag(domain))
		domainCountScope := c.metricsClient.Scope(metrics.SessionCountStatsScope, metrics.DomainTag(domain))
		emitSessionUpdateStats(domainSizeScope, domainCountScope, stats)
	}

	// N.B - always emit domain "all" metrics for aggregate information as we
	// need a way to look at aggregate stats across all domain
	allSizeScope := c.metricsClient.Scope(metrics.SessionSizeStatsScope, metrics.DomainAllTag())
	allCountScope := c.metricsClient.Scope(metrics.SessionCountStatsScope, metrics.DomainAllTag())
	emitSessionUpdateStats(allSizeScope, allCountScope, stats)
}

func (c *workflowExecutionContextImpl) emitWorkflowCompletionStats(event *workflow.HistoryEvent) {
	domain := ""
	if entry, err := c.shard.GetDomainCache().GetDomainByID(c.domainID); err == nil && entry != nil && entry.GetInfo() != nil {
		domain = entry.GetInfo().Name
	}

	if len(domain) > 0 {
		domainScope := c.metricsClient.Scope(metrics.WorkflowCompletionStatsScope, metrics.DomainTag(domain))
		emitWorkflowCompletionStats(domainScope, event)
	}
	scope := c.metricsClient.Scope(metrics.WorkflowCompletionStatsScope, metrics.DomainAllTag())
	emitWorkflowCompletionStats(scope, event)
}

func emitWorkflowExecutionStats(sizeScope, countScope metrics.Scope, stats *persistence.MutableStateStats, executionInfoHistorySize int64) {
	sizeScope.RecordTimer(metrics.HistorySize, time.Duration(executionInfoHistorySize))
	sizeScope.RecordTimer(metrics.MutableStateSize, time.Duration(stats.MutableStateSize))
	sizeScope.RecordTimer(metrics.ExecutionInfoSize, time.Duration(stats.MutableStateSize))
	sizeScope.RecordTimer(metrics.ActivityInfoSize, time.Duration(stats.ActivityInfoSize))
	sizeScope.RecordTimer(metrics.TimerInfoSize, time.Duration(stats.TimerInfoSize))
	sizeScope.RecordTimer(metrics.ChildInfoSize, time.Duration(stats.ChildInfoSize))
	sizeScope.RecordTimer(metrics.SignalInfoSize, time.Duration(stats.SignalInfoSize))
	sizeScope.RecordTimer(metrics.BufferedEventsSize, time.Duration(stats.BufferedEventsSize))
	sizeScope.RecordTimer(metrics.BufferedReplicationTasksSize, time.Duration(stats.BufferedReplicationTasksSize))

	countScope.RecordTimer(metrics.ActivityInfoCount, time.Duration(stats.ActivityInfoCount))
	countScope.RecordTimer(metrics.TimerInfoCount, time.Duration(stats.TimerInfoCount))
	countScope.RecordTimer(metrics.ChildInfoCount, time.Duration(stats.ChildInfoCount))
	countScope.RecordTimer(metrics.SignalInfoCount, time.Duration(stats.SignalInfoCount))
	countScope.RecordTimer(metrics.RequestCancelInfoCount, time.Duration(stats.RequestCancelInfoCount))
	countScope.RecordTimer(metrics.BufferedEventsCount, time.Duration(stats.BufferedEventsCount))
	countScope.RecordTimer(metrics.BufferedReplicationTasksCount, time.Duration(stats.BufferedReplicationTasksCount))
}

func emitSessionUpdateStats(sizeScope, countScope metrics.Scope, stats *persistence.MutableStateUpdateSessionStats) {
	sizeScope.RecordTimer(metrics.MutableStateSize, time.Duration(stats.MutableStateSize))
	sizeScope.RecordTimer(metrics.ExecutionInfoSize, time.Duration(stats.ExecutionInfoSize))
	sizeScope.RecordTimer(metrics.ActivityInfoSize, time.Duration(stats.ActivityInfoSize))
	sizeScope.RecordTimer(metrics.TimerInfoSize, time.Duration(stats.TimerInfoSize))
	sizeScope.RecordTimer(metrics.ChildInfoSize, time.Duration(stats.ChildInfoSize))
	sizeScope.RecordTimer(metrics.SignalInfoSize, time.Duration(stats.SignalInfoSize))
	sizeScope.RecordTimer(metrics.BufferedEventsSize, time.Duration(stats.BufferedEventsSize))
	sizeScope.RecordTimer(metrics.BufferedReplicationTasksSize, time.Duration(stats.BufferedReplicationTasksSize))

	countScope.RecordTimer(metrics.ActivityInfoCount, time.Duration(stats.ActivityInfoCount))
	countScope.RecordTimer(metrics.TimerInfoCount, time.Duration(stats.TimerInfoCount))
	countScope.RecordTimer(metrics.ChildInfoCount, time.Duration(stats.ChildInfoCount))
	countScope.RecordTimer(metrics.SignalInfoCount, time.Duration(stats.SignalInfoCount))
	countScope.RecordTimer(metrics.RequestCancelInfoCount, time.Duration(stats.RequestCancelInfoCount))
	countScope.RecordTimer(metrics.DeleteActivityInfoCount, time.Duration(stats.DeleteActivityInfoCount))
	countScope.RecordTimer(metrics.DeleteTimerInfoCount, time.Duration(stats.DeleteTimerInfoCount))
	countScope.RecordTimer(metrics.DeleteChildInfoCount, time.Duration(stats.DeleteChildInfoCount))
	countScope.RecordTimer(metrics.DeleteSignalInfoCount, time.Duration(stats.DeleteSignalInfoCount))
	countScope.RecordTimer(metrics.DeleteRequestCancelInfoCount, time.Duration(stats.DeleteRequestCancelInfoCount))
}

func emitWorkflowCompletionStats(scope metrics.Scope, event *workflow.HistoryEvent) {
	if event.EventType == nil {
		return
	}
	switch *event.EventType {
	case shared.EventTypeWorkflowExecutionCompleted:
		scope.IncCounter(metrics.WorkflowSuccessCount)
	case shared.EventTypeWorkflowExecutionCanceled:
		scope.IncCounter(metrics.WorkflowCancelCount)
	case shared.EventTypeWorkflowExecutionFailed:
		scope.IncCounter(metrics.WorkflowFailedCount)
	case shared.EventTypeWorkflowExecutionTimedOut:
		scope.IncCounter(metrics.WorkflowTimeoutCount)
	case shared.EventTypeWorkflowExecutionTerminated:
		scope.IncCounter(metrics.WorkflowTerminateCount)
	}
}

// validateNoEventsAfterWorkflowFinish perform check on history event batch
// NOTE: do not apply this check on every batch, since transient
// decision && workflow finish will be broken (the first batch)
func (c *workflowExecutionContextImpl) validateNoEventsAfterWorkflowFinish(input []*workflow.HistoryEvent) error {
	if len(input) == 0 {
		return nil
	}

	// if workflow is still running, no check is necessary
	if c.msBuilder.IsWorkflowExecutionRunning() {
		return nil
	}

	// workflow close
	// this will perform check on the last event of last batch
	// NOTE: do not apply this check on every batch, since transient
	// decision && workflow finish will be broken (the first batch)
	lastEvent := input[len(input)-1]
	switch lastEvent.GetEventType() {
	case workflow.EventTypeWorkflowExecutionCompleted,
		workflow.EventTypeWorkflowExecutionFailed,
		workflow.EventTypeWorkflowExecutionTimedOut,
		workflow.EventTypeWorkflowExecutionTerminated,
		workflow.EventTypeWorkflowExecutionContinuedAsNew,
		workflow.EventTypeWorkflowExecutionCanceled:

		return nil

	default:
		c.logger.Error("encounter case where events appears after workflow finish.",
			tag.WorkflowID(c.workflowExecution.GetWorkflowId()),
			tag.WorkflowRunID(c.workflowExecution.GetRunId()),
			tag.WorkflowDomainID(c.domainID))

		return ErrEventsAterWorkflowFinish
	}

}
