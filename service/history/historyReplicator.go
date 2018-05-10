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
	"errors"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/persistence"
)

type (
	historyReplicator struct {
		shard             ShardContext
		historyEngine     *historyEngineImpl
		historyCache      *historyCache
		domainCache       cache.DomainCache
		historyMgr        persistence.HistoryManager
		historySerializer persistence.HistorySerializer
		metadataMgr       cluster.Metadata
		logger            bark.Logger
	}
)

func newHistoryReplicator(shard ShardContext, historyEngine *historyEngineImpl, historyCache *historyCache, domainCache cache.DomainCache,
	historyMgr persistence.HistoryManager, logger bark.Logger) *historyReplicator {
	replicator := &historyReplicator{
		shard:             shard,
		historyEngine:     historyEngine,
		historyCache:      historyCache,
		domainCache:       domainCache,
		historyMgr:        historyMgr,
		historySerializer: persistence.NewJSONHistorySerializer(),
		metadataMgr:       shard.GetService().GetClusterMetadata(),
		logger:            logger,
	}

	return replicator
}

func (r *historyReplicator) ApplyEvents(request *h.ReplicateEventsRequest) (retError error) {
	if request == nil || request.History == nil || len(request.History.Events) == 0 {
		r.logger.Warn("Dropping empty replication task")
		return nil
	}

	domainID, err := validateDomainUUID(request.DomainUUID)
	if err != nil {
		return err
	}

	execution := *request.WorkflowExecution

	var context *workflowExecutionContext
	var msBuilder *mutableStateBuilder
	firstEvent := request.History.Events[0]
	switch firstEvent.GetEventType() {
	case shared.EventTypeWorkflowExecutionStarted:
		msBuilder = newMutableStateBuilderWithReplicationState(r.shard.GetConfig(), r.logger, request.GetVersion())

	default:
		var release releaseWorkflowExecutionFunc
		context, release, err = r.historyCache.getOrCreateWorkflowExecution(domainID, execution)
		if err != nil {
			return err
		}
		defer func() { release(retError) }()

		msBuilder, err = context.loadWorkflowExecution()
		if err != nil {
			return err
		}

		rState := msBuilder.replicationState
		// Check if this is a stale event
		if rState.CurrentVersion > request.GetVersion() {
			// Replication state is already on a higher version, we can drop this event
			// TODO: We need to replay external events like signal to the new version
			r.logger.Warnf("Dropping stale replication task.  Current Version: %v, Task Version: %v", rState.CurrentVersion,
				request.GetVersion())
			return nil
		}

		// Check if this is the first event after failover
		if rState.LastWriteVersion < request.GetVersion() {
			previousActiveCluster := r.metadataMgr.ClusterNameForFailoverVersion(rState.CurrentVersion)
			ri, ok := request.ReplicationInfo[previousActiveCluster]
			if !ok {
				r.logger.Errorf("No replication information found for previous active cluster.  Previous: %v, Current: %v",
					previousActiveCluster, request.GetSourceCluster())

				// TODO: Handle missing replication information
				return nil
			}

			// Detect conflict
			if ri.GetLastEventId() != rState.LastWriteEventID {
				r.logger.Infof("Conflict detected.  State: {Version: %, LastWriteEventID: %v}, Task: {SourceCluster: %v, Version: %v, LastEventID: %v}",
					rState.CurrentVersion, rState.LastWriteEventID, request.GetSourceCluster(), ri.GetVersion(),
					ri.GetLastEventId())

				resolver := newConflictResolver(r.shard, context, r.historyMgr, r.logger)
				msBuilder, err = resolver.reset(ri.GetLastEventId())
				if err != nil {
					return err
				}
			}
		}

		// Check for duplicate processing of replication task
		if firstEvent.GetEventId() < msBuilder.GetNextEventID() {
			return nil
		}

		// Check for out of order replication task and store it in the buffer
		if firstEvent.GetEventId() > msBuilder.GetNextEventID() {
			if err := msBuilder.BufferReplicationTask(request); err != nil {
				return errors.New("failed to add buffered replication task")
			}

			return nil
		}
	}

	// First check if there are events which needs to be flushed before applying the update
	err = r.FlushBuffer(context, msBuilder, request)
	if err != nil {
		return err
	}

	// Apply the replication task
	err = r.ApplyReplicationTask(context, msBuilder, request)
	if err != nil {
		return err
	}

	// Flush buffered replication tasks after applying the update
	err = r.FlushBuffer(context, msBuilder, request)

	return err
}

func (r *historyReplicator) ApplyReplicationTask(context *workflowExecutionContext, msBuilder *mutableStateBuilder,
	request *h.ReplicateEventsRequest) error {

	domainID, err := validateDomainUUID(request.DomainUUID)
	if err != nil {
		return err
	}
	if len(request.History.Events) == 0 {
		return nil
	}

	execution := *request.WorkflowExecution

	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	sBuilder := newStateBuilder(r.shard, msBuilder, r.logger)
	lastEvent, di, newRunStateBuilder, err := sBuilder.applyEvents(request.GetVersion(), request.GetSourceCluster(),
		domainID, requestID, execution, request.History, request.NewRunHistory)
	if err != nil {
		return err
	}

	// If replicated events has ContinueAsNew event, then create the new run history
	if newRunStateBuilder != nil {
		// Generate a transaction ID for appending events to history
		transactionID, err := r.shard.GetNextTransferTaskID()
		if err != nil {
			return err
		}
		err = context.replicateContinueAsNewWorkflowExecution(newRunStateBuilder, sBuilder.newRunTransferTasks,
			sBuilder.newRunTimerTasks, transactionID)
		if err != nil {
			return err
		}
	}

	firstEvent := request.History.Events[0]
	switch firstEvent.GetEventType() {
	case shared.EventTypeWorkflowExecutionStarted:
		// TODO: Support for child execution
		var parentExecution *shared.WorkflowExecution
		initiatedID := emptyEventID
		parentDomainID := ""

		// Serialize the history
		serializedHistory, serializedError := r.Serialize(request.History)
		if serializedError != nil {
			logging.LogHistorySerializationErrorEvent(r.logger, serializedError, fmt.Sprintf(
				"HistoryEventBatch serialization error on start workflow.  WorkflowID: %v, RunID: %v",
				execution.GetWorkflowId(), execution.GetRunId()))
			return serializedError
		}

		// Generate a transaction ID for appending events to history
		transactionID, err2 := r.shard.GetNextTransferTaskID()
		if err2 != nil {
			return err2
		}

		err = r.shard.AppendHistoryEvents(&persistence.AppendHistoryEventsRequest{
			DomainID:      domainID,
			Execution:     execution,
			TransactionID: transactionID,
			FirstEventID:  firstEvent.GetEventId(),
			Events:        serializedHistory,
		})
		if err != nil {
			return err
		}

		nextEventID := lastEvent.GetEventId() + 1
		msBuilder.executionInfo.NextEventID = nextEventID
		msBuilder.executionInfo.LastFirstEventID = firstEvent.GetEventId()

		failoverVersion := request.GetVersion()
		replicationState := &persistence.ReplicationState{
			CurrentVersion:   failoverVersion,
			StartVersion:     failoverVersion,
			LastWriteVersion: failoverVersion,
			LastWriteEventID: lastEvent.GetEventId(),
		}

		// Set decision attributes after replication of history events
		decisionVersionID := emptyVersion
		decisionScheduleID := emptyEventID
		decisionStartID := emptyEventID
		decisionTimeout := int32(0)
		if di != nil {
			decisionVersionID = di.Version
			decisionScheduleID = di.ScheduleID
			decisionStartID = di.StartedID
			decisionTimeout = di.DecisionTimeout
		}
		setTaskVersion(msBuilder.GetCurrentVersion(), sBuilder.transferTasks, sBuilder.timerTasks)

		createWorkflow := func(isBrandNew bool, prevRunID string) (string, error) {
			_, err = r.shard.CreateWorkflowExecution(&persistence.CreateWorkflowExecutionRequest{
				RequestID:                   requestID,
				DomainID:                    domainID,
				Execution:                   execution,
				ParentDomainID:              parentDomainID,
				ParentExecution:             parentExecution,
				InitiatedID:                 initiatedID,
				TaskList:                    msBuilder.executionInfo.TaskList,
				WorkflowTypeName:            msBuilder.executionInfo.WorkflowTypeName,
				WorkflowTimeout:             msBuilder.executionInfo.WorkflowTimeout,
				DecisionTimeoutValue:        msBuilder.executionInfo.DecisionTimeoutValue,
				ExecutionContext:            nil,
				NextEventID:                 msBuilder.GetNextEventID(),
				LastProcessedEvent:          emptyEventID,
				TransferTasks:               sBuilder.transferTasks,
				DecisionVersion:             decisionVersionID,
				DecisionScheduleID:          decisionScheduleID,
				DecisionStartedID:           decisionStartID,
				DecisionStartToCloseTimeout: decisionTimeout,
				TimerTasks:                  sBuilder.timerTasks,
				ContinueAsNew:               !isBrandNew,
				PreviousRunID:               prevRunID,
				ReplicationState:            replicationState,
			})

			if err != nil {
				return "", err
			}

			return execution.GetRunId(), nil
		}

		// try to create the workflow execution
		isBrandNew := true
		_, err = createWorkflow(isBrandNew, "")
		// if err still non nil, see if retry
		/*if errExist, ok := err.(*persistence.WorkflowExecutionAlreadyStartedError); ok {
			if err = workflowExistsErrHandler(errExist); err == nil {
				isBrandNew = false
				_, err = createWorkflow(isBrandNew, errExist.RunID)
			}
		}*/

	default:
		// Generate a transaction ID for appending events to history
		transactionID, err2 := r.shard.GetNextTransferTaskID()
		if err2 != nil {
			return err2
		}
		err = context.replicateWorkflowExecution(request, sBuilder.transferTasks, sBuilder.timerTasks,
			lastEvent.GetEventId(), transactionID)
	}

	if err == nil {
		now := time.Unix(0, lastEvent.GetTimestamp())
		r.notify(request.GetSourceCluster(), now, sBuilder.transferTasks, sBuilder.timerTasks)
	}

	return err
}

func (r *historyReplicator) FlushBuffer(context *workflowExecutionContext, msBuilder *mutableStateBuilder,
	request *h.ReplicateEventsRequest) error {

	// Keep on applying on applying buffered replication tasks in a loop
	for msBuilder.HasBufferedReplicationTasks() {
		nextEventID := msBuilder.GetNextEventID()
		bt, ok := msBuilder.GetBufferedReplicationTask(nextEventID)
		if !ok {
			// Bail out if nextEventID is not in the buffer
			return nil
		}

		// We need to delete the task from buffer first to make sure delete update is queued up
		// Applying replication task commits the transaction along with the delete
		msBuilder.DeleteBufferedReplicationTask(nextEventID)

		req := &h.ReplicateEventsRequest{
			SourceCluster:     request.SourceCluster,
			DomainUUID:        request.DomainUUID,
			WorkflowExecution: request.WorkflowExecution,
			FirstEventId:      common.Int64Ptr(bt.FirstEventID),
			NextEventId:       common.Int64Ptr(bt.NextEventID),
			Version:           common.Int64Ptr(bt.Version),
			History:           msBuilder.GetBufferedHistory(bt.History),
			NewRunHistory:     msBuilder.GetBufferedHistory(bt.NewRunHistory),
		}

		// Apply replication task to workflow execution
		if err := r.ApplyReplicationTask(context, msBuilder, req); err != nil {
			return err
		}
	}

	return nil
}

func (r *historyReplicator) Serialize(history *shared.History) (*persistence.SerializedHistoryEventBatch, error) {
	eventBatch := persistence.NewHistoryEventBatch(persistence.GetDefaultHistoryVersion(), history.Events)
	h, err := r.historySerializer.Serialize(eventBatch)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (r *historyReplicator) notify(clusterName string, now time.Time, transferTasks []persistence.Task,
	timerTasks []persistence.Task) {
	r.shard.SetCurrentTime(clusterName, now)
	r.historyEngine.txProcessor.NotifyNewTask(clusterName, now, transferTasks)
	r.historyEngine.timerProcessor.NotifyNewTimers(clusterName, now, timerTasks)
}
