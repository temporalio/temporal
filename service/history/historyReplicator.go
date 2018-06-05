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
		logger:            logger.WithField(logging.TagWorkflowComponent, logging.TagValueHistoryReplicatorComponent),
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

	context, release, err := r.historyCache.getOrCreateWorkflowExecution(domainID, execution)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	logger := r.logger.WithFields(bark.Fields{
		logging.TagWorkflowExecutionID: execution.GetWorkflowId(),
		logging.TagWorkflowRunID:       execution.GetRunId(),
		logging.TagSourceCluster:       request.GetSourceCluster(),
		logging.TagVersion:             request.GetVersion(),
		logging.TagFirstEventID:        request.GetFirstEventId(),
		logging.TagNextEventID:         request.GetNextEventId(),
	})
	var msBuilder *mutableStateBuilder
	firstEvent := request.History.Events[0]
	switch firstEvent.GetEventType() {
	case shared.EventTypeWorkflowExecutionStarted:
		msBuilder, err = context.loadWorkflowExecution()
		if err == nil {
			// Workflow execution already exist, looks like a duplicate start event, it is safe to ignore it
			logger.Info("Dropping stale replication task for start event.")
			return nil
		}

		// GetWorkflowExecution failed with some transient error.  Return err so we can retry the task later
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			return err
		}

		// WorkflowExecution does not exist, lets proceed with processing of the task
		msBuilder = newMutableStateBuilderWithReplicationState(r.shard.GetConfig(), r.logger, request.GetVersion())

	default:
		msBuilder, err = context.loadWorkflowExecution()
		if err != nil {
			return err
		}

		rState := msBuilder.replicationState
		// Check if this is a stale event
		if rState.LastWriteVersion > request.GetVersion() {
			// Replication state is already on a higher version, we can drop this event
			// TODO: We need to replay external events like signal to the new version
			logger.Warnf("Dropping stale replication task.  CurrentV: %v, LastWriteV: %v, LastWriteEvent: %v",
				rState.CurrentVersion, rState.LastWriteVersion, rState.LastWriteEventID)
			return nil
		}

		// Check if this is the first event after failover
		if rState.LastWriteVersion < request.GetVersion() {
			logger.Infof("First Event after replication.  CurrentV: %v, LastWriteV: %v, LastWriteEvent: %v",
				rState.CurrentVersion, rState.LastWriteVersion, rState.LastWriteEventID)
			previousActiveCluster := r.metadataMgr.ClusterNameForFailoverVersion(rState.LastWriteVersion)
			ri, ok := request.ReplicationInfo[previousActiveCluster]
			if !ok {
				logger.Errorf("No replication information found for previous active cluster.  Previous: %v, Request: %v, ReplicationInfo: %v",
					previousActiveCluster, request.GetSourceCluster(), request.ReplicationInfo)

				// TODO: Handle missing replication information
				return nil
			}

			// Detect conflict
			if ri.GetLastEventId() != rState.LastWriteEventID {
				logger.Infof("Conflict detected.  State: {V: %v, LastWriteV: %v, LastWriteEvent: %v}, ReplicationInfo: {PrevC: %v, V: %v, LastEvent: %v}",
					rState.CurrentVersion, rState.LastWriteVersion, rState.LastWriteEventID,
					previousActiveCluster, ri.GetVersion(), ri.GetLastEventId())

				resolver := newConflictResolver(r.shard, context, r.historyMgr, r.logger)
				msBuilder, err = resolver.reset(uuid.New(), ri.GetLastEventId(), msBuilder.executionInfo.StartTimestamp)
				logger.Infof("Completed Resetting of workflow execution.  NextEventID: %v. Err: %v", msBuilder.GetNextEventID(), err)
				if err != nil {
					return err
				}
			}
		}

		// Check for duplicate processing of replication task
		if firstEvent.GetEventId() < msBuilder.GetNextEventID() {
			logger.Debugf("Dropping replication task.  State: {NextEvent: %v, Version: %v, LastWriteV: %v, LastWriteEvent: %v}",
				msBuilder.GetNextEventID(), msBuilder.replicationState.CurrentVersion,
				msBuilder.replicationState.LastWriteVersion, msBuilder.replicationState.LastWriteEventID)
			return nil
		}

		// Check for out of order replication task and store it in the buffer
		if firstEvent.GetEventId() > msBuilder.GetNextEventID() {
			logger.Debugf("Buffer out of order replication task.  NextEvent: %v, FirstEvent: %v",
				msBuilder.GetNextEventID(), firstEvent.GetEventId())

			if err := msBuilder.BufferReplicationTask(request); err != nil {
				logger.Errorf("Failed to buffer out of order replication task.  Err: %v", err)
				return errors.New("failed to add buffered replication task")
			}

			return nil
		}
	}

	// First check if there are events which needs to be flushed before applying the update
	err = r.FlushBuffer(context, msBuilder, request)
	if err != nil {
		logger.Errorf("Fail to flush buffer.  NextEvent: %v, FirstEvent: %v, Err: %v", msBuilder.GetNextEventID(),
			firstEvent.GetEventId(), err)
		return err
	}

	// Apply the replication task
	err = r.ApplyReplicationTask(context, msBuilder, request)
	if err != nil {
		logger.Errorf("Fail to Apply Replication task.  NextEvent: %v, FirstEvent: %v, Err: %v", msBuilder.GetNextEventID(),
			firstEvent.GetEventId(), err)
		return err
	}

	// Flush buffered replication tasks after applying the update
	err = r.FlushBuffer(context, msBuilder, request)
	if err != nil {
		logger.Errorf("Fail to flush buffer.  NextEvent: %v, FirstEvent: %v, Err: %v", msBuilder.GetNextEventID(),
			firstEvent.GetEventId(), err)
	}

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
	lastEvent, di, newRunStateBuilder, err := sBuilder.applyEvents(domainID, requestID, execution, request.History, request.NewRunHistory)
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
		var parentExecution *shared.WorkflowExecution
		initiatedID := common.EmptyEventID
		parentDomainID := ""
		if msBuilder.executionInfo.ParentDomainID != "" {
			initiatedID = msBuilder.executionInfo.InitiatedID
			parentDomainID = msBuilder.executionInfo.ParentDomainID
			parentExecution = &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(msBuilder.executionInfo.ParentWorkflowID),
				RunId:      common.StringPtr(msBuilder.executionInfo.ParentRunID),
			}
		}

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
		decisionVersionID := common.EmptyVersion
		decisionScheduleID := common.EmptyEventID
		decisionStartID := common.EmptyEventID
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
				LastProcessedEvent:          common.EmptyEventID,
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

		// TODO
		// The failover version checking && overwriting should be performed here: #675
		// TODO

		// try to create the workflow execution
		isBrandNew := true
		_, err = createWorkflow(isBrandNew, "")
		// if err still non nil, see if retry
		if errExist, ok := err.(*persistence.WorkflowExecutionAlreadyStartedError); ok {
			prevRunID := errExist.RunID
			prevState := errExist.State

			// Check for duplicate processing of StartWorkflowExecution replication task
			if prevRunID == execution.GetRunId() {
				r.logger.Infof("Dropping stale replication task for start event.  WorkflowID: %v, RunID: %v, Version: %v",
					execution.GetWorkflowId(), execution.GetRunId(), request.GetVersion())
				return nil
			}

			// Some other workflow is running, for now let's keep on retrying this replication event by an error
			// to wait for current run to finish so this event could be applied.
			// TODO: We also need to deal with conflict resolution when workflow with same ID is started on 2 different
			// clusters.
			if prevState != persistence.WorkflowStateCompleted {
				return err
			}

			// if the existing workflow is completed, ignore the worklow ID reuse policy
			// since the policy should be applied by the active cluster,
			// standby cluster should apply this event without question.
			isBrandNew = false
			_, err = createWorkflow(isBrandNew, errExist.RunID)
		}

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
