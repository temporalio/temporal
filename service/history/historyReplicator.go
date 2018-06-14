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
	ctx "context"
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
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	conflictResolverProvider func(context *workflowExecutionContext, logger bark.Logger) conflictResolver
	stateBuilderProvider     func(msBuilder mutableState, logger bark.Logger) stateBuilder
	mutableStateProvider     func(version int64, logger bark.Logger) mutableState

	historyReplicator struct {
		shard             ShardContext
		historyEngine     *historyEngineImpl
		historyCache      *historyCache
		domainCache       cache.DomainCache
		historyMgr        persistence.HistoryManager
		historySerializer persistence.HistorySerializer
		clusterMetadata   cluster.Metadata
		metricsClient     metrics.Client
		logger            bark.Logger

		getNewConflictResolver conflictResolverProvider
		getNewStateBuilder     stateBuilderProvider
		getNewMutableState     mutableStateProvider
	}
)

var (
	// ErrRetryEntityNotExists is returned to indicate workflow execution is not created yet and replicator should
	// try this task again after a small delay.
	ErrRetryEntityNotExists = &shared.RetryTaskError{Message: "workflow execution not found"}
	// ErrRetryExecutionAlreadyStarted is returned to indicate another workflow execution already started,
	// this error can be return if we encounter race condition, i.e. terminating the target workflow while
	// the target workflow has done continue as new.
	// try this task again after a small delay.
	ErrRetryExecutionAlreadyStarted = &shared.RetryTaskError{Message: "another workflow execution is running"}
	// ErrMissingReplicationInfo is returned when replication task is missing replication information from source cluster
	ErrMissingReplicationInfo = &shared.BadRequestError{Message: "replication task is missing cluster replication info"}
	// ErrCorruptedReplicationInfo is returned when replication task has corrupted replication information from source cluster
	ErrCorruptedReplicationInfo = &shared.BadRequestError{Message: "replication task is has corrupted cluster replication info"}
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
		clusterMetadata:   shard.GetService().GetClusterMetadata(),
		metricsClient:     shard.GetMetricsClient(),
		logger:            logger.WithField(logging.TagWorkflowComponent, logging.TagValueHistoryReplicatorComponent),

		getNewConflictResolver: func(context *workflowExecutionContext, logger bark.Logger) conflictResolver {
			return newConflictResolver(shard, context, historyMgr, logger)
		},
		getNewStateBuilder: func(msBuilder mutableState, logger bark.Logger) stateBuilder {
			return newStateBuilder(shard, msBuilder, logger)
		},
		getNewMutableState: func(version int64, logger bark.Logger) mutableState {
			return newMutableStateBuilderWithReplicationState(shard.GetConfig(), logger, version)
		},
	}

	return replicator
}

func (r *historyReplicator) ApplyEvents(request *h.ReplicateEventsRequest) (retError error) {
	defer func() {
		if retError != nil {
			switch retError.(type) {
			case *shared.EntityNotExistsError:
				r.logger.Warnf("Encounter EntityNotExistsError: %v", retError)
				retError = ErrRetryEntityNotExists
			case *shared.WorkflowExecutionAlreadyStartedError:
				r.logger.Warnf("Encounter WorkflowExecutionAlreadyStartedError: %v", retError)
				retError = ErrRetryExecutionAlreadyStarted
			}
		}
	}()

	if request == nil || request.History == nil || len(request.History.Events) == 0 {
		r.logger.Warn("Dropping empty replication task")
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
		return nil
	}
	domainID, err := validateDomainUUID(request.DomainUUID)
	if err != nil {
		return err
	}

	logger := r.logger.WithFields(bark.Fields{
		logging.TagWorkflowExecutionID: request.WorkflowExecution.GetWorkflowId(),
		logging.TagWorkflowRunID:       request.WorkflowExecution.GetRunId(),
		logging.TagSourceCluster:       request.GetSourceCluster(),
		logging.TagVersion:             request.GetVersion(),
		logging.TagFirstEventID:        request.GetFirstEventId(),
		logging.TagNextEventID:         request.GetNextEventId(),
	})

	execution := *request.WorkflowExecution
	context, release, err := r.historyCache.getOrCreateWorkflowExecution(domainID, execution)
	if err != nil {
		// for get workflow execution context, with valid run id
		// err will not be of type EntityNotExistsError
		return err
	}
	defer func() { release(retError) }()

	firstEvent := request.History.Events[0]
	switch firstEvent.GetEventType() {
	case shared.EventTypeWorkflowExecutionStarted:
		_, err := context.loadWorkflowExecution()
		if err == nil {
			// Workflow execution already exist, looks like a duplicate start event, it is safe to ignore it
			logger.Info("Dropping stale replication task for start event.")
			r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
			return nil
		}
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			// GetWorkflowExecution failed with some transient error. Return err so we can retry the task later
			return err
		}
		return r.ApplyStartEvent(context, request, logger)

	default:
		// apply events, other than simple start workflow execution
		// the continue as new + start workflow execution combination will also be processed here
		msBuilder, err := context.loadWorkflowExecution()
		if err != nil {
			if _, ok := err.(*shared.EntityNotExistsError); !ok {
				return err
			}
			// mutable state for the target workflow ID & run ID combination does not exist
			// we need to check the existing workflow ID
			release(err)
			return r.ApplyOtherEventsMissingMutableState(domainID, request.WorkflowExecution.GetWorkflowId(), firstEvent.GetVersion(), logger)
		}

		msBuilder, err = r.ApplyOtherEventsVersionChecking(context, msBuilder, request, logger)
		if err != nil || msBuilder == nil {
			return err
		}
		return r.ApplyOtherEvents(context, msBuilder, request, logger)
	}
}

func (r *historyReplicator) ApplyStartEvent(context *workflowExecutionContext, request *h.ReplicateEventsRequest, logger bark.Logger) error {
	msBuilder := r.getNewMutableState(request.GetVersion(), logger)
	err := r.ApplyReplicationTask(context, msBuilder, request, logger)
	if err != nil {
		logger.Errorf("Fail to Apply Replication task.  NextEvent: %v, FirstEvent: %v, Err: %v", msBuilder.GetNextEventID(),
			request.GetFirstEventId(), err)
	}
	return err
}

func (r *historyReplicator) ApplyOtherEventsMissingMutableState(domainID string, workflowID string, incomingVersion int64, logger bark.Logger) error {
	// we need to check the current workflow execution
	currentRunID, currentLastWriteVersion, _, err := r.getCurrentWorkflowInfo(domainID, workflowID)
	if err != nil {
		return err
	}

	// we can also use the start version
	if currentLastWriteVersion > incomingVersion {
		logger.Infof("Dropping replication task. Current RunID: %v, Current LastWriteVersion: %v, Incoming Version: %v.",
			currentRunID, currentLastWriteVersion, incomingVersion)
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
		return nil
	}
	// currentLastWriteVersion <= incomingVersion
	logger.Debugf("Retrying replication task. Current RunID: %v, Current LastWriteVersion: %v, Incoming Version: %v.",
		currentRunID, currentLastWriteVersion, incomingVersion)
	return ErrRetryEntityNotExists
}

func (r *historyReplicator) ApplyOtherEventsVersionChecking(context *workflowExecutionContext, msBuilder mutableState,
	request *h.ReplicateEventsRequest, logger bark.Logger) (mutableState, error) {
	var err error
	// check if to buffer / drop / conflict resolution
	incomingVersion := request.GetVersion()
	replicationInfo := request.ReplicationInfo
	rState := msBuilder.GetReplicationState()
	if rState.LastWriteVersion > incomingVersion {
		// Replication state is already on a higher version, we can drop this event
		// TODO: We need to replay external events like signal to the new version
		logger.Warnf("Dropping stale replication task. CurrentV: %v, LastWriteV: %v, LastWriteEvent: %v, IncomingV: %v.",
			rState.CurrentVersion, rState.LastWriteVersion, rState.LastWriteEventID, incomingVersion)
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
		return nil, nil
	}

	if rState.LastWriteVersion == incomingVersion {
		// for ri.GetLastEventId() == rState.LastWriteEventID, ideally we should not do anything
		return msBuilder, nil
	}

	// we have rState.LastWriteVersion < incomingVersion

	// Check if this is the first event after failover
	logger.Infof("First Event after replication. CurrentV: %v, LastWriteV: %v, LastWriteEvent: %v, IncomingV: %v.",
		rState.CurrentVersion, rState.LastWriteVersion, rState.LastWriteEventID, incomingVersion)
	previousActiveCluster := r.clusterMetadata.ClusterNameForFailoverVersion(rState.LastWriteVersion)
	ri, ok := replicationInfo[previousActiveCluster]
	if !ok {
		logger.Errorf("No ReplicationInfo Found For Previous Active Cluster. Previous Active Cluster: %v, Request Source Cluster: %v, Request ReplicationInfo: %v.",
			previousActiveCluster, request.GetSourceCluster(), request.ReplicationInfo)
		// TODO: Handle missing replication information, #840
		// Returning BadRequestError to force the message to land into DLQ
		return nil, ErrMissingReplicationInfo
	}

	// Detect conflict
	if ri.GetLastEventId() > rState.LastWriteEventID {
		// if there is any bug in the replication protocol or implementation, this case can happen
		logger.Errorf("Conflict detected, but cannot resolve. State: {CurrentV: %v, LastWriteV: %v, LastWriteEvent: %v}, ReplicationInfo: {PrevActiveCluster: %v, V: %v, LastEventID: %v}",
			rState.CurrentVersion, rState.LastWriteVersion, rState.LastWriteEventID,
			previousActiveCluster, ri.GetVersion(), ri.GetLastEventId())
		// Returning BadRequestError to force the message to land into DLQ
		return nil, ErrCorruptedReplicationInfo
	}

	if ri.GetLastEventId() < rState.LastWriteEventID {
		logger.Infof("Conflict detected. State: {CurrentV: %v, LastWriteV: %v, LastWriteEvent: %v}, ReplicationInfo: {PrevActiveCluster: %v, V: %v, LastEventID: %v}",
			rState.CurrentVersion, rState.LastWriteVersion, rState.LastWriteEventID,
			previousActiveCluster, ri.GetVersion(), ri.GetLastEventId())
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.HistoryConflictsCounter)

		// handling edge case when resetting a workflow, and this workflow has done continue
		// we need to terminate the continue as new-ed workflow
		err = r.conflictResolutionTerminateContinueAsNew(msBuilder)
		if err != nil {
			return nil, err
		}
		resolver := r.getNewConflictResolver(context, logger)
		msBuilder, err = resolver.reset(uuid.New(), ri.GetLastEventId(), msBuilder.GetExecutionInfo().StartTimestamp)
		logger.Infof("Completed Resetting of workflow execution.  NextEventID: %v. Err: %v", msBuilder.GetNextEventID(), err)
		if err != nil {
			return nil, err
		}
	}
	return msBuilder, nil
}

func (r *historyReplicator) ApplyOtherEvents(context *workflowExecutionContext, msBuilder mutableState,
	request *h.ReplicateEventsRequest, logger bark.Logger) error {
	var err error
	firstEventID := request.GetFirstEventId()
	if firstEventID < msBuilder.GetNextEventID() {
		// duplicate replication task
		replicationState := msBuilder.GetReplicationState()
		logger.Debugf("Dropping replication task.  State: {NextEvent: %v, Version: %v, LastWriteV: %v, LastWriteEvent: %v}",
			msBuilder.GetNextEventID(), replicationState.CurrentVersion, replicationState.LastWriteVersion, replicationState.LastWriteEventID)
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
		return nil
	}
	if firstEventID > msBuilder.GetNextEventID() {
		// out of order replication task and store it in the buffer
		logger.Debugf("Buffer out of order replication task.  NextEvent: %v, FirstEvent: %v",
			msBuilder.GetNextEventID(), firstEventID)
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.BufferedReplicationTaskCounter)
		err = msBuilder.BufferReplicationTask(request)
		if err != nil {
			logger.Errorf("Failed to buffer out of order replication task.  Err: %v", err)
			return errors.New("failed to add buffered replication task")
		}
		return nil
	}

	// apply the events normally
	// First check if there are events which needs to be flushed before applying the update
	err = r.FlushBuffer(context, msBuilder, request, logger)
	if err != nil {
		logger.Errorf("Fail to flush buffer.  NextEvent: %v, FirstEvent: %v, Err: %v",
			msBuilder.GetNextEventID(), firstEventID, err)
		return err
	}

	// Apply the replication task
	err = r.ApplyReplicationTask(context, msBuilder, request, logger)
	if err != nil {
		logger.Errorf("Fail to Apply Replication task.  NextEvent: %v, FirstEvent: %v, Err: %v",
			msBuilder.GetNextEventID(), firstEventID, err)
		return err
	}

	// Flush buffered replication tasks after applying the update
	err = r.FlushBuffer(context, msBuilder, request, logger)
	if err != nil {
		logger.Errorf("Fail to flush buffer.  NextEvent: %v, FirstEvent: %v, Err: %v",
			msBuilder.GetNextEventID(), firstEventID, err)
	}

	return err
}

func (r *historyReplicator) ApplyReplicationTask(context *workflowExecutionContext, msBuilder mutableState,
	request *h.ReplicateEventsRequest, logger bark.Logger) error {

	domainID, err := validateDomainUUID(request.DomainUUID)
	if err != nil {
		return err
	}
	if len(request.History.Events) == 0 {
		return nil
	}

	execution := *request.WorkflowExecution

	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	sBuilder := r.getNewStateBuilder(msBuilder, logger)
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
		err = context.replicateContinueAsNewWorkflowExecution(newRunStateBuilder, sBuilder.getNewRunTransferTasks(),
			sBuilder.getNewRunTimerTasks(), transactionID)
		if err != nil {
			return err
		}
	}

	firstEvent := request.History.Events[0]
	switch firstEvent.GetEventType() {
	case shared.EventTypeWorkflowExecutionStarted:
		err = r.replicateWorkflowStarted(context, msBuilder, di, request.GetSourceCluster(), request.History, sBuilder, logger)
	default:
		// Generate a transaction ID for appending events to history
		transactionID, err2 := r.shard.GetNextTransferTaskID()
		if err2 != nil {
			return err2
		}
		err = context.replicateWorkflowExecution(request, sBuilder.getTransferTasks(), sBuilder.getTimerTasks(),
			lastEvent.GetEventId(), transactionID)
	}

	if err == nil {
		now := time.Unix(0, lastEvent.GetTimestamp())
		r.notify(request.GetSourceCluster(), now, sBuilder.getTransferTasks(), sBuilder.getTimerTasks())
	}

	return err
}

func (r *historyReplicator) FlushBuffer(context *workflowExecutionContext, msBuilder mutableState,
	request *h.ReplicateEventsRequest, logger bark.Logger) error {

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
		if err := r.ApplyReplicationTask(context, msBuilder, req, logger); err != nil {
			return err
		}
	}

	return nil
}

func (r *historyReplicator) replicateWorkflowStarted(context *workflowExecutionContext, msBuilder mutableState, di *decisionInfo,
	sourceCluster string, history *shared.History, sBuilder stateBuilder, logger bark.Logger) error {
	executionInfo := msBuilder.GetExecutionInfo()
	domainID := executionInfo.DomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr(executionInfo.WorkflowID),
		RunId:      common.StringPtr(executionInfo.RunID),
	}
	var parentExecution *shared.WorkflowExecution
	initiatedID := common.EmptyEventID
	parentDomainID := ""
	if executionInfo.ParentDomainID != "" {
		initiatedID = executionInfo.InitiatedID
		parentDomainID = executionInfo.ParentDomainID
		parentExecution = &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(executionInfo.ParentWorkflowID),
			RunId:      common.StringPtr(executionInfo.ParentRunID),
		}
	}
	firstEvent := history.Events[0]
	lastEvent := history.Events[len(history.Events)-1]

	// Serialize the history
	serializedHistory, serializedError := r.Serialize(history)
	if serializedError != nil {
		logging.LogHistorySerializationErrorEvent(logger, serializedError, fmt.Sprintf(
			"HistoryEventBatch serialization error on start workflow.  WorkflowID: %v, RunID: %v",
			execution.GetWorkflowId(), execution.GetRunId()))
		return serializedError
	}

	// Generate a transaction ID for appending events to history
	transactionID, err := r.shard.GetNextTransferTaskID()
	if err != nil {
		return err
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

	// TODO this pile of logic should be merge into workflow execution context / mutable state
	executionInfo.LastFirstEventID = firstEvent.GetEventId()
	executionInfo.NextEventID = lastEvent.GetEventId() + 1
	incomingVersion := firstEvent.GetVersion()
	msBuilder.UpdateReplicationStateLastEventID(sourceCluster, incomingVersion, lastEvent.GetEventId())
	replicationState := msBuilder.GetReplicationState()

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
	transferTasks := sBuilder.getTransferTasks()
	timerTasks := sBuilder.getTimerTasks()
	setTaskVersion(msBuilder.GetCurrentVersion(), transferTasks, timerTasks)

	createWorkflow := func(isBrandNew bool, prevRunID string) error {
		_, err = r.shard.CreateWorkflowExecution(&persistence.CreateWorkflowExecutionRequest{
			RequestID:                   executionInfo.CreateRequestID,
			DomainID:                    domainID,
			Execution:                   execution,
			ParentDomainID:              parentDomainID,
			ParentExecution:             parentExecution,
			InitiatedID:                 initiatedID,
			TaskList:                    executionInfo.TaskList,
			WorkflowTypeName:            executionInfo.WorkflowTypeName,
			WorkflowTimeout:             executionInfo.WorkflowTimeout,
			DecisionTimeoutValue:        executionInfo.DecisionTimeoutValue,
			ExecutionContext:            nil,
			NextEventID:                 msBuilder.GetNextEventID(),
			LastProcessedEvent:          common.EmptyEventID,
			TransferTasks:               transferTasks,
			DecisionVersion:             decisionVersionID,
			DecisionScheduleID:          decisionScheduleID,
			DecisionStartedID:           decisionStartID,
			DecisionStartToCloseTimeout: decisionTimeout,
			TimerTasks:                  timerTasks,
			ContinueAsNew:               !isBrandNew,
			PreviousRunID:               prevRunID,
			ReplicationState:            replicationState,
		})
		return err
	}
	deleteHistory := func() {
		// this function should be only called when we drop start workflow execution
		r.shard.GetHistoryManager().DeleteWorkflowExecutionHistory(&persistence.DeleteWorkflowExecutionHistoryRequest{
			DomainID:  domainID,
			Execution: execution,
		})
	}

	// try to create the workflow execution
	isBrandNew := true
	err = createWorkflow(isBrandNew, "")
	if err == nil {
		return nil
	}
	if _, ok := err.(*persistence.WorkflowExecutionAlreadyStartedError); !ok {
		deleteHistory()
		return err
	}

	// we have WorkflowExecutionAlreadyStartedError
	errExist := err.(*persistence.WorkflowExecutionAlreadyStartedError)
	currentRunID := errExist.RunID
	currentState := errExist.State
	currentStartVersion := errExist.StartVersion

	if currentRunID == execution.GetRunId() {
		logger.Warnf("Dropping stale start replication task. Current StartV: %v, IncomingV: %v.",
			currentStartVersion, incomingVersion)
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
		return nil
	}

	// current workflow is completed
	if currentState == persistence.WorkflowStateCompleted {
		if currentStartVersion > incomingVersion {
			logger.Warnf("Dropping stale start replication task. Current StartV: %v, IncomingV: %v.",
				currentStartVersion, incomingVersion)
			r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
			deleteHistory()
			return nil
		}
		// proceed to create workflow
		isBrandNew = false
		return createWorkflow(isBrandNew, currentRunID)
	}

	// current workflow is still running
	if currentStartVersion > incomingVersion {
		logger.Warnf("Dropping stale start replication task. Current StartV: %v, IncomingV: %v.",
			currentStartVersion, incomingVersion)
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
		deleteHistory()
		return nil
	}
	if currentStartVersion == incomingVersion {
		return ErrRetryEntityNotExists
	}

	// currentStartVersion < incomingVersion && current workflow still running
	// this can happen during the failover; since we have no idea
	// whether the remote active cluster is aware of the current running workflow,
	// the only thing we can do is to terminate the current workflow and
	// start the new workflow from the request

	// same workflow ID, same shard
	err = r.terminateWorkflow(domainID, executionInfo.WorkflowID, currentRunID)
	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			return err
		}
		// if workflow is completed just when the call is made, will get EntityNotExistsError
		// we are not sure whether the workflow to be terminated ends with continue as new or not
		// so when encounter EntityNotExistsError, just contiue to execute, if err occurs,
		// there will be retry on the worker level
	}
	isBrandNew = false
	return createWorkflow(isBrandNew, currentRunID)
}

func (r *historyReplicator) conflictResolutionTerminateContinueAsNew(msBuilder mutableState) (retError error) {
	// this function aims to solve the edge case when this workflow, when going through
	// reset, has already started a next generation (continue as new-ed workflow)

	if msBuilder.IsWorkflowExecutionRunning() {
		// workflow still running, no continued as new edge case to solve
		return nil
	}

	if msBuilder.GetExecutionInfo().CloseStatus != persistence.WorkflowCloseStatusContinuedAsNew {
		// workflow close status not being continue as new
		return nil
	}

	// the close status is continue as new
	// so it is impossible that the current running workflow (one with the same workflow ID)
	// has the same run ID as "this" workflow
	// meaning there is no chance that when we grab the current running workflow (same workflow ID)
	// and enounter a dead lock
	domainID := msBuilder.GetExecutionInfo().DomainID
	workflowID := msBuilder.GetExecutionInfo().WorkflowID
	currentRunID, _, closeStatus, err := r.getCurrentWorkflowInfo(domainID, workflowID)
	if err != nil {
		return err
	}
	if closeStatus != persistence.WorkflowCloseStatusNone {
		// current workflow finished
		// note, it is impassoble that a current workflow ends with continue as new as close status
		return nil
	}

	getPrevRunID := func(domainID string, workflowID string, runID string) (string, error) {
		response, err := r.historyMgr.GetWorkflowExecutionHistory(&persistence.GetWorkflowExecutionHistoryRequest{
			DomainID: domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			FirstEventID:  common.FirstEventID,
			NextEventID:   common.FirstEventID + 1,
			PageSize:      defaultHistoryPageSize,
			NextPageToken: nil,
		})
		if err != nil {
			return "", err
		}
		if len(response.Events) == 0 {
			return "", fmt.Errorf("no history found for domainID: %v, workflowID: %v, runID: %v",
				domainID, workflowID, runID)
		}
		serializedHistoryEventBatch := response.Events[0]
		persistence.SetSerializedHistoryDefaults(&serializedHistoryEventBatch)
		serializer, err := persistence.NewHistorySerializerFactory().Get(serializedHistoryEventBatch.EncodingType)
		if err != nil {
			return "", err
		}
		history, err := serializer.Deserialize(&serializedHistoryEventBatch)
		if err != nil {
			return "", err
		}
		if len(history.Events) == 0 {
			return "", fmt.Errorf("no history events found for domainID: %v, workflowID: %v, runID: %v",
				domainID, workflowID, runID)
		}

		return history.Events[0].WorkflowExecutionStartedEventAttributes.GetContinuedExecutionRunId(), nil
	}

	targetRunID := msBuilder.GetExecutionInfo().RunID
	runID := currentRunID
	for err == nil && runID != "" && runID != targetRunID {
		// using the current running workflow to trace back (assuming continue as new)
		runID, err = getPrevRunID(domainID, workflowID, runID)
	}
	if err != nil {
		return err
	}
	if runID == "" {
		// cannot relate the current running workflow to the workflow which events are being resetted.
		return nil
	}

	// we have runID == targetRunID
	// meaning the current workflow is a result of continue as new of the workflow to be resetted

	// if workflow is completed just when the call is made, will get EntityNotExistsError
	// we are not sure whether the workflow to be terminated ends with continue as new or not
	// so when encounter EntityNotExistsError, as well as other error, just return the err
	// we will retry on the worker level

	// same workflow ID, same shard
	return r.terminateWorkflow(domainID, workflowID, currentRunID)
}

func (r *historyReplicator) Serialize(history *shared.History) (*persistence.SerializedHistoryEventBatch, error) {
	eventBatch := persistence.NewHistoryEventBatch(persistence.GetDefaultHistoryVersion(), history.Events)
	h, err := r.historySerializer.Serialize(eventBatch)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (r *historyReplicator) getCurrentWorkflowInfo(domainID string, workflowID string) (runID string, lastWriteVersion int64, closeStatus int, retError error) {
	// we need to check the current workflow execution
	context, release, err := r.historyCache.getOrCreateWorkflowExecution(
		domainID,
		// only use the workflow ID, to get the current running one
		shared.WorkflowExecution{WorkflowId: common.StringPtr(workflowID)},
	)
	if err != nil {
		return "", common.EmptyVersion, persistence.WorkflowCloseStatusNone, err
	}
	defer func() { release(retError) }()

	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		// no matter what error happen, we need to retry
		return "", common.EmptyVersion, persistence.WorkflowCloseStatusNone, err
	}
	lastWriteVersion = msBuilder.GetLastWriteVersion()
	runID = msBuilder.GetExecutionInfo().RunID
	closeStatus = msBuilder.GetExecutionInfo().CloseStatus
	return
}

func (r *historyReplicator) terminateWorkflow(domainID string, workflowID string, runID string) error {
	domainEntry, err := r.domainCache.GetDomainByID(domainID)
	if err != nil {
		return err
	}
	// same workflow ID, same shard
	return r.historyEngine.TerminateWorkflowExecution(ctx.Background(), &h.TerminateWorkflowExecutionRequest{
		DomainUUID: common.StringPtr(domainID),
		TerminateRequest: &shared.TerminateWorkflowExecutionRequest{
			Domain: common.StringPtr(domainEntry.GetInfo().Name),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			Reason:   common.StringPtr("Terminate Workflow Due To Version Conflict."),
			Details:  nil,
			Identity: common.StringPtr("worker-service"),
		},
	})
}

func (r *historyReplicator) notify(clusterName string, now time.Time, transferTasks []persistence.Task,
	timerTasks []persistence.Task) {
	r.shard.SetCurrentTime(clusterName, now)
	r.historyEngine.txProcessor.NotifyNewTask(clusterName, now, transferTasks)
	r.historyEngine.timerProcessor.NotifyNewTimers(clusterName, now, timerTasks)
}
