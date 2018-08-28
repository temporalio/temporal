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
	"errors"
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

var (
	errNoHistoryFound = errors.New("no history events found")

	workflowTerminationReason   = "Terminate Workflow Due To Version Conflict."
	workflowTerminationIdentity = "worker-service"
)

type (
	conflictResolverProvider func(ctx *workflowExecutionContext, logger bark.Logger) conflictResolver
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
	// ErrRetryExistingWorkflow is returned when events are arriving out of order, and there is another workflow with same version running
	ErrRetryExistingWorkflow = &shared.RetryTaskError{Message: "workflow with same version is running"}
	// ErrRetryBufferEvents is returned when events are arriving out of order, should retry, or specify force apply
	ErrRetryBufferEvents = &shared.RetryTaskError{Message: "retry on applying buffer events"}
	// ErrRetryExecutionAlreadyStarted is returned to indicate another workflow execution already started,
	// this error can be return if we encounter race condition, i.e. terminating the target workflow while
	// the target workflow has done continue as new.
	// try this task again after a small delay.
	ErrRetryExecutionAlreadyStarted = &shared.RetryTaskError{Message: "another workflow execution is running"}
	// ErrCorruptedReplicationInfo is returned when replication task has corrupted replication information from source cluster
	ErrCorruptedReplicationInfo = &shared.BadRequestError{Message: "replication task is has corrupted cluster replication info"}
	// ErrMoreThan2DC is returned when there are more than 2 data center
	ErrMoreThan2DC = &shared.BadRequestError{Message: "more than 2 data center"}
	// ErrImpossibleLocalRemoteMissingReplicationInfo is returned when replication task is missing replication info, as well as local replication info being empty
	ErrImpossibleLocalRemoteMissingReplicationInfo = &shared.BadRequestError{Message: "local and remote both are missing replication info"}
	// ErrImpossibleRemoteClaimSeenHigherVersion is returned when replication info contains higher version then this cluster ever emitted.
	ErrImpossibleRemoteClaimSeenHigherVersion = &shared.BadRequestError{Message: "replication info contains higher version then this cluster ever emitted"}
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
			return newMutableStateBuilderWithReplicationState(
				shard.GetService().GetClusterMetadata().GetCurrentClusterName(),
				shard.GetConfig(),
				logger,
				version,
			)
		},
	}

	return replicator
}

func (r *historyReplicator) ApplyEvents(ctx context.Context, request *h.ReplicateEventsRequest) (retError error) {
	logger := r.logger.WithFields(bark.Fields{
		logging.TagWorkflowExecutionID: request.WorkflowExecution.GetWorkflowId(),
		logging.TagWorkflowRunID:       request.WorkflowExecution.GetRunId(),
		logging.TagSourceCluster:       request.GetSourceCluster(),
		logging.TagIncomingVersion:     request.GetVersion(),
		logging.TagFirstEventID:        request.GetFirstEventId(),
		logging.TagNextEventID:         request.GetNextEventId(),
	})

	r.metricsClient.RecordTimer(
		metrics.ReplicateHistoryEventsScope,
		metrics.ReplicationEventsSizeTimer,
		time.Duration(len(request.History.Events)),
	)

	defer func() {
		if retError != nil {
			switch retError.(type) {
			case *shared.EntityNotExistsError:
				logger.Debugf("Encounter EntityNotExistsError: %v", retError)
				retError = ErrRetryEntityNotExists
			case *shared.WorkflowExecutionAlreadyStartedError:
				logger.Debugf("Encounter WorkflowExecutionAlreadyStartedError: %v", retError)
				retError = ErrRetryExecutionAlreadyStarted
			case *persistence.WorkflowExecutionAlreadyStartedError:
				logger.Debugf("Encounter WorkflowExecutionAlreadyStartedError: %v", retError)
				retError = ErrRetryExecutionAlreadyStarted
			}
		}
	}()

	if request == nil || request.History == nil || len(request.History.Events) == 0 {
		logger.Warn("Dropping empty replication task")
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.EmptyReplicationEventsCounter)
		return nil
	}
	domainID, err := validateDomainUUID(request.DomainUUID)
	if err != nil {
		return err
	}

	execution := *request.WorkflowExecution
	context, release, err := r.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, execution)
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
			logger.Debugf("Dropping stale replication task for start event.")
			r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.DuplicateReplicationEventsCounter)
			return nil
		}
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			// GetWorkflowExecution failed with some transient error. Return err so we can retry the task later
			return err
		}
		return r.ApplyStartEvent(ctx, context, request, logger)

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
			return r.ApplyOtherEventsMissingMutableState(ctx, domainID, request.WorkflowExecution.GetWorkflowId(),
				firstEvent.GetVersion(), logger)
		}

		logger.WithField(logging.TagCurrentVersion, msBuilder.GetReplicationState().LastWriteVersion)
		err = r.FlushBuffer(ctx, context, msBuilder, logger)
		if err != nil {
			logError(logger, "Fail to pre-flush buffer.", err)
			return err
		}
		msBuilder, err = r.ApplyOtherEventsVersionChecking(ctx, context, msBuilder, request, logger)
		if err != nil || msBuilder == nil {
			return err
		}
		return r.ApplyOtherEvents(ctx, context, msBuilder, request, logger)
	}
}

func (r *historyReplicator) ApplyStartEvent(ctx context.Context, context *workflowExecutionContext,
	request *h.ReplicateEventsRequest,
	logger bark.Logger) error {
	msBuilder := r.getNewMutableState(request.GetVersion(), logger)
	err := r.ApplyReplicationTask(ctx, context, msBuilder, request, logger)
	return err
}

func (r *historyReplicator) ApplyOtherEventsMissingMutableState(ctx context.Context, domainID string, workflowID string,
	incomingVersion int64, logger bark.Logger) error {
	// we need to check the current workflow execution
	_, currentMutableState, currentRelease, err := r.getCurrentWorkflowMutableState(ctx, domainID, workflowID)
	if err != nil {
		return err
	}
	currentRunID := currentMutableState.GetExecutionInfo().RunID
	currentLastWriteVersion := currentMutableState.GetLastWriteVersion()
	currentRelease(nil)

	// we can also use the start version
	if currentLastWriteVersion > incomingVersion {
		logger.Info("Dropping replication task.")
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
		return nil
	}
	// currentLastWriteVersion <= incomingVersion
	logger.Debugf("Retrying replication task. Current RunID: %v, Current LastWriteVersion: %v, Incoming Version: %v.",
		currentRunID, currentLastWriteVersion, incomingVersion)

	// try flush the current workflow buffer
	err = r.flushCurrentWorkflowBuffer(ctx, domainID, workflowID, logger)
	if err != nil {
		return err
	}
	return ErrRetryEntityNotExists
}

func (r *historyReplicator) ApplyOtherEventsVersionChecking(ctx context.Context, context *workflowExecutionContext,
	msBuilder mutableState, request *h.ReplicateEventsRequest, logger bark.Logger) (mutableState, error) {
	var err error
	// check if to buffer / drop / conflict resolution
	incomingVersion := request.GetVersion()
	replicationInfo := request.ReplicationInfo
	rState := msBuilder.GetReplicationState()
	if rState.LastWriteVersion > incomingVersion {
		// Replication state is already on a higher version, we can drop this event
		// TODO: We need to replay external events like signal to the new version
		logger.Info("Dropping stale replication task.")
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
		return nil, nil
	}

	if rState.LastWriteVersion == incomingVersion {
		// for ri.GetLastEventId() == rState.LastWriteEventID, ideally we should not do anything
		return msBuilder, nil
	}

	// we have rState.LastWriteVersion < incomingVersion

	// the code below only deal with 2 data center case
	// for multiple data center cases, wait for #840

	// Check if this is the first event after failover
	previousActiveCluster := r.clusterMetadata.ClusterNameForFailoverVersion(rState.LastWriteVersion)
	logger.WithFields(bark.Fields{
		logging.TagPrevActiveCluster: previousActiveCluster,
		logging.TagReplicationInfo:   request.ReplicationInfo,
	})
	logger.Info("First Event after replication.")

	// first check whether the replication info
	// the reason is, if current cluster was active, and sent out replication task
	// to remote, there is no guarantee that the replication task is going to be applied,
	// if not applied, the replication info will not be up to date.

	if previousActiveCluster != r.clusterMetadata.GetCurrentClusterName() {
		// this cluster is previously NOT active, this also means there is no buffered event
		if r.clusterMetadata.IsVersionFromSameCluster(incomingVersion, rState.LastWriteVersion) {
			// it is possible that a workflow will not generate any event in few rounds of failover
			// meaning that the incoming version > last write version and
			// (incoming version - last write version) % failover version increment == 0
			return msBuilder, nil
		}

		err = ErrMoreThan2DC
		logError(logger, err.Error(), err)
		return nil, err
	}

	// previousActiveCluster == current cluster
	ri, ok := replicationInfo[previousActiveCluster]
	// this cluster is previously active, we need to check whether the events is applied by remote cluster
	if !ok || rState.LastWriteVersion > ri.GetVersion() {
		logger.Info("Encounter case where events are rejected by remote.")
		// use the last valid version && event ID to do a reset
		lastValidVersion, lastValidEventID := r.getLatestCheckpoint(replicationInfo, rState.LastReplicationInfo)

		if lastValidVersion == common.EmptyVersion {
			err = ErrImpossibleLocalRemoteMissingReplicationInfo
			logError(logger, err.Error(), err)
			return nil, err
		}
		logger.Info("Reset to latest common checkpoint.")

		// NOTE: this conflict resolution do not handle fast >= 2 failover
		lastEvent := request.History.Events[len(request.History.Events)-1]
		incomingTimestamp := lastEvent.GetTimestamp()
		return r.resetMutableState(ctx, context, msBuilder, lastValidEventID, incomingVersion, incomingTimestamp, logger)
	}
	if rState.LastWriteVersion < ri.GetVersion() {
		err = ErrImpossibleRemoteClaimSeenHigherVersion
		logError(logger, err.Error(), err)
		return nil, err
	}

	// remote replication info last write version is the same as local last write version, check reset
	// Detect conflict
	if ri.GetLastEventId() > rState.LastWriteEventID {
		// if there is any bug in the replication protocol or implementation, this case can happen
		logError(logger, "Conflict detected, but cannot resolve.", ErrCorruptedReplicationInfo)
		// Returning BadRequestError to force the message to land into DLQ
		return nil, ErrCorruptedReplicationInfo
	}

	if ri.GetLastEventId() < rState.LastWriteEventID || msBuilder.HasBufferedEvents() {
		// the reason to reset mutable state if mutable state has buffered events
		// is: what buffered event actually do is delay generation of event ID,
		// the actual action of those buffered event are already applied to mutable state.

		logger.Info("Conflict detected.")
		lastEvent := request.History.Events[len(request.History.Events)-1]
		incomingTimestamp := lastEvent.GetTimestamp()
		return r.resetMutableState(ctx, context, msBuilder, ri.GetLastEventId(), incomingVersion, incomingTimestamp, logger)
	}

	// event ID match, no reset
	return msBuilder, nil
}

func (r *historyReplicator) ApplyOtherEvents(ctx context.Context, context *workflowExecutionContext,
	msBuilder mutableState, request *h.ReplicateEventsRequest, logger bark.Logger) error {
	var err error
	firstEventID := request.GetFirstEventId()
	if firstEventID < msBuilder.GetNextEventID() {
		// duplicate replication task
		replicationState := msBuilder.GetReplicationState()
		logger.Debugf("Dropping replication task.  State: {NextEvent: %v, Version: %v, LastWriteV: %v, LastWriteEvent: %v}",
			msBuilder.GetNextEventID(), replicationState.CurrentVersion, replicationState.LastWriteVersion, replicationState.LastWriteEventID)
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.DuplicateReplicationEventsCounter)
		return nil
	}
	if firstEventID > msBuilder.GetNextEventID() {

		if !msBuilder.IsWorkflowExecutionRunning() {
			logger.Warnf("Workflow already terminated due to conflict resolution.")
			return nil
		}

		// out of order replication task and store it in the buffer
		logger.Debugf("Buffer out of order replication task.  NextEvent: %v, FirstEvent: %v",
			msBuilder.GetNextEventID(), firstEventID)

		if !request.GetForceBufferEvents() {
			return ErrRetryBufferEvents
		}

		r.metricsClient.RecordTimer(
			metrics.ReplicateHistoryEventsScope,
			metrics.BufferReplicationTaskTimer,
			time.Duration(len(request.History.Events)),
		)

		bt, ok := msBuilder.GetBufferedReplicationTask(request.GetFirstEventId())
		if ok && bt.Version >= request.GetVersion() {
			// Have an existing replication task
			return nil
		}

		err = msBuilder.BufferReplicationTask(request)
		if err != nil {
			logError(logger, "Failed to buffer out of order replication task.", err)
			return errors.New("failed to add buffered replication task")
		}
		return r.updateBufferedReplicationTask(context, msBuilder)
	}

	// Apply the replication task
	err = r.ApplyReplicationTask(ctx, context, msBuilder, request, logger)
	if err != nil {
		logError(logger, "Fail to Apply Replication task.", err)
		return err
	}

	// Flush buffered replication tasks after applying the update
	err = r.FlushBuffer(ctx, context, msBuilder, logger)
	if err != nil {
		logError(logger, "Fail to flush buffer.", err)
	}

	return err
}

func (r *historyReplicator) ApplyReplicationTask(ctx context.Context, context *workflowExecutionContext,
	msBuilder mutableState, request *h.ReplicateEventsRequest, logger bark.Logger) error {

	if !msBuilder.IsWorkflowExecutionRunning() {
		logger.Warnf("Workflow already terminated due to conflict resolution.")
		return nil
	}

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
		err = context.replicateContinueAsNewWorkflowExecution(newRunStateBuilder, transactionID)
		if err != nil {
			return err
		}
	}

	firstEvent := request.History.Events[0]
	switch firstEvent.GetEventType() {
	case shared.EventTypeWorkflowExecutionStarted:
		err = r.replicateWorkflowStarted(ctx, context, msBuilder, di, request.GetSourceCluster(), request.History, sBuilder,
			logger)
	default:
		// Generate a transaction ID for appending events to history
		transactionID, err2 := r.shard.GetNextTransferTaskID()
		if err2 != nil {
			return err2
		}
		now := time.Unix(0, lastEvent.GetTimestamp())
		err = context.replicateWorkflowExecution(request, sBuilder.getTransferTasks(), sBuilder.getTimerTasks(), lastEvent.GetEventId(), transactionID, now)
	}

	if err == nil {
		now := time.Unix(0, lastEvent.GetTimestamp())
		r.notify(request.GetSourceCluster(), now, sBuilder.getTransferTasks(), sBuilder.getTimerTasks())
	}

	return err
}

func (r *historyReplicator) FlushBuffer(ctx context.Context, context *workflowExecutionContext, msBuilder mutableState,
	logger bark.Logger) error {
	domainID := msBuilder.GetExecutionInfo().DomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr(msBuilder.GetExecutionInfo().WorkflowID),
		RunId:      common.StringPtr(msBuilder.GetExecutionInfo().RunID),
	}

	flushedCount := 0
	defer func() {
		r.metricsClient.RecordTimer(
			metrics.ReplicateHistoryEventsScope,
			metrics.UnbufferReplicationTaskTimer,
			time.Duration(flushedCount),
		)
	}()

	// Keep on applying on applying buffered replication tasks in a loop
	for msBuilder.HasBufferedReplicationTasks() {
		nextEventID := msBuilder.GetNextEventID()
		bt, ok := msBuilder.GetBufferedReplicationTask(nextEventID)
		if !ok {
			// Bail out if nextEventID is not in the buffer or version is stale
			return nil
		}

		// We need to delete the task from buffer first to make sure delete update is queued up
		// Applying replication task commits the transaction along with the delete
		msBuilder.DeleteBufferedReplicationTask(nextEventID)

		if bt.Version < msBuilder.GetLastWriteVersion() {
			return r.updateBufferedReplicationTask(context, msBuilder)
		}

		sourceCluster := r.clusterMetadata.ClusterNameForFailoverVersion(bt.Version)
		req := &h.ReplicateEventsRequest{
			SourceCluster:     common.StringPtr(sourceCluster),
			DomainUUID:        common.StringPtr(domainID),
			WorkflowExecution: &execution,
			FirstEventId:      common.Int64Ptr(bt.FirstEventID),
			NextEventId:       common.Int64Ptr(bt.NextEventID),
			Version:           common.Int64Ptr(bt.Version),
			History:           msBuilder.GetBufferedHistory(bt.History),
			NewRunHistory:     msBuilder.GetBufferedHistory(bt.NewRunHistory),
		}

		// Apply replication task to workflow execution
		if err := r.ApplyReplicationTask(ctx, context, msBuilder, req, logger); err != nil {
			return err
		}
		flushedCount += int(bt.NextEventID - bt.FirstEventID)
	}

	return nil
}

func (r *historyReplicator) replicateWorkflowStarted(ctx context.Context, context *workflowExecutionContext,
	msBuilder mutableState, di *decisionInfo,
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
		logging.LogHistorySerializationErrorEvent(logger, serializedError, "HistoryEventBatch serialization error on start workflow.")
		return serializedError
	}

	// Generate a transaction ID for appending events to history
	transactionID, err := r.shard.GetNextTransferTaskID()
	if err != nil {
		return err
	}

	err = r.shard.AppendHistoryEvents(&persistence.AppendHistoryEventsRequest{
		DomainID:          domainID,
		Execution:         execution,
		TransactionID:     transactionID,
		FirstEventID:      firstEvent.GetEventId(),
		EventBatchVersion: firstEvent.GetVersion(),
		Events:            serializedHistory,
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
	setTaskInfo(
		msBuilder.GetCurrentVersion(),
		time.Unix(0, lastEvent.GetTimestamp()),
		transferTasks,
		timerTasks,
	)

	createWorkflow := func(isBrandNew bool, prevRunID string) error {
		_, err = r.shard.CreateWorkflowExecution(&persistence.CreateWorkflowExecutionRequest{
			// NOTE: should not set the replication task, since we are in the standby
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

	logger.WithField(logging.TagCurrentVersion, currentStartVersion)
	if currentRunID == execution.GetRunId() {
		logger.Info("Dropping stale start replication task.")
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.DuplicateReplicationEventsCounter)
		return nil
	}

	// current workflow is completed
	if currentState == persistence.WorkflowStateCompleted {
		if currentStartVersion > incomingVersion {
			logger.Info("Dropping stale start replication task.")
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
		logger.Info("Dropping stale start replication task.")
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
		deleteHistory()
		return nil
	}
	if currentStartVersion == incomingVersion {
		err = r.flushCurrentWorkflowBuffer(ctx, domainID, execution.GetWorkflowId(), logger)
		if err != nil {
			return err
		}
		return ErrRetryExistingWorkflow
	}

	// currentStartVersion < incomingVersion && current workflow still running
	// this can happen during the failover; since we have no idea
	// whether the remote active cluster is aware of the current running workflow,
	// the only thing we can do is to terminate the current workflow and
	// start the new workflow from the request

	// same workflow ID, same shard
	incomingTimestamp := lastEvent.GetTimestamp()
	err = r.terminateWorkflow(ctx, domainID, executionInfo.WorkflowID, currentRunID, incomingVersion, incomingTimestamp, logger)
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

func (r *historyReplicator) flushCurrentWorkflowBuffer(ctx context.Context, domainID string, workflowID string,
	logger bark.Logger) error {
	currentContext, currentMutableState, currentRelease, err := r.getCurrentWorkflowMutableState(ctx, domainID,
		workflowID)
	if err != nil {
		return err
	}
	// since this new workflow cannnot make progress due to existing workflow being open
	// try flush the existing workflow's buffer see if we can make it move forward
	// First check if there are events which needs to be flushed before applying the update
	err = r.FlushBuffer(ctx, currentContext, currentMutableState, logger)
	currentRelease(err)
	if err != nil {
		logError(logger, "Fail to flush buffer for current workflow.", err)
		return err
	}
	return nil
}

func (r *historyReplicator) conflictResolutionTerminateCurrentRunningIfNotSelf(ctx context.Context,
	msBuilder mutableState, incomingVersion int64, incomingTimestamp int64, logger bark.Logger) (currentRunID string, retError error) {
	// this function aims to solve the edge case when this workflow, when going through
	// reset, has already started a next generation (continue as new-ed workflow)

	if msBuilder.IsWorkflowExecutionRunning() {
		// workflow still running, no continued as new edge case to solve
		logger.Info("Conflict resolution self workflow running, skip.")
		return msBuilder.GetExecutionInfo().RunID, nil
	}

	// terminate the current running workflow
	// cannot use history cache to get current workflow since there can be deadlock
	domainID := msBuilder.GetExecutionInfo().DomainID
	workflowID := msBuilder.GetExecutionInfo().WorkflowID
	resp, err := r.shard.GetExecutionManager().GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	})
	if err != nil {
		logError(logger, "Conflict resolution error getting current workflow.", err)
		return "", err
	}
	currentRunID = resp.RunID
	currentCloseStatus := resp.CloseStatus

	if currentCloseStatus != persistence.WorkflowCloseStatusNone {
		// current workflow finished
		// note, it is impossible that a current workflow ends with continue as new as close status
		logger.Info("Conflict resolution current workflow finished.")
		return currentRunID, nil
	}

	// need to terminate the current workflow
	// same workflow ID, same shard
	err = r.terminateWorkflow(ctx, domainID, workflowID, currentRunID, incomingVersion, incomingTimestamp, logger)
	if err != nil {
		logError(logger, "Conflict resolution err terminating current workflow.", err)
	}
	return currentRunID, err
}

func getWorkflowStartedEvent(historyMgr persistence.HistoryManager, logger bark.Logger, domainID, workflowID, runID string) (*shared.HistoryEvent, error) {
	response, err := historyMgr.GetWorkflowExecutionHistory(&persistence.GetWorkflowExecutionHistoryRequest{
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
		logger.WithFields(bark.Fields{
			logging.TagErr: err,
		}).Error("Conflict resolution current workflow finished.", err)
		return nil, err
	}
	if len(response.History.Events) == 0 {
		logger.WithFields(bark.Fields{
			logging.TagWorkflowExecutionID: workflowID,
			logging.TagWorkflowRunID:       runID,
		})
		logError(logger, errNoHistoryFound.Error(), errNoHistoryFound)
		return nil, errNoHistoryFound
	}

	return response.History.Events[0], nil
}

func (r *historyReplicator) Serialize(history *shared.History) (*persistence.SerializedHistoryEventBatch, error) {
	eventBatch := persistence.NewHistoryEventBatch(persistence.GetDefaultHistoryVersion(), history.Events)
	h, err := r.historySerializer.Serialize(eventBatch)
	if err != nil {
		return nil, err
	}
	return h, nil
}

// func (r *historyReplicator) getCurrentWorkflowInfo(domainID string, workflowID string) (runID string, lastWriteVersion int64, closeStatus int, retError error) {
func (r *historyReplicator) getCurrentWorkflowMutableState(ctx context.Context, domainID string,
	workflowID string) (*workflowExecutionContext, mutableState, releaseWorkflowExecutionFunc, error) {
	// we need to check the current workflow execution
	context, release, err := r.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx,
		domainID,
		// only use the workflow ID, to get the current running one
		shared.WorkflowExecution{WorkflowId: common.StringPtr(workflowID)},
	)
	if err != nil {
		return nil, nil, nil, err
	}

	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		// no matter what error happen, we need to retry
		release(err)
		return nil, nil, nil, err
	}
	return context, msBuilder, release, nil
}

func (r *historyReplicator) terminateWorkflow(ctx context.Context, domainID string, workflowID string,
	runID string, incomingVersion int64, incomingTimestamp int64, logger bark.Logger) (retError error) {

	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}
	context, release, err := r.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, execution)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		return err
	}
	if !msBuilder.IsWorkflowExecutionRunning() {
		return nil
	}

	nextEventID := msBuilder.GetNextEventID()
	sourceCluster := r.clusterMetadata.ClusterNameForFailoverVersion(incomingVersion)
	terminationEvent := &shared.HistoryEvent{
		EventId:   common.Int64Ptr(nextEventID),
		Timestamp: common.Int64Ptr(incomingTimestamp),
		Version:   common.Int64Ptr(incomingVersion),
		EventType: shared.EventTypeWorkflowExecutionTerminated.Ptr(),
		WorkflowExecutionTerminatedEventAttributes: &shared.WorkflowExecutionTerminatedEventAttributes{
			Reason:   common.StringPtr(workflowTerminationReason),
			Identity: common.StringPtr(workflowTerminationIdentity),
			Details:  nil,
		},
	}
	history := &shared.History{Events: []*shared.HistoryEvent{terminationEvent}}

	req := &h.ReplicateEventsRequest{
		SourceCluster:     common.StringPtr(sourceCluster),
		DomainUUID:        common.StringPtr(domainID),
		WorkflowExecution: &execution,
		FirstEventId:      common.Int64Ptr(nextEventID),
		NextEventId:       common.Int64Ptr(nextEventID + 1),
		Version:           common.Int64Ptr(incomingVersion),
		History:           history,
		NewRunHistory:     nil,
	}
	return r.ApplyReplicationTask(ctx, context, msBuilder, req, logger)
}

func (r *historyReplicator) getLatestCheckpoint(replicationInfoRemote map[string]*h.ReplicationInfo,
	replicationInfoLocal map[string]*persistence.ReplicationInfo) (int64, int64) {

	// this only applies to 2 data center case

	lastValidVersion := common.EmptyVersion
	lastValidEventID := common.EmptyEventID

	for _, ri := range replicationInfoRemote {
		if lastValidVersion == common.EmptyVersion || ri.GetVersion() > lastValidVersion {
			lastValidVersion = ri.GetVersion()
			lastValidEventID = ri.GetLastEventId()
		}
	}

	for _, ri := range replicationInfoLocal {
		if lastValidVersion == common.EmptyVersion || ri.Version > lastValidVersion {
			lastValidVersion = ri.Version
			lastValidEventID = ri.LastEventID
		}
	}

	return lastValidVersion, lastValidEventID
}

func (r *historyReplicator) resetMutableState(ctx context.Context, context *workflowExecutionContext,
	msBuilder mutableState, lastEventID int64, incomingVersion int64, incomingTimestamp int64, logger bark.Logger) (mutableState, error) {

	r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.HistoryConflictsCounter)

	// handling edge case when resetting a workflow, and this workflow has done continue as new
	// we need to terminate the continue as new-ed workflow
	currentRunID, err := r.conflictResolutionTerminateCurrentRunningIfNotSelf(ctx, msBuilder, incomingVersion, incomingTimestamp, logger)
	if err != nil {
		return nil, err
	}

	resolver := r.getNewConflictResolver(context, logger)
	msBuilder, err = resolver.reset(currentRunID, uuid.New(), lastEventID, msBuilder.GetExecutionInfo().StartTimestamp)
	logger.Info("Completed Resetting of workflow execution.")
	if err != nil {
		return nil, err
	}
	return msBuilder, nil
}

func (r *historyReplicator) updateBufferedReplicationTask(context *workflowExecutionContext, msBuilder mutableState) error {
	// Generate a transaction ID for appending events to history
	transactionID, err := r.shard.GetNextTransferTaskID()
	if err != nil {
		return err
	}
	// we need to handcraft some of the variables
	// since this is a persisting the buffer replication task,
	// so nothing on the replication state should be changed
	lastWriteVersion := msBuilder.GetLastWriteVersion()
	sourceCluster := r.clusterMetadata.ClusterNameForFailoverVersion(lastWriteVersion)
	return context.updateHelper(nil, nil, transactionID, time.Time{}, false, nil, sourceCluster)
}

func (r *historyReplicator) notify(clusterName string, now time.Time, transferTasks []persistence.Task,
	timerTasks []persistence.Task) {
	now = now.Add(-r.shard.GetConfig().StandbyClusterDelay())
	r.shard.SetCurrentTime(clusterName, now)
	r.historyEngine.txProcessor.NotifyNewTask(clusterName, transferTasks)
	r.historyEngine.timerProcessor.NotifyNewTimers(clusterName, now, timerTasks)
}

func logError(logger bark.Logger, msg string, err error) {
	logger.WithFields(bark.Fields{
		logging.TagErr: err,
	}).Error(msg)
}
