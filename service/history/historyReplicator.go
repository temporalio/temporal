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
	"fmt"
	"time"

	"github.com/pborman/uuid"
	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/shared"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

var (
	errNoHistoryFound = errors.NewInternalFailureError("no history events found")

	workflowTerminationReason   = "Terminate Workflow Due To Version Conflict."
	workflowTerminationIdentity = "worker-service"
)

type (
	conflictResolverProvider func(context workflowExecutionContext, logger log.Logger) conflictResolver
	stateBuilderProvider     func(msBuilder mutableState, logger log.Logger) stateBuilder
	mutableStateProvider     func(version int64, logger log.Logger) mutableState

	historyReplicator struct {
		shard             ShardContext
		historyEngine     *historyEngineImpl
		historyCache      *historyCache
		domainCache       cache.DomainCache
		historySerializer persistence.PayloadSerializer
		historyMgr        persistence.HistoryManager
		clusterMetadata   cluster.Metadata
		metricsClient     metrics.Client
		logger            log.Logger
		resetor           workflowResetor

		getNewConflictResolver conflictResolverProvider
		getNewStateBuilder     stateBuilderProvider
		getNewMutableState     mutableStateProvider
	}
)

var (
	// ErrRetryEntityNotExists is returned to indicate workflow execution is not created yet and replicator should
	// try this task again after a small delay.
	ErrRetryEntityNotExists = &shared.RetryTaskError{Message: "entity not exists"}
	// ErrRetrySyncActivityMsg is returned when sync activity replication tasks are arriving out of order, should retry
	ErrRetrySyncActivityMsg = "retry on applying sync activity"
	// ErrRetryBufferEventsMsg is returned when events are arriving out of order, should retry, or specify force apply
	ErrRetryBufferEventsMsg = "retry on applying buffer events"
	// ErrWorkflowNotFoundMsg is returned when workflow not found
	ErrWorkflowNotFoundMsg = "retry on workflow not found"
	// ErrRetryExistingWorkflowMsg is returned when events are arriving out of order, and there is another workflow with same version running
	ErrRetryExistingWorkflowMsg = "workflow with same version is running"
	// ErrRetryExecutionAlreadyStarted is returned to indicate another workflow execution already started,
	// this error can be return if we encounter race condition, i.e. terminating the target workflow while
	// the target workflow has done continue as new.
	// try this task again after a small delay.
	ErrRetryExecutionAlreadyStarted = &shared.RetryTaskError{Message: "another workflow execution is running"}
	// ErrCorruptedReplicationInfo is returned when replication task has corrupted replication information from source cluster
	ErrCorruptedReplicationInfo = &shared.BadRequestError{Message: "replication task is has corrupted cluster replication info"}
	// ErrCorruptedMutableStateDecision is returned when mutable state decision is corrupted
	ErrCorruptedMutableStateDecision = &shared.BadRequestError{Message: "mutable state decision is corrupted"}
	// ErrMoreThan2DC is returned when there are more than 2 data center
	ErrMoreThan2DC = &shared.BadRequestError{Message: "more than 2 data center"}
	// ErrImpossibleLocalRemoteMissingReplicationInfo is returned when replication task is missing replication info, as well as local replication info being empty
	ErrImpossibleLocalRemoteMissingReplicationInfo = &shared.BadRequestError{Message: "local and remote both are missing replication info"}
	// ErrImpossibleRemoteClaimSeenHigherVersion is returned when replication info contains higher version then this cluster ever emitted.
	ErrImpossibleRemoteClaimSeenHigherVersion = &shared.BadRequestError{Message: "replication info contains higher version then this cluster ever emitted"}
	// ErrInternalFailure is returned when encounter code bug
	ErrInternalFailure = &shared.BadRequestError{Message: "fail to apply history events due bug"}
	// ErrEmptyHistoryRawEventBatch indicate that one single batch of history raw events is of size 0
	ErrEmptyHistoryRawEventBatch = &shared.BadRequestError{Message: "encounter empty history batch"}
	// ErrUnknownEncodingType indicate that the encoding type is unknown
	ErrUnknownEncodingType = &shared.BadRequestError{Message: "unknown encoding type"}
)

func newHistoryReplicator(shard ShardContext, historyEngine *historyEngineImpl, historyCache *historyCache, domainCache cache.DomainCache,
	historyMgr persistence.HistoryManager, historyV2Mgr persistence.HistoryV2Manager, logger log.Logger) *historyReplicator {
	replicator := &historyReplicator{
		shard:             shard,
		historyEngine:     historyEngine,
		historyCache:      historyCache,
		domainCache:       domainCache,
		historySerializer: persistence.NewPayloadSerializer(),
		historyMgr:        historyMgr,
		clusterMetadata:   shard.GetService().GetClusterMetadata(),
		metricsClient:     shard.GetMetricsClient(),
		logger:            logger.WithTags(tag.ComponentHistoryReplicator),

		getNewConflictResolver: func(context workflowExecutionContext, logger log.Logger) conflictResolver {
			return newConflictResolver(shard, context, historyMgr, historyV2Mgr, logger)
		},
		getNewStateBuilder: func(msBuilder mutableState, logger log.Logger) stateBuilder {
			return newStateBuilder(shard, msBuilder, logger)
		},
		getNewMutableState: func(version int64, logger log.Logger) mutableState {
			return newMutableStateBuilderWithReplicationState(
				shard.GetService().GetClusterMetadata().GetCurrentClusterName(),
				shard,
				shard.GetEventsCache(),
				logger,
				version,
			)
		},
	}
	replicator.resetor = newWorkflowResetor(historyEngine, replicator)

	return replicator
}

func (r *historyReplicator) SyncActivity(ctx ctx.Context, request *h.SyncActivityRequest) (retError error) {

	// sync activity info will only be sent from active side, when
	// 1. activity has retry policy and activity got started
	// 2. activity heart beat
	// no sync activity task will be sent when active side fail / timeout activity,
	// since standby side does not have activity retry timer

	domainID := request.GetDomainId()
	execution := workflow.WorkflowExecution{
		WorkflowId: request.WorkflowId,
		RunId:      request.RunId,
	}

	context, release, err := r.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, execution)
	if err != nil {
		// for get workflow execution context, with valid run id
		// err will not be of type EntityNotExistsError
		return err
	}
	defer func() { release(retError) }()

	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			return err
		}

		// this can happen if the workflow start event and this sync activity task are out of order
		// or the target workflow is long gone
		// the safe solution to this is to throw away the sync activity task
		// or otherwise, worker attempt will exceeds limit and put this message to DLQ
		return nil
	}

	if !msBuilder.IsWorkflowExecutionRunning() {
		// perhaps conflict resolution force termination
		return nil
	}

	version := request.GetVersion()
	scheduleID := request.GetScheduledId()
	if scheduleID >= msBuilder.GetNextEventID() {
		if version < msBuilder.GetLastWriteVersion() {
			// activity version < workflow last write version
			// this can happen if target workflow has
			return nil
		}

		// version >= last write version
		// this can happen if out of order delivery heppens
		return newRetryTaskErrorWithHint(ErrRetrySyncActivityMsg, domainID, execution.GetWorkflowId(), execution.GetRunId(), msBuilder.GetNextEventID())
	}

	ai, isRunning := msBuilder.GetActivityInfo(scheduleID)
	if !isRunning {
		// this should not retry, can be caused by out of order delivery
		// since the activity is already finished
		return nil
	}

	if ai.Version > request.GetVersion() {
		// this should not retry, can be caused by failover or reset
		return nil
	}

	if ai.Version == request.GetVersion() {
		if ai.Attempt > request.GetAttempt() {
			// this should not retry, can be caused by failover or reset
			return nil
		}
		if ai.Attempt == request.GetAttempt() {
			lastHeartbeatTime := time.Unix(0, request.GetLastHeartbeatTime())
			if ai.LastHeartBeatUpdatedTime.After(lastHeartbeatTime) {
				// this should not retry, can be caused by out of order delivery
				return nil
			}
			// version equal & attempt equal & last heartbeat after existing heartbeat
			// should update activity
		}
		// version equal & attempt larger then existing, should update activity
	}
	// version larger then existing, should update activity

	// calculate whether to reset the activity timer task status bits
	// reset timer task status bits if
	// 1. same source cluster & attempt changes
	// 2. different source cluster
	resetActivityTimerTaskStatus := false
	if !r.clusterMetadata.IsVersionFromSameCluster(request.GetVersion(), ai.Version) {
		resetActivityTimerTaskStatus = true
	} else if ai.Attempt < request.GetAttempt() {
		resetActivityTimerTaskStatus = true
	}
	err = msBuilder.ReplicateActivityInfo(request, resetActivityTimerTaskStatus)
	if err != nil {
		return err
	}

	// see whether we need to refresh the activity timer
	eventTime := request.GetScheduledTime()
	if eventTime < request.GetStartedTime() {
		eventTime = request.GetStartedTime()
	}
	if eventTime < request.GetLastHeartbeatTime() {
		eventTime = request.GetLastHeartbeatTime()
	}
	now := time.Unix(0, eventTime)
	timerTasks := []persistence.Task{}
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)
	timerBuilder := newTimerBuilder(r.shard.GetConfig(), r.logger, timeSource)
	if tt := timerBuilder.GetActivityTimerTaskIfNeeded(msBuilder); tt != nil {
		timerTasks = append(timerTasks, tt)
	}

	return r.updateMutableStateWithTimer(context, msBuilder, now, timerTasks)
}

func (r *historyReplicator) ApplyRawEvents(ctx ctx.Context, requestIn *h.ReplicateRawEventsRequest) (retError error) {
	var err error
	var events []*workflow.HistoryEvent
	var newRunEvents []*workflow.HistoryEvent

	events, err = r.deserializeBlob(requestIn.History)
	if err != nil {
		return err
	}

	version := events[0].GetVersion()
	firstEventID := events[0].GetEventId()
	nextEventID := events[len(events)-1].GetEventId() + 1
	sourceCluster := r.clusterMetadata.ClusterNameForFailoverVersion(version)

	requestOut := &h.ReplicateEventsRequest{
		SourceCluster:           common.StringPtr(sourceCluster),
		DomainUUID:              requestIn.DomainUUID,
		WorkflowExecution:       requestIn.WorkflowExecution,
		FirstEventId:            common.Int64Ptr(firstEventID),
		NextEventId:             common.Int64Ptr(nextEventID),
		Version:                 common.Int64Ptr(version),
		ReplicationInfo:         requestIn.ReplicationInfo,
		History:                 &shared.History{Events: events},
		EventStoreVersion:       requestIn.EventStoreVersion,
		NewRunHistory:           nil,
		NewRunEventStoreVersion: nil,
	}

	if requestIn.NewRunHistory != nil {
		newRunEvents, err = r.deserializeBlob(requestIn.NewRunHistory)
		if err != nil {
			return err
		}
		requestOut.NewRunHistory = &shared.History{Events: newRunEvents}
		requestOut.NewRunEventStoreVersion = requestIn.NewRunEventStoreVersion
	}

	return r.ApplyEvents(ctx, requestOut)
}

func (r *historyReplicator) ApplyEvents(ctx ctx.Context, request *h.ReplicateEventsRequest) (retError error) {
	logger := r.logger.WithTags(
		tag.WorkflowID(request.WorkflowExecution.GetWorkflowId()),
		tag.WorkflowRunID(request.WorkflowExecution.GetRunId()),
		tag.SourceCluster(request.GetSourceCluster()),
		tag.IncomingVersion(request.GetVersion()),
		tag.FirstEventVersion(request.GetFirstEventId()),
		tag.WorkflowNextEventID(request.GetNextEventId()))

	r.metricsClient.RecordTimer(
		metrics.ReplicateHistoryEventsScope,
		metrics.ReplicationEventsSizeTimer,
		time.Duration(len(request.History.Events)),
	)

	defer func() {
		if retError != nil {
			switch retError.(type) {
			case *shared.EntityNotExistsError:
				logger.Debug(fmt.Sprintf("Encounter EntityNotExistsError: %v", retError))
				retError = ErrRetryEntityNotExists
			case *shared.WorkflowExecutionAlreadyStartedError:
				logger.Debug(fmt.Sprintf("Encounter WorkflowExecutionAlreadyStartedError: %v", retError))
				retError = ErrRetryExecutionAlreadyStarted
			case *persistence.WorkflowExecutionAlreadyStartedError:
				logger.Debug(fmt.Sprintf("Encounter WorkflowExecutionAlreadyStartedError: %v", retError))
				retError = ErrRetryExecutionAlreadyStarted
			case *errors.InternalFailureError:
				logError(logger, "Encounter InternalFailure.", retError)
				retError = ErrInternalFailure
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
			logger.Debug(fmt.Sprintf("Dropping stale replication task for start event."))
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
				request.WorkflowExecution.GetRunId(), request, logger)
		}

		logger.WithTags(tag.CurrentVersion(msBuilder.GetReplicationState().LastWriteVersion))
		msBuilder, err = r.ApplyOtherEventsVersionChecking(ctx, context, msBuilder, request, logger)
		if err != nil || msBuilder == nil {
			return err
		}
		return r.ApplyOtherEvents(ctx, context, msBuilder, request, logger)
	}
}

func (r *historyReplicator) ApplyStartEvent(ctx ctx.Context, context workflowExecutionContext,
	request *h.ReplicateEventsRequest,
	logger log.Logger) error {
	msBuilder := r.getNewMutableState(request.GetVersion(), logger)
	err := r.ApplyReplicationTask(ctx, context, msBuilder, request, logger)
	return err
}

func (r *historyReplicator) ApplyOtherEventsMissingMutableState(ctx ctx.Context, domainID string, workflowID string,
	runID string, request *h.ReplicateEventsRequest, logger log.Logger) (retError error) {

	// size check already done
	lastEvent := request.History.Events[len(request.History.Events)-1]

	// we need to check the current workflow execution
	_, currentMutableState, currentRelease, err := r.getCurrentWorkflowMutableState(ctx, domainID, workflowID)
	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			return err
		}
		return newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, domainID, workflowID, runID, common.FirstEventID)
	}
	currentRunID := currentMutableState.GetExecutionInfo().RunID
	currentLastEventTaskID := currentMutableState.GetExecutionInfo().LastEventTaskID
	currentNextEventID := currentMutableState.GetNextEventID()
	currentLastWriteVersion := currentMutableState.GetLastWriteVersion()
	currentStillRunning := currentMutableState.IsWorkflowExecutionRunning()
	currentRelease(nil)

	if currentLastWriteVersion > lastEvent.GetVersion() {
		logger.Info("Dropping replication task.")
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
		return nil
	}
	if currentLastWriteVersion < lastEvent.GetVersion() {
		if currentStillRunning {
			err = r.terminateWorkflow(ctx, domainID, workflowID, currentRunID, lastEvent.GetVersion(), lastEvent.GetTimestamp(), logger)
			if err != nil {
				if _, ok := err.(*shared.EntityNotExistsError); !ok {
					return err
				}
				// if workflow is completed just when the call is made, will get EntityNotExistsError
				// we are not sure whether the workflow to be terminated ends with continue as new or not
				// so when encounter EntityNotExistsError, just continue to execute, if err occurs,
				// there will be retry on the worker level
			}
		}
		if request.GetResetWorkflow() {
			return r.resetor.ApplyResetEvent(ctx, request, domainID, workflowID, currentRunID)
		}
		return newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, domainID, workflowID, runID, common.FirstEventID)
	}

	// currentLastWriteVersion == incomingVersion
	if currentStillRunning {
		if lastEvent.GetTaskId() < currentLastEventTaskID {
			return nil
		}
		return newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, domainID, workflowID, currentRunID, currentNextEventID)
	}

	if request.GetResetWorkflow() {
		//Note that at this point, current run is already closed and currentLastWriteVersion <= incomingVersion
		return r.resetor.ApplyResetEvent(ctx, request, domainID, workflowID, currentRunID)
	}
	return newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, domainID, workflowID, runID, common.FirstEventID)
}

func (r *historyReplicator) ApplyOtherEventsVersionChecking(ctx ctx.Context, context workflowExecutionContext,
	msBuilder mutableState, request *h.ReplicateEventsRequest, logger log.Logger) (mutableState, error) {
	var err error
	// check if to buffer / drop / conflict resolution
	incomingVersion := request.GetVersion()
	replicationInfo := request.ReplicationInfo
	rState := msBuilder.GetReplicationState()
	if rState.LastWriteVersion > incomingVersion {
		// Replication state is already on a higher version, we can drop this event
		logger.Info("Dropping stale replication task.")
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
		_, err = r.garbageCollectSignals(context, msBuilder, request.History.Events)
		return nil, err
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
	logger.WithTags(tag.PrevActiveCluster(previousActiveCluster),
		tag.ReplicationInfo(request.ReplicationInfo))
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

	err = r.flushEventsBuffer(context, msBuilder)
	if err != nil {
		return nil, err
	}

	if ri.GetLastEventId() < msBuilder.GetReplicationState().LastWriteEventID || msBuilder.HasBufferedEvents() {
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

func (r *historyReplicator) ApplyOtherEvents(ctx ctx.Context, context workflowExecutionContext,
	msBuilder mutableState, request *h.ReplicateEventsRequest, logger log.Logger) error {
	var err error
	firstEventID := request.GetFirstEventId()
	if firstEventID < msBuilder.GetNextEventID() {
		// duplicate replication task
		replicationState := msBuilder.GetReplicationState()
		logger.Debug(fmt.Sprintf("Dropping replication task.  State: {NextEvent: %v, Version: %v, LastWriteV: %v, LastWriteEvent: %v}",
			msBuilder.GetNextEventID(), replicationState.CurrentVersion, replicationState.LastWriteVersion, replicationState.LastWriteEventID))
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.DuplicateReplicationEventsCounter)
		return nil
	}
	if firstEventID > msBuilder.GetNextEventID() {

		if !msBuilder.IsWorkflowExecutionRunning() {
			logger.Warn("Workflow already terminated due to conflict resolution.")
			return nil
		}

		return newRetryTaskErrorWithHint(
			ErrRetryBufferEventsMsg,
			context.getDomainID(),
			context.getExecution().GetWorkflowId(),
			context.getExecution().GetRunId(),
			msBuilder.GetNextEventID(),
		)
	}

	// Apply the replication task
	err = r.ApplyReplicationTask(ctx, context, msBuilder, request, logger)
	if err != nil {
		logError(logger, "Fail to Apply Replication task.", err)
	}
	return err
}

func (r *historyReplicator) ApplyReplicationTask(ctx ctx.Context, context workflowExecutionContext,
	msBuilder mutableState, request *h.ReplicateEventsRequest, logger log.Logger) error {

	if !msBuilder.IsWorkflowExecutionRunning() {
		logger.Warn("Workflow already terminated due to conflict resolution.")
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
	var newRunHistory []*shared.HistoryEvent
	if request.NewRunHistory != nil {
		newRunHistory = request.NewRunHistory.Events
	}

	// directly use stateBuilder to apply events for other events(including continueAsNew)
	lastEvent, _, newRunStateBuilder, err := sBuilder.applyEvents(domainID, requestID, execution, request.History.Events, newRunHistory, request.GetEventStoreVersion(), request.GetNewRunEventStoreVersion())
	if err != nil {
		return err
	}

	// If replicated events has ContinueAsNew event, then append the new run history
	if newRunStateBuilder != nil {
		// Generate a transaction ID for appending events to history
		transactionID, err := r.shard.GetNextTransferTaskID()
		if err != nil {
			return err
		}
		// continueAsNew
		err = context.appendFirstBatchHistoryForContinueAsNew(newRunStateBuilder, transactionID)
		if err != nil {
			return err
		}
	}

	firstEvent := request.History.Events[0]
	switch firstEvent.GetEventType() {
	case shared.EventTypeWorkflowExecutionStarted:
		err = r.replicateWorkflowStarted(ctx, context, msBuilder, request.GetSourceCluster(), request.History, sBuilder,
			logger)
	default:
		now := time.Unix(0, lastEvent.GetTimestamp())
		err = context.replicateWorkflowExecution(request, sBuilder.getTransferTasks(), sBuilder.getTimerTasks(), lastEvent.GetEventId(), now)
	}

	if err == nil {
		now := time.Unix(0, lastEvent.GetTimestamp())
		r.notify(request.GetSourceCluster(), now, sBuilder.getTransferTasks(), sBuilder.getTimerTasks())
	}

	return err
}

func (r *historyReplicator) replicateWorkflowStarted(ctx ctx.Context, context workflowExecutionContext,
	msBuilder mutableState, sourceCluster string, history *shared.History, sBuilder stateBuilder, logger log.Logger) error {

	executionInfo := msBuilder.GetExecutionInfo()
	domainID := executionInfo.DomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr(executionInfo.WorkflowID),
		RunId:      common.StringPtr(executionInfo.RunID),
	}
	firstEvent := history.Events[0]
	incomingVersion := firstEvent.GetVersion()
	lastEvent := history.Events[len(history.Events)-1]
	executionInfo.SetLastFirstEventID(firstEvent.GetEventId())
	executionInfo.SetNextEventID(lastEvent.GetEventId() + 1)

	_, _, err := context.appendFirstBatchEventsForStandby(msBuilder, history.Events)
	if err != nil {
		return err
	}

	// workflow passive side logic should not generate any replication task
	createReplicationTask := false
	transferTasks := sBuilder.getTransferTasks()
	var replicationTasks []persistence.Task // passive side generates no replication tasks
	timerTasks := sBuilder.getTimerTasks()
	now := time.Unix(0, lastEvent.GetTimestamp())

	deleteHistory := func() {
		// this function should be only called when we drop start workflow execution
		if msBuilder.GetEventStoreVersion() == persistence.EventStoreVersionV2 {
			r.shard.GetHistoryV2Manager().DeleteHistoryBranch(&persistence.DeleteHistoryBranchRequest{
				BranchToken: msBuilder.GetCurrentBranch(),
				ShardID:     common.IntPtr(r.shard.GetShardID()),
			})
		} else {
			r.shard.GetHistoryManager().DeleteWorkflowExecutionHistory(&persistence.DeleteWorkflowExecutionHistoryRequest{
				DomainID:  domainID,
				Execution: execution,
			})
		}

	}

	// try to create the workflow execution
	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	err = context.createWorkflowExecution(
		msBuilder, sourceCluster, createReplicationTask, now, transferTasks, replicationTasks, timerTasks,
		createMode, prevRunID, prevLastWriteVersion,
	)
	if err == nil {
		return nil
	}
	if _, ok := err.(*persistence.WorkflowExecutionAlreadyStartedError); !ok {
		logger.Info("Create workflow failed after appending history events.", tag.Error(err))
		return err
	}

	// we have WorkflowExecutionAlreadyStartedError
	errExist := err.(*persistence.WorkflowExecutionAlreadyStartedError)
	currentRunID := errExist.RunID
	currentState := errExist.State
	currentLastWriteVersion := errExist.LastWriteVersion

	logger.WithTags(tag.CurrentVersion(currentLastWriteVersion))
	if currentRunID == execution.GetRunId() {
		logger.Info("Dropping stale start replication task.")
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.DuplicateReplicationEventsCounter)
		return nil
	}

	// current workflow is completed
	if currentState == persistence.WorkflowStateCompleted {
		// allow the application of workflow creation if currentLastWriteVersion > incomingVersion
		// because this can be caused by missing replication events
		// proceed to create workflow
		createMode = persistence.CreateWorkflowModeWorkflowIDReuse
		prevRunID = currentRunID
		prevLastWriteVersion = currentLastWriteVersion
		return context.createWorkflowExecution(
			msBuilder, sourceCluster, createReplicationTask, now, transferTasks, replicationTasks, timerTasks,
			createMode, prevRunID, prevLastWriteVersion,
		)
	}

	// current workflow is still running
	if currentLastWriteVersion > incomingVersion {
		logger.Info("Dropping stale start replication task.")
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
		deleteHistory()
		return nil
	}
	if currentLastWriteVersion == incomingVersion {
		_, currentMutableState, currentRelease, err := r.getCurrentWorkflowMutableState(ctx, domainID, execution.GetWorkflowId())
		if err != nil {
			return err
		}
		currentRunID := currentMutableState.GetExecutionInfo().RunID
		currentLastEventTaskID := currentMutableState.GetExecutionInfo().LastEventTaskID
		currentNextEventID := currentMutableState.GetNextEventID()
		currentRelease(nil)

		if executionInfo.LastEventTaskID < currentLastEventTaskID {
			return nil
		}
		return newRetryTaskErrorWithHint(ErrRetryExistingWorkflowMsg, domainID, execution.GetWorkflowId(), currentRunID, currentNextEventID)
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
	createMode = persistence.CreateWorkflowModeWorkflowIDReuse
	prevRunID = currentRunID
	prevLastWriteVersion = incomingVersion
	return context.createWorkflowExecution(
		msBuilder, sourceCluster, createReplicationTask, now, transferTasks, replicationTasks, timerTasks,
		createMode, prevRunID, prevLastWriteVersion,
	)
}

func (r *historyReplicator) conflictResolutionTerminateCurrentRunningIfNotSelf(ctx ctx.Context,
	msBuilder mutableState, incomingVersion int64, incomingTimestamp int64, logger log.Logger) (currentRunID string, retError error) {
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
	currentLastWriteVetsion := resp.LastWriteVersion

	// this handle the edge case where
	// local run 1 do continue as new -> run 2L
	// remote run 1 become active, and do continue as new -> run 2R
	// run 2R comes earlier, force terminate run 2L and replicate 2R
	// remote run 1's version trigger a conflict resolution trying to force terminate run 2R
	// conflict resolution should only force terminate workflow if that workflow has lower last write version
	if incomingVersion <= currentLastWriteVetsion {
		return "", nil
	}

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

// func (r *historyReplicator) getCurrentWorkflowInfo(domainID string, workflowID string) (runID string, lastWriteVersion int64, closeStatus int, retError error) {
func (r *historyReplicator) getCurrentWorkflowMutableState(ctx ctx.Context, domainID string,
	workflowID string) (workflowExecutionContext, mutableState, releaseWorkflowExecutionFunc, error) {
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

func (r *historyReplicator) terminateWorkflow(ctx ctx.Context, domainID string, workflowID string,
	runID string, incomingVersion int64, incomingTimestamp int64, logger log.Logger) (retError error) {

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
		// TaskId is default to 0 since this event is not generated by remote
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

func (r *historyReplicator) getLatestCheckpoint(replicationInfoRemote map[string]*workflow.ReplicationInfo,
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

func (r *historyReplicator) resetMutableState(ctx ctx.Context, context workflowExecutionContext,
	msBuilder mutableState, lastEventID int64, incomingVersion int64, incomingTimestamp int64, logger log.Logger) (mutableState, error) {

	r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.HistoryConflictsCounter)

	// handling edge case when resetting a workflow, and this workflow has done continue as new
	// we need to terminate the continue as new-ed workflow
	currentRunID, err := r.conflictResolutionTerminateCurrentRunningIfNotSelf(ctx, msBuilder, incomingVersion, incomingTimestamp, logger)
	if err != nil {
		return nil, err
	}

	// if cannot force terminate a workflow (meaning that workflow has last version >= incoming version)
	// just abandon force termination & conflict resolution
	// since the current workflow is after this one
	if currentRunID == "" {
		return nil, nil
	}

	resolver := r.getNewConflictResolver(context, logger)
	msBuilder, err = resolver.reset(currentRunID, uuid.New(), lastEventID, msBuilder.GetExecutionInfo())
	logger.Info("Completed Resetting of workflow execution.")
	if err != nil {
		return nil, err
	}
	return msBuilder, nil
}

func (r *historyReplicator) updateMutableStateOnly(context workflowExecutionContext, msBuilder mutableState) error {
	return r.updateMutableStateWithTimer(context, msBuilder, time.Time{}, nil)
}

func (r *historyReplicator) updateMutableStateWithTimer(context workflowExecutionContext, msBuilder mutableState, now time.Time, timerTasks []persistence.Task) error {
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
	return context.updateWorkflowExecutionForStandby(nil, timerTasks, transactionID, now, false, nil, sourceCluster)
}

func (r *historyReplicator) notify(clusterName string, now time.Time, transferTasks []persistence.Task,
	timerTasks []persistence.Task) {
	now = now.Add(-r.shard.GetConfig().StandbyClusterDelay())
	r.shard.SetCurrentTime(clusterName, now)
	r.historyEngine.txProcessor.NotifyNewTask(clusterName, transferTasks)
	r.historyEngine.timerProcessor.NotifyNewTimers(clusterName, now, timerTasks)
}

func (r *historyReplicator) deserializeBlob(blob *workflow.DataBlob) ([]*workflow.HistoryEvent, error) {

	if blob.GetEncodingType() != workflow.EncodingTypeThriftRW {
		return nil, ErrUnknownEncodingType
	}
	historyEvents, err := r.historySerializer.DeserializeBatchEvents(&persistence.DataBlob{
		Encoding: common.EncodingTypeThriftRW,
		Data:     blob.Data,
	})
	if err != nil {
		return nil, err
	}
	if len(historyEvents) == 0 {
		return nil, ErrEmptyHistoryRawEventBatch
	}
	return historyEvents, nil
}

func (r *historyReplicator) flushEventsBuffer(context workflowExecutionContext, msBuilder mutableState) error {

	if !msBuilder.IsWorkflowExecutionRunning() || !msBuilder.HasBufferedEvents() || !r.canModifyWorkflow(msBuilder) {
		return nil
	}

	di, ok := msBuilder.GetInFlightDecisionTask()
	if !ok {
		return ErrCorruptedMutableStateDecision
	}
	msBuilder.UpdateReplicationStateVersion(msBuilder.GetLastWriteVersion(), true)
	msBuilder.AddDecisionTaskFailedEvent(di.ScheduleID, di.StartedID,
		workflow.DecisionTaskFailedCauseFailoverCloseDecision, nil, identityHistoryService, "", "", "", 0)

	// there is no need to generate a new decision and corresponding decision timer task
	// here, the intent is to flush the buffered events

	transactionID, err := r.shard.GetNextTransferTaskID()
	if err != nil {
		return err
	}
	return context.updateWorkflowExecution(nil, nil, transactionID)
}

func (r *historyReplicator) garbageCollectSignals(context workflowExecutionContext,
	msBuilder mutableState, events []*workflow.HistoryEvent) (bool, error) {

	// this function modify the mutable state passed in applying stale signals
	// so the check of workflow still running and the ability to modify this workflow
	// is utterly necessary
	if !msBuilder.IsWorkflowExecutionRunning() || !r.canModifyWorkflow(msBuilder) {
		return false, nil
	}

	// we are garbage collecting signals already applied to mutable states,
	// so targeting child workflow only check is not necessary

	// TODO should we also include the request ID in the signal request in the event?
	updateMutableState := false
	msBuilder.UpdateReplicationStateVersion(msBuilder.GetLastWriteVersion(), true)
	for _, event := range events {
		switch event.GetEventType() {
		case workflow.EventTypeWorkflowExecutionSignaled:
			updateMutableState = true
			attr := event.WorkflowExecutionSignaledEventAttributes
			if msBuilder.AddWorkflowExecutionSignaled(attr.GetSignalName(), attr.Input, attr.GetIdentity()) == nil {
				return false, &workflow.InternalServiceError{Message: "Unable to signal workflow execution."}
			}
		}
	}

	if !updateMutableState {
		return false, nil
	}

	transactionID, err := r.shard.GetNextTransferTaskID()
	if err != nil {
		return false, err
	}
	return true, context.updateWorkflowExecution(nil, nil, transactionID)
}

func (r *historyReplicator) canModifyWorkflow(msBuilder mutableState) bool {
	lastWriteVersion := msBuilder.GetLastWriteVersion()
	return r.clusterMetadata.ClusterNameForFailoverVersion(lastWriteVersion) == r.clusterMetadata.GetCurrentClusterName()
}

func logError(logger log.Logger, msg string, err error) {
	logger.Error(msg, tag.Error(err))
}

func newRetryTaskErrorWithHint(msg string, domainID string, workflowID string, runID string, nextEventID int64) *shared.RetryTaskError {
	return &shared.RetryTaskError{
		Message:     msg,
		DomainId:    common.StringPtr(domainID),
		WorkflowId:  common.StringPtr(workflowID),
		RunId:       common.StringPtr(runID),
		NextEventId: common.Int64Ptr(nextEventID),
	}
}
