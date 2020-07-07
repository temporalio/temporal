// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/temporal-proto/common/v1"
	enumspb "go.temporal.io/temporal-proto/enums/v1"
	historypb "go.temporal.io/temporal-proto/history/v1"
	"go.temporal.io/temporal-proto/serviceerror"
	"go.temporal.io/temporal-proto/workflowservice/v1"

	enumsspb "github.com/temporalio/temporal/api/enums/v1"
	"github.com/temporalio/temporal/api/historyservice/v1"
	replicationspb "github.com/temporalio/temporal/api/replication/v1"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/payloads"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/persistence/serialization"
)

type (
	conflictResolverProvider func(context workflowExecutionContext, logger log.Logger) conflictResolver

	historyReplicator struct {
		shard             ShardContext
		timeSource        clock.TimeSource
		historyEngine     *historyEngineImpl
		historyCache      *historyCache
		namespaceCache    cache.NamespaceCache
		historySerializer persistence.PayloadSerializer
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
	ErrRetryEntityNotExists = serviceerror.NewRetryTask("entity not exists", "", "", "", common.EmptyEventID)
	// ErrRetryRaceCondition is returned to indicate logic race condition encountered and replicator should
	// try this task again after a small delay.
	ErrRetryRaceCondition = serviceerror.NewRetryTask("encounter race condition, retry", "", "", "", common.EmptyEventID)
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
	ErrRetryExecutionAlreadyStarted = serviceerror.NewRetryTask("another workflow execution is running", "", "", "", common.EmptyEventID)
	// ErrCorruptedReplicationInfo is returned when replication task has corrupted replication information from source cluster
	ErrCorruptedReplicationInfo = serviceerror.NewInvalidArgument("replication task is has corrupted cluster replication info")
	// ErrCorruptedMutableStateDecision is returned when mutable state decision is corrupted
	ErrCorruptedMutableStateDecision = serviceerror.NewInvalidArgument("mutable state decision is corrupted")
	// ErrMoreThan2DC is returned when there are more than 2 data center
	ErrMoreThan2DC = serviceerror.NewInvalidArgument("more than 2 data center")
	// ErrImpossibleLocalRemoteMissingReplicationInfo is returned when replication task is missing replication info, as well as local replication info being empty
	ErrImpossibleLocalRemoteMissingReplicationInfo = serviceerror.NewInvalidArgument("local and remote both are missing replication info")
	// ErrImpossibleRemoteClaimSeenHigherVersion is returned when replication info contains higher version then this cluster ever emitted.
	ErrImpossibleRemoteClaimSeenHigherVersion = serviceerror.NewInvalidArgument("replication info contains higher version then this cluster ever emitted")
	// ErrInternalFailure is returned when encounter code bug
	ErrInternalFailure = serviceerror.NewInvalidArgument("fail to apply history events due bug")
	// ErrEmptyHistoryRawEventBatch indicate that one single batch of history raw events is of size 0
	ErrEmptyHistoryRawEventBatch = serviceerror.NewInvalidArgument("encounter empty history batch")
	// ErrUnknownEncodingType indicate that the encoding type is unknown
	ErrUnknownEncodingType = serviceerror.NewInvalidArgument("unknown encoding type")
	// ErrUnreappliableEvent indicate that the event is not reappliable
	ErrUnreappliableEvent = serviceerror.NewInvalidArgument("event is not reappliable")
	// ErrWorkflowMutationDecision indicate that something is wrong with mutating workflow, i.e. adding decision to workflow
	ErrWorkflowMutationDecision = serviceerror.NewInvalidArgument("error encountered when mutating workflow adding decision")
	// ErrWorkflowMutationSignal indicate that something is wrong with mutating workflow, i.e. adding signal to workflow
	ErrWorkflowMutationSignal = serviceerror.NewInvalidArgument("error encountered when mutating workflow adding signal")
)

func newHistoryReplicator(
	shard ShardContext,
	timeSource clock.TimeSource,
	historyEngine *historyEngineImpl,
	historyCache *historyCache,
	namespaceCache cache.NamespaceCache,
	historyV2Mgr persistence.HistoryManager,
	logger log.Logger,
) *historyReplicator {

	replicator := &historyReplicator{
		shard:             shard,
		timeSource:        timeSource,
		historyEngine:     historyEngine,
		historyCache:      historyCache,
		namespaceCache:    namespaceCache,
		historySerializer: persistence.NewPayloadSerializer(),
		clusterMetadata:   shard.GetService().GetClusterMetadata(),
		metricsClient:     shard.GetMetricsClient(),
		logger:            logger.WithTags(tag.ComponentHistoryReplicator),

		getNewConflictResolver: func(context workflowExecutionContext, logger log.Logger) conflictResolver {
			return newConflictResolver(shard, context, historyV2Mgr, logger)
		},
		getNewStateBuilder: func(msBuilder mutableState, logger log.Logger) stateBuilder {
			return newStateBuilder(
				shard,
				logger,
				msBuilder,
				func(mutableState mutableState) mutableStateTaskGenerator {
					return newMutableStateTaskGenerator(shard.GetNamespaceCache(), logger, mutableState)
				},
			)
		},
		getNewMutableState: func(namespaceEntry *cache.NamespaceCacheEntry, logger log.Logger) mutableState {
			return newMutableStateBuilderWithReplicationState(
				shard,
				shard.GetEventsCache(),
				logger,
				namespaceEntry,
			)
		},
	}
	replicator.resetor = newWorkflowResetor(historyEngine)

	return replicator
}

func (r *historyReplicator) ApplyRawEvents(
	ctx context.Context,
	requestIn *historyservice.ReplicateRawEventsRequest,
) (retError error) {

	var err error
	var events []*historypb.HistoryEvent
	var newRunEvents []*historypb.HistoryEvent

	events, err = r.deserializeBlob(requestIn.History)
	if err != nil {
		return err
	}

	version := events[0].GetVersion()
	firstEventID := events[0].GetEventId()
	nextEventID := events[len(events)-1].GetEventId() + 1
	sourceCluster := r.clusterMetadata.ClusterNameForFailoverVersion(version)

	requestOut := &historyservice.ReplicateEventsRequest{
		SourceCluster:     sourceCluster,
		NamespaceId:       requestIn.GetNamespaceId(),
		WorkflowExecution: requestIn.WorkflowExecution,
		FirstEventId:      firstEventID,
		NextEventId:       nextEventID,
		Version:           version,
		ReplicationInfo:   requestIn.ReplicationInfo,
		History:           &historypb.History{Events: events},
		NewRunHistory:     nil,
	}

	if requestIn.NewRunHistory != nil {
		newRunEvents, err = r.deserializeBlob(requestIn.NewRunHistory)
		if err != nil {
			return err
		}
		requestOut.NewRunHistory = &historypb.History{Events: newRunEvents}
	}

	return r.ApplyEvents(ctx, requestOut)
}

func (r *historyReplicator) ApplyEvents(
	ctx context.Context,
	request *historyservice.ReplicateEventsRequest,
) (retError error) {

	logger := r.logger.WithTags(
		tag.WorkflowID(request.WorkflowExecution.GetWorkflowId()),
		tag.WorkflowRunID(request.WorkflowExecution.GetRunId()),
		tag.SourceCluster(request.GetSourceCluster()),
		tag.IncomingVersion(request.GetVersion()),
		tag.WorkflowFirstEventID(request.GetFirstEventId()),
		tag.WorkflowNextEventID(request.GetNextEventId()))

	r.metricsClient.RecordTimer(
		metrics.ReplicateHistoryEventsScope,
		metrics.ReplicationEventsSizeTimer,
		time.Duration(len(request.History.Events)),
	)

	defer func() {
		if retError != nil {
			switch retError.(type) {
			case *serviceerror.NotFound:
				logger.Debug("Encounter EntityNotExistsError", tag.Error(retError))
				retError = ErrRetryEntityNotExists
			case *serviceerror.WorkflowExecutionAlreadyStarted:
				logger.Debug("Encounter WorkflowExecutionAlreadyStartedError", tag.Error(retError))
				retError = ErrRetryExecutionAlreadyStarted
			case *persistence.WorkflowExecutionAlreadyStartedError:
				logger.Debug("Encounter WorkflowExecutionAlreadyStartedError", tag.Error(retError))
				retError = ErrRetryExecutionAlreadyStarted
			case *serviceerror.Internal:
				logError(logger, "Encounter Internal.", retError)
				retError = ErrInternalFailure
			}
		}
	}()

	if request == nil || request.History == nil || len(request.History.Events) == 0 {
		logger.Warn("Dropping empty replication task")
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.EmptyReplicationEventsCounter)
		return nil
	}
	namespaceID, err := validateNamespaceUUID(request.GetNamespaceId())
	if err != nil {
		return err
	}

	execution := *request.WorkflowExecution
	weContext, release, err := r.historyCache.getOrCreateWorkflowExecution(ctx, namespaceID, execution)
	if err != nil {
		// for get workflow execution context, with valid run id
		// err will not be of type EntityNotExistsError
		return err
	}
	defer func() { release(retError) }()

	firstEvent := request.History.Events[0]
	switch firstEvent.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
		_, err := weContext.loadWorkflowExecution()
		if err == nil {
			// Workflow execution already exist, looks like a duplicate start event, it is safe to ignore it
			logger.Debug("Dropping stale replication task for start event.")
			r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.DuplicateReplicationEventsCounter)
			return nil
		}
		if _, ok := err.(*serviceerror.NotFound); !ok {
			// GetWorkflowExecution failed with some transient error. Return err so we can retry the task later
			return err
		}
		return r.ApplyStartEvent(ctx, weContext, request, logger)

	default:
		// apply events, other than simple start workflow execution
		// the continue as new + start workflow execution combination will also be processed here
		var mutableState mutableState
		var err error
		namespaceEntry, err := r.namespaceCache.GetNamespaceByID(weContext.getNamespaceID())
		if err != nil {
			return err
		}

		if r.shard.GetConfig().ReplicationEventsFromCurrentCluster(namespaceEntry.GetInfo().Name) {
			// this branch is used when replicating events (generated from current cluster)from remote cluster to current cluster.
			// this could happen when the events are lost in current cluster and plan to recover them from remote cluster.
			// if the incoming version equals last write version, skip to fail in-flight decision.
			mutableState, err = weContext.loadWorkflowExecutionForReplication(request.GetVersion())
		} else {
			mutableState, err = weContext.loadWorkflowExecution()
		}

		if err != nil {
			if _, ok := err.(*serviceerror.NotFound); !ok {
				return err
			}
			// mutable state for the target workflow ID & run ID combination does not exist
			// we need to check the existing workflow ID
			release(err)
			return r.ApplyOtherEventsMissingMutableState(ctx, namespaceID, request.WorkflowExecution.GetWorkflowId(),
				request.WorkflowExecution.GetRunId(), request, logger)
		}

		// Sanity check to make only 2DC mutable state here
		if mutableState.GetReplicationState() == nil {
			return serviceerror.NewInternal("The mutable state does not support 2DC.")
		}

		logger.WithTags(tag.CurrentVersion(mutableState.GetReplicationState().LastWriteVersion))
		mutableState, err = r.ApplyOtherEventsVersionChecking(ctx, weContext, mutableState, request, logger)
		if err != nil || mutableState == nil {
			return err
		}
		return r.ApplyOtherEvents(ctx, weContext, mutableState, request, logger)
	}
}

func (r *historyReplicator) ApplyStartEvent(
	ctx context.Context,
	context workflowExecutionContext,
	request *historyservice.ReplicateEventsRequest,
	logger log.Logger,
) error {

	namespaceEntry, err := r.namespaceCache.GetNamespaceByID(context.getNamespaceID())
	if err != nil {
		return err
	}
	msBuilder := r.getNewMutableState(namespaceEntry, logger)
	return r.ApplyReplicationTask(ctx, context, msBuilder, request, logger)
}

func (r *historyReplicator) ApplyOtherEventsMissingMutableState(
	ctx context.Context,
	namespaceID string,
	workflowID string,
	runID string,
	request *historyservice.ReplicateEventsRequest,
	logger log.Logger,
) (retError error) {

	// size check already done
	lastEvent := request.History.Events[len(request.History.Events)-1]

	// we need to check the current workflow execution
	currentContext, currentMutableState, currentRelease, err := r.getCurrentWorkflowMutableState(ctx, namespaceID, workflowID)
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}
		return newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, namespaceID, workflowID, runID, common.FirstEventID)
	}
	defer func() { currentRelease(retError) }()

	currentRunID := currentMutableState.GetExecutionInfo().RunID
	currentLastEventTaskID := currentMutableState.GetExecutionInfo().LastEventTaskID
	currentNextEventID := currentMutableState.GetNextEventID()
	currentLastWriteVersion, err := currentMutableState.GetLastWriteVersion()
	if err != nil {
		return err
	}
	currentStillRunning := currentMutableState.IsWorkflowExecutionRunning()

	if currentLastWriteVersion > lastEvent.GetVersion() {
		logger.Info("Dropping replication task.")
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
		return r.reapplyEvents(ctx, currentContext, currentMutableState, request.History.Events, logger)
	}

	// release for better lock management
	currentRelease(nil)

	if currentLastWriteVersion < lastEvent.GetVersion() {
		if currentStillRunning {
			_, err = r.terminateWorkflow(ctx, namespaceID, workflowID, currentRunID, lastEvent.GetVersion(), logger)
			if err != nil {
				return err
			}

		}
		if request.GetResetWorkflow() {
			return r.resetor.ApplyResetEvent(ctx, request, namespaceID, workflowID, currentRunID)
		}
		return newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, namespaceID, workflowID, runID, common.FirstEventID)
	}

	// currentLastWriteVersion == incomingVersion
	if currentStillRunning {
		if lastEvent.GetTaskId() < currentLastEventTaskID {
			// versions are the same, so not necessary to re-apply signals
			return nil
		}
		return newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, namespaceID, workflowID, currentRunID, currentNextEventID)
	}

	if request.GetResetWorkflow() {
		//Note that at this point, current run is already closed and currentLastWriteVersion <= incomingVersion
		return r.resetor.ApplyResetEvent(ctx, request, namespaceID, workflowID, currentRunID)
	}
	return newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, namespaceID, workflowID, runID, common.FirstEventID)
}

func (r *historyReplicator) ApplyOtherEventsVersionChecking(
	ctx context.Context,
	context workflowExecutionContext,
	msBuilder mutableState,
	request *historyservice.ReplicateEventsRequest,
	logger log.Logger,
) (mutableState, error) {
	var err error
	// check if to buffer / drop / conflict resolution
	incomingVersion := request.GetVersion()
	replicationInfo := request.ReplicationInfo
	rState := msBuilder.GetReplicationState()
	if rState.LastWriteVersion > incomingVersion {
		// Replication state is already on a higher version, we can drop this event
		logger.Info("Dropping stale replication task.")
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)

		events := request.History.Events
		// this workflow running, try re-apply events to it
		// NOTE: if a workflow is running, then it must be the current workflow
		if msBuilder.IsWorkflowExecutionRunning() {
			err = r.reapplyEvents(ctx, context, msBuilder, events, logger)
			return nil, err
		}

		// must get the current run ID first
		// if trying to getCurrentWorkflowRunID function (which use mutable state cache)
		// there can be deadlock if current workflow is this workflow
		currentRunID, err := r.getCurrentWorkflowRunID(context.getNamespaceID(), context.getExecution().GetWorkflowId())
		if currentRunID == context.getExecution().GetRunId() {
			err = r.reapplyEvents(ctx, context, msBuilder, events, logger)
			return nil, err
		}
		currentContext, currentMutableState, currentRelease, err := r.getCurrentWorkflowMutableState(
			ctx, context.getNamespaceID(), context.getExecution().GetWorkflowId(),
		)
		if err != nil {
			return nil, err
		}
		defer func() { currentRelease(err) }()
		err = r.reapplyEvents(ctx, currentContext, currentMutableState, events, logger)
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
		// Returning InvalidArgument to force the message to land into DLQ
		return nil, ErrCorruptedReplicationInfo
	}

	err = r.flushEventsBuffer(context, msBuilder)
	if err != nil {
		return nil, err
	}

	if ri.GetLastEventId() < msBuilder.GetReplicationState().LastWriteEventID {
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

func (r *historyReplicator) ApplyOtherEvents(
	ctx context.Context,
	context workflowExecutionContext,
	msBuilder mutableState,
	request *historyservice.ReplicateEventsRequest,
	logger log.Logger,
) error {
	var err error
	firstEventID := request.GetFirstEventId()
	if firstEventID < msBuilder.GetNextEventID() {
		// duplicate replication task
		replicationState := msBuilder.GetReplicationState()
		logger.Debug("Dropping replication task", tag.WorkflowNextEventID(msBuilder.GetNextEventID()), tag.ReplicationState(replicationState))
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.DuplicateReplicationEventsCounter)
		return nil
	}
	if firstEventID > msBuilder.GetNextEventID() {

		if !msBuilder.IsWorkflowExecutionRunning() {
			logger.Warn("Workflow already terminated due to conflict resolution")
			return nil
		}

		return newRetryTaskErrorWithHint(
			ErrRetryBufferEventsMsg,
			context.getNamespaceID(),
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

func (r *historyReplicator) ApplyReplicationTask(
	ctx context.Context,
	context workflowExecutionContext,
	msBuilder mutableState,
	request *historyservice.ReplicateEventsRequest,
	logger log.Logger,
) error {

	if !msBuilder.IsWorkflowExecutionRunning() {
		logger.Warn("Workflow already terminated due to conflict resolution.")
		return nil
	}

	namespaceID, err := validateNamespaceUUID(request.GetNamespaceId())
	if err != nil {
		return err
	}
	if len(request.History.Events) == 0 {
		return nil
	}
	lastEvent := request.History.Events[len(request.History.Events)-1]

	execution := *request.WorkflowExecution

	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	sBuilder := r.getNewStateBuilder(msBuilder, logger)
	var newRunHistory []*historypb.HistoryEvent
	if request.NewRunHistory != nil {
		newRunHistory = request.NewRunHistory.Events
	}

	// directly use stateBuilder to apply events for other events(including continueAsNew)
	newMutableState, err := sBuilder.applyEvents(
		namespaceID, requestID, execution, request.History.Events, newRunHistory, request.GetNewRunNdc(),
	)
	if err != nil {
		return err
	}

	firstEvent := request.History.Events[0]
	switch firstEvent.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
		err = r.replicateWorkflowStarted(ctx, context, msBuilder, request.History, sBuilder, logger)
	default:
		now := time.Unix(0, lastEvent.GetTimestamp())
		var newContext workflowExecutionContext
		if newMutableState != nil {
			newExecutionInfo := newMutableState.GetExecutionInfo()
			newContext = newWorkflowExecutionContext(
				newExecutionInfo.NamespaceID,
				commonpb.WorkflowExecution{
					WorkflowId: newExecutionInfo.WorkflowID,
					RunId:      newExecutionInfo.RunID,
				},
				r.shard,
				r.shard.GetExecutionManager(),
				r.logger,
			)
		}
		err = context.updateWorkflowExecutionWithNewAsPassive(now, newContext, newMutableState)
	}

	if err == nil {
		now := time.Unix(0, lastEvent.GetTimestamp())
		notify(r.shard, request.GetSourceCluster(), now)
	}

	return err
}

func (r *historyReplicator) replicateWorkflowStarted(
	ctx context.Context,
	context workflowExecutionContext,
	msBuilder mutableState,
	history *historypb.History,
	sBuilder stateBuilder,
	logger log.Logger,
) (retError error) {

	executionInfo := msBuilder.GetExecutionInfo()
	namespaceID := executionInfo.NamespaceID
	execution := commonpb.WorkflowExecution{
		WorkflowId: executionInfo.WorkflowID,
		RunId:      executionInfo.RunID,
	}
	firstEvent := history.Events[0]
	incomingVersion := firstEvent.GetVersion()
	lastEvent := history.Events[len(history.Events)-1]

	now := time.Unix(0, lastEvent.GetTimestamp())
	newWorkflow, workflowEventsSeq, err := msBuilder.CloseTransactionAsSnapshot(
		now,
		transactionPolicyPassive,
	)
	if err != nil {
		return err
	}
	historySize, err := context.persistFirstWorkflowEvents(workflowEventsSeq[0])
	if err != nil {
		return err
	}
	// TODO add a check here guarantee that no replication tasks will be persisted
	newWorkflow.ReplicationTasks = nil

	deleteHistory := func() {
		// this function should be only called when we drop start workflow execution
		currentBranchToken, err := msBuilder.GetCurrentBranchToken()
		if err == nil {
			shardId := r.shard.GetShardID()
			r.shard.GetHistoryManager().DeleteHistoryBranch(&persistence.DeleteHistoryBranchRequest{
				BranchToken: currentBranchToken,
				ShardID:     &shardId,
			})
		}
	}

	// try to create the workflow execution
	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	err = context.createWorkflowExecution(
		newWorkflow, historySize, now,
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
	if currentState == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// allow the application of workflow creation if currentLastWriteVersion > incomingVersion
		// because this can be caused by the combination of missing replication events and failovers
		// proceed to create workflow
		createMode = persistence.CreateWorkflowModeWorkflowIDReuse
		prevRunID = currentRunID
		prevLastWriteVersion = currentLastWriteVersion
		return context.createWorkflowExecution(
			newWorkflow, historySize, now,
			createMode, prevRunID, prevLastWriteVersion,
		)
	}

	// current workflow is still running
	if currentLastWriteVersion > incomingVersion {
		logger.Info("Dropping stale start replication task.")
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.StaleReplicationEventsCounter)
		deleteHistory()

		currentContext, currentMutableState, currentRelease, err := r.getCurrentWorkflowMutableState(ctx, namespaceID, execution.GetWorkflowId())
		if err != nil {
			return err
		}
		defer func() { currentRelease(retError) }()
		return r.reapplyEvents(ctx, currentContext, currentMutableState, history.Events, logger)
	}

	if currentLastWriteVersion == incomingVersion {
		_, currentMutableState, currentRelease, err := r.getCurrentWorkflowMutableState(
			ctx,
			namespaceID,
			execution.GetWorkflowId(),
		)
		if err != nil {
			return err
		}
		currentRunID := currentMutableState.GetExecutionInfo().RunID
		currentLastEventTaskID := currentMutableState.GetExecutionInfo().LastEventTaskID
		currentNextEventID := currentMutableState.GetNextEventID()
		currentRelease(nil)

		if executionInfo.LastEventTaskID < currentLastEventTaskID {
			// versions are the same, so not necessary to re-apply signals
			return nil
		}
		return newRetryTaskErrorWithHint(
			ErrRetryExistingWorkflowMsg,
			namespaceID,
			execution.GetWorkflowId(),
			currentRunID,
			currentNextEventID,
		)
	}

	// currentStartVersion < incomingVersion && current workflow still running
	// this can happen during the failover; since we have no idea
	// whether the remote active cluster is aware of the current running workflow,
	// the only thing we can do is to terminate the current workflow and
	// start the new workflow from the request

	// same workflow ID, same shard
	currentLastWriteVersion, err = r.terminateWorkflow(
		ctx,
		namespaceID,
		executionInfo.WorkflowID,
		currentRunID,
		incomingVersion,
		logger,
	)
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}
		// if workflow is completed just when the call is made, will get EntityNotExistsError
		// we are not sure whether the workflow to be terminated ends with continue as new or not
		// so when encounter EntityNotExistsError, just continue to execute, if err occurs,
		// there will be retry on the worker level
	}
	createMode = persistence.CreateWorkflowModeWorkflowIDReuse
	prevRunID = currentRunID
	prevLastWriteVersion = currentLastWriteVersion
	return context.createWorkflowExecution(
		newWorkflow, historySize, now,
		createMode, prevRunID, prevLastWriteVersion,
	)
}

func (r *historyReplicator) conflictResolutionTerminateCurrentRunningIfNotSelf(
	ctx context.Context,
	msBuilder mutableState,
	incomingVersion int64,
	incomingTimestamp int64,
	logger log.Logger,
) (string, int64, enumsspb.WorkflowExecutionState, error) {

	// this function aims to solve the edge case when this workflow, when going through
	// reset, has already started a next generation (continue as new-ed workflow)

	if msBuilder.IsWorkflowExecutionRunning() {
		// workflow still running, no continued as new edge case to solve
		logger.Info("Conflict resolution self workflow running, skip.")
		executionInfo := msBuilder.GetExecutionInfo()
		lastWriteVersion, err := msBuilder.GetLastWriteVersion()
		if err != nil {
			return "", 0, 0, err
		}
		return executionInfo.RunID, lastWriteVersion, executionInfo.State, nil
	}

	// terminate the current running workflow
	// cannot use history cache to get current workflow since there can be deadlock
	namespaceID := msBuilder.GetExecutionInfo().NamespaceID
	workflowID := msBuilder.GetExecutionInfo().WorkflowID
	resp, err := r.shard.GetExecutionManager().GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	})
	if err != nil {
		logError(logger, "Conflict resolution error getting current workflow.", err)
		return "", 0, 0, err
	}
	currentRunID := resp.RunID
	currentState := resp.State
	currentStatus := resp.Status
	currentLastWriteVetsion := resp.LastWriteVersion

	// this handle the edge case where
	// local run 1 do continue as new -> run 2L
	// remote run 1 become active, and do continue as new -> run 2R
	// run 2R comes earlier, force terminate run 2L and replicate 2R
	// remote run 1's version trigger a conflict resolution trying to force terminate run 2R
	// conflict resolution should only force terminate workflow if that workflow has lower last write version
	if incomingVersion <= currentLastWriteVetsion {
		logger.Info("Conflict resolution current workflow has equal or higher version.")
		return "", 0, 0, nil
	}

	if currentStatus != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		// current workflow finished
		// note, it is impossible that a current workflow ends with continue as new as close status
		logger.Info("Conflict resolution current workflow finished.")
		return currentRunID, currentLastWriteVetsion, currentState, nil
	}

	// need to terminate the current workflow
	// same workflow ID, same shard
	currentLastWriteVetsion, err = r.terminateWorkflow(
		ctx,
		namespaceID,
		workflowID,
		currentRunID,
		incomingVersion,
		logger,
	)
	if err != nil {
		logError(logger, "Conflict resolution err terminating current workflow.", err)
		return "", 0, 0, err
	}
	return currentRunID, currentLastWriteVetsion, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, nil
}

func (r *historyReplicator) getCurrentWorkflowMutableState(
	ctx context.Context,
	namespaceID string,
	workflowID string,
) (workflowExecutionContext, mutableState, releaseWorkflowExecutionFunc, error) {
	// we need to check the current workflow execution
	context, release, err := r.historyCache.getOrCreateWorkflowExecution(ctx,
		namespaceID,
		// only use the workflow ID, to get the current running one
		commonpb.WorkflowExecution{WorkflowId: workflowID},
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

func (r *historyReplicator) getCurrentWorkflowRunID(namespaceID string, workflowID string) (string, error) {
	resp, err := r.historyEngine.executionManager.GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	})
	if err != nil {
		return "", err
	}
	return resp.RunID, nil
}

func (r *historyReplicator) terminateWorkflow(
	ctx context.Context,
	namespaceID string,
	workflowID string,
	runID string,
	incomingVersion int64,
	logger log.Logger,
) (int64, error) {

	// same workflow ID, same shard
	execution := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	var currentLastWriteVersion int64
	var err error
	err = r.historyEngine.updateWorkflowExecution(ctx, namespaceID, execution, false,
		func(context workflowExecutionContext, msBuilder mutableState) error {

			// compare the current last write version first
			// since this function has assumption that
			// incomingVersion <= currentLastWriteVersion
			// if assumption is broken (race condition), then retry
			currentLastWriteVersion, err = msBuilder.GetLastWriteVersion()
			if err != nil {
				return err
			}
			if incomingVersion <= currentLastWriteVersion {
				return newRetryTaskErrorWithHint(
					ErrRetryExistingWorkflowMsg,
					namespaceID,
					workflowID,
					runID,
					msBuilder.GetNextEventID(),
				)
			}

			if !msBuilder.IsWorkflowExecutionRunning() {
				return ErrWorkflowCompleted
			}

			// incomingVersion > currentLastWriteVersion

			// need to check if able to force terminate the workflow, by using last write version
			// if last write version indicates not from current cluster, need to fetch from remote
			sourceCluster := r.clusterMetadata.ClusterNameForFailoverVersion(currentLastWriteVersion)
			if sourceCluster != r.clusterMetadata.GetCurrentClusterName() {
				return newRetryTaskErrorWithHint(
					ErrRetryExistingWorkflowMsg,
					namespaceID,
					workflowID,
					runID,
					msBuilder.GetNextEventID(),
				)
			}

			// setting the current version to be the last write version
			if err := msBuilder.UpdateCurrentVersion(currentLastWriteVersion, true); err != nil {
				return err
			}

			eventBatchFirstEventID := msBuilder.GetNextEventID()
			if _, err := msBuilder.AddWorkflowExecutionTerminatedEvent(
				eventBatchFirstEventID,
				workflowTerminationReason,
				payloads.EncodeString(fmt.Sprintf("terminated by version: %v", incomingVersion)),
				workflowTerminationIdentity,
			); err != nil {
				return serviceerror.NewInternal("Unable to terminate workflow execution.")
			}

			return nil
		})

	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return 0, err
		}
		err = nil
	}
	return currentLastWriteVersion, nil
}

func (r *historyReplicator) getLatestCheckpoint(
	replicationInfoRemote map[string]*replicationspb.ReplicationInfo,
	replicationInfoLocal map[string]*replicationspb.ReplicationInfo,
) (int64, int64) {

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
			lastValidEventID = ri.LastEventId
		}
	}

	return lastValidVersion, lastValidEventID
}

func (r *historyReplicator) resetMutableState(
	ctx context.Context,
	context workflowExecutionContext,
	msBuilder mutableState,
	lastEventID int64,
	incomingVersion int64,
	incomingTimestamp int64,
	logger log.Logger,
) (mutableState, error) {

	r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.HistoryConflictsCounter)

	// handling edge case when resetting a workflow, and this workflow has done continue as new
	// we need to terminate the continue as new-ed workflow
	currentRunID, currentLastWriteVersion, currentState, err := r.conflictResolutionTerminateCurrentRunningIfNotSelf(
		ctx,
		msBuilder,
		incomingVersion,
		incomingTimestamp,
		logger,
	)
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
	msBuilder, err = resolver.reset(
		currentRunID,
		currentLastWriteVersion,
		currentState,
		uuid.New(),
		lastEventID,
		msBuilder.GetExecutionInfo(),
		msBuilder.GetUpdateCondition(),
	)
	logger.Info("Completed Resetting of workflow execution.")
	if err != nil {
		return nil, err
	}
	return msBuilder, nil
}

func (r *historyReplicator) deserializeBlob(
	blob *commonpb.DataBlob,
) ([]*historypb.HistoryEvent, error) {

	if blob.GetEncodingType() != enumspb.ENCODING_TYPE_PROTO3 {
		return nil, ErrUnknownEncodingType
	}
	historyEvents, err := r.historySerializer.DeserializeBatchEvents(&serialization.DataBlob{
		Encoding: common.EncodingTypeProto3,
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

func (r *historyReplicator) flushEventsBuffer(
	context workflowExecutionContext,
	msBuilder mutableState,
) error {

	if !msBuilder.IsWorkflowExecutionRunning() || !msBuilder.HasBufferedEvents() {
		return nil
	}
	canMutateWorkflow, err := r.prepareWorkflowMutation(msBuilder)
	if err != nil || !canMutateWorkflow {
		return err
	}

	decision, ok := msBuilder.GetInFlightDecision()
	if !ok {
		return ErrCorruptedMutableStateDecision
	}
	if _, err = msBuilder.AddDecisionTaskFailedEvent(
		decision.ScheduleID,
		decision.StartedID,
		enumspb.DECISION_TASK_FAILED_CAUSE_FAILOVER_CLOSE_DECISION,
		nil, identityHistoryService,
		"",
		"",
		"",
		0,
	); err != nil {
		return err
	}

	return r.persistWorkflowMutation(context, msBuilder, []persistence.Task{}, []persistence.Task{})
}

func (r *historyReplicator) reapplyEvents(
	ctx context.Context,
	context workflowExecutionContext,
	msBuilder mutableState,
	events []*historypb.HistoryEvent,
	logger log.Logger,
) error {

	var reapplyEvents []*historypb.HistoryEvent
	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			reapplyEvents = append(reapplyEvents, event)
		}
	}

	if len(reapplyEvents) == 0 {
		return nil
	}

	if msBuilder.IsWorkflowExecutionRunning() {
		return r.reapplyEventsToCurrentRunningWorkflow(ctx, context, msBuilder, reapplyEvents, logger)
	}

	return r.reapplyEventsToCurrentClosedWorkflow(ctx, context, msBuilder, reapplyEvents, logger)
}

func (r *historyReplicator) reapplyEventsToCurrentRunningWorkflow(
	ctx context.Context,
	context workflowExecutionContext,
	msBuilder mutableState,
	events []*historypb.HistoryEvent,
	logger log.Logger,
) error {

	canMutateWorkflow, err := r.prepareWorkflowMutation(msBuilder)
	if err != nil || !canMutateWorkflow {
		return err
	}

	numSignals := 0
	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			attr := event.GetWorkflowExecutionSignaledEventAttributes()
			if _, err := msBuilder.AddWorkflowExecutionSignaled(
				attr.GetSignalName(),
				attr.Input,
				attr.GetIdentity()); err != nil {
				return ErrWorkflowMutationSignal
			}
			numSignals++

		default:
			return ErrUnreappliableEvent
		}
	}

	r.logger.Info("reapplying signals", tag.Counter(numSignals))
	return r.persistWorkflowMutation(context, msBuilder, []persistence.Task{}, []persistence.Task{})
}

func (r *historyReplicator) reapplyEventsToCurrentClosedWorkflow(
	ctx context.Context,
	context workflowExecutionContext,
	msBuilder mutableState,
	events []*historypb.HistoryEvent,
	logger log.Logger,
) (retError error) {

	namespaceID := msBuilder.GetExecutionInfo().NamespaceID
	workflowID := msBuilder.GetExecutionInfo().WorkflowID

	namespaceEntry, err := r.namespaceCache.GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	resetRequestID := uuid.New()
	// workflow event buffer guarantee that the event immediately
	// after the decision task started is decision task finished event
	lastDecisionTaskStartEventID := msBuilder.GetPreviousStartedEventID()
	if lastDecisionTaskStartEventID == common.EmptyEventID {
		// TODO when https://github.com/temporalio/temporal/issues/2420 is finished
		//  reset to workflow finish event
		errStr := "cannot reapply signal due to workflow missing decision"
		logger.Error(errStr)
		return serviceerror.NewInvalidArgument(errStr)

	}

	resetDecisionFinishID := lastDecisionTaskStartEventID + 1

	baseContext := context
	baseMutableState := msBuilder
	currContext := context
	currMutableState := msBuilder
	resp, err := r.resetor.ResetWorkflowExecution(
		ctx,
		&workflowservice.ResetWorkflowExecutionRequest{
			Namespace:             namespaceEntry.GetInfo().Name,
			WorkflowExecution:     context.getExecution(),
			Reason:                workflowResetReason,
			DecisionFinishEventId: resetDecisionFinishID,
			RequestId:             resetRequestID,
		},
		baseContext,
		baseMutableState,
		currContext,
		currMutableState,
	)
	if err != nil {
		if _, ok := err.(*serviceerror.NamespaceNotActive); ok {
			return nil
		}
		return err
	}

	resetNewExecution := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      resp.GetRunId(),
	}
	resetNewContext, resetNewRelease, err := r.historyCache.getOrCreateWorkflowExecution(
		ctx, namespaceID, resetNewExecution,
	)
	if err != nil {
		return err
	}
	defer func() { resetNewRelease(retError) }()
	resetNewMsBuilder, err := resetNewContext.loadWorkflowExecution()
	if err != nil {
		return err
	}
	if resetNewMsBuilder.IsWorkflowExecutionRunning() {
		return ErrRetryRaceCondition
	}

	return r.reapplyEventsToCurrentRunningWorkflow(ctx, resetNewContext, resetNewMsBuilder, events, logger)
}

func (r *historyReplicator) prepareWorkflowMutation(
	msBuilder mutableState,
) (bool, error) {

	// for replication stack to modify workflow re-applying events
	// we need to check 2 things
	// 1. if the workflow's last write version indicates that workflow is active here
	// 2. if the namespace entry says this namespace is active and failover version in the namespace entry >= workflow's last write version
	// if either of the above is true, then the workflow can be mutated

	lastWriteVersion, err := msBuilder.GetLastWriteVersion()
	if err != nil {
		return false, err
	}
	lastWriteVersionActive := r.clusterMetadata.ClusterNameForFailoverVersion(lastWriteVersion) == r.clusterMetadata.GetCurrentClusterName()
	if lastWriteVersionActive {
		if err := msBuilder.UpdateCurrentVersion(
			lastWriteVersion,
			true,
		); err != nil {
			return false, err
		}
		return true, nil
	}

	namespaceEntry, err := r.namespaceCache.GetNamespaceByID(msBuilder.GetExecutionInfo().NamespaceID)
	if err != nil {
		return false, err
	}

	namespaceFailoverVersion := namespaceEntry.GetFailoverVersion()
	namespaceActive := namespaceEntry.GetReplicationConfig().ActiveClusterName == r.clusterMetadata.GetCurrentClusterName() &&
		namespaceFailoverVersion >= lastWriteVersion

	if namespaceActive {
		if err := msBuilder.UpdateCurrentVersion(
			lastWriteVersion,
			true,
		); err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

func (r *historyReplicator) persistWorkflowMutation(
	context workflowExecutionContext,
	msBuilder mutableState,
	transferTasks []persistence.Task,
	timerTasks []persistence.Task,
) error {

	if !msBuilder.HasPendingDecision() {
		_, err := msBuilder.AddDecisionTaskScheduledEvent(false)
		if err != nil {
			return ErrWorkflowMutationDecision
		}
	}

	now := clock.NewRealTimeSource().Now() // this is on behalf of active logic
	return context.updateWorkflowExecutionAsActive(now)
}

func logError(
	logger log.Logger,
	msg string,
	err error,
) {
	logger.Error(msg, tag.Error(err))
}

func newRetryTaskErrorWithHint(
	msg string,
	namespaceID string,
	workflowID string,
	runID string,
	nextEventID int64,
) *serviceerror.RetryTask {

	return serviceerror.NewRetryTask(
		msg,
		namespaceID,
		workflowID,
		runID,
		nextEventID,
	)
}

func notify(
	shard ShardContext,
	clusterName string,
	now time.Time,
) {

	now = now.Add(-shard.GetConfig().StandbyClusterDelay())
	shard.SetCurrentTime(clusterName, now)
}
