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
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/shard"
)

var (
	workflowTerminationReason   = "Terminate Workflow Due To Version Conflict."
	workflowTerminationIdentity = "worker-service"
	workflowResetReason         = "Reset Workflow Due To Events Re-application."
)

const (
	mutableStateMissingMessage = "Resend events due to missing mutable state"
)

type (
	stateBuilderProvider func(
		mutableState mutableState,
		logger log.Logger) stateBuilder

	mutableStateProvider func(
		namespaceEntry *cache.NamespaceCacheEntry,
		startTime time.Time,
		logger log.Logger,
	) mutableState

	nDCBranchMgrProvider func(
		context workflowExecutionContext,
		mutableState mutableState,
		logger log.Logger,
	) nDCBranchMgr

	nDCConflictResolverProvider func(
		context workflowExecutionContext,
		mutableState mutableState,
		logger log.Logger,
	) nDCConflictResolver

	nDCWorkflowResetterProvider func(
		namespaceID string,
		workflowID string,
		baseRunID string,
		newContext workflowExecutionContext,
		newRunID string,
		logger log.Logger,
	) nDCWorkflowResetter

	nDCHistoryReplicator interface {
		ApplyEvents(
			ctx context.Context,
			request *historyservice.ReplicateEventsV2Request,
		) error
	}

	nDCHistoryReplicatorImpl struct {
		shard             shard.Context
		clusterMetadata   cluster.Metadata
		historyV2Mgr      persistence.HistoryManager
		historySerializer persistence.PayloadSerializer
		metricsClient     metrics.Client
		namespaceCache    cache.NamespaceCache
		historyCache      *historyCache
		eventsReapplier   nDCEventsReapplier
		transactionMgr    nDCTransactionMgr
		logger            log.Logger

		newBranchMgr        nDCBranchMgrProvider
		newConflictResolver nDCConflictResolverProvider
		newWorkflowResetter nDCWorkflowResetterProvider
		newStateBuilder     stateBuilderProvider
		newMutableState     mutableStateProvider
	}
)

var errPanic = serviceerror.NewInternal("encounter panic")

func newNDCHistoryReplicator(
	shard shard.Context,
	historyCache *historyCache,
	eventsReapplier nDCEventsReapplier,
	logger log.Logger,
) *nDCHistoryReplicatorImpl {

	transactionMgr := newNDCTransactionMgr(shard, historyCache, eventsReapplier, logger)
	replicator := &nDCHistoryReplicatorImpl{
		shard:             shard,
		clusterMetadata:   shard.GetService().GetClusterMetadata(),
		historyV2Mgr:      shard.GetHistoryManager(),
		historySerializer: persistence.NewPayloadSerializer(),
		metricsClient:     shard.GetMetricsClient(),
		namespaceCache:    shard.GetNamespaceCache(),
		historyCache:      historyCache,
		transactionMgr:    transactionMgr,
		eventsReapplier:   eventsReapplier,
		logger:            log.With(logger, tag.ComponentHistoryReplicator),

		newBranchMgr: func(
			context workflowExecutionContext,
			mutableState mutableState,
			logger log.Logger,
		) nDCBranchMgr {
			return newNDCBranchMgr(shard, context, mutableState, logger)
		},
		newConflictResolver: func(
			context workflowExecutionContext,
			mutableState mutableState,
			logger log.Logger,
		) nDCConflictResolver {
			return newNDCConflictResolver(shard, context, mutableState, logger)
		},
		newWorkflowResetter: func(
			namespaceID string,
			workflowID string,
			baseRunID string,
			newContext workflowExecutionContext,
			newRunID string,
			logger log.Logger,
		) nDCWorkflowResetter {
			return newNDCWorkflowResetter(shard, transactionMgr, namespaceID, workflowID, baseRunID, newContext, newRunID, logger)
		},
		newStateBuilder: func(
			state mutableState,
			logger log.Logger,
		) stateBuilder {

			return newStateBuilder(
				shard,
				logger,
				state,
				func(mutableState mutableState) mutableStateTaskGenerator {
					return newMutableStateTaskGenerator(shard.GetNamespaceCache(), logger, mutableState)
				},
			)
		},
		newMutableState: func(
			namespaceEntry *cache.NamespaceCacheEntry,
			startTime time.Time,
			logger log.Logger,
		) mutableState {
			return newMutableStateBuilder(
				shard,
				shard.GetEventsCache(),
				logger,
				namespaceEntry,
				startTime,
			)
		},
	}

	return replicator
}

func (r *nDCHistoryReplicatorImpl) ApplyEvents(
	ctx context.Context,
	request *historyservice.ReplicateEventsV2Request,
) (retError error) {

	startTime := time.Now().UTC()
	task, err := newNDCReplicationTask(
		r.clusterMetadata,
		r.historySerializer,
		startTime,
		r.logger,
		request,
	)
	if err != nil {
		return err
	}

	return r.applyEvents(ctx, task)
}

func (r *nDCHistoryReplicatorImpl) applyEvents(
	ctx context.Context,
	task nDCReplicationTask,
) (retError error) {

	context, releaseFn, err := r.historyCache.getOrCreateWorkflowExecution(
		ctx,
		task.getNamespaceID(),
		*task.getExecution(),
	)
	if err != nil {
		// for get workflow execution context, with valid run id
		// err will not be of type EntityNotExistsError
		return err
	}
	defer func() {
		if rec := recover(); rec != nil {
			releaseFn(errPanic)
			panic(rec)
		} else {
			releaseFn(retError)
		}
	}()

	switch task.getFirstEvent().GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
		return r.applyStartEvents(ctx, context, releaseFn, task)

	default:
		// apply events, other than simple start workflow execution
		// the continue as new + start workflow execution combination will also be processed here
		var mutableState mutableState
		var err error
		namespaceEntry, err := r.namespaceCache.GetNamespaceByID(context.getNamespaceID())
		if err != nil {
			return err
		}

		if r.shard.GetConfig().ReplicationEventsFromCurrentCluster(namespaceEntry.GetInfo().Name) {
			// this branch is used when replicating events (generated from current cluster)from remote cluster to current cluster.
			// this could happen when the events are lost in current cluster and plan to recover them from remote cluster.
			mutableState, err = context.loadWorkflowExecutionForReplication(task.getVersion())
		} else {
			mutableState, err = context.loadWorkflowExecution()
		}
		switch err.(type) {
		case nil:
			// Sanity check to make only 3DC mutable state here
			if mutableState.GetExecutionInfo().GetVersionHistories() == nil {
				return serviceerror.NewInternal("The mutable state does not support 3DC.")
			}

			doContinue, branchIndex, err := r.applyNonStartEventsPrepareBranch(ctx, context, mutableState, task)
			if err != nil {
				return err
			} else if !doContinue {
				r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.DuplicateReplicationEventsCounter)
				return nil
			}

			mutableState, isRebuilt, err := r.applyNonStartEventsPrepareMutableState(ctx, context, mutableState, branchIndex, task)
			if err != nil {
				return err
			}

			if mutableState.GetExecutionInfo().GetVersionHistories().GetCurrentVersionHistoryIndex() == branchIndex {
				return r.applyNonStartEventsToCurrentBranch(ctx, context, mutableState, isRebuilt, releaseFn, task)
			}
			return r.applyNonStartEventsToNoneCurrentBranch(ctx, context, mutableState, branchIndex, releaseFn, task)

		case *serviceerror.NotFound:
			// mutable state not created, check if is workflow reset
			mutableState, err := r.applyNonStartEventsMissingMutableState(ctx, context, task)
			if err != nil {
				return err
			}

			return r.applyNonStartEventsResetWorkflow(ctx, context, mutableState, task)

		default:
			// unable to get mutable state, return err so we can retry the task later
			return err
		}
	}
}

func (r *nDCHistoryReplicatorImpl) applyStartEvents(
	ctx context.Context,
	context workflowExecutionContext,
	releaseFn releaseWorkflowExecutionFunc,
	task nDCReplicationTask,
) (retError error) {

	namespaceEntry, err := r.namespaceCache.GetNamespaceByID(task.getNamespaceID())
	if err != nil {
		return err
	}
	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	mutableState := r.newMutableState(namespaceEntry, timestamp.TimeValue(task.getFirstEvent().GetEventTime()), task.getLogger())
	stateBuilder := r.newStateBuilder(mutableState, task.getLogger())

	// use state builder for workflow mutable state mutation
	_, err = stateBuilder.applyEvents(
		task.getNamespaceID(),
		requestID,
		*task.getExecution(),
		task.getEvents(),
		task.getNewEvents(),
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to apply events when applyStartEvents",
			tag.Error(err),
		)
		return err
	}

	err = r.transactionMgr.createWorkflow(
		ctx,
		task.getEventTime(),
		newNDCWorkflow(
			ctx,
			r.namespaceCache,
			r.clusterMetadata,
			context,
			mutableState,
			releaseFn,
		),
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to create workflow when applyStartEvents",
			tag.Error(err),
		)
	} else {
		r.notify(task.getSourceCluster(), task.getEventTime())
	}
	return err
}

func (r *nDCHistoryReplicatorImpl) applyNonStartEventsPrepareBranch(
	ctx context.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	task nDCReplicationTask,
) (bool, int32, error) {

	incomingVersionHistory := task.getVersionHistory()
	branchMgr := r.newBranchMgr(context, mutableState, task.getLogger())
	doContinue, versionHistoryIndex, err := branchMgr.prepareVersionHistory(
		ctx,
		incomingVersionHistory,
		task.getFirstEvent().GetEventId(),
		task.getFirstEvent().GetVersion(),
	)
	switch err.(type) {
	case nil:
		return doContinue, versionHistoryIndex, nil
	case *serviceerrors.RetryReplication:
		// replication message can arrive out of order
		// do not log
		return false, 0, err
	default:
		task.getLogger().Error(
			"nDCHistoryReplicator unable to prepare version history when applyNonStartEventsPrepareBranch",
			tag.Error(err),
		)
		return false, 0, err
	}
}

func (r *nDCHistoryReplicatorImpl) applyNonStartEventsPrepareMutableState(
	ctx context.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	branchIndex int32,
	task nDCReplicationTask,
) (mutableState, bool, error) {

	incomingVersion := task.getVersion()
	conflictResolver := r.newConflictResolver(context, mutableState, task.getLogger())
	mutableState, isRebuilt, err := conflictResolver.prepareMutableState(
		ctx,
		branchIndex,
		incomingVersion,
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to prepare mutable state when applyNonStartEventsPrepareMutableState",
			tag.Error(err),
		)
	}
	return mutableState, isRebuilt, err
}

func (r *nDCHistoryReplicatorImpl) applyNonStartEventsToCurrentBranch(
	ctx context.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	isRebuilt bool,
	releaseFn releaseWorkflowExecutionFunc,
	task nDCReplicationTask,
) error {

	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	stateBuilder := r.newStateBuilder(mutableState, task.getLogger())
	newMutableState, err := stateBuilder.applyEvents(
		task.getNamespaceID(),
		requestID,
		*task.getExecution(),
		task.getEvents(),
		task.getNewEvents(),
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to apply events when applyNonStartEventsToCurrentBranch",
			tag.Error(err),
		)
		return err
	}

	targetWorkflow := newNDCWorkflow(
		ctx,
		r.namespaceCache,
		r.clusterMetadata,
		context,
		mutableState,
		releaseFn,
	)

	var newWorkflow nDCWorkflow
	if newMutableState != nil {
		newExecutionInfo := newMutableState.GetExecutionInfo()
		newExecutionState := newMutableState.GetExecutionState()
		newContext := newWorkflowExecutionContext(
			newExecutionInfo.NamespaceId,
			commonpb.WorkflowExecution{
				WorkflowId: newExecutionInfo.WorkflowId,
				RunId:      newExecutionState.RunId,
			},
			r.shard,
			r.shard.GetExecutionManager(),
			r.logger,
		)

		newWorkflow = newNDCWorkflow(
			ctx,
			r.namespaceCache,
			r.clusterMetadata,
			newContext,
			newMutableState,
			noopReleaseFn,
		)
	}

	err = r.transactionMgr.updateWorkflow(
		ctx,
		task.getEventTime(),
		isRebuilt,
		targetWorkflow,
		newWorkflow,
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to update workflow when applyNonStartEventsToCurrentBranch",
			tag.Error(err),
		)
	} else {
		r.notify(task.getSourceCluster(), task.getEventTime())
	}
	return err
}

func (r *nDCHistoryReplicatorImpl) applyNonStartEventsToNoneCurrentBranch(
	ctx context.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	branchIndex int32,
	releaseFn releaseWorkflowExecutionFunc,
	task nDCReplicationTask,
) error {

	if len(task.getNewEvents()) != 0 {
		return r.applyNonStartEventsToNoneCurrentBranchWithContinueAsNew(
			ctx,
			context,
			releaseFn,
			task,
		)
	}

	return r.applyNonStartEventsToNoneCurrentBranchWithoutContinueAsNew(
		ctx,
		context,
		mutableState,
		branchIndex,
		releaseFn,
		task,
	)
}

func (r *nDCHistoryReplicatorImpl) applyNonStartEventsToNoneCurrentBranchWithoutContinueAsNew(
	ctx context.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	branchIndex int32,
	releaseFn releaseWorkflowExecutionFunc,
	task nDCReplicationTask,
) error {

	versionHistoryItem := versionhistory.NewVersionHistoryItem(
		task.getLastEvent().GetEventId(),
		task.getLastEvent().GetVersion(),
	)
	versionHistory, err := versionhistory.GetVersionHistory(mutableState.GetExecutionInfo().GetVersionHistories(), branchIndex)
	if err != nil {
		return err
	}
	if err = versionhistory.AddOrUpdateVersionHistoryItem(versionHistory, versionHistoryItem); err != nil {
		return err
	}

	transactionID, err := r.shard.GenerateTransferTaskID()
	if err != nil {
		return err
	}

	err = r.transactionMgr.backfillWorkflow(
		ctx,
		task.getEventTime(),
		newNDCWorkflow(
			ctx,
			r.namespaceCache,
			r.clusterMetadata,
			context,
			mutableState,
			releaseFn,
		),
		&persistence.WorkflowEvents{
			NamespaceID: task.getNamespaceID(),
			WorkflowID:  task.getExecution().GetWorkflowId(),
			RunID:       task.getExecution().GetRunId(),
			BranchToken: versionHistory.GetBranchToken(),
			PrevTxnID:   0, // TODO @wxing1292 events chaining will not work for backfill case
			TxnID:       transactionID,
			Events:      task.getEvents(),
		},
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to backfill workflow when applyNonStartEventsToNoneCurrentBranch",
			tag.Error(err),
		)
		return err
	}
	return nil
}

func (r *nDCHistoryReplicatorImpl) applyNonStartEventsToNoneCurrentBranchWithContinueAsNew(
	ctx context.Context,
	context workflowExecutionContext,
	releaseFn releaseWorkflowExecutionFunc,
	task nDCReplicationTask,
) error {

	// workflow backfill to non current branch with continue as new
	// first, release target workflow lock & create the new workflow as zombie
	// NOTE: need to release target workflow due to target workflow
	//  can potentially be the current workflow causing deadlock

	// 1. clear all in memory changes & release target workflow lock
	// 2. apply new workflow first
	// 3. apply target workflow

	// step 1
	context.clear()
	releaseFn(nil)

	// step 2
	startTime := time.Now().UTC()
	task, newTask, err := task.splitTask(startTime)
	if err != nil {
		return err
	}
	if err := r.applyEvents(ctx, newTask); err != nil {
		newTask.getLogger().Error(
			"nDCHistoryReplicator unable to create new workflow when applyNonStartEventsToNoneCurrentBranchWithContinueAsNew",
			tag.Error(err),
		)
		return err
	}

	// step 3
	if err := r.applyEvents(ctx, task); err != nil {
		newTask.getLogger().Error(
			"nDCHistoryReplicator unable to create target workflow when applyNonStartEventsToNoneCurrentBranchWithContinueAsNew",
			tag.Error(err),
		)
		return err
	}
	return nil
}

func (r *nDCHistoryReplicatorImpl) applyNonStartEventsMissingMutableState(
	ctx context.Context,
	newContext workflowExecutionContext,
	task nDCReplicationTask,
) (mutableState, error) {

	// for non reset workflow execution replication task, just do re-replication
	if !task.isWorkflowReset() {
		firstEvent := task.getFirstEvent()
		return nil, serviceerrors.NewRetryReplication(
			mutableStateMissingMessage,
			task.getNamespaceID(),
			task.getWorkflowID(),
			task.getRunID(),
			common.EmptyEventID,
			common.EmptyVersion,
			firstEvent.GetEventId(),
			firstEvent.GetVersion(),
		)
	}

	workflowTaskFailedEvent := task.getFirstEvent()
	attr := workflowTaskFailedEvent.GetWorkflowTaskFailedEventAttributes()
	baseRunID := attr.GetBaseRunId()
	baseEventID := workflowTaskFailedEvent.GetEventId() - 1
	baseEventVersion := attr.GetForkEventVersion()
	newRunID := attr.GetNewRunId()

	workflowResetter := r.newWorkflowResetter(
		task.getNamespaceID(),
		task.getWorkflowID(),
		baseRunID,
		newContext,
		newRunID,
		task.getLogger(),
	)

	resetMutableState, err := workflowResetter.resetWorkflow(
		ctx,
		task.getEventTime(),
		baseEventID,
		baseEventVersion,
		task.getFirstEvent().GetEventId(),
		task.getVersion(),
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to reset workflow when applyNonStartEventsMissingMutableState",
			tag.Error(err),
		)
		return nil, err
	}
	return resetMutableState, nil
}

func (r *nDCHistoryReplicatorImpl) applyNonStartEventsResetWorkflow(
	ctx context.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	task nDCReplicationTask,
) error {

	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	stateBuilder := r.newStateBuilder(mutableState, task.getLogger())
	_, err := stateBuilder.applyEvents(
		task.getNamespaceID(),
		requestID,
		*task.getExecution(),
		task.getEvents(),
		task.getNewEvents(),
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to apply events when applyNonStartEventsResetWorkflow",
			tag.Error(err),
		)
		return err
	}

	targetWorkflow := newNDCWorkflow(
		ctx,
		r.namespaceCache,
		r.clusterMetadata,
		context,
		mutableState,
		noopReleaseFn,
	)

	err = r.transactionMgr.createWorkflow(
		ctx,
		task.getEventTime(),
		targetWorkflow,
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to create workflow when applyNonStartEventsResetWorkflow",
			tag.Error(err),
		)
	} else {
		r.notify(task.getSourceCluster(), task.getEventTime())
	}
	return err
}

func (r *nDCHistoryReplicatorImpl) notify(
	clusterName string,
	now time.Time,
) {
	if clusterName == r.clusterMetadata.GetCurrentClusterName() {
		// this is a valid use case for testing, but not for production
		r.logger.Warn("nDCHistoryReplicator applying events generated by current cluster")
		return
	}
	now = now.Add(-r.shard.GetConfig().StandbyClusterDelay())
	r.shard.SetCurrentTime(clusterName, now)
}
