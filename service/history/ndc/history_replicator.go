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

package ndc

import (
	"context"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	workflowpb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	mutableStateMissingMessage = "Resend events due to missing mutable state"
)

type (
	stateBuilderProvider func(
		mutableState workflow.MutableState,
		logger log.Logger,
	) workflow.MutableStateRebuilder

	mutableStateProvider func(
		namespaceEntry *namespace.Namespace,
		startTime time.Time,
		logger log.Logger,
	) workflow.MutableState

	bufferEventFlusherProvider func(
		wfContext workflow.Context,
		mutableState workflow.MutableState,
		logger log.Logger,
	) BufferEventFlusher

	branchMgrProvider func(
		wfContext workflow.Context,
		mutableState workflow.MutableState,
		logger log.Logger,
	) BranchMgr

	conflictResolverProvider func(
		wfContext workflow.Context,
		mutableState workflow.MutableState,
		logger log.Logger,
	) ConflictResolver

	workflowResetterProvider func(
		namespaceID namespace.ID,
		workflowID string,
		baseRunID string,
		newContext workflow.Context,
		newRunID string,
		logger log.Logger,
	) resetter

	EventBlobs struct {
		CurrentRunEvents commonpb.DataBlob
		NewRunEvents     *commonpb.DataBlob
	}

	HistoryReplicator interface {
		ApplyEvents(
			ctx context.Context,
			request *historyservice.ReplicateEventsV2Request,
		) error
		// ApplyEventBlobs is the batch version of ApplyEvents
		// NOTE:
		//  1. all history events should have the same version
		//  2. all history events should share the same version history
		ApplyEventBlobs(
			ctx context.Context,
			workflowKey definition.WorkflowKey,
			baseExecutionInfo *workflowpb.BaseExecutionInfo,
			versionHistoryItems []*historyspb.VersionHistoryItem,
			events [][]*historypb.HistoryEvent,
			newEvents []*historypb.HistoryEvent,
		) error
	}

	HistoryReplicatorImpl struct {
		shardContext      shard.Context
		clusterMetadata   cluster.Metadata
		historySerializer serialization.Serializer
		metricsHandler    metrics.Handler
		namespaceRegistry namespace.Registry
		workflowCache     wcache.Cache
		eventsReapplier   EventsReapplier
		transactionMgr    transactionMgr
		logger            log.Logger

		newBufferEventFlusherProvider bufferEventFlusherProvider
		newBranchMgr                  branchMgrProvider
		newConflictResolver           conflictResolverProvider
		newResetter                   workflowResetterProvider
		newStateBuilder               stateBuilderProvider
		newMutableState               mutableStateProvider
	}

	rawHistoryData struct {
		rawHistory *commonpb.DataBlob
		nodeID     int64
	}
)

var errPanic = serviceerror.NewInternal("encountered panic")

func NewHistoryReplicator(
	shardContext shard.Context,
	workflowCache wcache.Cache,
	eventsReapplier EventsReapplier,
	eventSerializer serialization.Serializer,
	logger log.Logger,
) *HistoryReplicatorImpl {

	transactionMgr := newTransactionMgr(shardContext, workflowCache, eventsReapplier, logger)
	replicator := &HistoryReplicatorImpl{
		shardContext:      shardContext,
		clusterMetadata:   shardContext.GetClusterMetadata(),
		historySerializer: eventSerializer,
		metricsHandler:    shardContext.GetMetricsHandler(),
		namespaceRegistry: shardContext.GetNamespaceRegistry(),
		workflowCache:     workflowCache,
		transactionMgr:    transactionMgr,
		eventsReapplier:   eventsReapplier,
		logger:            log.With(logger, tag.ComponentHistoryReplicator),

		newBufferEventFlusherProvider: func(
			wfContext workflow.Context,
			mutableState workflow.MutableState,
			logger log.Logger,
		) BufferEventFlusher {
			return NewBufferEventFlusher(shardContext, wfContext, mutableState, logger)
		},
		newBranchMgr: func(
			wfContext workflow.Context,
			mutableState workflow.MutableState,
			logger log.Logger,
		) BranchMgr {
			return NewBranchMgr(shardContext, wfContext, mutableState, logger)
		},
		newConflictResolver: func(
			wfContext workflow.Context,
			mutableState workflow.MutableState,
			logger log.Logger,
		) ConflictResolver {
			return NewConflictResolver(shardContext, wfContext, mutableState, logger)
		},
		newResetter: func(
			namespaceID namespace.ID,
			workflowID string,
			baseRunID string,
			newContext workflow.Context,
			newRunID string,
			logger log.Logger,
		) resetter {
			return NewResetter(shardContext, transactionMgr, namespaceID, workflowID, baseRunID, newContext, newRunID, logger)
		},
		newStateBuilder: func(
			state workflow.MutableState,
			logger log.Logger,
		) workflow.MutableStateRebuilder {
			return workflow.NewMutableStateRebuilder(
				shardContext,
				logger,
				state,
			)
		},
		newMutableState: func(
			namespaceEntry *namespace.Namespace,
			startTime time.Time,
			logger log.Logger,
		) workflow.MutableState {
			return workflow.NewMutableState(
				shardContext,
				shardContext.GetEventsCache(),
				logger,
				namespaceEntry,
				startTime,
			)
		},
	}

	return replicator
}

func (r *HistoryReplicatorImpl) ApplyEvents(
	ctx context.Context,
	request *historyservice.ReplicateEventsV2Request,
) (retError error) {

	task, err := newReplicationTaskFromRequest(
		r.clusterMetadata,
		r.historySerializer,
		r.logger,
		request,
	)
	if err != nil {
		return err
	}

	return r.doApplyEvents(ctx, task)
}

func (r *HistoryReplicatorImpl) ApplyEventBlobs(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	baseExecutionInfo *workflowpb.BaseExecutionInfo,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	eventsSlice [][]*historypb.HistoryEvent,
	newEvents []*historypb.HistoryEvent,
) error {
	task, err := newReplicationTaskFromBatch(
		r.clusterMetadata,
		r.logger,
		workflowKey,
		baseExecutionInfo,
		versionHistoryItems,
		eventsSlice,
		newEvents,
	)
	if err != nil {
		return err
	}

	return r.doApplyEvents(ctx, task)
}

func (r *HistoryReplicatorImpl) doApplyEvents(
	ctx context.Context,
	task replicationTask,
) (retError error) {

	wfContext, releaseFn, err := r.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		r.shardContext,
		task.getNamespaceID(),
		*task.getExecution(),
		workflow.LockPriorityHigh,
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
		return r.applyStartEvents(ctx, wfContext, releaseFn, task)

	default:
		// apply events, other than simple start workflow execution
		// the continue as new + start workflow execution combination will also be processed here
		mutableState, err := wfContext.LoadMutableState(ctx, r.shardContext)
		switch err.(type) {
		case nil:
			wfContext, mutableState, err := r.applyNonStartEventsFlushBufferEvents(ctx, wfContext, mutableState, task)
			if err != nil {
				return err
			}
			doContinue, branchIndex, err := r.applyNonStartEventsPrepareBranch(ctx, wfContext, mutableState, task)
			if err != nil {
				return err
			} else if !doContinue {
				r.metricsHandler.Counter(metrics.DuplicateReplicationEventsCounter.GetMetricName()).Record(
					1,
					metrics.OperationTag(metrics.ReplicateHistoryEventsScope))
				return nil
			}

			mutableState, isRebuilt, err := r.applyNonStartEventsPrepareMutableState(ctx, wfContext, mutableState, branchIndex, task)
			if err != nil {
				return err
			}

			if mutableState.GetExecutionInfo().GetVersionHistories().GetCurrentVersionHistoryIndex() == branchIndex {
				return r.applyNonStartEventsToCurrentBranch(ctx, wfContext, mutableState, isRebuilt, releaseFn, task)
			}
			return r.applyNonStartEventsToNonCurrentBranch(ctx, wfContext, mutableState, branchIndex, releaseFn, task)

		case *serviceerror.NotFound:
			// mutable state not created, check if is workflow reset
			mutableState, err := r.applyNonStartEventsMissingMutableState(ctx, wfContext, task)
			if err != nil {
				return err
			}

			return r.applyNonStartEventsResetWorkflow(ctx, wfContext, mutableState, task)

		default:
			// unable to get mutable state, return err, so we can retry the task later
			return err
		}
	}
}

func (r *HistoryReplicatorImpl) applyStartEvents(
	ctx context.Context,
	wfContext workflow.Context,
	releaseFn wcache.ReleaseCacheFunc,
	task replicationTask,
) error {

	namespaceEntry, err := r.namespaceRegistry.GetNamespaceByID(task.getNamespaceID())
	if err != nil {
		return err
	}
	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	mutableState := r.newMutableState(namespaceEntry, timestamp.TimeValue(task.getFirstEvent().GetEventTime()), task.getLogger())
	stateBuilder := r.newStateBuilder(mutableState, task.getLogger())

	// use state builder for workflow mutable state mutation
	_, err = stateBuilder.ApplyEvents(
		ctx,
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
		NewWorkflow(
			r.clusterMetadata,
			wfContext,
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

func (r *HistoryReplicatorImpl) applyNonStartEventsFlushBufferEvents(
	ctx context.Context,
	wfContext workflow.Context,
	mutableState workflow.MutableState,
	task replicationTask,
) (workflow.Context, workflow.MutableState, error) {
	bufferEventFlusher := r.newBufferEventFlusherProvider(wfContext, mutableState, task.getLogger())
	return bufferEventFlusher.flush(ctx)
}

func (r *HistoryReplicatorImpl) applyNonStartEventsPrepareBranch(
	ctx context.Context,
	wfContext workflow.Context,
	mutableState workflow.MutableState,
	task replicationTask,
) (bool, int32, error) {

	incomingVersionHistory := task.getVersionHistory()
	branchMgr := r.newBranchMgr(wfContext, mutableState, task.getLogger())
	doContinue, versionHistoryIndex, err := branchMgr.prepareBranch(
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

func (r *HistoryReplicatorImpl) applyNonStartEventsPrepareMutableState(
	ctx context.Context,
	wfContext workflow.Context,
	mutableState workflow.MutableState,
	branchIndex int32,
	task replicationTask,
) (workflow.MutableState, bool, error) {

	incomingVersion := task.getVersion()
	conflictResolver := r.newConflictResolver(wfContext, mutableState, task.getLogger())
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

func (r *HistoryReplicatorImpl) applyNonStartEventsToCurrentBranch(
	ctx context.Context,
	wfContext workflow.Context,
	mutableState workflow.MutableState,
	isRebuilt bool,
	releaseFn wcache.ReleaseCacheFunc,
	task replicationTask,
) error {

	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	stateBuilder := r.newStateBuilder(mutableState, task.getLogger())
	newMutableState, err := stateBuilder.ApplyEvents(
		ctx,
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

	targetWorkflow := NewWorkflow(
		r.clusterMetadata,
		wfContext,
		mutableState,
		releaseFn,
	)

	var newWorkflow Workflow
	if newMutableState != nil {
		newExecutionInfo := newMutableState.GetExecutionInfo()
		newExecutionState := newMutableState.GetExecutionState()
		newContext := workflow.NewContext(
			r.shardContext.GetConfig(),
			definition.NewWorkflowKey(
				newExecutionInfo.NamespaceId,
				newExecutionInfo.WorkflowId,
				newExecutionState.RunId,
			),
			r.logger,
			r.shardContext.GetThrottledLogger(),
			r.shardContext.GetMetricsHandler(),
		)

		newWorkflow = NewWorkflow(
			r.clusterMetadata,
			newContext,
			newMutableState,
			wcache.NoopReleaseFn,
		)
	}

	err = r.transactionMgr.updateWorkflow(
		ctx,
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

func (r *HistoryReplicatorImpl) applyNonStartEventsToNonCurrentBranch(
	ctx context.Context,
	wfContext workflow.Context,
	mutableState workflow.MutableState,
	branchIndex int32,
	releaseFn wcache.ReleaseCacheFunc,
	task replicationTask,
) error {

	if len(task.getNewEvents()) != 0 {
		return r.applyNonStartEventsToNonCurrentBranchWithContinueAsNew(
			ctx,
			wfContext,
			releaseFn,
			task,
		)
	}

	return r.applyNonStartEventsToNonCurrentBranchWithoutContinueAsNew(
		ctx,
		wfContext,
		mutableState,
		branchIndex,
		releaseFn,
		task,
	)
}

func (r *HistoryReplicatorImpl) applyNonStartEventsToNonCurrentBranchWithoutContinueAsNew(
	ctx context.Context,
	wfContext workflow.Context,
	mutableState workflow.MutableState,
	branchIndex int32,
	releaseFn wcache.ReleaseCacheFunc,
	task replicationTask,
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

	transactionIDs, err := r.shardContext.GenerateTaskIDs(len(task.getEvents()))
	if err != nil {
		return err
	}

	eventsSlice := make([]*persistence.WorkflowEvents, len(task.getEvents()))
	for i, events := range task.getEvents() {
		eventsSlice[i] = &persistence.WorkflowEvents{
			NamespaceID: task.getNamespaceID().String(),
			WorkflowID:  task.getExecution().GetWorkflowId(),
			RunID:       task.getExecution().GetRunId(),
			BranchToken: versionHistory.GetBranchToken(),
			PrevTxnID:   0, // TODO @wxing1292 events chaining will not work for backfill case
			TxnID:       transactionIDs[i],
			Events:      events,
		}
	}
	err = r.transactionMgr.backfillWorkflow(
		ctx,
		NewWorkflow(
			r.clusterMetadata,
			wfContext,
			mutableState,
			releaseFn,
		),
		eventsSlice...,
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to backfill workflow when applyNonStartEventsToNonCurrentBranch",
			tag.Error(err),
		)
		return err
	}
	return nil
}

func (r *HistoryReplicatorImpl) applyNonStartEventsToNonCurrentBranchWithContinueAsNew(
	ctx context.Context,
	wfContext workflow.Context,
	releaseFn wcache.ReleaseCacheFunc,
	task replicationTask,
) error {

	// workflow backfill to non current branch with continue as new
	// first, release target workflow lock & create the new workflow as zombie
	// NOTE: need to release target workflow due to target workflow
	//  can potentially be the current workflow causing deadlock

	// 1. clear all in memory changes & release target workflow Lock
	// 2. apply new workflow first
	// 3. apply target workflow

	// step 1
	wfContext.Clear()
	releaseFn(nil)

	// step 2
	task, newTask, err := task.splitTask()
	if err != nil {
		return err
	}
	if err := r.doApplyEvents(ctx, newTask); err != nil {
		newTask.getLogger().Error(
			"nDCHistoryReplicator unable to create new workflow when applyNonStartEventsToNonCurrentBranchWithContinueAsNew",
			tag.Error(err),
		)
		return err
	}

	// step 3
	if err := r.doApplyEvents(ctx, task); err != nil {
		newTask.getLogger().Error(
			"nDCHistoryReplicator unable to create target workflow when applyNonStartEventsToNonCurrentBranchWithContinueAsNew",
			tag.Error(err),
		)
		return err
	}
	return nil
}

func (r *HistoryReplicatorImpl) applyNonStartEventsMissingMutableState(
	ctx context.Context,
	newWFContext workflow.Context,
	task replicationTask,
) (workflow.MutableState, error) {

	// for non reset workflow execution replication task, just do re-replication
	if !task.isWorkflowReset() {
		startEventId := common.EmptyEventID
		startEventVersion := common.EmptyVersion
		if task.getBaseWorkflowInfo() != nil {
			startEventId = task.getBaseWorkflowInfo().LowestCommonAncestorEventId
			startEventVersion = task.getBaseWorkflowInfo().LowestCommonAncestorEventVersion
		}
		firstEvent := task.getFirstEvent()
		endEventId := firstEvent.GetEventId()
		endEventVersion := firstEvent.GetVersion()
		return nil, serviceerrors.NewRetryReplication(
			mutableStateMissingMessage,
			task.getNamespaceID().String(),
			task.getWorkflowID(),
			task.getRunID(),
			startEventId,
			startEventVersion,
			endEventId,
			endEventVersion,
		)
	}

	baseWorkflowInfo := task.getBaseWorkflowInfo()
	baseRunID := baseWorkflowInfo.RunId
	baseEventID := baseWorkflowInfo.LowestCommonAncestorEventId
	baseEventVersion := baseWorkflowInfo.LowestCommonAncestorEventVersion
	newRunID := newWFContext.GetWorkflowKey().RunID

	workflowResetter := r.newResetter(
		task.getNamespaceID(),
		task.getWorkflowID(),
		baseRunID,
		newWFContext,
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

func (r *HistoryReplicatorImpl) applyNonStartEventsResetWorkflow(
	ctx context.Context,
	wfContext workflow.Context,
	mutableState workflow.MutableState,
	task replicationTask,
) error {

	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	stateBuilder := r.newStateBuilder(mutableState, task.getLogger())
	_, err := stateBuilder.ApplyEvents(
		ctx,
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

	targetWorkflow := NewWorkflow(
		r.clusterMetadata,
		wfContext,
		mutableState,
		wcache.NoopReleaseFn,
	)

	err = r.transactionMgr.createWorkflow(
		ctx,
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

func (r *HistoryReplicatorImpl) notify(
	clusterName string,
	now time.Time,
) {
	if clusterName == r.clusterMetadata.GetCurrentClusterName() {
		// this is a valid use case for testing, but not for production
		r.logger.Warn("nDCHistoryReplicator applying events generated by current cluster")
		return
	}
	now = now.Add(-r.shardContext.GetConfig().StandbyClusterDelay())
	r.shardContext.SetCurrentTime(clusterName, now)
}
