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
	"fmt"
	"sort"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"golang.org/x/exp/slices"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowpb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
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

	branchMgrProvider func(
		context workflow.Context,
		mutableState workflow.MutableState,
		logger log.Logger,
	) BranchMgr

	conflictResolverProvider func(
		context workflow.Context,
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
		ApplyWorkflowState(
			ctx context.Context,
			request *historyservice.ReplicateWorkflowStateRequest,
		) error
	}

	HistoryReplicatorImpl struct {
		shard             shard.Context
		clusterMetadata   cluster.Metadata
		executionMgr      persistence.ExecutionManager
		historySerializer serialization.Serializer
		metricsHandler    metrics.Handler
		namespaceRegistry namespace.Registry
		workflowCache     wcache.Cache
		eventsReapplier   EventsReapplier
		transactionMgr    transactionMgr
		logger            log.Logger

		newBranchMgr        branchMgrProvider
		newConflictResolver conflictResolverProvider
		newResetter         workflowResetterProvider
		newStateBuilder     stateBuilderProvider
		newMutableState     mutableStateProvider
	}

	rawHistoryData struct {
		rawHistory *commonpb.DataBlob
		nodeID     int64
	}
)

var errPanic = serviceerror.NewInternal("encountered panic")

func NewHistoryReplicator(
	shard shard.Context,
	workflowCache wcache.Cache,
	eventsReapplier EventsReapplier,
	logger log.Logger,
	eventSerializer serialization.Serializer,
) *HistoryReplicatorImpl {

	transactionMgr := newTransactionMgr(shard, workflowCache, eventsReapplier, logger)
	replicator := &HistoryReplicatorImpl{
		shard:             shard,
		clusterMetadata:   shard.GetClusterMetadata(),
		executionMgr:      shard.GetExecutionManager(),
		historySerializer: eventSerializer,
		metricsHandler:    shard.GetMetricsHandler(),
		namespaceRegistry: shard.GetNamespaceRegistry(),
		workflowCache:     workflowCache,
		transactionMgr:    transactionMgr,
		eventsReapplier:   eventsReapplier,
		logger:            log.With(logger, tag.ComponentHistoryReplicator),

		newBranchMgr: func(
			context workflow.Context,
			mutableState workflow.MutableState,
			logger log.Logger,
		) BranchMgr {
			return NewBranchMgr(shard, context, mutableState, logger)
		},
		newConflictResolver: func(
			context workflow.Context,
			mutableState workflow.MutableState,
			logger log.Logger,
		) ConflictResolver {
			return NewConflictResolver(shard, context, mutableState, logger)
		},
		newResetter: func(
			namespaceID namespace.ID,
			workflowID string,
			baseRunID string,
			newContext workflow.Context,
			newRunID string,
			logger log.Logger,
		) resetter {
			return NewResetter(shard, transactionMgr, namespaceID, workflowID, baseRunID, newContext, newRunID, logger)
		},
		newStateBuilder: func(
			state workflow.MutableState,
			logger log.Logger,
		) workflow.MutableStateRebuilder {

			return workflow.NewMutableStateRebuilder(
				shard,
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

func (r *HistoryReplicatorImpl) ApplyWorkflowState(
	ctx context.Context,
	request *historyservice.ReplicateWorkflowStateRequest,
) (retError error) {
	executionInfo := request.GetWorkflowState().GetExecutionInfo()
	executionState := request.GetWorkflowState().GetExecutionState()
	namespaceID := namespace.ID(executionInfo.GetNamespaceId())
	wid := executionInfo.GetWorkflowId()
	rid := executionState.GetRunId()
	if executionState.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		return serviceerror.NewInternal("Replicate non completed workflow state is not supported.")
	}

	wfCtx, releaseFn, err := r.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		workflow.LockPriorityLow,
	)
	if err != nil {
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

	// Handle existing workflows
	ms, err := wfCtx.LoadMutableState(ctx)
	switch err.(type) {
	case *serviceerror.NotFound:
		// no-op, continue to replicate workflow state
	case nil:
		// workflow exists, do resend if version histories are not match.
		localVersionHistory, err := versionhistory.GetCurrentVersionHistory(ms.GetExecutionInfo().GetVersionHistories())
		if err != nil {
			return err
		}
		localHistoryLastItem, err := versionhistory.GetLastVersionHistoryItem(localVersionHistory)
		if err != nil {
			return err
		}
		incomingVersionHistory, err := versionhistory.GetCurrentVersionHistory(request.GetWorkflowState().GetExecutionInfo().GetVersionHistories())
		if err != nil {
			return err
		}
		incomingHistoryLastItem, err := versionhistory.GetLastVersionHistoryItem(incomingVersionHistory)
		if err != nil {
			return err
		}
		if !versionhistory.IsEqualVersionHistoryItem(localHistoryLastItem, incomingHistoryLastItem) {
			return serviceerrors.NewRetryReplication(
				"Failed to sync workflow state due to version history mismatch",
				namespaceID.String(),
				wid,
				rid,
				localHistoryLastItem.GetEventId(),
				localHistoryLastItem.GetVersion(),
				common.EmptyEventID,
				common.EmptyVersion,
			)
		}
		return nil
	default:
		return err
	}

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(executionInfo.VersionHistories)
	if err != nil {
		return err
	}
	lastEventItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return err
	}

	// The following sanitizes the branch token from the source cluster to this target cluster by re-initializing it.

	branchInfo, err := r.shard.GetExecutionManager().GetHistoryBranchUtil().ParseHistoryBranchInfo(
		currentVersionHistory.GetBranchToken(),
	)
	if err != nil {
		return err
	}
	newHistoryBranchToken, err := r.shard.GetExecutionManager().GetHistoryBranchUtil().NewHistoryBranch(
		request.NamespaceId,
		branchInfo.GetTreeId(),
		&branchInfo.BranchId,
		branchInfo.Ancestors,
		nil,
		nil,
		nil,
	)
	if err != nil {
		return err
	}

	_, lastFirstTxnID, err := r.backfillHistory(
		ctx,
		request.GetRemoteCluster(),
		namespaceID,
		wid,
		rid,
		lastEventItem.GetEventId(),
		lastEventItem.GetVersion(),
		newHistoryBranchToken,
	)
	if err != nil {
		return err
	}

	ns, err := r.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	mutableState, err := workflow.NewSanitizedMutableState(
		r.shard,
		r.shard.GetEventsCache(),
		r.logger,
		ns,
		request.GetWorkflowState(),
		lastFirstTxnID,
		lastEventItem.GetVersion(),
	)
	if err != nil {
		return err
	}

	err = mutableState.SetCurrentBranchToken(newHistoryBranchToken)
	if err != nil {
		return err
	}

	taskRefresh := workflow.NewTaskRefresher(r.shard, r.shard.GetConfig(), r.namespaceRegistry, r.logger)
	err = taskRefresh.RefreshTasks(ctx, mutableState)
	if err != nil {
		return err
	}
	return r.transactionMgr.createWorkflow(
		ctx,
		NewWorkflow(
			ctx,
			r.namespaceRegistry,
			r.clusterMetadata,
			wfCtx,
			mutableState,
			releaseFn,
		),
	)
}

func (r *HistoryReplicatorImpl) doApplyEvents(
	ctx context.Context,
	task replicationTask,
) (retError error) {

	context, releaseFn, err := r.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
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
		return r.applyStartEvents(ctx, context, releaseFn, task)

	default:
		// apply events, other than simple start workflow execution
		// the continue as new + start workflow execution combination will also be processed here
		mutableState, err := context.LoadMutableState(ctx)
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
				r.metricsHandler.Counter(metrics.DuplicateReplicationEventsCounter.GetMetricName()).Record(
					1,
					metrics.OperationTag(metrics.ReplicateHistoryEventsScope))
				return nil
			}

			mutableState, isRebuilt, err := r.applyNonStartEventsPrepareMutableState(ctx, context, mutableState, branchIndex, task)
			if err != nil {
				return err
			}

			if mutableState.GetExecutionInfo().GetVersionHistories().GetCurrentVersionHistoryIndex() == branchIndex {
				return r.applyNonStartEventsToCurrentBranch(ctx, context, mutableState, isRebuilt, releaseFn, task)
			}
			return r.applyNonStartEventsToNonCurrentBranch(ctx, context, mutableState, branchIndex, releaseFn, task)

		case *serviceerror.NotFound:
			// mutable state not created, check if is workflow reset
			mutableState, err := r.applyNonStartEventsMissingMutableState(ctx, context, task)
			if err != nil {
				return err
			}

			return r.applyNonStartEventsResetWorkflow(ctx, context, mutableState, task)

		default:
			// unable to get mutable state, return err, so we can retry the task later
			return err
		}
	}
}

func (r *HistoryReplicatorImpl) applyStartEvents(
	ctx context.Context,
	context workflow.Context,
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
			ctx,
			r.namespaceRegistry,
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

func (r *HistoryReplicatorImpl) applyNonStartEventsPrepareBranch(
	ctx context.Context,
	context workflow.Context,
	mutableState workflow.MutableState,
	task replicationTask,
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

func (r *HistoryReplicatorImpl) applyNonStartEventsPrepareMutableState(
	ctx context.Context,
	context workflow.Context,
	mutableState workflow.MutableState,
	branchIndex int32,
	task replicationTask,
) (workflow.MutableState, bool, error) {

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

func (r *HistoryReplicatorImpl) applyNonStartEventsToCurrentBranch(
	ctx context.Context,
	context workflow.Context,
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
		ctx,
		r.namespaceRegistry,
		r.clusterMetadata,
		context,
		mutableState,
		releaseFn,
	)

	var newWorkflow Workflow
	if newMutableState != nil {
		newExecutionInfo := newMutableState.GetExecutionInfo()
		newExecutionState := newMutableState.GetExecutionState()
		newContext := workflow.NewContext(
			r.shard,
			definition.NewWorkflowKey(
				newExecutionInfo.NamespaceId,
				newExecutionInfo.WorkflowId,
				newExecutionState.RunId,
			),
			r.logger,
		)

		newWorkflow = NewWorkflow(
			ctx,
			r.namespaceRegistry,
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
	context workflow.Context,
	mutableState workflow.MutableState,
	branchIndex int32,
	releaseFn wcache.ReleaseCacheFunc,
	task replicationTask,
) error {

	if len(task.getNewEvents()) != 0 {
		return r.applyNonStartEventsToNonCurrentBranchWithContinueAsNew(
			ctx,
			context,
			releaseFn,
			task,
		)
	}

	return r.applyNonStartEventsToNonCurrentBranchWithoutContinueAsNew(
		ctx,
		context,
		mutableState,
		branchIndex,
		releaseFn,
		task,
	)
}

func (r *HistoryReplicatorImpl) applyNonStartEventsToNonCurrentBranchWithoutContinueAsNew(
	ctx context.Context,
	context workflow.Context,
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

	transactionID, err := r.shard.GenerateTaskID()
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
			TxnID:       transactionID,
			Events:      events,
		}
	}
	err = r.transactionMgr.backfillWorkflow(
		ctx,
		NewWorkflow(
			ctx,
			r.namespaceRegistry,
			r.clusterMetadata,
			context,
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
	context workflow.Context,
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
	context.Clear()
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
	newContext workflow.Context,
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
	newRunID := newContext.GetWorkflowKey().RunID

	workflowResetter := r.newResetter(
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

func (r *HistoryReplicatorImpl) applyNonStartEventsResetWorkflow(
	ctx context.Context,
	context workflow.Context,
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
		ctx,
		r.namespaceRegistry,
		r.clusterMetadata,
		context,
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
	now = now.Add(-r.shard.GetConfig().StandbyClusterDelay())
	r.shard.SetCurrentTime(clusterName, now)
}

func (r *HistoryReplicatorImpl) backfillHistory(
	ctx context.Context,
	remoteClusterName string,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	lastEventID int64,
	lastEventVersion int64,
	branchToken []byte,
) (*time.Time, int64, error) {

	// Get the last batch node id to check if the history data is already in DB.
	localHistoryIterator := collection.NewPagingIterator(r.getHistoryFromLocalPaginationFn(
		ctx,
		branchToken,
		lastEventID,
	))
	var lastBatchNodeID int64
	for localHistoryIterator.HasNext() {
		localHistoryBatch, err := localHistoryIterator.Next()
		switch err.(type) {
		case nil:
			if len(localHistoryBatch.GetEvents()) > 0 {
				lastBatchNodeID = localHistoryBatch.GetEvents()[0].GetEventId()
			}
		case *serviceerror.NotFound:
		default:
			return nil, common.EmptyEventTaskID, err
		}
	}

	remoteHistoryIterator := collection.NewPagingIterator(r.getHistoryFromRemotePaginationFn(
		ctx,
		remoteClusterName,
		namespaceID,
		workflowID,
		runID,
		lastEventID,
		lastEventVersion),
	)
	historyBranchUtil := r.executionMgr.GetHistoryBranchUtil()
	historyBranch, err := historyBranchUtil.ParseHistoryBranchInfo(branchToken)
	if err != nil {
		return nil, common.EmptyEventTaskID, err
	}

	prevTxnID := common.EmptyEventTaskID
	var lastHistoryBatch *commonpb.DataBlob
	var prevBranchID string
	sortedAncestors := sortAncestors(historyBranch.GetAncestors())
	sortedAncestorsIdx := 0
	var ancestors []*persistencespb.HistoryBranchRange

BackfillLoop:
	for remoteHistoryIterator.HasNext() {
		historyBlob, err := remoteHistoryIterator.Next()
		if err != nil {
			return nil, common.EmptyEventTaskID, err
		}

		if historyBlob.nodeID <= lastBatchNodeID {
			// The history batch already in DB.
			continue BackfillLoop
		}

		branchID := historyBranch.GetBranchId()
		if sortedAncestorsIdx < len(sortedAncestors) {
			currentAncestor := sortedAncestors[sortedAncestorsIdx]
			if historyBlob.nodeID >= currentAncestor.GetEndNodeId() {
				// update ancestor
				ancestors = append(ancestors, currentAncestor)
				sortedAncestorsIdx++
			}
			if sortedAncestorsIdx < len(sortedAncestors) {
				// use ancestor branch id
				currentAncestor = sortedAncestors[sortedAncestorsIdx]
				branchID = currentAncestor.GetBranchId()
				if historyBlob.nodeID < currentAncestor.GetBeginNodeId() || historyBlob.nodeID >= currentAncestor.GetEndNodeId() {
					return nil, common.EmptyEventTaskID, serviceerror.NewInternal(
						fmt.Sprintf("The backfill history blob node id %d is not in acestoer range [%d, %d]",
							historyBlob.nodeID,
							currentAncestor.GetBeginNodeId(),
							currentAncestor.GetEndNodeId()),
					)
				}
			}
		}

		filteredHistoryBranch, err := historyBranchUtil.UpdateHistoryBranchInfo(
			branchToken,
			&persistencespb.HistoryBranch{
				TreeId:    historyBranch.GetTreeId(),
				BranchId:  branchID,
				Ancestors: ancestors,
			},
		)
		if err != nil {
			return nil, common.EmptyEventTaskID, err
		}
		txnID, err := r.shard.GenerateTaskID()
		if err != nil {
			return nil, common.EmptyEventTaskID, err
		}
		_, err = r.executionMgr.AppendRawHistoryNodes(ctx, &persistence.AppendRawHistoryNodesRequest{
			ShardID:           r.shard.GetShardID(),
			IsNewBranch:       prevBranchID != branchID,
			BranchToken:       filteredHistoryBranch,
			History:           historyBlob.rawHistory,
			PrevTransactionID: prevTxnID,
			TransactionID:     txnID,
			NodeID:            historyBlob.nodeID,
			Info: persistence.BuildHistoryGarbageCleanupInfo(
				namespaceID.String(),
				workflowID,
				runID,
			),
		})
		if err != nil {
			return nil, common.EmptyEventTaskID, err
		}
		prevTxnID = txnID
		prevBranchID = branchID
		lastHistoryBatch = historyBlob.rawHistory
	}

	var lastEventTime *time.Time
	events, _ := r.historySerializer.DeserializeEvents(lastHistoryBatch)
	if len(events) > 0 {
		lastEventTime = events[len(events)-1].EventTime
	}
	return lastEventTime, prevTxnID, nil
}

func sortAncestors(ans []*persistencespb.HistoryBranchRange) []*persistencespb.HistoryBranchRange {
	if len(ans) > 0 {
		// sort ans based onf EndNodeID so that we can set BeginNodeID
		sort.Slice(ans, func(i, j int) bool { return ans[i].GetEndNodeId() < ans[j].GetEndNodeId() })
		ans[0].BeginNodeId = int64(1)
		for i := 1; i < len(ans); i++ {
			ans[i].BeginNodeId = ans[i-1].GetEndNodeId()
		}
	}
	return ans
}

func (r *HistoryReplicatorImpl) getHistoryFromRemotePaginationFn(
	ctx context.Context,
	remoteClusterName string,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	endEventID int64,
	endEventVersion int64,
) collection.PaginationFn[*rawHistoryData] {

	return func(paginationToken []byte) ([]*rawHistoryData, []byte, error) {

		adminClient, err := r.shard.GetRemoteAdminClient(remoteClusterName)
		if err != nil {
			return nil, nil, err
		}
		response, err := adminClient.GetWorkflowExecutionRawHistoryV2(ctx, &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId:     namespaceID.String(),
			Execution:       &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			EndEventId:      endEventID + 1,
			EndEventVersion: endEventVersion,
			MaximumPageSize: 1000,
			NextPageToken:   paginationToken,
		})
		if err != nil {
			return nil, nil, err
		}

		batches := make([]*rawHistoryData, 0, len(response.GetHistoryBatches()))
		for idx, blob := range response.GetHistoryBatches() {
			batches = append(batches, &rawHistoryData{
				rawHistory: blob,
				nodeID:     response.GetHistoryNodeIds()[idx],
			})
		}
		return batches, response.NextPageToken, nil
	}
}

func (r *HistoryReplicatorImpl) getHistoryFromLocalPaginationFn(
	ctx context.Context,
	branchToken []byte,
	lastEventID int64,
) collection.PaginationFn[*historypb.History] {

	return func(paginationToken []byte) ([]*historypb.History, []byte, error) {
		response, err := r.executionMgr.ReadHistoryBranchByBatch(ctx, &persistence.ReadHistoryBranchRequest{
			ShardID:       r.shard.GetShardID(),
			BranchToken:   branchToken,
			MinEventID:    common.FirstEventID,
			MaxEventID:    lastEventID + 1,
			PageSize:      100,
			NextPageToken: paginationToken,
		})
		if err != nil {
			return nil, nil, err
		}
		return slices.Clone(response.History), response.NextPageToken, nil
	}
}
