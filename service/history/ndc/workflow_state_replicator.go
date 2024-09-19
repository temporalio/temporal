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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination workflow_state_replicator_mock.go

package ndc

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	repication "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	WorkflowStateReplicator interface {
		SyncWorkflowState(
			ctx context.Context,
			request *historyservice.ReplicateWorkflowStateRequest,
		) error
		ReplicateMutableState(
			ctx context.Context,
			request *shard.ReplicateMutableStateRequest,
		) error
	}

	WorkflowStateReplicatorImpl struct {
		shardContext      shard.Context
		namespaceRegistry namespace.Registry
		workflowCache     wcache.Cache
		clusterMetadata   cluster.Metadata
		executionMgr      persistence.ExecutionManager
		historySerializer serialization.Serializer
		transactionMgr    TransactionManager
		logger            log.Logger
	}
)

func NewWorkflowStateReplicator(
	shardContext shard.Context,
	workflowCache wcache.Cache,
	eventsReapplier EventsReapplier,
	eventSerializer serialization.Serializer,
	logger log.Logger,
) *WorkflowStateReplicatorImpl {

	logger = log.With(logger, tag.ComponentWorkflowStateReplicator)
	return &WorkflowStateReplicatorImpl{
		shardContext:      shardContext,
		namespaceRegistry: shardContext.GetNamespaceRegistry(),
		workflowCache:     workflowCache,
		clusterMetadata:   shardContext.GetClusterMetadata(),
		executionMgr:      shardContext.GetExecutionManager(),
		historySerializer: eventSerializer,
		transactionMgr:    NewTransactionManager(shardContext, workflowCache, eventsReapplier, logger, false),
		logger:            logger,
	}
}

func (r *WorkflowStateReplicatorImpl) SyncWorkflowState(
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
		r.shardContext,
		namespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		locks.PriorityLow,
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
	ms, err := wfCtx.LoadMutableState(ctx, r.shardContext)
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

		// release the workflow lock here otherwise SyncHSM will deadlock
		releaseFn(nil)

		engine, err := r.shardContext.GetEngine(ctx)
		if err != nil {
			return err
		}

		// we don't care about activity state here as activity can't run after workflow is closed.
		return engine.SyncHSM(ctx, &shard.SyncHSMRequest{
			WorkflowKey: ms.GetWorkflowKey(),
			StateMachineNode: &persistencespb.StateMachineNode{
				Children: executionInfo.SubStateMachinesByType,
			},
			EventVersionHistory: incomingVersionHistory,
		})
	default:
		return err
	}
	return r.applySnapshotWhenWorkflowNotExist(ctx, namespaceID, wid, rid, wfCtx, releaseFn, request.GetWorkflowState(), request.RemoteCluster, nil)
}

func (r *WorkflowStateReplicatorImpl) ReplicateMutableState(
	ctx context.Context,
	request *shard.ReplicateMutableStateRequest,
) (retError error) {
	if request.Snapshot == nil && request.Mutation == nil {
		return serviceerror.NewInvalidArgument("both snapshot and mutation are nil")
	}
	executionState, executionInfo := func() (*persistencespb.WorkflowExecutionState, *persistencespb.WorkflowExecutionInfo) {
		if request.Snapshot != nil {
			return request.Snapshot.ExecutionState, request.Snapshot.ExecutionInfo
		}
		return request.Mutation.ExecutionState, request.Mutation.ExecutionInfo
	}()

	namespaceID := namespace.ID(executionInfo.GetNamespaceId())
	wid := executionInfo.GetWorkflowId()
	rid := executionState.GetRunId()

	wfCtx, releaseFn, err := r.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		r.shardContext,
		namespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		locks.PriorityLow,
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

	ms, err := wfCtx.LoadMutableState(ctx, r.shardContext)
	switch err.(type) {
	case *serviceerror.NotFound:
		return r.applySnapshot(ctx, namespaceID, wid, rid, wfCtx, releaseFn, nil, request)
	case nil:
		localTransitionHistory := ms.GetExecutionInfo().TransitionHistory
		if len(localTransitionHistory) == 0 {
			// This could happen when versioned transition feature is just enabled
			// TODO: Direct sync snapshot may bring current state to an older version.
			// fix this case as part of rollout/rollback plan
			return r.applySnapshot(ctx, namespaceID, wid, rid, wfCtx, releaseFn, ms, request)
		}

		sourceTransitionHistory := executionInfo.TransitionHistory
		err = workflow.TransitionHistoryStalenessCheck(localTransitionHistory, sourceTransitionHistory[len(sourceTransitionHistory)-1])
		switch err {
		case nil:
			// verify tasks
		case consts.ErrStaleState:
			// local is stale, try to apply mutable state update
			if request.Snapshot != nil {
				return r.applySnapshot(ctx, namespaceID, wid, rid, wfCtx, releaseFn, ms, request)
			}
			return r.applyMutation(ctx, namespaceID, wid, rid, wfCtx, ms, releaseFn, request)
		case consts.ErrStaleReference:
			// backfill history
		default:
			return err
		}
	default:
		return err
	}
	return nil
}

func (r *WorkflowStateReplicatorImpl) applyMutation(
	ctx context.Context,
	namespaceId namespace.ID,
	workflowId string,
	runId string,
	wfCtx workflow.Context,
	localMutableState workflow.MutableState,
	releaseFn wcache.ReleaseCacheFunc,
	request *shard.ReplicateMutableStateRequest,
) error {
	if request.Mutation == nil {
		return serviceerror.NewInvalidArgument("mutation is nil")
	}
	if localMutableState == nil {
		return serviceerrors.NewSyncState(
			"failed to apply mutation due to missing mutable state",
			namespaceId.String(),
			workflowId,
			runId,
			nil,
		)
	}
	localTransitionHistory := localMutableState.GetExecutionInfo().TransitionHistory
	sourceTransitionHistory := request.Mutation.ExecutionInfo.TransitionHistory

	// make sure mutation range is extension of local range
	if workflow.TransitionHistoryStalenessCheck(localTransitionHistory, request.ExclusiveStartedVersionTransition) != nil ||
		workflow.TransitionHistoryStalenessCheck(sourceTransitionHistory, localTransitionHistory[len(localTransitionHistory)-1]) != nil {
		return serviceerrors.NewSyncState(
			fmt.Sprintf("Failed to apply mutation due to version check failed. local transition history: %v, source transition history: %v", localTransitionHistory, sourceTransitionHistory),
			namespaceId.String(),
			workflowId,
			runId,
			localTransitionHistory[len(localTransitionHistory)-1],
		)
	}

	err := r.bringLocalEventsUpToSourceCurrentBranch(
		ctx,
		namespaceId,
		workflowId,
		runId,
		request.SourceClusterName,
		wfCtx,
		localMutableState,
		request.Mutation.ExecutionInfo.VersionHistories,
		request.EventBlobs,
	)
	if err != nil {
		return err
	}
	// TODO: localMutableState.ApplyMutation

	var newRunWorkflow Workflow
	if request.NewRunInfo != nil {
		newRunWorkflow, err = r.getNewRunWorkflow(ctx, namespaceId, workflowId, localMutableState, request)
		if err != nil {
			return err
		}
	}
	targetWorkflow := NewWorkflow(
		r.clusterMetadata,
		wfCtx,
		localMutableState,
		releaseFn,
	)
	return r.transactionMgr.UpdateWorkflow(
		ctx,
		false,
		targetWorkflow,
		newRunWorkflow,
	)
}

func (r *WorkflowStateReplicatorImpl) applySnapshot(
	ctx context.Context,
	namespaceId namespace.ID,
	workflowId string,
	runId string,
	wfCtx workflow.Context,
	releaseFn wcache.ReleaseCacheFunc,
	localMutableState workflow.MutableState,
	request *shard.ReplicateMutableStateRequest,
) error {
	if request.Snapshot == nil {
		return serviceerrors.NewSyncState(
			"failed to apply mutation due to missing mutable state",
			namespaceId.String(),
			workflowId,
			runId,
			nil,
		)
	}
	if localMutableState == nil {
		return r.applySnapshotWhenWorkflowNotExist(ctx, namespaceId, workflowId, runId, wfCtx, releaseFn, request.Snapshot, request.SourceClusterName, request.NewRunInfo)
	}
	var isBranchSwitched bool
	localTransitionHistory := localMutableState.GetExecutionInfo().TransitionHistory
	sourceTransitionHistory := request.Snapshot.ExecutionInfo.TransitionHistory
	err := workflow.TransitionHistoryStalenessCheck(sourceTransitionHistory, localTransitionHistory[len(localTransitionHistory)-1])
	switch err {
	case nil:
		// no branch switch
	case consts.ErrStaleState:
		return serviceerrors.NewSyncState(
			fmt.Sprintf("apply snapshot encountered stale snapshot. local transition history: %v, source transition history: %v", localTransitionHistory, sourceTransitionHistory),
			namespaceId.String(),
			workflowId,
			runId,
			localTransitionHistory[len(localTransitionHistory)-1],
		)
	case consts.ErrStaleReference:
		// local versioned transition is stale
		isBranchSwitched = true
	default:
		return err
	}

	err = r.bringLocalEventsUpToSourceCurrentBranch(
		ctx,
		namespaceId,
		workflowId,
		runId,
		request.SourceClusterName,
		wfCtx,
		localMutableState,
		request.Snapshot.ExecutionInfo.VersionHistories,
		request.EventBlobs,
	)
	if err != nil {
		return err
	}
	// Todo: localMutableState.ApplySnapshot

	var newRunWorkflow Workflow
	if request.NewRunInfo != nil {
		newRunWorkflow, err = r.getNewRunWorkflow(ctx, namespaceId, workflowId, localMutableState, request)
		if err != nil {
			return err
		}
	}
	targetWorkflow := NewWorkflow(
		r.clusterMetadata,
		wfCtx,
		localMutableState,
		releaseFn,
	)

	return r.transactionMgr.UpdateWorkflow(
		ctx,
		isBranchSwitched,
		targetWorkflow,
		newRunWorkflow,
	)
}

func (r *WorkflowStateReplicatorImpl) getNewRunWorkflow(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	originalMutableState workflow.MutableState,
	request *shard.ReplicateMutableStateRequest,
) (Workflow, error) {
	newMutableState, err := r.getNewRunMutableState(ctx, namespaceID, workflowID, request.NewRunInfo.GetRunId(), originalMutableState, request.NewRunInfo.EventBatch)
	if err != nil {
		return nil, err
	}
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

	return NewWorkflow(
		r.clusterMetadata,
		newContext,
		newMutableState,
		wcache.NoopReleaseFn,
	), nil
}

func (r *WorkflowStateReplicatorImpl) getNewRunMutableState(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	newRunID string,
	originalMutableState workflow.MutableState,
	newRunEventsBlob *commonpb.DataBlob,
) (workflow.MutableState, error) {
	newRunHistory, err := r.historySerializer.DeserializeEvents(newRunEventsBlob)
	if err != nil {
		return nil, err
	}
	sameWorkflowChain := false
	newRunFirstEvent := newRunHistory[0]
	if newRunFirstEvent.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
		newRunFirstRunID := newRunFirstEvent.GetWorkflowExecutionStartedEventAttributes().FirstExecutionRunId
		sameWorkflowChain = newRunFirstRunID == originalMutableState.GetExecutionInfo().FirstExecutionRunId
	}

	var newRunMutableState workflow.MutableState
	if sameWorkflowChain {
		newRunMutableState, err = workflow.NewMutableStateInChain(
			r.shardContext,
			r.shardContext.GetEventsCache(),
			r.logger,
			originalMutableState.GetNamespaceEntry(),
			workflowID,
			newRunID,
			timestamp.TimeValue(newRunHistory[0].GetEventTime()),
			originalMutableState,
		)
		if err != nil {
			return nil, err
		}
	} else {
		newRunMutableState = workflow.NewMutableState(
			r.shardContext,
			r.shardContext.GetEventsCache(),
			r.logger,
			originalMutableState.GetNamespaceEntry(),
			workflowID,
			newRunID,
			timestamp.TimeValue(newRunHistory[0].GetEventTime()),
		)
	}

	newRunStateBuilder := workflow.NewMutableStateRebuilder(r.shardContext, r.logger, newRunMutableState)

	_, err = newRunStateBuilder.ApplyEvents(
		ctx,
		namespaceID,
		uuid.New(),
		&commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      newRunID,
		},
		[][]*historypb.HistoryEvent{newRunHistory},
		nil,
		"",
	)
	if err != nil {
		return nil, err
	}

	return newRunMutableState, nil
}

// this function does not handle reset case, so it should only be used for the case where workflow run is found at local cluster
func (r *WorkflowStateReplicatorImpl) bringLocalEventsUpToSourceCurrentBranch(
	ctx context.Context,
	namespaceId namespace.ID,
	workflowId string,
	runId string,
	sourceClusterName string,
	wfCtx workflow.Context,
	localMutableState workflow.MutableState,
	sourceVersionHistories *history.VersionHistories,
	eventBlobs []*commonpb.DataBlob,
) error {
	sourceVersionHistory, err := versionhistory.GetCurrentVersionHistory(sourceVersionHistories)
	if err != nil {
		return err
	}
	localVersionHistories := localMutableState.GetExecutionInfo().GetVersionHistories()
	index, isNewBranch, err := func() (int32, bool, error) {
		lcaItem, index, err := versionhistory.FindLCAVersionHistoryItemAndIndex(localMutableState.GetExecutionInfo().VersionHistories, sourceVersionHistory)
		if err != nil {
			return 0, false, err
		}

		localHistory, err := versionhistory.GetVersionHistory(localVersionHistories, index)
		if err != nil {
			return 0, false, err
		}
		if versionhistory.IsLCAVersionHistoryItemAppendable(localHistory, lcaItem) {
			return index, false, nil
		}
		newVersionHistory, err := versionhistory.CopyVersionHistoryUntilLCAVersionHistoryItem(localHistory, lcaItem)
		if err != nil {
			return 0, false, err
		}
		branchManager := NewBranchMgr(r.shardContext, wfCtx, localMutableState, r.logger)
		newVersionHistoryIndex, err := branchManager.createNewBranch(
			ctx,
			localHistory.GetBranchToken(),
			lcaItem.GetEventId(),
			newVersionHistory,
		)
		if err != nil {
			return 0, false, err
		}

		return newVersionHistoryIndex, true, nil
	}()

	if err != nil {
		return err
	}
	versionHistoryToAppend, err := versionhistory.GetVersionHistory(localVersionHistories, index)
	if err != nil {
		return err
	}
	localLastItem, err := versionhistory.GetLastVersionHistoryItem(versionHistoryToAppend)
	if err != nil {
		return err
	}
	sourceLastItem, err := versionhistory.GetLastVersionHistoryItem(sourceVersionHistory)
	if err != nil {
		return err
	}
	if versionhistory.IsEqualVersionHistoryItem(localLastItem, sourceLastItem) {
		return nil
	}

	startEventId := localLastItem.GetEventId()
	startEventVersion := localLastItem.GetVersion()
	endEventId := sourceLastItem.GetEventId()
	endEventVersion := sourceLastItem.GetVersion()
	var historyEvents [][]*historypb.HistoryEvent
	for _, blob := range eventBlobs {
		events, err := r.historySerializer.DeserializeEvents(blob)
		if err != nil {
			return err
		}
		historyEvents = append(historyEvents, events)
	}

	prevTxnID := common.EmptyEventTaskID
	fetchFromRemoteAndAppend := func(
		startId, // exclusive
		startVersion,
		endId, // inclusive
		endVersion int64) error {
		remoteHistoryIterator := collection.NewPagingIterator(r.getHistoryFromRemotePaginationFn(
			ctx,
			sourceClusterName,
			namespaceId,
			workflowId,
			runId,
			startId,
			startVersion,
			endId,
			endVersion),
		)
		for remoteHistoryIterator.HasNext() {
			historyBlob, err := remoteHistoryIterator.Next()
			if err != nil {
				return err
			}

			txnID, err := r.shardContext.GenerateTaskID()
			if err != nil {
				return err
			}
			_, err = r.executionMgr.AppendRawHistoryNodes(ctx, &persistence.AppendRawHistoryNodesRequest{
				ShardID:           r.shardContext.GetShardID(),
				IsNewBranch:       isNewBranch,
				BranchToken:       versionHistoryToAppend.BranchToken,
				History:           historyBlob.rawHistory,
				PrevTransactionID: prevTxnID,
				TransactionID:     txnID,
				NodeID:            historyBlob.nodeID,
				Info: persistence.BuildHistoryGarbageCleanupInfo(
					namespaceId.String(),
					workflowId,
					runId,
				),
			})
			if err != nil {
				return err
			}
			prevTxnID = txnID
			isNewBranch = false
		}
		return nil
	}
	// Fill the gap between local last event and request's first event
	if historyEvents[0][0].EventId > startEventId+1 {
		err := fetchFromRemoteAndAppend(localLastItem.EventId, localLastItem.Version, historyEvents[0][0].EventId, historyEvents[0][0].Version)
		if err != nil {
			return err
		}
		startEventId = historyEvents[0][0].EventId
		startEventVersion = historyEvents[0][0].Version
	}

	// add events from request
	for _, events := range historyEvents {
		if events[0].EventId <= startEventId {
			continue
		}
		txnID, err := r.shardContext.GenerateTaskID()
		if err != nil {
			return err
		}
		_, err = r.executionMgr.AppendHistoryNodes(ctx, &persistence.AppendHistoryNodesRequest{
			ShardID:           r.shardContext.GetShardID(),
			IsNewBranch:       isNewBranch,
			BranchToken:       versionHistoryToAppend.BranchToken,
			PrevTransactionID: prevTxnID,
			TransactionID:     txnID,
			Events:            events,
			Info: persistence.BuildHistoryGarbageCleanupInfo(
				namespaceId.String(),
				workflowId,
				runId,
			),
		})
		if err != nil {
			return err
		}
		prevTxnID = txnID
		isNewBranch = false
		startEventId = events[len(events)-1].EventId
		startEventVersion = events[len(events)-1].Version
	}
	// add more events if there is any
	if startEventId < endEventId {
		err = fetchFromRemoteAndAppend(startEventId, startEventVersion, endEventId, endEventVersion)
		if err != nil {
			return err
		}
	}
	versionHistoryToAppend.Items = versionhistory.CopyVersionHistoryItems(sourceVersionHistory.Items)
	return nil
}

func (r *WorkflowStateReplicatorImpl) applySnapshotWhenWorkflowNotExist(
	ctx context.Context,
	namespaceId namespace.ID,
	workflowId string,
	runId string,
	wfCtx workflow.Context,
	releaseFn wcache.ReleaseCacheFunc,
	sourceMutableState *persistencespb.WorkflowMutableState,
	sourceCluster string,
	newRunInfo *repication.NewRunInfo,
) error {
	executionInfo := sourceMutableState.ExecutionInfo
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(executionInfo.VersionHistories)
	if err != nil {
		return err
	}
	lastEventItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return err
	}

	// The following sanitizes the branch token from the source cluster to this target cluster by re-initializing it.

	branchInfo, err := r.shardContext.GetExecutionManager().GetHistoryBranchUtil().ParseHistoryBranchInfo(
		currentVersionHistory.GetBranchToken(),
	)
	if err != nil {
		return err
	}
	newHistoryBranchToken, err := r.shardContext.GetExecutionManager().GetHistoryBranchUtil().NewHistoryBranch(
		namespaceId.String(),
		workflowId,
		runId,
		branchInfo.GetTreeId(),
		&branchInfo.BranchId,
		branchInfo.Ancestors,
		time.Duration(0),
		time.Duration(0),
		time.Duration(0),
	)
	if err != nil {
		return err
	}

	lastFirstTxnID, err := r.backfillHistory(
		ctx,
		sourceCluster,
		namespaceId,
		workflowId,
		runId,
		// TODO: The original run id is in the workflow started history event but not in mutable state.
		// Use the history tree id to be the original run id.
		// https://github.com/temporalio/temporal/issues/6501
		branchInfo.GetTreeId(),
		lastEventItem.GetEventId(),
		lastEventItem.GetVersion(),
		newHistoryBranchToken,
	)
	if err != nil {
		return err
	}

	ns, err := r.namespaceRegistry.GetNamespaceByID(namespaceId)
	if err != nil {
		return err
	}

	mutableState, err := workflow.NewSanitizedMutableState(
		r.shardContext,
		r.shardContext.GetEventsCache(),
		r.logger,
		ns,
		sourceMutableState,
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
	if newRunInfo != nil {
		err := func() error {
			newRunWfContext, newRunReleaseFn, newRunErr := r.workflowCache.GetOrCreateWorkflowExecution(
				ctx,
				r.shardContext,
				namespaceId,
				&commonpb.WorkflowExecution{
					WorkflowId: workflowId,
					RunId:      newRunInfo.GetRunId(),
				},
				locks.PriorityHigh,
			)
			if newRunErr != nil {
				return newRunErr
			}
			defer func() {
				if rec := recover(); rec != nil {
					newRunReleaseFn(errPanic)
					panic(rec)
				} else {
					newRunReleaseFn(newRunErr)
				}
			}()
			_, newRunErr = newRunWfContext.LoadMutableState(ctx, r.shardContext)
			switch newRunErr.(type) {
			case nil:
				return nil
			case *serviceerror.NotFound:
				newRunMutableState, err := r.getNewRunMutableState(
					ctx,
					namespaceId,
					workflowId,
					newRunInfo.GetRunId(),
					mutableState,
					newRunInfo.EventBatch,
				)
				if err != nil {
					return err
				}
				return r.transactionMgr.CreateWorkflow(
					ctx,
					NewWorkflow(
						r.clusterMetadata,
						newRunWfContext,
						newRunMutableState,
						newRunReleaseFn,
					),
				)
			default:
				return newRunErr
			}
		}()
		if err != nil {
			return err
		}
	}

	taskRefresher := workflow.NewTaskRefresher(r.shardContext)
	err = taskRefresher.Refresh(ctx, mutableState)
	if err != nil {
		return err
	}
	return r.transactionMgr.CreateWorkflow(
		ctx,
		NewWorkflow(
			r.clusterMetadata,
			wfCtx,
			mutableState,
			releaseFn,
		),
	)
}

func (r *WorkflowStateReplicatorImpl) backfillHistory(
	ctx context.Context,
	remoteClusterName string,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	originalRunID string,
	lastEventID int64,
	lastEventVersion int64,
	branchToken []byte,
) (taskID int64, retError error) {

	if runID != originalRunID {
		// At this point, it already acquired the workflow lock on the run ID.
		// Get the lock of root run id to make sure no concurrent backfill history across multiple runs.
		_, rootRunReleaseFn, err := r.workflowCache.GetOrCreateWorkflowExecution(
			ctx,
			r.shardContext,
			namespaceID,
			&commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      originalRunID,
			},
			locks.PriorityLow,
		)
		if err != nil {
			return common.EmptyEventTaskID, err
		}
		defer func() {
			if rec := recover(); rec != nil {
				rootRunReleaseFn(errPanic)
				panic(rec)
			}
			rootRunReleaseFn(retError)
		}()
	}

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
			return common.EmptyEventTaskID, err
		}
	}

	remoteHistoryIterator := collection.NewPagingIterator(r.getHistoryFromRemotePaginationFn(
		ctx,
		remoteClusterName,
		namespaceID,
		workflowID,
		runID,
		common.EmptyEventID,
		common.EmptyVersion,
		lastEventID,
		lastEventVersion),
	)
	historyBranchUtil := r.executionMgr.GetHistoryBranchUtil()
	historyBranch, err := historyBranchUtil.ParseHistoryBranchInfo(branchToken)
	if err != nil {
		return common.EmptyEventTaskID, err
	}

	prevTxnID := common.EmptyEventTaskID
	var prevBranchID string
	sortedAncestors := sortAncestors(historyBranch.GetAncestors())
	sortedAncestorsIdx := 0
	var ancestors []*persistencespb.HistoryBranchRange

BackfillLoop:
	for remoteHistoryIterator.HasNext() {
		historyBlob, err := remoteHistoryIterator.Next()
		if err != nil {
			return common.EmptyEventTaskID, err
		}

		if historyBlob.nodeID <= lastBatchNodeID {
			// The history batch already in DB.
			if len(sortedAncestors) > sortedAncestorsIdx {
				currentAncestor := sortedAncestors[sortedAncestorsIdx]

				if historyBlob.nodeID >= currentAncestor.GetEndNodeId() {
					// Update ancestor.
					ancestors = append(ancestors, currentAncestor)
					sortedAncestorsIdx++
				}
			}
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
					return common.EmptyEventTaskID, serviceerror.NewInternal(
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
			runID,
		)
		if err != nil {
			return common.EmptyEventTaskID, err
		}
		txnID, err := r.shardContext.GenerateTaskID()
		if err != nil {
			return common.EmptyEventTaskID, err
		}
		_, err = r.executionMgr.AppendRawHistoryNodes(ctx, &persistence.AppendRawHistoryNodesRequest{
			ShardID:           r.shardContext.GetShardID(),
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
			return common.EmptyEventTaskID, err
		}
		prevTxnID = txnID
		prevBranchID = branchID
	}

	return prevTxnID, nil
}

func (r *WorkflowStateReplicatorImpl) getHistoryFromLocalPaginationFn(
	ctx context.Context,
	branchToken []byte,
	lastEventID int64,
) collection.PaginationFn[*historypb.History] {

	return func(paginationToken []byte) ([]*historypb.History, []byte, error) {
		response, err := r.executionMgr.ReadHistoryBranchByBatch(ctx, &persistence.ReadHistoryBranchRequest{
			ShardID:       r.shardContext.GetShardID(),
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

func (r *WorkflowStateReplicatorImpl) getHistoryFromRemotePaginationFn(
	ctx context.Context,
	remoteClusterName string,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	startEventID int64,
	startEventVersion int64,
	endEventID int64,
	endEventVersion int64,
) collection.PaginationFn[*rawHistoryData] {

	return func(paginationToken []byte) ([]*rawHistoryData, []byte, error) {

		adminClient, err := r.shardContext.GetRemoteAdminClient(remoteClusterName)
		if err != nil {
			return nil, nil, err
		}
		response, err := adminClient.GetWorkflowExecutionRawHistoryV2(ctx, &adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId:       namespaceID.String(),
			Execution:         &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			StartEventId:      startEventID,
			StartEventVersion: startEventVersion,
			EndEventId:        endEventID + 1,
			EndEventVersion:   endEventVersion,
			MaximumPageSize:   1000,
			NextPageToken:     paginationToken,
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
