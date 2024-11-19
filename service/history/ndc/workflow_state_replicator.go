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
	"errors"
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
	"go.temporal.io/server/service/history/historybuilder"
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
		ReplicateVersionedTransition(
			ctx context.Context,
			versionedTransition *repication.VersionedTransitionArtifact,
			sourceClusterName string,
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
		taskRefresher     workflow.TaskRefresher
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
		taskRefresher:     workflow.NewTaskRefresher(shardContext),
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
		}
		releaseFn(retError)
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
	return r.applySnapshotWhenWorkflowNotExist(ctx, namespaceID, wid, rid, wfCtx, releaseFn, request.GetWorkflowState(), request.RemoteCluster, nil, false)
}

//nolint:revive // cognitive complexity 37 (> max enabled 25)
func (r *WorkflowStateReplicatorImpl) ReplicateVersionedTransition(
	ctx context.Context,
	versionedTransition *repication.VersionedTransitionArtifact,
	sourceClusterName string,
) (retError error) {
	if versionedTransition.StateAttributes == nil {
		return serviceerror.NewInvalidArgument("both snapshot and mutation are nil")
	}
	var mutation *repication.SyncWorkflowStateMutationAttributes
	var snapshot *repication.SyncWorkflowStateSnapshotAttributes
	switch artifactType := versionedTransition.StateAttributes.(type) {
	case *repication.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes:
		snapshot = versionedTransition.GetSyncWorkflowStateSnapshotAttributes()
	case *repication.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes:
		mutation = versionedTransition.GetSyncWorkflowStateMutationAttributes()
	default:
		return serviceerror.NewInvalidArgument(fmt.Sprintf("unknown artifact type %T", artifactType))
	}
	executionState, executionInfo := func() (*persistencespb.WorkflowExecutionState, *persistencespb.WorkflowExecutionInfo) {
		if snapshot != nil {
			return snapshot.State.ExecutionState, snapshot.State.ExecutionInfo
		}
		return mutation.StateMutation.ExecutionState, mutation.StateMutation.ExecutionInfo
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
		}
		releaseFn(retError)
	}()

	ms, err := wfCtx.LoadMutableState(ctx, r.shardContext)
	switch err.(type) {
	case *serviceerror.NotFound:
		return r.applySnapshot(ctx, namespaceID, wid, rid, wfCtx, releaseFn, nil, versionedTransition, sourceClusterName)
	case nil:
		localTransitionHistory := ms.GetExecutionInfo().TransitionHistory
		if len(localTransitionHistory) == 0 {
			// This could happen when versioned transition feature is just enabled
			// TODO: Revisit these logic when working on roll out/back plan
			if snapshot == nil {
				return serviceerrors.NewSyncState(
					"failed to apply mutation due to missing mutable state",
					namespaceID.String(),
					wid,
					rid,
					nil,
					ms.GetExecutionInfo().VersionHistories,
				)
			}
			localCurrentHistory, err := versionhistory.GetCurrentVersionHistory(ms.GetExecutionInfo().VersionHistories)
			if err != nil {
				return err
			}
			localLastHistoryItem, err := versionhistory.GetLastVersionHistoryItem(localCurrentHistory)
			if err != nil {
				return err
			}
			sourceCurrentHistory, err := versionhistory.GetCurrentVersionHistory(executionInfo.VersionHistories)
			if err != nil {
				return err
			}
			sourceLastHistoryItem, err := versionhistory.GetLastVersionHistoryItem(sourceCurrentHistory)
			if err != nil {
				return err
			}
			sourceTransitionHistory := executionInfo.TransitionHistory
			localLastWriteVersion, err := ms.GetLastWriteVersion()
			if err != nil {
				return err
			}
			sourceLastWriteVersion := sourceTransitionHistory[len(sourceTransitionHistory)-1].NamespaceFailoverVersion

			if localLastWriteVersion > sourceLastWriteVersion {
				// local is newer, try backfill events
				return r.backFillEvents(ctx, namespaceID, wid, rid, executionInfo.VersionHistories, versionedTransition.EventBatches, versionedTransition.NewRunInfo, sourceClusterName, sourceTransitionHistory[len(sourceTransitionHistory)-1])
			}
			if localLastWriteVersion < sourceLastWriteVersion ||
				localLastHistoryItem.GetEventId() <= sourceLastHistoryItem.EventId {
				return r.applySnapshot(ctx, namespaceID, wid, rid, wfCtx, releaseFn, ms, versionedTransition, sourceClusterName)
			}
			return nil
		}

		sourceTransitionHistory := executionInfo.TransitionHistory
		err = workflow.TransitionHistoryStalenessCheck(localTransitionHistory, sourceTransitionHistory[len(sourceTransitionHistory)-1])
		switch {
		case err == nil:
			// verify tasks
		case errors.Is(err, consts.ErrStaleState):
			// local is stale, try to apply mutable state update
			if snapshot != nil {
				return r.applySnapshot(ctx, namespaceID, wid, rid, wfCtx, releaseFn, ms, versionedTransition, sourceClusterName)
			}
			return r.applyMutation(ctx, namespaceID, wid, rid, wfCtx, ms, releaseFn, versionedTransition, sourceClusterName)
		case errors.Is(err, consts.ErrStaleReference):
			return r.backFillEvents(ctx, namespaceID, wid, rid, executionInfo.VersionHistories, versionedTransition.EventBatches, versionedTransition.NewRunInfo, sourceClusterName, sourceTransitionHistory[len(sourceTransitionHistory)-1])
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
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	wfCtx workflow.Context,
	localMutableState workflow.MutableState,
	releaseFn wcache.ReleaseCacheFunc,
	versionedTransition *repication.VersionedTransitionArtifact,
	sourceClusterName string,
) error {
	mutation := versionedTransition.GetSyncWorkflowStateMutationAttributes()
	if mutation == nil {
		return serviceerror.NewInvalidArgument("mutation is nil")
	}
	if localMutableState == nil {
		return serviceerrors.NewSyncState(
			"failed to apply mutation due to missing mutable state",
			namespaceID.String(),
			workflowID,
			runID,
			nil,
			nil,
		)
	}
	localTransitionHistory := workflow.CopyVersionedTransitions(localMutableState.GetExecutionInfo().TransitionHistory)
	sourceTransitionHistory := mutation.StateMutation.ExecutionInfo.TransitionHistory

	// make sure mutation range is extension of local range
	if workflow.TransitionHistoryStalenessCheck(localTransitionHistory, mutation.ExclusiveStartVersionedTransition) != nil ||
		workflow.TransitionHistoryStalenessCheck(sourceTransitionHistory, localTransitionHistory[len(localTransitionHistory)-1]) != nil {
		return serviceerrors.NewSyncState(
			fmt.Sprintf("Failed to apply mutation due to version check failed. local transition history: %v, source transition history: %v", localTransitionHistory, sourceTransitionHistory),
			namespaceID.String(),
			workflowID,
			runID,
			localTransitionHistory[len(localTransitionHistory)-1],
			localMutableState.GetExecutionInfo().VersionHistories,
		)
	}

	err := r.bringLocalEventsUpToSourceCurrentBranch(
		ctx,
		namespaceID,
		workflowID,
		runID,
		sourceClusterName,
		wfCtx,
		localMutableState,
		mutation.StateMutation.ExecutionInfo.VersionHistories,
		versionedTransition.EventBatches,
	)
	if err != nil {
		return err
	}
	err = localMutableState.ApplyMutation(mutation.StateMutation)
	if err != nil {
		return err
	}

	var newRunWorkflow Workflow
	if versionedTransition.NewRunInfo != nil {
		newRunWorkflow, err = r.getNewRunWorkflow(ctx, namespaceID, workflowID, localMutableState, versionedTransition.NewRunInfo)
		if err != nil {
			return err
		}
	}

	err = r.taskRefresher.PartialRefresh(ctx, localMutableState, localTransitionHistory[len(localTransitionHistory)-1])
	if err != nil {
		return err
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
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	wfCtx workflow.Context,
	releaseFn wcache.ReleaseCacheFunc,
	localMutableState workflow.MutableState,
	versionedTransition *repication.VersionedTransitionArtifact,
	sourceClusterName string,
) error {
	attribute := versionedTransition.GetSyncWorkflowStateSnapshotAttributes()
	if attribute == nil || attribute.State == nil {
		var versionHistories *history.VersionHistories
		if localMutableState != nil {
			versionHistories = localMutableState.GetExecutionInfo().VersionHistories
		}
		return serviceerrors.NewSyncState(
			"failed to apply mutation due to missing mutable state",
			namespaceID.String(),
			workflowID,
			runID,
			nil,
			versionHistories,
		)
	}
	snapshot := attribute.State
	if localMutableState == nil {
		return r.applySnapshotWhenWorkflowNotExist(ctx, namespaceID, workflowID, runID, wfCtx, releaseFn, snapshot, sourceClusterName, versionedTransition.NewRunInfo, true)
	}
	var isBranchSwitched bool
	var localTransitionHistory []*persistencespb.VersionedTransition
	if len(localMutableState.GetExecutionInfo().TransitionHistory) != 0 {
		localTransitionHistory = workflow.CopyVersionedTransitions(localMutableState.GetExecutionInfo().TransitionHistory)
		sourceTransitionHistory := snapshot.ExecutionInfo.TransitionHistory
		err := workflow.TransitionHistoryStalenessCheck(sourceTransitionHistory, localTransitionHistory[len(localTransitionHistory)-1])
		switch {
		case err == nil:
			// no branch switch
		case errors.Is(err, consts.ErrStaleState):
			return serviceerrors.NewSyncState(
				fmt.Sprintf("apply snapshot encountered stale snapshot. local transition history: %v, source transition history: %v", localTransitionHistory, sourceTransitionHistory),
				namespaceID.String(),
				workflowID,
				runID,
				localTransitionHistory[len(localTransitionHistory)-1],
				localMutableState.GetExecutionInfo().VersionHistories,
			)
		case errors.Is(err, consts.ErrStaleReference):
			// local versioned transition is stale
			isBranchSwitched = true
		default:
			return err
		}
	} else {
		localCurrentHistory, err := versionhistory.GetCurrentVersionHistory(localMutableState.GetExecutionInfo().VersionHistories)
		if err != nil {
			return err
		}
		sourceCurrentHistory, err := versionhistory.GetCurrentVersionHistory(snapshot.ExecutionInfo.VersionHistories)
		if err != nil {
			return err
		}
		if !versionhistory.IsVersionHistoryItemsInSameBranch(localCurrentHistory.Items, sourceCurrentHistory.Items) {
			isBranchSwitched = true
		}
	}

	err := r.bringLocalEventsUpToSourceCurrentBranch(
		ctx,
		namespaceID,
		workflowID,
		runID,
		sourceClusterName,
		wfCtx,
		localMutableState,
		snapshot.ExecutionInfo.VersionHistories,
		versionedTransition.EventBatches,
	)
	if err != nil {
		return err
	}

	err = localMutableState.ApplySnapshot(snapshot)
	if err != nil {
		return err
	}
	localMutableState.PopTasks() // tasks are refreshed manually below

	var newRunWorkflow Workflow
	if versionedTransition.NewRunInfo != nil {
		newRunWorkflow, err = r.getNewRunWorkflow(ctx, namespaceID, workflowID, localMutableState, versionedTransition.NewRunInfo)
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
	if isBranchSwitched || len(localTransitionHistory) == 0 {
		// TODO: If branch switched, maybe refresh from LCA?
		err = r.taskRefresher.Refresh(ctx, localMutableState)
		if err != nil {
			return err
		}
	} else {
		err = r.taskRefresher.PartialRefresh(ctx, localMutableState, localTransitionHistory[len(localTransitionHistory)-1])
		if err != nil {
			return err
		}
	}

	return r.transactionMgr.UpdateWorkflow(
		ctx,
		isBranchSwitched,
		targetWorkflow,
		newRunWorkflow,
	)
}

func (r *WorkflowStateReplicatorImpl) backFillEvents(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	sourceVersionHistories *history.VersionHistories,
	eventBatches []*commonpb.DataBlob,
	newRunInfo *repication.NewRunInfo,
	sourceClusterName string,
	destinationVersionedTransition *persistencespb.VersionedTransition,
) error {
	engine, err := r.shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	sourceCurrentVersionHistory, err := versionhistory.GetCurrentVersionHistory(sourceVersionHistories)
	if err != nil {
		return err
	}
	var events [][]*historypb.HistoryEvent
	for _, blob := range eventBatches {
		e, err := r.historySerializer.DeserializeEvents(blob)
		if err != nil {
			return err
		}
		events = append(events, e)
	}
	var newRunEvents []*historypb.HistoryEvent
	var newRunID string
	if newRunInfo != nil {
		newRunEvents, err = r.historySerializer.DeserializeEvents(newRunInfo.EventBatch)
		if err != nil {
			return err
		}
		newRunID = newRunInfo.RunId
	}
	return engine.BackfillHistoryEvents(ctx, &shard.BackfillHistoryEventsRequest{
		WorkflowKey:         definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		SourceClusterName:   sourceClusterName,
		VersionedHistory:    destinationVersionedTransition,
		VersionHistoryItems: sourceCurrentVersionHistory.Items,
		Events:              events,
		NewEvents:           newRunEvents,
		NewRunID:            newRunID,
	})
}

func (r *WorkflowStateReplicatorImpl) getNewRunWorkflow(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	originalMutableState workflow.MutableState,
	newRunInfo *repication.NewRunInfo,
) (Workflow, error) {
	// TODO: Refactor. Copied from mutableStateRebuilder.applyNewRunHistory
	newMutableState, err := r.getNewRunMutableState(ctx, namespaceID, workflowID, newRunInfo.RunId, originalMutableState, newRunInfo.EventBatch, true)
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
	isStateBased bool,
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

	if isStateBased {
		newRunMutableState.InitTransitionHistory()
	}

	return newRunMutableState, nil
}

// this function does not handle reset case, so it should only be used for the case where workflow run is found at local cluster
// TODO: Future improvement:
// we may need to some checkpoint mechanism for backfilling to handle large histories.
// One idea can be: create a temp branch in version histories and use that to record how many events have been backfilled.
//
//nolint:revive // cognitive complexity 27 (> max enabled 25)
func (r *WorkflowStateReplicatorImpl) bringLocalEventsUpToSourceCurrentBranch(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
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
	localVersionHistories.CurrentVersionHistoryIndex = index
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

	startEventID := localLastItem.GetEventId() // exclusive
	startEventVersion := localLastItem.GetVersion()
	endEventID := sourceLastItem.GetEventId() // inclusive
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
		startID, // exclusive
		startVersion,
		endID, // exclusive
		endVersion int64) error {
		remoteHistoryIterator := collection.NewPagingIterator(r.getHistoryFromRemotePaginationFn(
			ctx,
			sourceClusterName,
			namespaceID,
			workflowID,
			runID,
			startID,
			startVersion,
			endID,
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
					namespaceID.String(),
					workflowID,
					runID,
				),
			})
			if err != nil {
				return err
			}
			prevTxnID = txnID
			isNewBranch = false
			localMutableState.GetExecutionInfo().ExecutionStats.HistorySize += int64(len(historyBlob.rawHistory.Data))
		}
		return nil
	}
	// Fill the gap between local last event and request's first event
	if len(historyEvents) > 0 && historyEvents[0][0].EventId > startEventID+1 {
		err := fetchFromRemoteAndAppend(localLastItem.EventId, localLastItem.Version, historyEvents[0][0].EventId, historyEvents[0][0].Version)
		if err != nil {
			return err
		}
		startEventID = historyEvents[0][0].EventId - 1
		startEventVersion, err = versionhistory.GetVersionHistoryEventVersion(sourceVersionHistory, startEventID)
		if err != nil {
			return err
		}
	}

	// add events from request
	for i, events := range historyEvents {
		if events[0].EventId <= startEventID {
			continue
		}
		txnID, err := r.shardContext.GenerateTaskID()
		if err != nil {
			return err
		}
		_, err = r.executionMgr.AppendRawHistoryNodes(ctx, &persistence.AppendRawHistoryNodesRequest{
			ShardID:           r.shardContext.GetShardID(),
			IsNewBranch:       isNewBranch,
			BranchToken:       versionHistoryToAppend.BranchToken,
			History:           eventBlobs[i],
			PrevTransactionID: prevTxnID,
			TransactionID:     txnID,
			NodeID:            events[0].EventId,
			Info: persistence.BuildHistoryGarbageCleanupInfo(
				namespaceID.String(),
				workflowID,
				runID,
			),
		})
		if err != nil {
			return err
		}
		prevTxnID = txnID
		isNewBranch = false
		startEventID = events[len(events)-1].EventId
		startEventVersion = events[len(events)-1].Version
		localMutableState.GetExecutionInfo().ExecutionStats.HistorySize += int64(len(eventBlobs[i].Data))
	}
	// add more events if there is any
	if startEventID < endEventID {
		err = fetchFromRemoteAndAppend(startEventID, startEventVersion, endEventID+1, endEventVersion)
		if err != nil {
			return err
		}
	}
	versionHistoryToAppend.Items = versionhistory.CopyVersionHistoryItems(sourceVersionHistory.Items)
	localMutableState.SetHistoryBuilder(historybuilder.NewImmutableForUpdateNextEventID(sourceLastItem))
	return nil
}

func (r *WorkflowStateReplicatorImpl) applySnapshotWhenWorkflowNotExist(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	wfCtx workflow.Context,
	releaseFn wcache.ReleaseCacheFunc,
	sourceMutableState *persistencespb.WorkflowMutableState,
	sourceCluster string,
	newRunInfo *repication.NewRunInfo,
	isStateBased bool,
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
		namespaceID.String(),
		workflowID,
		runID,
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
		namespaceID,
		workflowID,
		runID,
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

	ns, err := r.namespaceRegistry.GetNamespaceByID(namespaceID)
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
		err = r.createNewRunWorkflow(
			ctx,
			namespaceID,
			workflowID,
			newRunInfo,
			mutableState,
			isStateBased,
		)
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

func (r *WorkflowStateReplicatorImpl) createNewRunWorkflow(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	newRunInfo *repication.NewRunInfo,
	originalMutableState workflow.MutableState,
	isStateBased bool,
) error {
	newRunWfContext, newRunReleaseFn, newRunErr := r.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		r.shardContext,
		namespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: workflowID,
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
		}
		newRunReleaseFn(newRunErr)

	}()
	_, newRunErr = newRunWfContext.LoadMutableState(ctx, r.shardContext)
	switch newRunErr.(type) {
	case nil:
		return nil
	case *serviceerror.NotFound:
		newRunMutableState, err := r.getNewRunMutableState(
			ctx,
			namespaceID,
			workflowID,
			newRunInfo.GetRunId(),
			originalMutableState,
			newRunInfo.EventBatch,
			isStateBased,
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
		lastEventID+1,
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
			EndEventId:        endEventID,
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
