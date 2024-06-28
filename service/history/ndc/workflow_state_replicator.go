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
	"sort"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"golang.org/x/exp/slices"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
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
		request.NamespaceId,
		wid,
		rid,
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
		r.shardContext,
		r.shardContext.GetEventsCache(),
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

	taskRefresh := workflow.NewTaskRefresher(r.shardContext, r.logger)
	err = taskRefresh.RefreshTasks(ctx, mutableState)
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
	lastEventID int64,
	lastEventVersion int64,
	branchToken []byte,
) (taskID int64, retError error) {

	// Get the current run lock to make sure no concurrent backfill history across multiple runs.
	currentRunReleaseFn, err := r.workflowCache.GetOrCreateCurrentWorkflowExecution(
		ctx,
		r.shardContext,
		namespaceID,
		workflowID,
		workflow.LockPriorityLow,
	)
	if err != nil {
		return common.EmptyEventTaskID, err
	}
	defer func() {
		if rec := recover(); rec != nil {
			currentRunReleaseFn(errPanic)
			panic(rec)
		}
		currentRunReleaseFn(retError)
	}()

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
	endEventID int64,
	endEventVersion int64,
) collection.PaginationFn[*rawHistoryData] {

	return func(paginationToken []byte) ([]*rawHistoryData, []byte, error) {

		adminClient, err := r.shardContext.GetRemoteAdminClient(remoteClusterName)
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
