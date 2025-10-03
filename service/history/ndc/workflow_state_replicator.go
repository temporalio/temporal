//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination workflow_state_replicator_mock.go

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
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
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
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/historybuilder"
	historyi "go.temporal.io/server/service/history/interfaces"
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
			versionedTransition *replicationspb.VersionedTransitionArtifact,
			sourceClusterName string,
		) error
	}

	WorkflowStateReplicatorImpl struct {
		shardContext      historyi.ShardContext
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
	shardContext historyi.ShardContext,
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

	// SyncWorkflowState is not used by new state-based replication stack,
	// but CHASM only uses state-based replication, so we can continue to use
	// GetOrCreateWorkflowExecution here.
	wfCtx, releaseFn, err := r.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		r.shardContext,
		namespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		locks.PriorityHigh,
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
		if len(request.WorkflowState.ExecutionInfo.TransitionHistory) != 0 {
			return r.applySnapshotWhenWorkflowExist(ctx, namespaceID, wid, rid, wfCtx, releaseFn, ms, request.WorkflowState, nil, nil, request.RemoteCluster)
		}
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
		return engine.SyncHSM(ctx, &historyi.SyncHSMRequest{
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
	versionedTransition *replicationspb.VersionedTransitionArtifact,
	sourceClusterName string,
) (retError error) {
	if versionedTransition.StateAttributes == nil {
		return serviceerror.NewInvalidArgument("both snapshot and mutation are nil")
	}
	var mutation *replicationspb.SyncWorkflowStateMutationAttributes
	var snapshot *replicationspb.SyncWorkflowStateSnapshotAttributes
	switch artifactType := versionedTransition.StateAttributes.(type) {
	case *replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes:
		snapshot = versionedTransition.GetSyncWorkflowStateSnapshotAttributes()
	case *replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes:
		mutation = versionedTransition.GetSyncWorkflowStateMutationAttributes()
	default:
		return serviceerror.NewInvalidArgumentf("unknown artifact type %T", artifactType)
	}

	if versionedTransition.IsFirstSync {
		// this is the first replication task for this workflow
		// TODO: Handle reset case to reduce the amount of history events write
		err := r.handleFirstReplicationTask(ctx, versionedTransition, sourceClusterName)
		if !errors.Is(err, consts.ErrDuplicate) {
			// if ErrDuplicate is returned from creation, it means the workflow is already existed, continue to apply mutation
			return err
		}
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

	wfCtx, releaseFn, err := r.workflowCache.GetOrCreateChasmEntity(
		ctx,
		r.shardContext,
		namespaceID,
		&commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      rid,
		},
		chasm.ArchetypeAny,
		locks.PriorityHigh,
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
					"failed to apply versioned transition due to missing snapshot",
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
			sourceLastWriteVersion := transitionhistory.LastVersionedTransition(sourceTransitionHistory).NamespaceFailoverVersion

			if localLastWriteVersion > sourceLastWriteVersion {
				// local is newer, try backfill events
				releaseFn(nil)
				return r.backFillEvents(ctx, namespaceID, wid, rid, executionInfo.VersionHistories, versionedTransition.EventBatches, versionedTransition.NewRunInfo, sourceClusterName, transitionhistory.LastVersionedTransition(sourceTransitionHistory))
			}
			if localLastWriteVersion < sourceLastWriteVersion ||
				localLastHistoryItem.GetEventId() <= sourceLastHistoryItem.EventId {
				return r.applySnapshot(ctx, namespaceID, wid, rid, wfCtx, releaseFn, ms, versionedTransition, sourceClusterName)
			}
			return consts.ErrDuplicate
		}

		sourceTransitionHistory := executionInfo.TransitionHistory
		err = transitionhistory.StalenessCheck(localTransitionHistory, transitionhistory.LastVersionedTransition(sourceTransitionHistory))
		switch {
		case err == nil:
			return consts.ErrDuplicate
		case errors.Is(err, consts.ErrStaleState):
			// local is stale, try to apply mutable state update
			if snapshot != nil {
				return r.applySnapshot(ctx, namespaceID, wid, rid, wfCtx, releaseFn, ms, versionedTransition, sourceClusterName)
			}
			return r.applyMutation(ctx, namespaceID, wid, rid, wfCtx, ms, releaseFn, versionedTransition, sourceClusterName)
		case errors.Is(err, consts.ErrStaleReference):
			releaseFn(nil)
			return r.backFillEvents(ctx, namespaceID, wid, rid, executionInfo.VersionHistories, versionedTransition.EventBatches, versionedTransition.NewRunInfo, sourceClusterName, transitionhistory.LastVersionedTransition(sourceTransitionHistory))
		default:
			return err
		}
	default:
		return err
	}
}

func (r *WorkflowStateReplicatorImpl) handleFirstReplicationTask(
	ctx context.Context,
	versionedTransition *replicationspb.VersionedTransitionArtifact,
	sourceClusterName string,
) (retErr error) {
	var mutation *replicationspb.SyncWorkflowStateMutationAttributes
	var snapshot *replicationspb.SyncWorkflowStateSnapshotAttributes
	switch artifactType := versionedTransition.StateAttributes.(type) {
	case *replicationspb.VersionedTransitionArtifact_SyncWorkflowStateSnapshotAttributes:
		snapshot = versionedTransition.GetSyncWorkflowStateSnapshotAttributes()
	case *replicationspb.VersionedTransitionArtifact_SyncWorkflowStateMutationAttributes:
		mutation = versionedTransition.GetSyncWorkflowStateMutationAttributes()
	default:
		return serviceerror.NewInvalidArgumentf("unknown artifact type %T", artifactType)
	}
	executionState, executionInfo := func() (*persistencespb.WorkflowExecutionState, *persistencespb.WorkflowExecutionInfo) {
		if snapshot != nil {
			return snapshot.State.ExecutionState, snapshot.State.ExecutionInfo
		}
		return mutation.StateMutation.ExecutionState, mutation.StateMutation.ExecutionInfo
	}()

	wfCtx, releaseFn, err := r.workflowCache.GetOrCreateChasmEntity(
		ctx,
		r.shardContext,
		namespace.ID(executionInfo.NamespaceId),
		&commonpb.WorkflowExecution{
			WorkflowId: executionInfo.WorkflowId,
			RunId:      executionState.RunId,
		},
		chasm.ArchetypeAny,
		locks.PriorityHigh,
	)
	if err != nil {
		return err
	}
	defer func() {
		if rec := recover(); rec != nil {
			releaseFn(errPanic)
			panic(rec) //nolint:forbidigo
		}
		releaseFn(retErr)
	}()

	nsEntry, err := r.namespaceRegistry.GetNamespaceByID(namespace.ID(executionInfo.NamespaceId))
	if err != nil {
		return err
	}
	localMutableState := workflow.NewMutableState(
		r.shardContext,
		r.shardContext.GetEventsCache(),
		r.logger,
		nsEntry,
		executionInfo.WorkflowId,
		executionState.RunId,
		timestamp.TimeValue(executionState.StartTime),
	)
	err = localMutableState.SetHistoryTree(executionInfo.WorkflowExecutionTimeout, executionInfo.WorkflowRunTimeout, executionState.RunId)
	if err != nil {
		return err
	}
	newBranchToken, err := r.bringLocalEventsUpToSourceCurrentBranch(
		ctx,
		namespace.ID(executionInfo.NamespaceId),
		executionInfo.WorkflowId,
		executionState.RunId,
		sourceClusterName,
		wfCtx,
		localMutableState,
		executionInfo.VersionHistories,
		versionedTransition.EventBatches,
		true,
	)
	defer func() {
		r.deleteNewBranchWhenError(ctx, newBranchToken, retErr)
	}()
	if err != nil {
		return err
	}

	if mutation != nil {
		err = localMutableState.ApplyMutation(mutation.StateMutation)
	} else {
		err = localMutableState.ApplySnapshot(snapshot.State)
	}
	if err != nil {
		return err
	}

	if versionedTransition.NewRunInfo != nil {
		err = r.createNewRunWorkflow(
			ctx,
			namespace.ID(executionInfo.NamespaceId),
			executionInfo.WorkflowId,
			versionedTransition.NewRunInfo,
			localMutableState,
			true,
		)
		if err != nil {
			return err
		}
	}

	err = r.taskRefresher.Refresh(ctx, localMutableState)
	if err != nil {
		return err
	}

	return r.transactionMgr.CreateWorkflow(
		ctx,
		NewWorkflow(
			r.clusterMetadata,
			wfCtx,
			localMutableState,
			releaseFn,
		),
	)
}

func (r *WorkflowStateReplicatorImpl) deleteNewBranchWhenError(
	ctx context.Context,
	newBranchToken []byte,
	err error,
) {
	if err != nil && len(newBranchToken) > 0 {
		if err := r.shardContext.GetExecutionManager().DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{
			ShardID:     r.shardContext.GetShardID(),
			BranchToken: newBranchToken,
		}); err != nil {
			r.logger.Error("failed to clean up workflow execution", tag.Error(err))
		}
	}
}

func (r *WorkflowStateReplicatorImpl) applyMutation(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	wfCtx historyi.WorkflowContext,
	localMutableState historyi.MutableState,
	releaseFn historyi.ReleaseWorkflowContextFunc,
	versionedTransition *replicationspb.VersionedTransitionArtifact,
	sourceClusterName string,
) (retErr error) {
	mutation := versionedTransition.GetSyncWorkflowStateMutationAttributes()
	if mutation == nil {
		return serviceerror.NewInvalidArgument("mutation is nil")
	}
	if localMutableState == nil {
		return serviceerrors.NewSyncState(
			"failed to apply mutation due to missing local mutable state",
			namespaceID.String(),
			workflowID,
			runID,
			nil,
			nil,
		)
	}
	localTransitionHistory := transitionhistory.CopyVersionedTransitions(localMutableState.GetExecutionInfo().TransitionHistory)
	localVersionedTransition := transitionhistory.LastVersionedTransition(localTransitionHistory)
	sourceTransitionHistory := mutation.StateMutation.ExecutionInfo.TransitionHistory

	// make sure mutation range is extension of local range
	if transitionhistory.StalenessCheck(localTransitionHistory, mutation.ExclusiveStartVersionedTransition) != nil ||
		transitionhistory.StalenessCheck(sourceTransitionHistory, localVersionedTransition) != nil {
		return serviceerrors.NewSyncState(
			fmt.Sprintf("Failed to apply mutation due to version check failed. local transition history: %v, source transition history: %v, exclusiveStartVersionedTransition: %v",
				localTransitionHistory, sourceTransitionHistory, mutation.ExclusiveStartVersionedTransition),
			namespaceID.String(),
			workflowID,
			runID,
			localVersionedTransition,
			localMutableState.GetExecutionInfo().VersionHistories,
		)
	}

	newBranchToken, err := r.bringLocalEventsUpToSourceCurrentBranch(
		ctx,
		namespaceID,
		workflowID,
		runID,
		sourceClusterName,
		wfCtx,
		localMutableState,
		mutation.StateMutation.ExecutionInfo.VersionHistories,
		versionedTransition.EventBatches,
		false,
	)
	defer func() {
		r.deleteNewBranchWhenError(ctx, newBranchToken, retErr)
	}()
	if err != nil {
		return err
	}

	prevPendingChildIds := localMutableState.GetPendingChildIds()

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
	nextVersionedTransition := transitionhistory.CopyVersionedTransition(localVersionedTransition)
	nextVersionedTransition.TransitionCount++
	err = r.taskRefresher.PartialRefresh(ctx, localMutableState, nextVersionedTransition, prevPendingChildIds)
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
	wfCtx historyi.WorkflowContext,
	releaseFn historyi.ReleaseWorkflowContextFunc,
	localMutableState historyi.MutableState,
	versionedTransition *replicationspb.VersionedTransitionArtifact,
	sourceClusterName string,
) error {
	attribute := versionedTransition.GetSyncWorkflowStateSnapshotAttributes()
	if attribute == nil || attribute.State == nil {
		var versionHistories *historyspb.VersionHistories
		if localMutableState != nil {
			versionHistories = localMutableState.GetExecutionInfo().VersionHistories
		}
		return serviceerrors.NewSyncState(
			"failed to apply snapshot due to missing snapshot attributes",
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
	return r.applySnapshotWhenWorkflowExist(ctx, namespaceID, workflowID, runID, wfCtx, releaseFn, localMutableState, snapshot, versionedTransition.EventBatches, versionedTransition.NewRunInfo, sourceClusterName)
}

func (r *WorkflowStateReplicatorImpl) applySnapshotWhenWorkflowExist(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	wfCtx historyi.WorkflowContext,
	releaseFn historyi.ReleaseWorkflowContextFunc,
	localMutableState historyi.MutableState,
	sourceMutableState *persistencespb.WorkflowMutableState,
	eventBlobs []*commonpb.DataBlob,
	newRunInfo *replicationspb.NewRunInfo,
	sourceClusterName string,
) (retErr error) {
	var isBranchSwitched bool
	var localTransitionHistory []*persistencespb.VersionedTransition
	var localVersionedTransition *persistencespb.VersionedTransition
	if len(localMutableState.GetExecutionInfo().TransitionHistory) != 0 {
		localTransitionHistory = transitionhistory.CopyVersionedTransitions(localMutableState.GetExecutionInfo().TransitionHistory)
		localVersionedTransition = transitionhistory.LastVersionedTransition(localTransitionHistory)
		sourceTransitionHistory := sourceMutableState.ExecutionInfo.TransitionHistory
		err := transitionhistory.StalenessCheck(sourceTransitionHistory, localVersionedTransition)
		switch {
		case err == nil:
			// no branch switch
		case errors.Is(err, consts.ErrStaleState):
			return serviceerrors.NewSyncState(
				fmt.Sprintf("apply snapshot encountered stale snapshot. local transition history: %v, source transition history: %v", localTransitionHistory, sourceTransitionHistory),
				namespaceID.String(),
				workflowID,
				runID,
				localVersionedTransition,
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
		sourceCurrentHistory, err := versionhistory.GetCurrentVersionHistory(sourceMutableState.ExecutionInfo.VersionHistories)
		if err != nil {
			return err
		}
		if !versionhistory.IsVersionHistoryItemsInSameBranch(localCurrentHistory.Items, sourceCurrentHistory.Items) {
			isBranchSwitched = true
		}
	}

	newBranchToken, err := r.bringLocalEventsUpToSourceCurrentBranch(
		ctx,
		namespaceID,
		workflowID,
		runID,
		sourceClusterName,
		wfCtx,
		localMutableState,
		sourceMutableState.ExecutionInfo.VersionHistories,
		eventBlobs,
		false,
	)
	defer func() {
		r.deleteNewBranchWhenError(ctx, newBranchToken, retErr)
	}()
	if err != nil {
		return err
	}

	prevPendingChildIds := localMutableState.GetPendingChildIds()

	err = localMutableState.ApplySnapshot(sourceMutableState)
	if err != nil {
		return err
	}

	var newRunWorkflow Workflow
	if newRunInfo != nil {
		newRunWorkflow, err = r.getNewRunWorkflow(ctx, namespaceID, workflowID, localMutableState, newRunInfo)
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
		nextVersionedTransition := transitionhistory.CopyVersionedTransition(localVersionedTransition)
		nextVersionedTransition.TransitionCount++
		err = r.taskRefresher.PartialRefresh(ctx, localMutableState, nextVersionedTransition, prevPendingChildIds)
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
	sourceVersionHistories *historyspb.VersionHistories,
	eventBatches []*commonpb.DataBlob,
	newRunInfo *replicationspb.NewRunInfo,
	sourceClusterName string,
	destinationVersionedTransition *persistencespb.VersionedTransition,
) error {
	if len(eventBatches) == 0 {
		return nil
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
	var err error
	if newRunInfo != nil {
		newRunEvents, err = r.historySerializer.DeserializeEvents(newRunInfo.EventBatch)
		if err != nil {
			return err
		}
		newRunID = newRunInfo.RunId
	}

	engine, err := r.shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	sourceCurrentVersionHistory, err := versionhistory.GetCurrentVersionHistory(sourceVersionHistories)
	if err != nil {
		return err
	}
	return engine.BackfillHistoryEvents(ctx, &historyi.BackfillHistoryEventsRequest{
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
	originalMutableState historyi.MutableState,
	newRunInfo *replicationspb.NewRunInfo,
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
	originalMutableState historyi.MutableState,
	newRunEventsBlob *commonpb.DataBlob,
	isStateBased bool,
) (historyi.MutableState, error) {
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

	var newRunMutableState historyi.MutableState
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
	wfCtx historyi.WorkflowContext,
	localMutableState historyi.MutableState,
	sourceVersionHistories *historyspb.VersionHistories,
	eventBlobs []*commonpb.DataBlob,
	isNewMutableState bool,
) ([]byte, error) {
	sourceVersionHistory, err := versionhistory.GetCurrentVersionHistory(sourceVersionHistories)
	if err != nil {
		return nil, err
	}
	localVersionHistories := localMutableState.GetExecutionInfo().GetVersionHistories()
	localVersionHistory, err := versionhistory.GetCurrentVersionHistory(localVersionHistories)
	if err != nil {
		return nil, err
	}

	if versionhistory.IsEmptyVersionHistory(sourceVersionHistory) {
		// we don't need to insert events but need to switch localMutableState to use
		// an empty version history if it is not already using one.
		if !versionhistory.IsEmptyVersionHistory(localVersionHistory) {
			newIndex := versionhistory.AddEmptyVersionHistory(localVersionHistories)
			localVersionHistories.CurrentVersionHistoryIndex = newIndex
		}
		return nil, nil
	}

	index, isNewBranch, err := r.getBranchToAppend(
		ctx,
		wfCtx,
		localMutableState,
		sourceVersionHistory,
		isNewMutableState)
	if err != nil {
		return nil, err
	}
	localVersionHistories.CurrentVersionHistoryIndex = index
	var newBranchToken []byte
	if isNewBranch {
		newBranchToken = localVersionHistories.Histories[index].BranchToken
	}
	versionHistoryToAppend, err := versionhistory.GetVersionHistory(localVersionHistories, index)
	if err != nil {
		return newBranchToken, err
	}
	var localLastItem *historyspb.VersionHistoryItem
	if isNewMutableState {
		localLastItem = &historyspb.VersionHistoryItem{
			EventId: 0,
			Version: 0,
		}
	} else {
		localLastItem, err = versionhistory.GetLastVersionHistoryItem(versionHistoryToAppend)
		if err != nil {
			return newBranchToken, err
		}
	}

	sourceLastItem, err := versionhistory.GetLastVersionHistoryItem(sourceVersionHistory)
	if err != nil {
		return newBranchToken, err
	}
	if versionhistory.IsEqualVersionHistoryItem(localLastItem, sourceLastItem) {
		localMutableState.SetHistoryBuilder(historybuilder.NewImmutableForUpdateNextEventID(sourceLastItem))
		return newBranchToken, nil
	}

	startEventID := localLastItem.GetEventId() // exclusive
	startEventVersion := localLastItem.GetVersion()
	endEventID := sourceLastItem.GetEventId() // inclusive
	endEventVersion := sourceLastItem.GetVersion()
	expectedEventID := startEventID + 1

	eventsConsecutiveCheck := func(currentEventId, currentEventVersion int64) error {
		if expectedEventID != currentEventId {
			return fmt.Errorf("%w Expected %v, but got %v", ErrEventSlicesNotConsecutive, expectedEventID, currentEventId)
		}
		version, err := versionhistory.GetVersionHistoryEventVersion(sourceVersionHistory, currentEventId)
		if err != nil {
			return serviceerror.NewInternalf("Failed to get version for event id %v from history %v", currentEventId, sourceVersionHistory)
		}
		if version != currentEventVersion {
			return serviceerror.NewInternalf("Event Version does not match. Expected %v, but got %v", version, currentEventVersion)
		}
		expectedEventID = currentEventId + 1
		return nil
	}
	var historyEvents [][]*historypb.HistoryEvent
	for _, blob := range eventBlobs {
		events, err := r.historySerializer.DeserializeEvents(blob)
		if err != nil {
			return newBranchToken, err
		}
		historyEvents = append(historyEvents, events)
	}

	prevTxnID := localMutableState.GetExecutionInfo().LastFirstEventTxnId
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
			events, err := r.historySerializer.DeserializeEvents(historyBlob.rawHistory)
			if err != nil {
				return err
			}
			for _, event := range events {
				err := eventsConsecutiveCheck(event.EventId, event.Version)
				if err != nil {
					return err
				}
				localMutableState.AddReapplyCandidateEvent(event)
				r.addEventToCache(localMutableState.GetWorkflowKey(), event)
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
			return newBranchToken, err
		}
		startEventID = historyEvents[0][0].EventId - 1
		startEventVersion, err = versionhistory.GetVersionHistoryEventVersion(sourceVersionHistory, startEventID)
		if err != nil {
			return newBranchToken, err
		}
	}

	// add events from request
	for i, events := range historyEvents {
		if events[0].EventId <= startEventID {
			continue
		}
		txnID, err := r.shardContext.GenerateTaskID()
		if err != nil {
			return newBranchToken, err
		}
		for _, event := range events {
			err := eventsConsecutiveCheck(event.EventId, event.Version)
			if err != nil {
				return newBranchToken, err
			}
			localMutableState.AddReapplyCandidateEvent(event)
			r.addEventToCache(localMutableState.GetWorkflowKey(), event)
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
			return newBranchToken, err
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
			return newBranchToken, err
		}
	}
	if expectedEventID != endEventID+1 {
		return newBranchToken, serviceerror.NewInternalf("Event not match. Expected %v, but got %v", expectedEventID, endEventID+1)
	}
	versionHistoryToAppend.Items = versionhistory.CopyVersionHistoryItems(sourceVersionHistory.Items)
	localMutableState.SetHistoryBuilder(historybuilder.NewImmutableForUpdateNextEventID(sourceLastItem))
	localMutableState.GetExecutionInfo().LastFirstEventTxnId = prevTxnID
	return newBranchToken, nil
}

func (r *WorkflowStateReplicatorImpl) getBranchToAppend(
	ctx context.Context,
	wfCtx historyi.WorkflowContext,
	localMutableState historyi.MutableState,
	sourceVersionHistory *historyspb.VersionHistory,
	isNewMutableState bool,
) (int32, bool, error) {
	if isNewMutableState {
		return 0, true, nil
	}

	localVersionHistories := localMutableState.GetExecutionInfo().VersionHistories
	lcaItem, index, err := versionhistory.FindLCAVersionHistoryItemAndIndex(localVersionHistories, sourceVersionHistory)
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

	newIndex, err := NewBranchMgr(r.shardContext, wfCtx, localMutableState, r.logger).createNewBranch(
		ctx, localHistory.GetBranchToken(), lcaItem.GetEventId(), newVersionHistory,
	)
	if err != nil {
		return 0, false, err
	}

	return newIndex, true, nil
}

func (r *WorkflowStateReplicatorImpl) applySnapshotWhenWorkflowNotExist(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	wfCtx historyi.WorkflowContext,
	releaseFn historyi.ReleaseWorkflowContextFunc,
	sourceMutableState *persistencespb.WorkflowMutableState,
	sourceCluster string,
	newRunInfo *replicationspb.NewRunInfo,
	isStateBased bool,
) error {
	var lastWriteVersion int64
	executionInfo := sourceMutableState.ExecutionInfo
	if transitionHistory := executionInfo.GetTransitionHistory(); len(transitionHistory) != 0 {
		lastWriteVersion = transitionhistory.LastVersionedTransition(transitionHistory).NamespaceFailoverVersion
	} else {
		// TODO: remove following logic once transition history is fully enabled.
		currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(executionInfo.VersionHistories)
		if err != nil {
			return err
		}
		lastEventItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
		if err != nil {
			return err
		}
		lastWriteVersion = lastEventItem.GetVersion()
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
		lastWriteVersion,
	)
	if err != nil {
		return err
	}

	if err := r.backfillHistory(
		ctx,
		sourceCluster,
		namespaceID,
		workflowID,
		runID,
		mutableState,
		isStateBased,
	); err != nil {
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
	newRunInfo *replicationspb.NewRunInfo,
	originalMutableState historyi.MutableState,
	isStateBased bool,
) error {
	// CHASM runs don't have new run, so we can continue using GetOrCreateWorkflowExecution here.
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
	mutableState *workflow.MutableStateImpl,
	isStateBased bool,
) (retError error) {
	versionHistories := mutableState.GetExecutionInfo().VersionHistories
	isEmpty, err := versionhistory.IsCurrentVersionHistoryEmpty(versionHistories)
	if err != nil || isEmpty {
		return err
	}

	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
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
	backfillBranchToken, err := r.shardContext.GetExecutionManager().GetHistoryBranchUtil().NewHistoryBranch(
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

	// TODO: The original run id is in the workflow started history event but not in mutable state.
	// Use the history tree id to be the original run id.
	// https://github.com/temporalio/temporal/issues/6501
	originalRunID := branchInfo.GetTreeId()
	if runID != originalRunID {
		// At this point, it already acquired the workflow lock on the run ID.
		// Get the lock of root run id to make sure no concurrent backfill history across multiple runs.
		//
		// CHASM runs have no history, so we can continue to use GetOrCreateWorkflowExecution here.
		_, rootRunReleaseFn, err := r.workflowCache.GetOrCreateWorkflowExecution(
			ctx,
			r.shardContext,
			namespaceID,
			&commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      originalRunID,
			},
			locks.PriorityHigh,
		)
		if err != nil {
			return err
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
		backfillBranchToken,
		lastEventItem.EventId,
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
			return err
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
		lastEventItem.EventId+1,
		lastEventItem.Version),
	)
	historyBranchUtil := r.executionMgr.GetHistoryBranchUtil()
	historyBranch, err := historyBranchUtil.ParseHistoryBranchInfo(backfillBranchToken)
	if err != nil {
		return err
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
			return err
		}

		if isStateBased {
			// If backfill suceeds but later event reapply fails, during task's next retry,
			// we still need to reapply events that have been stored in local DB.
			events, err := r.historySerializer.DeserializeEvents(historyBlob.rawHistory)
			if err != nil {
				return err
			}
			for _, event := range events {
				mutableState.AddReapplyCandidateEvent(event)
				r.addEventToCache(mutableState.GetWorkflowKey(), event)
			}
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
					return serviceerror.NewInternalf(
						"The backfill history blob node id %d is not in acestoer range [%d, %d]",
						historyBlob.nodeID,
						currentAncestor.GetBeginNodeId(),
						currentAncestor.GetEndNodeId(),
					)
				}
			}
		}

		filteredHistoryBranch, err := historyBranchUtil.UpdateHistoryBranchInfo(
			backfillBranchToken,
			&persistencespb.HistoryBranch{
				TreeId:    historyBranch.GetTreeId(),
				BranchId:  branchID,
				Ancestors: ancestors,
			},
			runID,
		)
		if err != nil {
			return err
		}
		txnID, err := r.shardContext.GenerateTaskID()
		if err != nil {
			return err
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
			return err
		}
		prevTxnID = txnID
		prevBranchID = branchID
	}

	mutableState.GetExecutionInfo().LastFirstEventTxnId = prevTxnID
	return mutableState.SetCurrentBranchToken(backfillBranchToken)
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

func (r *WorkflowStateReplicatorImpl) addEventToCache(
	workflowKey definition.WorkflowKey,
	event *historypb.HistoryEvent,
) {
	switch event.EventType {
	case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
		r.shardContext.GetEventsCache().PutEvent(
			events.EventKey{
				NamespaceID: namespace.ID(workflowKey.NamespaceID),
				WorkflowID:  workflowKey.WorkflowID,
				RunID:       workflowKey.RunID,
				EventID:     event.GetEventId(),
				Version:     event.GetVersion(),
			},
			event,
		)
	default:
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
