//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination hsm_state_replicator_mock.go

package ndc

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	HSMStateReplicator interface {
		SyncHSMState(
			ctx context.Context,
			request *historyi.SyncHSMRequest,
		) error
	}

	HSMStateReplicatorImpl struct {
		shardContext  historyi.ShardContext
		workflowCache wcache.Cache
		logger        log.Logger
	}
)

func NewHSMStateReplicator(
	shardContext historyi.ShardContext,
	workflowCache wcache.Cache,
	logger log.Logger,
) *HSMStateReplicatorImpl {

	return &HSMStateReplicatorImpl{
		shardContext:  shardContext,
		workflowCache: workflowCache,
		logger:        log.With(logger, tag.ComponentHSMStateReplicator),
	}
}

func (r *HSMStateReplicatorImpl) SyncHSMState(
	ctx context.Context,
	request *historyi.SyncHSMRequest,
) (retError error) {
	namespaceID := namespace.ID(request.WorkflowKey.GetNamespaceID())
	execution := &commonpb.WorkflowExecution{
		WorkflowId: request.WorkflowKey.GetWorkflowID(),
		RunId:      request.WorkflowKey.GetRunID(),
	}

	lastItem, err := versionhistory.GetLastVersionHistoryItem(request.EventVersionHistory)
	if err != nil {
		return err
	}

	workflowContext, release, err := r.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		r.shardContext,
		namespaceID,
		execution,
		locks.PriorityHigh,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := workflowContext.LoadMutableState(ctx, r.shardContext)
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
			return serviceerrors.NewRetryReplication(
				"sync HSM state encountered workflow not found",
				namespaceID.String(),
				execution.GetWorkflowId(),
				execution.GetRunId(),
				common.EmptyEventID,
				common.EmptyVersion,
				lastItem.EventId+1, // underlying resend logic is exclusive-exclusive
				lastItem.Version,
			)
		}
		return err
	}

	synced, err := r.syncHSMNode(mutableState, request)
	if err != nil {
		return err
	}
	if !synced {
		return consts.ErrDuplicate
	}

	if r.shardContext.GetConfig().EnableUpdateWorkflowModeIgnoreCurrent() {
		return workflowContext.UpdateWorkflowExecutionAsPassive(ctx, r.shardContext)
	}

	// TODO: remove following code once EnableUpdateWorkflowModeIgnoreCurrent config is deprecated.
	state, _ := mutableState.GetWorkflowStateStatus()
	if state == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		return workflowContext.SubmitClosedWorkflowSnapshot(
			ctx,
			r.shardContext,
			historyi.TransactionPolicyPassive,
		)
	}

	updateMode := persistence.UpdateWorkflowModeUpdateCurrent
	if state == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		updateMode = persistence.UpdateWorkflowModeBypassCurrent
	}

	return workflowContext.UpdateWorkflowExecutionWithNew(
		ctx,
		r.shardContext,
		updateMode,
		nil, // no new workflow
		nil, // no new workflow
		historyi.TransactionPolicyPassive,
		nil,
	)
}

func (r *HSMStateReplicatorImpl) syncHSMNode(
	mutableState historyi.MutableState,
	request *historyi.SyncHSMRequest,
) (bool, error) {

	shouldSync, err := r.compareVersionHistory(mutableState, request.EventVersionHistory)
	if err != nil || !shouldSync {
		return shouldSync, err
	}

	currentHSM := mutableState.HSM()

	// we don't care about the root here which is the entire mutable state
	incomingHSM, err := hsm.NewRoot(
		r.shardContext.StateMachineRegistry(),
		workflow.StateMachineType,
		mutableState,
		request.StateMachineNode.Children,
		mutableState,
	)
	if err != nil {
		return false, err
	}

	synced := false
	if err := incomingHSM.Walk(func(incomingNode *hsm.Node) error {

		if incomingNode.Parent == nil {
			// skip root which is the entire mutable state
			return nil
		}

		incomingNodePath := incomingNode.Path()
		currentNode, err := currentHSM.Child(incomingNodePath)
		if err != nil {
			// The node may not be found if the state machine was deleted in terminal a state and this cluster is
			// syncing from an older cluster that doesn't delete the state machine on completion.
			// Both state machine creation and deletion are always associated with an event, so any missing state
			// machine must have a corresponding event in history.
			if errors.Is(err, hsm.ErrStateMachineNotFound) {
				return nil
			}
			return err
		}

		if shouldSyncNode, err := r.shouldSyncNode(currentNode, incomingNode); err != nil || !shouldSyncNode {
			if err != nil && errors.Is(err, hsm.ErrInitialTransitionMismatch) {
				return nil
			}
			return err
		}

		synced = true
		return currentNode.Sync(incomingNode)
	}); err != nil {
		return false, err
	}

	return synced, nil
}

func (r *HSMStateReplicatorImpl) shouldSyncNode(
	currentNode, incomingNode *hsm.Node,
) (bool, error) {
	currentLastUpdated := currentNode.InternalRepr().LastUpdateVersionedTransition
	incomingLastUpdated := incomingNode.InternalRepr().LastUpdateVersionedTransition

	if currentLastUpdated.TransitionCount != 0 && incomingLastUpdated.TransitionCount != 0 {
		return transitionhistory.Compare(currentLastUpdated, incomingLastUpdated) < 0, nil
	}

	if currentLastUpdated.NamespaceFailoverVersion == incomingLastUpdated.NamespaceFailoverVersion {
		result, err := currentNode.CompareState(incomingNode)
		return result < 0, err
	}

	return currentLastUpdated.NamespaceFailoverVersion < incomingLastUpdated.NamespaceFailoverVersion, nil
}

func (r *HSMStateReplicatorImpl) compareVersionHistory(
	mutableState historyi.MutableState,
	incomingVersionHistory *historyspb.VersionHistory,
) (bool, error) {
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(
		mutableState.GetExecutionInfo().GetVersionHistories(),
	)
	if err != nil {
		return false, err
	}

	lastLocalItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return false, err
	}

	lastIncomingItem, err := versionhistory.GetLastVersionHistoryItem(incomingVersionHistory)
	if err != nil {
		return false, err
	}

	lcaItem, err := versionhistory.FindLCAVersionHistoryItem(currentVersionHistory, incomingVersionHistory)
	if err != nil {
		return false, err
	}

	if versionhistory.IsLCAVersionHistoryItemAppendable(currentVersionHistory, lcaItem) ||
		versionhistory.IsLCAVersionHistoryItemAppendable(incomingVersionHistory, lcaItem) {
		// not diverged, resend any missing events
		if versionhistory.CompareVersionHistoryItem(lastLocalItem, lastIncomingItem) >= 0 {
			return true, nil
		}

		workflowKey := mutableState.GetWorkflowKey()
		return false, serviceerrors.NewRetryReplication(
			"sync HSM state encountered missing events",
			workflowKey.NamespaceID,
			workflowKey.WorkflowID,
			workflowKey.RunID,
			lastLocalItem.EventId,
			lastLocalItem.Version,
			lastIncomingItem.EventId+1, // underlying resend logic is exclusive-exclusive
			lastIncomingItem.Version,
		)
	}

	// event version history has diverged
	if lastIncomingItem.GetVersion() < lastLocalItem.GetVersion() {
		// we can't sync state here, since state may depend on events not on the current branch.
		return false, nil
	}

	if lastIncomingItem.GetVersion() > lastLocalItem.GetVersion() {
		workflowKey := mutableState.GetWorkflowKey()
		return false, serviceerrors.NewRetryReplication(
			resendHigherVersionMessage,
			workflowKey.NamespaceID,
			workflowKey.WorkflowID,
			workflowKey.RunID,
			lcaItem.GetEventId(),
			lcaItem.GetVersion(),
			lastIncomingItem.EventId+1, // underlying resend logic is exclusive-exclusive
			lastIncomingItem.Version,
		)
	}

	return true, nil
}
