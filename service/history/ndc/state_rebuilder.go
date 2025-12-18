//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination state_rebuilder_mock.go

package ndc

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

type (
	StateRebuilder interface {
		Rebuild(
			ctx context.Context,
			now time.Time,
			baseWorkflowIdentifier definition.WorkflowKey,
			baseBranchToken []byte,
			baseLastEventID int64,
			baseLastEventVersion *int64,
			targetWorkflowIdentifier definition.WorkflowKey,
			targetBranchToken []byte,
			requestID string,
		) (historyi.MutableState, RebuildStats, error)
		RebuildWithCurrentMutableState(
			ctx context.Context,
			now time.Time,
			baseWorkflowIdentifier definition.WorkflowKey,
			baseBranchToken []byte,
			baseLastEventID int64,
			baseLastEventVersion *int64,
			targetWorkflowIdentifier definition.WorkflowKey,
			targetBranchToken []byte,
			requestID string,
			currentMutableState *persistencespb.WorkflowMutableState,
		) (historyi.MutableState, RebuildStats, error)
	}

	RebuildStats struct {
		HistorySize          int64
		ExternalPayloadSize  int64
		ExternalPayloadCount int64
	}

	StateRebuilderImpl struct {
		shard             historyi.ShardContext
		namespaceRegistry namespace.Registry
		eventsCache       events.Cache
		clusterMetadata   cluster.Metadata
		executionMgr      persistence.ExecutionManager
		taskRefresher     workflow.TaskRefresher

		rebuiltHistorySize          int64
		rebuiltExternalPayloadSize  int64
		rebuiltExternalPayloadCount int64
		logger                      log.Logger
	}

	HistoryBlobsPaginationItem struct {
		History       *historypb.History
		TransactionID int64
	}
)

var _ StateRebuilder = (*StateRebuilderImpl)(nil)

func NewStateRebuilder(
	shard historyi.ShardContext,
	logger log.Logger,
) *StateRebuilderImpl {

	return &StateRebuilderImpl{
		shard:                       shard,
		namespaceRegistry:           shard.GetNamespaceRegistry(),
		eventsCache:                 shard.GetEventsCache(),
		clusterMetadata:             shard.GetClusterMetadata(),
		executionMgr:                shard.GetExecutionManager(),
		taskRefresher:               workflow.NewTaskRefresher(shard),
		rebuiltHistorySize:          0,
		rebuiltExternalPayloadSize:  0,
		rebuiltExternalPayloadCount: 0,
		logger:                      logger,
	}
}

func (r *StateRebuilderImpl) Rebuild(
	ctx context.Context,
	now time.Time,
	baseWorkflowIdentifier definition.WorkflowKey,
	baseBranchToken []byte,
	baseLastEventID int64,
	baseLastEventVersion *int64,
	targetWorkflowIdentifier definition.WorkflowKey,
	targetBranchToken []byte,
	requestID string,
) (historyi.MutableState, RebuildStats, error) {
	rebuiltMutableState, lastTxnId, err := r.buildMutableStateFromEvent(
		ctx,
		now,
		baseWorkflowIdentifier,
		baseBranchToken,
		baseLastEventID,
		baseLastEventVersion,
		targetWorkflowIdentifier,
		targetBranchToken,
		requestID,
	)
	if err != nil {
		return nil, RebuildStats{}, err
	}

	// close rebuilt mutable state transaction clearing all generated tasks, etc.
	_, _, err = rebuiltMutableState.CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyPassive)
	if err != nil {
		return nil, RebuildStats{}, err
	}

	rebuiltMutableState.GetExecutionInfo().LastFirstEventTxnId = lastTxnId

	// refresh tasks to be generated
	// TODO: ideally the executionTimeoutTimerTaskStatus field should be carried over
	// from the base run. However, RefreshTasks always resets that field and
	// force regenerates the execution timeout timer task.
	if err := r.taskRefresher.Refresh(ctx, rebuiltMutableState, false); err != nil {
		return nil, RebuildStats{}, err
	}

	return rebuiltMutableState, RebuildStats{
		HistorySize:          r.rebuiltHistorySize,
		ExternalPayloadSize:  r.rebuiltExternalPayloadSize,
		ExternalPayloadCount: r.rebuiltExternalPayloadCount,
	}, nil
}

func (r *StateRebuilderImpl) RebuildWithCurrentMutableState(
	ctx context.Context,
	now time.Time,
	baseWorkflowIdentifier definition.WorkflowKey,
	baseBranchToken []byte,
	baseLastEventID int64,
	baseLastEventVersion *int64,
	targetWorkflowIdentifier definition.WorkflowKey,
	targetBranchToken []byte,
	requestID string,
	currentMutableState *persistencespb.WorkflowMutableState,
) (historyi.MutableState, RebuildStats, error) {
	rebuiltMutableState, lastTxnId, err := r.buildMutableStateFromEvent(
		ctx,
		now,
		baseWorkflowIdentifier,
		baseBranchToken,
		baseLastEventID,
		baseLastEventVersion,
		targetWorkflowIdentifier,
		targetBranchToken,
		requestID,
	)
	if err != nil {
		return nil, RebuildStats{}, err
	}
	copyToRebuildMutableState(rebuiltMutableState, currentMutableState)
	versionHistories := rebuiltMutableState.GetExecutionInfo().GetVersionHistories()
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	if err != nil {
		return nil, RebuildStats{}, err
	}
	items := versionhistory.CopyVersionHistoryItems(currentVersionHistory.Items)

	// This is a workaround to bypass the version history update check:
	// We need to use Active policy to close the transaction. We need to clear the version history items here to
	// let it pass the version history update logic and then re-assign the version history items after transaction.
	currentVersionHistory.Items = nil

	// close rebuilt mutable state transaction clearing all generated tasks, etc.
	_, _, err = rebuiltMutableState.CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyActive)
	if err != nil {
		return nil, RebuildStats{}, err
	}
	currentVersionHistory.Items = items

	rebuiltMutableState.GetExecutionInfo().LastFirstEventTxnId = lastTxnId

	// refresh tasks to be generated
	// TODO: ideally the executionTimeoutTimerTaskStatus field should be carried over
	// from the base run. However, RefreshTasks always resets that field and
	// force regenerates the execution timeout timer task.
	if err := r.taskRefresher.Refresh(ctx, rebuiltMutableState, false); err != nil {
		return nil, RebuildStats{}, err
	}

	return rebuiltMutableState, RebuildStats{
		HistorySize:          r.rebuiltHistorySize,
		ExternalPayloadSize:  r.rebuiltExternalPayloadSize,
		ExternalPayloadCount: r.rebuiltExternalPayloadCount,
	}, nil
}

func copyToRebuildMutableState(
	rebuiltMutableState historyi.MutableState,
	currentMutableState *persistencespb.WorkflowMutableState,
) {
	rebuiltMutableState.GetExecutionInfo().TransitionHistory = transitionhistory.CopyVersionedTransitions(currentMutableState.GetExecutionInfo().TransitionHistory)
	rebuiltMutableState.GetExecutionInfo().PreviousTransitionHistory = transitionhistory.CopyVersionedTransitions(currentMutableState.GetExecutionInfo().PreviousTransitionHistory)
	rebuiltMutableState.GetExecutionInfo().LastTransitionHistoryBreakPoint = transitionhistory.CopyVersionedTransition(currentMutableState.GetExecutionInfo().LastTransitionHistoryBreakPoint)
}

func (r *StateRebuilderImpl) buildMutableStateFromEvent(
	ctx context.Context,
	now time.Time,
	baseWorkflowIdentifier definition.WorkflowKey,
	baseBranchToken []byte,
	baseLastEventID int64,
	baseLastEventVersion *int64,
	targetWorkflowIdentifier definition.WorkflowKey,
	targetBranchToken []byte,
	requestID string,
) (historyi.MutableState, int64, error) {
	namespaceEntry, err := r.namespaceRegistry.GetNamespaceByID(namespace.ID(targetWorkflowIdentifier.NamespaceID))
	if err != nil {
		return nil, 0, err
	}

	iter := collection.NewPagingIterator(r.getPaginationFn(
		ctx,
		common.FirstEventID,
		baseLastEventID+1,
		baseBranchToken,
		namespaceEntry.Name().String(),
	))

	rebuiltMutableState, stateBuilder := r.initializeBuilders(
		namespaceEntry,
		targetWorkflowIdentifier,
		now,
	)

	var lastTxnId int64
	for iter.HasNext() {
		history, err := iter.Next()
		switch err.(type) {
		case nil:
			// noop
		case *serviceerror.DataLoss:
			r.logger.Error("encountered data loss event", tag.WorkflowNamespaceID(baseWorkflowIdentifier.NamespaceID), tag.WorkflowID(baseWorkflowIdentifier.WorkflowID), tag.WorkflowRunID(baseWorkflowIdentifier.RunID))
			return nil, 0, err
		default:
			return nil, 0, err
		}

		if err := r.applyEvents(
			ctx,
			targetWorkflowIdentifier,
			stateBuilder,
			history.History.Events,
			requestID,
		); err != nil {
			return nil, 0, err
		}

		lastTxnId = history.TransactionID
	}

	if err := rebuiltMutableState.SetCurrentBranchToken(targetBranchToken); err != nil {
		return nil, 0, err
	}
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(rebuiltMutableState.GetExecutionInfo().GetVersionHistories())
	if err != nil {
		return nil, 0, err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return nil, 0, err
	}

	if baseLastEventVersion != nil {
		if !lastItem.Equal(versionhistory.NewVersionHistoryItem(
			baseLastEventID,
			*baseLastEventVersion,
		)) {
			return nil, 0, serviceerror.NewInvalidArgumentf(
				"StateRebuilder unable to Rebuild mutable state to event ID: %v, version: %v, this event must be at the boundary",
				baseLastEventID,
				*baseLastEventVersion,
			)
		}
	}
	return rebuiltMutableState, lastTxnId, nil
}

func (r *StateRebuilderImpl) initializeBuilders(
	namespaceEntry *namespace.Namespace,
	workflowIdentifier definition.WorkflowKey,
	now time.Time,
) (historyi.MutableState, workflow.MutableStateRebuilder) {
	resetMutableState := workflow.NewMutableState(
		r.shard,
		r.shard.GetEventsCache(),
		r.logger,
		namespaceEntry,
		workflowIdentifier.GetWorkflowID(),
		workflowIdentifier.GetRunID(),
		now,
	)
	stateBuilder := workflow.NewMutableStateRebuilder(
		r.shard,
		r.logger,
		resetMutableState,
	)
	return resetMutableState, stateBuilder
}

func (r *StateRebuilderImpl) applyEvents(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	stateBuilder workflow.MutableStateRebuilder,
	events []*historypb.HistoryEvent,
	requestID string,
) error {

	_, err := stateBuilder.ApplyEvents(
		ctx,
		namespace.ID(workflowKey.NamespaceID),
		requestID,
		&commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
		[][]*historypb.HistoryEvent{events},
		nil, // no new run history when rebuilding mutable state
		"",
	)
	if err != nil {
		r.logger.Error("StateRebuilder unable to Rebuild mutable state.", tag.Error(err))
		return err
	}
	return nil
}

func (r *StateRebuilderImpl) getPaginationFn(
	ctx context.Context,
	firstEventID int64,
	nextEventID int64,
	branchToken []byte,
	namespaceName string,
) collection.PaginationFn[HistoryBlobsPaginationItem] {
	return func(paginationToken []byte) ([]HistoryBlobsPaginationItem, []byte, error) {
		resp, err := r.executionMgr.ReadHistoryBranchByBatch(ctx, &persistence.ReadHistoryBranchRequest{
			BranchToken:   branchToken,
			MinEventID:    firstEventID,
			MaxEventID:    nextEventID,
			PageSize:      defaultPageSize,
			NextPageToken: paginationToken,
			ShardID:       r.shard.GetShardID(),
		})
		if err != nil {
			return nil, nil, err
		}

		r.rebuiltHistorySize += int64(resp.Size)
		paginateItems := make([]HistoryBlobsPaginationItem, 0, len(resp.History))
		for i, history := range resp.History {
			nextBatch := HistoryBlobsPaginationItem{
				History:       history,
				TransactionID: resp.TransactionIDs[i],
			}
			paginateItems = append(paginateItems, nextBatch)

			// Calculate and accumulate external payload size and count for this batch of history events
			if r.shard.GetConfig().ExternalPayloadsEnabled(namespaceName) {
				externalPayloadSize, externalPayloadCount, err := workflow.CalculateExternalPayloadSize(history.Events)
				if err != nil {
					return nil, nil, err
				}
				r.rebuiltExternalPayloadSize += externalPayloadSize
				r.rebuiltExternalPayloadCount += externalPayloadCount
			}
		}
		return paginateItems, resp.NextPageToken, nil
	}
}
