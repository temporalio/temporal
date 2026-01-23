//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination conflict_resolver_mock.go

package ndc

import (
	"context"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/util"
	historyi "go.temporal.io/server/service/history/interfaces"
)

type (
	ConflictResolver interface {
		GetOrRebuildCurrentMutableState(
			ctx context.Context,
			branchIndex int32,
			incomingVersion int64,
		) (historyi.MutableState, bool, error)
		GetOrRebuildMutableState(
			ctx context.Context,
			branchIndex int32,
		) (historyi.MutableState, bool, error)
	}

	ConflictResolverImpl struct {
		shard          historyi.ShardContext
		stateRebuilder StateRebuilder

		context      historyi.WorkflowContext
		mutableState historyi.MutableState
		logger       log.Logger
	}
)

var _ ConflictResolver = (*ConflictResolverImpl)(nil)

func NewConflictResolver(
	shard historyi.ShardContext,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	logger log.Logger,
) *ConflictResolverImpl {

	return &ConflictResolverImpl{
		shard:          shard,
		stateRebuilder: NewStateRebuilder(shard, logger),

		context:      wfContext,
		mutableState: mutableState,
		logger:       logger,
	}
}

func (r *ConflictResolverImpl) GetOrRebuildCurrentMutableState(
	ctx context.Context,
	branchIndex int32,
	incomingVersion int64,
) (historyi.MutableState, bool, error) {
	versionHistories := r.mutableState.GetExecutionInfo().GetVersionHistories()
	currentVersionHistoryIndex := versionHistories.GetCurrentVersionHistoryIndex()
	currentVersionHistory, err := versionhistory.GetVersionHistory(versionHistories, currentVersionHistoryIndex)
	if err != nil {
		return nil, false, err
	}
	currentLastItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return nil, false, err
	}

	// mutable state does not need Rebuild
	if incomingVersion < currentLastItem.GetVersion() {
		return r.mutableState, false, nil
	}
	if incomingVersion == currentLastItem.GetVersion() && branchIndex != currentVersionHistoryIndex {
		return nil, false, serviceerror.NewInvalidArgument("ConflictResolver encountered replication task version == current branch last write version")
	}
	// incomingVersion > currentLastItem.GetVersion()
	return r.getOrRebuildMutableStateByIndex(ctx, branchIndex)
}

func (r *ConflictResolverImpl) GetOrRebuildMutableState(
	ctx context.Context,
	branchIndex int32,
) (historyi.MutableState, bool, error) {
	return r.getOrRebuildMutableStateByIndex(ctx, branchIndex)
}

func (r *ConflictResolverImpl) getOrRebuildMutableStateByIndex(
	ctx context.Context,
	branchIndex int32,
) (historyi.MutableState, bool, error) {

	versionHistories := r.mutableState.GetExecutionInfo().GetVersionHistories()
	currentVersionHistoryIndex := versionHistories.GetCurrentVersionHistoryIndex()

	// replication task to be applied to current branch
	if branchIndex == currentVersionHistoryIndex {
		return r.mutableState, false, nil
	}

	// task.getVersion() > currentLastItem
	// incoming replication task, after application, will become the current branch
	// (because higher version wins), we need to Rebuild the mutable state for that
	rebuiltMutableState, err := r.rebuild(ctx, branchIndex, uuid.NewString())
	if err != nil {
		return nil, false, err
	}
	return rebuiltMutableState, true, nil
}

func (r *ConflictResolverImpl) rebuild(
	ctx context.Context,
	branchIndex int32,
	requestID string,
) (historyi.MutableState, error) {

	versionHistories := r.mutableState.GetExecutionInfo().GetVersionHistories()
	replayVersionHistory, err := versionhistory.GetVersionHistory(versionHistories, branchIndex)
	if err != nil {
		return nil, err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(replayVersionHistory)
	if err != nil {
		return nil, err
	}

	executionInfo := r.mutableState.GetExecutionInfo()
	executionState := r.mutableState.GetExecutionState()
	workflowKey := definition.NewWorkflowKey(
		executionInfo.NamespaceId,
		executionInfo.WorkflowId,
		executionState.RunId,
	)
	historySize := r.mutableState.GetHistorySize()
	externalPayloadSize := r.mutableState.GetExternalPayloadSize()
	externalPayloadCount := r.mutableState.GetExternalPayloadCount()

	rebuildMutableState, _, err := r.stateRebuilder.Rebuild(
		ctx,
		timestamp.TimeValue(executionState.StartTime),
		workflowKey,
		replayVersionHistory.GetBranchToken(),
		lastItem.GetEventId(),
		util.Ptr(lastItem.GetVersion()),
		workflowKey,
		replayVersionHistory.GetBranchToken(),
		requestID,
	)
	if err != nil {
		return nil, err
	}

	// after rebuilt verification
	rebuildVersionHistories := rebuildMutableState.GetExecutionInfo().GetVersionHistories()
	rebuildVersionHistory, err := versionhistory.GetCurrentVersionHistory(rebuildVersionHistories)
	rebuildMutableState.GetExecutionInfo().PreviousTransitionHistory = r.mutableState.GetExecutionInfo().PreviousTransitionHistory
	rebuildMutableState.GetExecutionInfo().LastTransitionHistoryBreakPoint = r.mutableState.GetExecutionInfo().LastTransitionHistoryBreakPoint
	if err != nil {
		return nil, err
	}

	if !rebuildVersionHistory.Equal(replayVersionHistory) {
		return nil, serviceerror.NewInternal("ConflictResolver encounter mismatch version history after Rebuild")
	}

	// set the current branch index to target branch index
	if err := versionhistory.SetCurrentVersionHistoryIndex(versionHistories, branchIndex); err != nil {
		return nil, err
	}
	rebuildMutableState.GetExecutionInfo().VersionHistories = versionHistories
	rebuildMutableState.AddHistorySize(historySize)
	rebuildMutableState.AddExternalPayloadSize(externalPayloadSize)
	rebuildMutableState.AddExternalPayloadCount(externalPayloadCount)
	// set the update condition from original mutable state
	rebuildMutableState.SetUpdateCondition(r.mutableState.GetUpdateCondition())

	r.context.Clear()
	return rebuildMutableState, nil
}
