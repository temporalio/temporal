//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination branch_manager_mock.go

package ndc

import (
	"context"

	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	historyi "go.temporal.io/server/service/history/interfaces"
)

const (
	outOfOrderDeliveryMessage = "Resend events due to out of order delivery"
)

type (
	BranchMgr interface {
		GetOrCreate(
			ctx context.Context,
			incomingVersionHistory *historyspb.VersionHistory,
			incomingFirstEventID int64,
			incomingFirstEventVersion int64,
		) (bool, int32, error)
		Create(
			ctx context.Context,
			incomingVersionHistory *historyspb.VersionHistory,
			incomingFirstEventID int64,
			incomingFirstEventVersion int64,
		) (bool, int32, error)
	}

	BranchMgrImpl struct {
		shard             historyi.ShardContext
		namespaceRegistry namespace.Registry
		clusterMetadata   cluster.Metadata
		executionMgr      persistence.ExecutionManager

		context      historyi.WorkflowContext
		mutableState historyi.MutableState
		logger       log.Logger
	}
)

var _ BranchMgr = (*BranchMgrImpl)(nil)

func NewBranchMgr(
	shard historyi.ShardContext,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	logger log.Logger,
) *BranchMgrImpl {

	return &BranchMgrImpl{
		shard:             shard,
		namespaceRegistry: shard.GetNamespaceRegistry(),
		clusterMetadata:   shard.GetClusterMetadata(),
		executionMgr:      shard.GetExecutionManager(),

		context:      wfContext,
		mutableState: mutableState,
		logger:       logger,
	}
}

func (r *BranchMgrImpl) GetOrCreate(
	ctx context.Context,
	incomingVersionHistory *historyspb.VersionHistory,
	incomingFirstEventID int64,
	incomingFirstEventVersion int64,
) (bool, int32, error) {
	return r.prepareBranch(ctx, incomingVersionHistory, incomingFirstEventID, incomingFirstEventVersion, true)
}

func (r *BranchMgrImpl) Create(
	ctx context.Context,
	incomingVersionHistory *historyspb.VersionHistory,
	incomingFirstEventID int64,
	incomingFirstEventVersion int64,
) (bool, int32, error) {
	return r.prepareBranch(ctx, incomingVersionHistory, incomingFirstEventID, incomingFirstEventVersion, false)
}

func (r *BranchMgrImpl) prepareBranch(
	ctx context.Context,
	incomingVersionHistory *historyspb.VersionHistory,
	incomingFirstEventID int64,
	incomingFirstEventVersion int64,
	reuseBranch bool,
) (bool, int32, error) {

	localVersionHistories := r.mutableState.GetExecutionInfo().GetVersionHistories()
	lcaVersionHistoryItem, versionHistoryIndex, err := versionhistory.FindLCAVersionHistoryItemAndIndex(
		localVersionHistories,
		incomingVersionHistory,
	)
	if err != nil {
		return false, 0, err
	}
	versionHistory, err := versionhistory.GetVersionHistory(localVersionHistories, versionHistoryIndex)
	if err != nil {
		return false, 0, err
	}

	// if can directly append to a branch
	if reuseBranch && versionhistory.IsLCAVersionHistoryItemAppendable(versionHistory, lcaVersionHistoryItem) {
		doContinue, err := r.verifyEventsOrder(
			ctx,
			versionHistory,
			incomingFirstEventID,
			incomingFirstEventVersion,
		)
		if err != nil {
			return false, 0, err
		}
		return doContinue, versionHistoryIndex, nil
	}

	newVersionHistory, err := versionhistory.CopyVersionHistoryUntilLCAVersionHistoryItem(versionHistory, lcaVersionHistoryItem)
	if err != nil {
		return false, 0, err
	}

	// if cannot directly append to the new branch to be created
	doContinue, err := r.verifyEventsOrder(
		ctx,
		newVersionHistory,
		incomingFirstEventID,
		incomingFirstEventVersion,
	)
	if err != nil || !doContinue {
		return false, 0, err
	}

	newVersionHistoryIndex, err := r.createNewBranch(
		ctx,
		versionHistory.GetBranchToken(),
		lcaVersionHistoryItem.GetEventId(),
		newVersionHistory,
	)
	if err != nil {
		return false, 0, err
	}

	return true, newVersionHistoryIndex, nil
}

func (r *BranchMgrImpl) verifyEventsOrder(
	ctx context.Context,
	localVersionHistory *historyspb.VersionHistory,
	incomingFirstEventID int64,
	incomingFirstEventVersion int64,
) (bool, error) {

	lastVersionHistoryItem, err := versionhistory.GetLastVersionHistoryItem(localVersionHistory)
	if err != nil {
		return false, err
	}
	nextEventID := lastVersionHistoryItem.GetEventId() + 1

	if incomingFirstEventID < nextEventID {
		// duplicate replication task
		return false, nil
	}
	if incomingFirstEventID > nextEventID {
		executionInfo := r.mutableState.GetExecutionInfo()
		executionState := r.mutableState.GetExecutionState()
		return false, serviceerrors.NewRetryReplication(
			outOfOrderDeliveryMessage,
			executionInfo.NamespaceId,
			executionInfo.WorkflowId,
			executionState.RunId,
			lastVersionHistoryItem.GetEventId(),
			lastVersionHistoryItem.GetVersion(),
			incomingFirstEventID,
			incomingFirstEventVersion)
	}
	// task.getFirstEvent().GetEventId() == nextEventID
	return true, nil
}

func (r *BranchMgrImpl) createNewBranch(
	ctx context.Context,
	baseBranchToken []byte,
	baseBranchLastEventID int64,
	newVersionHistory *historyspb.VersionHistory,
) (newVersionHistoryIndex int32, retError error) {

	shardID := r.shard.GetShardID()
	executionInfo := r.mutableState.GetExecutionInfo()
	namespaceID := executionInfo.NamespaceId
	workflowID := executionInfo.WorkflowId
	runID := r.mutableState.GetExecutionState().RunId

	resp, err := r.executionMgr.ForkHistoryBranch(ctx, &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      baseBranchLastEventID + 1,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(namespaceID, workflowID, runID),
		ShardID:         shardID,
		NamespaceID:     namespaceID,
		NewRunID:        runID,
	})
	if err != nil {
		return 0, err
	}

	versionhistory.SetVersionHistoryBranchToken(newVersionHistory, resp.NewBranchToken)

	branchChanged, newIndex, err := versionhistory.AddAndSwitchVersionHistory(
		r.mutableState.GetExecutionInfo().GetVersionHistories(),
		newVersionHistory,
	)
	if err != nil {
		return 0, err
	}
	if branchChanged {
		return 0, serviceerror.NewInvalidArgument("BranchMgr encountered branch change during conflict resolution")
	}

	return newIndex, nil
}
