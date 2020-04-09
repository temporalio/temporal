//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCWorkflowResetter_mock.go

package history

import (
	"context"
	"time"

	"github.com/pborman/uuid"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/persistence"
)

const (
	resendOnResetWorkflowMessage = "Resend events due to reset workflow"
)

type (
	nDCWorkflowResetter interface {
		resetWorkflow(
			ctx context.Context,
			now time.Time,
			baseLastEventID int64,
			baseLastEventVersion int64,
			incomingFirstEventID int64,
			incomingFirstEventVersion int64,
		) (mutableState, error)
	}

	nDCWorkflowResetterImpl struct {
		shard          ShardContext
		transactionMgr nDCTransactionMgr
		historyV2Mgr   persistence.HistoryManager
		stateRebuilder nDCStateRebuilder

		namespaceID string
		workflowID  string
		baseRunID   string
		newContext  workflowExecutionContext
		newRunID    string

		logger log.Logger
	}
)

var _ nDCWorkflowResetter = (*nDCWorkflowResetterImpl)(nil)

func newNDCWorkflowResetter(
	shard ShardContext,
	transactionMgr nDCTransactionMgr,
	namespaceID string,
	workflowID string,
	baseRunID string,
	newContext workflowExecutionContext,
	newRunID string,
	logger log.Logger,
) *nDCWorkflowResetterImpl {

	return &nDCWorkflowResetterImpl{
		shard:          shard,
		transactionMgr: transactionMgr,
		historyV2Mgr:   shard.GetHistoryManager(),
		stateRebuilder: newNDCStateRebuilder(shard, logger),

		namespaceID: namespaceID,
		workflowID:  workflowID,
		baseRunID:   baseRunID,
		newContext:  newContext,
		newRunID:    newRunID,
		logger:      logger,
	}
}

func (r *nDCWorkflowResetterImpl) resetWorkflow(
	ctx context.Context,
	now time.Time,
	baseLastEventID int64,
	baseLastEventVersion int64,
	incomingFirstEventID int64,
	incomingFirstEventVersion int64,
) (mutableState, error) {

	baseBranchToken, err := r.getBaseBranchToken(
		ctx,
		baseLastEventID,
		baseLastEventVersion,
		incomingFirstEventID,
		incomingFirstEventVersion,
	)

	if err != nil {
		return nil, err
	}

	resetBranchToken, err := r.getResetBranchToken(ctx, baseBranchToken, baseLastEventID)

	requestID := uuid.New()
	rebuildMutableState, rebuiltHistorySize, err := r.stateRebuilder.rebuild(
		ctx,
		now,
		definition.NewWorkflowIdentifier(
			r.namespaceID,
			r.workflowID,
			r.baseRunID,
		),
		baseBranchToken,
		baseLastEventID,
		baseLastEventVersion,
		definition.NewWorkflowIdentifier(
			r.namespaceID,
			r.workflowID,
			r.newRunID,
		),
		resetBranchToken,
		requestID,
	)
	if err != nil {
		return nil, err
	}

	r.newContext.clear()
	r.newContext.setHistorySize(rebuiltHistorySize)
	return rebuildMutableState, nil
}

func (r *nDCWorkflowResetterImpl) getBaseBranchToken(
	ctx context.Context,
	baseLastEventID int64,
	baseLastEventVersion int64,
	incomingFirstEventID int64,
	incomingFirstEventVersion int64,
) (baseBranchToken []byte, retError error) {

	baseWorkflow, err := r.transactionMgr.loadNDCWorkflow(
		ctx,
		r.namespaceID,
		r.workflowID,
		r.baseRunID,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		baseWorkflow.getReleaseFn()(retError)
	}()

	baseVersionHistories := baseWorkflow.getMutableState().GetVersionHistories()
	index, err := baseVersionHistories.FindFirstVersionHistoryIndexByItem(
		persistence.NewVersionHistoryItem(baseLastEventID, baseLastEventVersion),
	)
	if err != nil {
		// the base event and incoming event are from different branch
		// only re-replicate the gap on the incoming branch
		// the base branch event will eventually arrived
		return nil, newNDCRetryTaskErrorWithHint(
			resendOnResetWorkflowMessage,
			r.namespaceID,
			r.workflowID,
			r.newRunID,
			common.EmptyEventID,
			common.EmptyVersion,
			incomingFirstEventID,
			incomingFirstEventVersion,
		)
	}

	baseVersionHistory, err := baseVersionHistories.GetVersionHistory(index)
	if err != nil {
		return nil, err
	}
	return baseVersionHistory.GetBranchToken(), nil
}

func (r *nDCWorkflowResetterImpl) getResetBranchToken(
	ctx context.Context,
	baseBranchToken []byte,
	baseLastEventID int64,
) ([]byte, error) {

	// fork a new history branch
	shardID := r.shard.GetShardID()
	resp, err := r.historyV2Mgr.ForkHistoryBranch(&persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      baseLastEventID + 1,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(r.namespaceID, r.workflowID, r.newRunID),
		ShardID:         common.IntPtr(shardID),
	})
	if err != nil {
		return nil, err
	}

	return resp.NewBranchToken, nil
}
