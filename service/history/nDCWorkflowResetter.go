// Copyright (c) 2019 Uber Technologies, Inc.
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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCWorkflowResetter_mock.go

package history

import (
	ctx "context"
	"time"

	"github.com/pborman/uuid"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
)

type (
	nDCWorkflowResetter interface {
		resetWorkflow(
			ctx ctx.Context,
			now time.Time,
			baseEventID int64,
			baseVersion int64,
		) (mutableState, error)
	}

	nDCWorkflowResetterImpl struct {
		shard          ShardContext
		transactionMgr nDCTransactionMgr
		historyV2Mgr   persistence.HistoryV2Manager
		stateRebuilder nDCStateRebuilder

		domainID   string
		workflowID string
		baseRunID  string
		newContext workflowExecutionContext
		newRunID   string

		logger log.Logger
	}
)

var _ nDCWorkflowResetter = (*nDCWorkflowResetterImpl)(nil)

func newNDCWorkflowResetter(
	shard ShardContext,
	transactionMgr nDCTransactionMgr,
	domainID string,
	workflowID string,
	baseRunID string,
	newContext workflowExecutionContext,
	newRunID string,
	logger log.Logger,
) *nDCWorkflowResetterImpl {

	return &nDCWorkflowResetterImpl{
		shard:          shard,
		transactionMgr: transactionMgr,
		historyV2Mgr:   shard.GetHistoryV2Manager(),
		stateRebuilder: newNDCStateRebuilder(shard, logger),

		domainID:   domainID,
		workflowID: workflowID,
		baseRunID:  baseRunID,
		newContext: newContext,
		newRunID:   newRunID,
		logger:     logger,
	}
}

func (r *nDCWorkflowResetterImpl) resetWorkflow(
	ctx ctx.Context,
	now time.Time,
	baseLastEventID int64,
	baseLastEventVersion int64,
) (mutableState, error) {

	baseBranchToken, err := r.getBaseBranchToken(ctx, baseLastEventID, baseLastEventVersion)
	if err != nil {
		return nil, err
	}

	resetBranchToken, err := r.getResetBranchToken(ctx, baseBranchToken, baseLastEventID)

	requestID := uuid.New()
	rebuildMutableState, rebuiltHistorySize, err := r.stateRebuilder.rebuild(
		ctx,
		now,
		definition.NewWorkflowIdentifier(
			r.domainID,
			r.workflowID,
			r.baseRunID,
		),
		baseBranchToken,
		baseLastEventID,
		baseLastEventVersion,
		definition.NewWorkflowIdentifier(
			r.domainID,
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
	ctx ctx.Context,
	baseEventID int64,
	baseVersion int64,
) (baseBranchToken []byte, retError error) {

	baseWorkflow, err := r.transactionMgr.loadNDCWorkflow(
		ctx,
		r.domainID,
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
		persistence.NewVersionHistoryItem(baseEventID, baseVersion),
	)
	if err != nil {
		return nil, newNDCRetryTaskErrorWithHint()
	}

	baseVersionHistory, err := baseVersionHistories.GetVersionHistory(index)
	if err != nil {
		return nil, err
	}
	return baseVersionHistory.GetBranchToken(), nil
}

func (r *nDCWorkflowResetterImpl) getResetBranchToken(
	ctx ctx.Context,
	baseBranchToken []byte,
	baseLastEventID int64,
) ([]byte, error) {

	// fork a new history branch
	shardID := r.shard.GetShardID()
	resp, err := r.historyV2Mgr.ForkHistoryBranch(&persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      baseLastEventID + 1,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(r.domainID, r.workflowID, r.newRunID),
		ShardID:         common.IntPtr(shardID),
	})
	if err != nil {
		return nil, err
	}

	resetBranchToken := resp.NewBranchToken

	if errComplete := r.historyV2Mgr.CompleteForkBranch(&persistence.CompleteForkBranchRequest{
		BranchToken: resetBranchToken,
		Success:     true, // past lessons learnt from Cassandra & gocql tells that we cannot possibly find all timeout errors
		ShardID:     common.IntPtr(shardID),
	}); errComplete != nil {
		r.logger.WithTags(
			tag.Error(errComplete),
		).Error("newNDCWorkflowResetter unable to complete creation of new branch.")
	}

	return resetBranchToken, nil
}
