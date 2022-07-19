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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCWorkflowResetter_mock.go

package history

import (
	"context"
	"time"

	"github.com/pborman/uuid"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
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
		) (workflow.MutableState, error)
	}

	nDCWorkflowResetterImpl struct {
		shard          shard.Context
		transactionMgr nDCTransactionMgr
		executionMgr   persistence.ExecutionManager
		stateRebuilder nDCStateRebuilder

		namespaceID namespace.ID
		workflowID  string
		baseRunID   string
		newContext  workflow.Context
		newRunID    string

		logger log.Logger
	}
)

var _ nDCWorkflowResetter = (*nDCWorkflowResetterImpl)(nil)

func newNDCWorkflowResetter(
	shard shard.Context,
	transactionMgr nDCTransactionMgr,
	namespaceID namespace.ID,
	workflowID string,
	baseRunID string,
	newContext workflow.Context,
	newRunID string,
	logger log.Logger,
) *nDCWorkflowResetterImpl {

	return &nDCWorkflowResetterImpl{
		shard:          shard,
		transactionMgr: transactionMgr,
		executionMgr:   shard.GetExecutionManager(),
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
) (workflow.MutableState, error) {

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
	if err != nil {
		return nil, err
	}

	requestID := uuid.New()
	rebuildMutableState, rebuiltHistorySize, err := r.stateRebuilder.rebuild(
		ctx,
		now,
		definition.NewWorkflowKey(
			r.namespaceID.String(),
			r.workflowID,
			r.baseRunID,
		),
		baseBranchToken,
		baseLastEventID,
		convert.Int64Ptr(baseLastEventVersion),
		definition.NewWorkflowKey(
			r.namespaceID.String(),
			r.workflowID,
			r.newRunID,
		),
		resetBranchToken,
		requestID,
	)
	if err != nil {
		return nil, err
	}

	r.newContext.Clear()
	r.newContext.SetHistorySize(rebuiltHistorySize)
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

	baseVersionHistories := baseWorkflow.getMutableState().GetExecutionInfo().GetVersionHistories()
	index, err := versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(
		baseVersionHistories,
		versionhistory.NewVersionHistoryItem(baseLastEventID, baseLastEventVersion),
	)
	if err != nil {
		// the base event and incoming event are from different branch
		// only re-replicate the gap on the incoming branch
		// the base branch event will eventually arrived
		return nil, serviceerrors.NewRetryReplication(
			resendOnResetWorkflowMessage,
			r.namespaceID.String(),
			r.workflowID,
			r.newRunID,
			common.EmptyEventID,
			common.EmptyVersion,
			incomingFirstEventID,
			incomingFirstEventVersion,
		)
	}

	baseVersionHistory, err := versionhistory.GetVersionHistory(baseVersionHistories, index)
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
	resp, err := r.executionMgr.ForkHistoryBranch(ctx, &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      baseLastEventID + 1,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(r.namespaceID.String(), r.workflowID, r.newRunID),
		ShardID:         shardID,
	})
	if err != nil {
		return nil, err
	}

	return resp.NewBranchToken, nil
}
