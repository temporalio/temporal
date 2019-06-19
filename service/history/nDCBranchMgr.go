// Copyright (c) 2017 Uber Technologies, Inc.
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

package history

import (
	ctx "context"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
)

type (
	nDCBranchMgr interface {
		prepareVersionHistory(
			ctx ctx.Context,
			incomingVersionHistory *persistence.VersionHistory,
		) (versionHistoryIndex int, retError error)
	}

	nDCBranchMgrImpl struct {
		context         workflowExecutionContext
		mutableState    mutableState
		shard           ShardContext
		clusterMetadata cluster.Metadata
		historyV2Mgr    persistence.HistoryV2Manager
		logger          log.Logger
	}
)

var _ nDCBranchMgr = (*nDCBranchMgrImpl)(nil)

func newNDCBranchMgr(
	context workflowExecutionContext,
	mutableState mutableState,
	shard ShardContext,
	historyV2Mgr persistence.HistoryV2Manager,
	logger log.Logger,
) *nDCBranchMgrImpl {

	return &nDCBranchMgrImpl{
		context:         context,
		mutableState:    mutableState,
		shard:           shard,
		clusterMetadata: shard.GetService().GetClusterMetadata(),
		historyV2Mgr:    historyV2Mgr,
		logger:          logger,
	}
}

func (r *nDCBranchMgrImpl) prepareVersionHistory(
	ctx ctx.Context,
	incomingVersionHistory *persistence.VersionHistory,
) (int, error) {

	localVersionHistories := r.mutableState.GetVersionHistories()

	versionHistoryIndex, lcaVersionHistoryItem, err := localVersionHistories.FindLCAVersionHistoryIndexAndItem(
		incomingVersionHistory,
	)
	if err != nil {
		return 0, err
	}
	versionHistory, err := localVersionHistories.GetVersionHistory(versionHistoryIndex)
	if err != nil {
		return 0, err
	}

	// if can directly append to a branch
	if versionHistory.IsLCAAppendable(lcaVersionHistoryItem) {
		return versionHistoryIndex, nil
	}

	newVersionHistory, err := versionHistory.DuplicateUntilLCAItem(lcaVersionHistoryItem)
	if err != nil {
		return 0, err
	}

	newVersionHistoryIndex, err := r.createNewBranch(
		ctx,
		versionHistory.GetBranchToken(),
		lcaVersionHistoryItem.GetEventID(),
		newVersionHistory,
	)
	if err != nil {
		return 0, err
	}

	return newVersionHistoryIndex, nil
}

func (r *nDCBranchMgrImpl) createNewBranch(
	ctx ctx.Context,
	baseBranchToken []byte,
	baseBranchLastEventID int64,
	newVersionHistory *persistence.VersionHistory,
) (newVersionHistoryIndex int, retError error) {

	shardID := r.shard.GetShardID()
	executionInfo := r.mutableState.GetExecutionInfo()
	domainID := executionInfo.DomainID
	workflowID := executionInfo.WorkflowID

	resp, err := r.historyV2Mgr.ForkHistoryBranch(&persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      baseBranchLastEventID + 1,
		Info:            historyGarbageCleanupInfo(domainID, workflowID, uuid.New()),
		ShardID:         common.IntPtr(shardID),
	})
	if err != nil {
		return 0, err
	}
	newBranchToken := resp.NewBranchToken
	defer func() {
		if errComplete := r.historyV2Mgr.CompleteForkBranch(&persistence.CompleteForkBranchRequest{
			BranchToken: newBranchToken,
			Success:     retError == nil || persistence.IsTimeoutError(retError),
			ShardID:     common.IntPtr(shardID),
		}); errComplete != nil {
			r.logger.WithTags(tag.Error(errComplete)).Error("unable to complete creation of new branch.")
		}
	}()

	if err := newVersionHistory.SetBranchToken(newBranchToken); err != nil {
		return 0, err
	}
	branchChanged, newIndex, err := r.mutableState.GetVersionHistories().AddVersionHistory(
		newVersionHistory,
	)
	if err != nil {
		return 0, err
	}
	if branchChanged {
		return 0, &shared.BadRequestError{
			Message: "conflict resolution branching should not change current branch",
		}
	}

	// Generate a transaction ID for appending events to history
	transactionID, err := r.shard.GetNextTransferTaskID()
	if err != nil {
		return 0, err
	}

	// TODO modify the logic below for 3+DC
	sourceCluster := r.clusterMetadata.ClusterNameForFailoverVersion(
		r.mutableState.GetLastWriteVersion(),
	)
	if err := r.context.updateWorkflowExecutionForStandby(
		nil,
		nil,
		transactionID,
		time.Time{},
		false,
		nil,
		sourceCluster,
	); err != nil {
		return 0, err
	}
	return newIndex, nil
}
