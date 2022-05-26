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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCBranchMgr_mock.go

package history

import (
	"context"

	"github.com/pborman/uuid"
	"go.temporal.io/api/serviceerror"

	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

const (
	outOfOrderDeliveryMessage = "Resend events due to out of order delivery"
)

type (
	nDCBranchMgr interface {
		prepareVersionHistory(
			ctx context.Context,
			incomingVersionHistory *historyspb.VersionHistory,
			incomingFirstEventID int64,
			incomingFirstEventVersion int64,
		) (bool, int32, error)
	}

	nDCBranchMgrImpl struct {
		shard             shard.Context
		namespaceRegistry namespace.Registry
		clusterMetadata   cluster.Metadata
		executionMgr      persistence.ExecutionManager

		context      workflow.Context
		mutableState workflow.MutableState
		logger       log.Logger
	}
)

var _ nDCBranchMgr = (*nDCBranchMgrImpl)(nil)

func newNDCBranchMgr(
	shard shard.Context,
	context workflow.Context,
	mutableState workflow.MutableState,
	logger log.Logger,
) *nDCBranchMgrImpl {

	return &nDCBranchMgrImpl{
		shard:             shard,
		namespaceRegistry: shard.GetNamespaceRegistry(),
		clusterMetadata:   shard.GetClusterMetadata(),
		executionMgr:      shard.GetExecutionManager(),

		context:      context,
		mutableState: mutableState,
		logger:       logger,
	}
}

func (r *nDCBranchMgrImpl) prepareVersionHistory(
	ctx context.Context,
	incomingVersionHistory *historyspb.VersionHistory,
	incomingFirstEventID int64,
	incomingFirstEventVersion int64,
) (bool, int32, error) {

	lcaVersionHistoryItem, versionHistoryIndex, err := r.flushBufferedEvents(ctx, incomingVersionHistory)
	if err != nil {
		return false, 0, err
	}

	localVersionHistories := r.mutableState.GetExecutionInfo().GetVersionHistories()
	versionHistory, err := versionhistory.GetVersionHistory(localVersionHistories, versionHistoryIndex)
	if err != nil {
		return false, 0, err
	}

	// if can directly append to a branch
	if versionhistory.IsLCAVersionHistoryItemAppendable(versionHistory, lcaVersionHistoryItem) {
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

func (r *nDCBranchMgrImpl) flushBufferedEvents(
	ctx context.Context,
	incomingVersionHistory *historyspb.VersionHistory,
) (*historyspb.VersionHistoryItem, int32, error) {

	localVersionHistories := r.mutableState.GetExecutionInfo().GetVersionHistories()

	lcaVersionHistoryItem, versionHistoryIndex, err := versionhistory.FindLCAVersionHistoryItemAndIndex(
		localVersionHistories,
		incomingVersionHistory,
	)
	if err != nil {
		return nil, 0, err
	}

	// check whether there are buffered events, if so, flush it
	// NOTE: buffered events does not show in version history or next event id
	if !r.mutableState.HasBufferedEvents() {
		if r.mutableState.HasTransientWorkflowTask() {
			if err := r.mutableState.ClearTransientWorkflowTask(); err != nil {
				return nil, 0, err
			}
			// now transient task is gone
		}
		return lcaVersionHistoryItem, versionHistoryIndex, nil
	}

	targetWorkflow := newNDCWorkflow(
		ctx,
		r.namespaceRegistry,
		r.clusterMetadata,
		r.context,
		r.mutableState,
		workflow.NoopReleaseFn,
	)
	if err := targetWorkflow.flushBufferedEvents(); err != nil {
		return nil, 0, err
	}
	// the workflow must be updated as active, to send out replication tasks
	if err := targetWorkflow.context.UpdateWorkflowExecutionAsActive(
		ctx,
		r.shard.GetTimeSource().Now(),
	); err != nil {
		return nil, 0, err
	}

	r.context = targetWorkflow.getContext()
	r.mutableState = targetWorkflow.getMutableState()

	localVersionHistories = r.mutableState.GetExecutionInfo().GetVersionHistories()
	return versionhistory.FindLCAVersionHistoryItemAndIndex(localVersionHistories, incomingVersionHistory)
}

func (r *nDCBranchMgrImpl) verifyEventsOrder(
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

func (r *nDCBranchMgrImpl) createNewBranch(
	ctx context.Context,
	baseBranchToken []byte,
	baseBranchLastEventID int64,
	newVersionHistory *historyspb.VersionHistory,
) (newVersionHistoryIndex int32, retError error) {

	shardID := r.shard.GetShardID()
	executionInfo := r.mutableState.GetExecutionInfo()
	namespaceID := executionInfo.NamespaceId
	workflowID := executionInfo.WorkflowId

	resp, err := r.executionMgr.ForkHistoryBranch(ctx, &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      baseBranchLastEventID + 1,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(namespaceID, workflowID, uuid.New()),
		ShardID:         shardID,
	})
	if err != nil {
		return 0, err
	}

	versionhistory.SetVersionHistoryBranchToken(newVersionHistory, resp.NewBranchToken)

	branchChanged, newIndex, err := versionhistory.AddVersionHistory(
		r.mutableState.GetExecutionInfo().GetVersionHistories(),
		newVersionHistory,
	)
	if err != nil {
		return 0, err
	}
	if branchChanged {
		return 0, serviceerror.NewInvalidArgument("nDCBranchMgr encountered branch change during conflict resolution")
	}

	return newIndex, nil
}
