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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCConflictResolver_mock.go

package history

import (
	"context"

	"github.com/pborman/uuid"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	nDCConflictResolver interface {
		prepareMutableState(
			ctx context.Context,
			branchIndex int32,
			incomingVersion int64,
		) (workflow.MutableState, bool, error)
	}

	nDCConflictResolverImpl struct {
		shard          shard.Context
		stateRebuilder nDCStateRebuilder

		context      workflow.Context
		mutableState workflow.MutableState
		logger       log.Logger
	}
)

var _ nDCConflictResolver = (*nDCConflictResolverImpl)(nil)

func newNDCConflictResolver(
	shard shard.Context,
	context workflow.Context,
	mutableState workflow.MutableState,
	logger log.Logger,
) *nDCConflictResolverImpl {

	return &nDCConflictResolverImpl{
		shard:          shard,
		stateRebuilder: newNDCStateRebuilder(shard, logger),

		context:      context,
		mutableState: mutableState,
		logger:       logger,
	}
}

func (r *nDCConflictResolverImpl) prepareMutableState(
	ctx context.Context,
	branchIndex int32,
	incomingVersion int64,
) (workflow.MutableState, bool, error) {

	versionHistories := r.mutableState.GetExecutionInfo().GetVersionHistories()
	currentVersionHistoryIndex := versionHistories.GetCurrentVersionHistoryIndex()

	// replication task to be applied to current branch
	if branchIndex == currentVersionHistoryIndex {
		return r.mutableState, false, nil
	}

	currentVersionHistory, err := versionhistory.GetVersionHistory(versionHistories, currentVersionHistoryIndex)
	if err != nil {
		return nil, false, err
	}
	currentLastItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return nil, false, err
	}

	// mutable state does not need rebuild
	if incomingVersion < currentLastItem.GetVersion() {
		return r.mutableState, false, nil
	}

	if incomingVersion == currentLastItem.GetVersion() {
		return nil, false, serviceerror.NewInvalidArgument("nDCConflictResolver encountered replication task version == current branch last write version")
	}

	// task.getVersion() > currentLastItem
	// incoming replication task, after application, will become the current branch
	// (because higher version wins), we need to rebuild the mutable state for that
	rebuiltMutableState, err := r.rebuild(ctx, branchIndex, uuid.New())
	if err != nil {
		return nil, false, err
	}
	return rebuiltMutableState, true, nil
}

func (r *nDCConflictResolverImpl) rebuild(
	ctx context.Context,
	branchIndex int32,
	requestID string,
) (workflow.MutableState, error) {

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

	rebuildMutableState, rebuiltHistorySize, err := r.stateRebuilder.rebuild(
		ctx,
		timestamp.TimeValue(executionInfo.StartTime),
		workflowKey,
		replayVersionHistory.GetBranchToken(),
		lastItem.GetEventId(),
		convert.Int64Ptr(lastItem.GetVersion()),
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
	if err != nil {
		return nil, err
	}

	if !rebuildVersionHistory.Equal(replayVersionHistory) {
		return nil, serviceerror.NewInternal("nDCConflictResolver encounter mismatch version history after rebuild")
	}

	// set the current branch index to target branch index
	// set the version history back
	//
	// caller can use the IsVersionHistoriesRebuilt function in VersionHistories
	// telling whether mutable state is rebuilt, before apply new history events
	if err := versionhistory.SetCurrentVersionHistoryIndex(versionHistories, branchIndex); err != nil {
		return nil, err
	}
	rebuildMutableState.GetExecutionInfo().VersionHistories = versionHistories
	// set the update condition from original mutable state
	rebuildMutableState.SetUpdateCondition(r.mutableState.GetUpdateCondition())

	r.context.Clear()
	r.context.SetHistorySize(rebuiltHistorySize)
	return rebuildMutableState, nil
}
