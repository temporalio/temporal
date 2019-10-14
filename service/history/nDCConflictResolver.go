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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCConflictResolver_mock.go

package history

import (
	ctx "context"

	"github.com/pborman/uuid"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
)

type (
	nDCConflictResolver interface {
		prepareMutableState(
			ctx ctx.Context,
			branchIndex int,
			incomingVersion int64,
		) (mutableState, bool, error)
	}

	nDCConflictResolverImpl struct {
		shard          ShardContext
		stateRebuilder nDCStateRebuilder

		context      workflowExecutionContext
		mutableState mutableState
		logger       log.Logger
	}
)

var _ nDCConflictResolver = (*nDCConflictResolverImpl)(nil)

func newNDCConflictResolver(
	shard ShardContext,
	context workflowExecutionContext,
	mutableState mutableState,
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
	ctx ctx.Context,
	branchIndex int,
	incomingVersion int64,
) (mutableState, bool, error) {

	versionHistories := r.mutableState.GetVersionHistories()
	currentVersionHistoryIndex := versionHistories.GetCurrentVersionHistoryIndex()

	// replication task to be applied to current branch
	if branchIndex == currentVersionHistoryIndex {
		return r.mutableState, false, nil
	}

	currentVersionHistory, err := versionHistories.GetVersionHistory(currentVersionHistoryIndex)
	if err != nil {
		return nil, false, err
	}
	currentLastItem, err := currentVersionHistory.GetLastItem()
	if err != nil {
		return nil, false, err
	}

	// mutable state does not need rebuild
	if incomingVersion < currentLastItem.GetVersion() {
		return r.mutableState, false, nil
	}

	if incomingVersion == currentLastItem.GetVersion() {
		return nil, false, &shared.BadRequestError{
			Message: "nDCConflictResolver encounter replication task version == current branch last write version",
		}
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
	ctx ctx.Context,
	branchIndex int,
	requestID string,
) (mutableState, error) {

	versionHistories := r.mutableState.GetVersionHistories()
	replayVersionHistory, err := versionHistories.GetVersionHistory(branchIndex)
	if err != nil {
		return nil, err
	}
	lastItem, err := replayVersionHistory.GetLastItem()
	if err != nil {
		return nil, err
	}

	executionInfo := r.mutableState.GetExecutionInfo()
	workflowIdentifier := definition.NewWorkflowIdentifier(
		executionInfo.DomainID,
		executionInfo.WorkflowID,
		executionInfo.RunID,
	)

	rebuildMutableState, rebuiltHistorySize, err := r.stateRebuilder.rebuild(
		ctx,
		executionInfo.StartTimestamp,
		workflowIdentifier,
		replayVersionHistory.GetBranchToken(),
		lastItem.GetEventID(),
		lastItem.GetVersion(),
		workflowIdentifier,
		replayVersionHistory.GetBranchToken(),
		requestID,
	)
	if err != nil {
		return nil, err
	}

	// after rebuilt verification
	rebuildVersionHistories := rebuildMutableState.GetVersionHistories()
	rebuildVersionHistory, err := rebuildVersionHistories.GetCurrentVersionHistory()
	if err != nil {
		return nil, err
	}

	if !rebuildVersionHistory.Equals(replayVersionHistory) {
		return nil, &shared.InternalServiceError{
			Message: "nDCConflictResolver encounter mismatch version history after rebuild",
		}
	}

	// set the current branch index to target branch index
	// set the version history back
	//
	// caller can use the IsRebuilt function in VersionHistories
	// telling whether mutable state is rebuilt, before apply new history events
	if err := versionHistories.SetCurrentVersionHistoryIndex(branchIndex); err != nil {
		return nil, err
	}
	if err = rebuildMutableState.SetVersionHistories(versionHistories); err != nil {
		return nil, err
	}
	// set the update condition from original mutable state
	rebuildMutableState.SetUpdateCondition(r.mutableState.GetUpdateCondition())

	r.context.clear()
	r.context.setHistorySize(rebuiltHistorySize)
	return rebuildMutableState, nil
}
