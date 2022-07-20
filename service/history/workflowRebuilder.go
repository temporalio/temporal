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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination workflowResetter_mock.go

package history

import (
	"context"
	"math"

	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	workflowRebuilder interface {
		// rebuild rebuilds a workflow, in case of any kind of corruption
		rebuild(
			ctx context.Context,
			workflowKey definition.WorkflowKey,
		) error
	}

	workflowRebuilderImpl struct {
		shard                      shard.Context
		workflowConsistencyChecker api.WorkflowConsistencyChecker
		newStateRebuilder          nDCStateRebuilderProvider
		transaction                workflow.Transaction
		logger                     log.Logger
	}
)

var _ workflowRebuilder = (*workflowRebuilderImpl)(nil)

func NewWorkflowRebuilder(
	shard shard.Context,
	workflowCache workflow.Cache,
	logger log.Logger,
) *workflowRebuilderImpl {
	return &workflowRebuilderImpl{
		shard:                      shard,
		workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(shard, workflowCache),
		newStateRebuilder: func() nDCStateRebuilder {
			return newNDCStateRebuilder(shard, logger)
		},
		transaction: workflow.NewTransaction(shard),
		logger:      logger,
	}
}

func (r *workflowRebuilderImpl) rebuild(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
) (retError error) {

	wfContext, err := r.workflowConsistencyChecker.GetWorkflowContext(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		workflowKey,
	)
	if err != nil {
		return err
	}
	defer func() {
		wfContext.GetReleaseFn()(retError)
		wfContext.GetContext().Clear()
	}()

	mutableState := wfContext.GetMutableState()
	_, dbRecordVersion := mutableState.GetUpdateCondition()

	requestID := mutableState.GetExecutionState().CreateRequestId
	versionHistories := mutableState.GetExecutionInfo().VersionHistories
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	if err != nil {
		return err
	}
	branchToken := currentVersionHistory.BranchToken
	stateTransitionCount := mutableState.GetExecutionInfo().StateTransitionCount

	rebuildMutableState, rebuildHistorySize, err := r.replayResetWorkflow(
		ctx,
		workflowKey,
		branchToken,
		stateTransitionCount,
		dbRecordVersion,
		requestID,
	)
	if err != nil {
		return err
	}
	return r.persistToDB(ctx, rebuildMutableState, rebuildHistorySize)
}

func (r *workflowRebuilderImpl) replayResetWorkflow(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	branchToken []byte,
	stateTransitionCount int64,
	dbRecordVersion int64,
	requestID string,
) (workflow.MutableState, int64, error) {

	rebuildMutableState, rebuildHistorySize, err := r.newStateRebuilder().rebuild(
		ctx,
		r.shard.GetTimeSource().Now(),
		workflowKey,
		branchToken,
		math.MaxInt64-1, // NOTE: this is last event ID, layer below will +1 to calculate the next event ID
		nil,             // skip event ID & version check
		workflowKey,
		branchToken,
		requestID,
	)
	if err != nil {
		return nil, 0, err
	}

	// note: this is an admin API, for operator to recover a corrupted mutable state, so state transition count
	// should remain the same, the -= 1 exists here since later CloseTransactionAsSnapshot will += 1 to state transition count
	rebuildMutableState.GetExecutionInfo().StateTransitionCount = stateTransitionCount - 1
	rebuildMutableState.SetUpdateCondition(rebuildMutableState.GetNextEventID(), dbRecordVersion)
	return rebuildMutableState, rebuildHistorySize, nil
}

func (r *workflowRebuilderImpl) persistToDB(
	ctx context.Context,
	mutableState workflow.MutableState,
	historySize int64,
) error {
	now := r.shard.GetTimeSource().Now()
	resetWorkflowSnapshot, resetWorkflowEventsSeq, err := mutableState.CloseTransactionAsSnapshot(
		now,
		workflow.TransactionPolicyPassive,
	)
	if err != nil {
		return err
	}
	if len(resetWorkflowEventsSeq) != 0 {
		return serviceerror.NewInternal("workflowRebuilder encountered new events when rebuilding mutable state")
	}

	resetWorkflowSnapshot.ExecutionInfo.ExecutionStats = &persistencespb.ExecutionStats{
		HistorySize: historySize,
	}
	if err := r.transaction.SetWorkflowExecution(
		ctx,
		resetWorkflowSnapshot,
		mutableState.GetNamespaceEntry().ActiveClusterName(),
	); err != nil {
		return err
	}
	return nil
}
