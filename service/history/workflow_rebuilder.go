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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination workflow_rebuilder_mock.go

package history

import (
	"context"
	"math"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	rebuildSpec struct {
		branchToken          []byte
		stateTransitionCount int64
		dbRecordVersion      int64
		requestID            string
	}
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
		transaction                workflow.Transaction
		logger                     log.Logger
	}
)

var _ workflowRebuilder = (*workflowRebuilderImpl)(nil)

func NewWorkflowRebuilder(
	shard shard.Context,
	workflowCache wcache.Cache,
	logger log.Logger,
) *workflowRebuilderImpl {
	return &workflowRebuilderImpl{
		shard:                      shard,
		workflowConsistencyChecker: api.NewWorkflowConsistencyChecker(shard, workflowCache),
		transaction:                workflow.NewTransaction(shard),
		logger:                     logger,
	}
}

func (r *workflowRebuilderImpl) rebuild(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
) (retError error) {

	wfCache := r.workflowConsistencyChecker.GetWorkflowCache()
	rebuildSpec, err := r.getRebuildSpecFromMutableState(ctx, &workflowKey)
	if err != nil {
		return err
	}
	wfContext, releaseFn, err := wfCache.GetOrCreateWorkflowExecution(
		ctx,
		r.shard,
		namespace.ID(workflowKey.NamespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
		workflow.LockPriorityHigh,
	)
	if err != nil {
		return err
	}
	defer func() {
		releaseFn(retError)
		wfContext.Clear()
	}()

	rebuildMutableState, err := r.replayResetWorkflow(
		ctx,
		workflowKey,
		rebuildSpec.branchToken,
		rebuildSpec.stateTransitionCount,
		rebuildSpec.dbRecordVersion,
		rebuildSpec.requestID,
	)
	if err != nil {
		return err
	}
	return r.overwriteToDB(ctx, rebuildMutableState)
}

func (r *workflowRebuilderImpl) getRebuildSpecFromMutableState(
	ctx context.Context,
	workflowKey *definition.WorkflowKey,
) (*rebuildSpec, error) {
	if workflowKey.RunID == "" {
		resp, err := r.shard.GetCurrentExecution(
			ctx,
			&persistence.GetCurrentExecutionRequest{
				ShardID:     r.shard.GetShardID(),
				NamespaceID: workflowKey.NamespaceID,
				WorkflowID:  workflowKey.WorkflowID,
			},
		)
		if err != nil && resp == nil {
			return nil, err
		}
		workflowKey.RunID = resp.RunID
	}
	resp, err := r.shard.GetWorkflowExecution(
		ctx,
		&persistence.GetWorkflowExecutionRequest{
			ShardID:     r.shard.GetShardID(),
			NamespaceID: workflowKey.NamespaceID,
			WorkflowID:  workflowKey.WorkflowID,
			RunID:       workflowKey.RunID,
		},
	)
	if err != nil && resp == nil {
		return nil, err
	}

	mutableState := resp.State
	versionHistories := mutableState.ExecutionInfo.VersionHistories
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	if err != nil {
		return nil, err
	}
	return &rebuildSpec{
		branchToken:          currentVersionHistory.BranchToken,
		stateTransitionCount: mutableState.ExecutionInfo.StateTransitionCount,
		dbRecordVersion:      resp.DBRecordVersion,
		requestID:            mutableState.ExecutionState.CreateRequestId,
	}, nil
}

func (r *workflowRebuilderImpl) replayResetWorkflow(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	branchToken []byte,
	stateTransitionCount int64,
	dbRecordVersion int64,
	requestID string,
) (workflow.MutableState, error) {

	rebuildMutableState, rebuildHistorySize, err := ndc.NewStateRebuilder(r.shard, r.logger).Rebuild(
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
		return nil, err
	}

	// note: this is an admin API, for operator to recover a corrupted mutable state, so state transition count
	// should remain the same, the -= 1 exists here since later CloseTransactionAsSnapshot will += 1 to state transition count
	rebuildMutableState.GetExecutionInfo().StateTransitionCount = stateTransitionCount - 1
	rebuildMutableState.AddHistorySize(rebuildHistorySize)
	rebuildMutableState.SetUpdateCondition(rebuildMutableState.GetNextEventID(), dbRecordVersion)
	return rebuildMutableState, nil
}

func (r *workflowRebuilderImpl) overwriteToDB(
	ctx context.Context,
	mutableState workflow.MutableState,
) error {
	resetWorkflowSnapshot, resetWorkflowEventsSeq, err := mutableState.CloseTransactionAsSnapshot(
		workflow.TransactionPolicyPassive,
	)
	if err != nil {
		return err
	}
	if len(resetWorkflowEventsSeq) != 0 {
		return serviceerror.NewInternal("workflowRebuilder encountered new events when rebuilding mutable state")
	}

	return r.transaction.SetWorkflowExecution(
		ctx,
		resetWorkflowSnapshot,
	)
}
