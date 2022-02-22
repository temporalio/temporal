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
	"fmt"
	"math"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	workflowRebuild interface {
		// workflowRebuild rebuilds a workflow, in case of any kind of corruption
		workflowRebuild(
			ctx context.Context,
			workflowKey definition.WorkflowKey,
		) error
	}

	workflowRebuilderImpl struct {
		shard          shard.Context
		workflowCache  workflow.Cache
		executionMgr   persistence.ExecutionManager
		stateRebuilder nDCStateRebuilderProvider
		transaction    workflow.Transaction
		logger         log.Logger
	}
)

var _ workflowRebuild = (*workflowRebuilderImpl)(nil)

func NewWorkflowRebuilder(
	shard shard.Context,
	workflowCache workflow.Cache,
	stateRebuilder nDCStateRebuilderProvider,
	transaction workflow.Transaction,
	logger log.Logger,
) *workflowRebuilderImpl {
	return &workflowRebuilderImpl{
		shard:          shard,
		workflowCache:  workflowCache,
		executionMgr:   shard.GetExecutionManager(),
		stateRebuilder: stateRebuilder,
		transaction:    transaction,
		logger:         logger,
	}
}

func (r *workflowRebuilderImpl) workflowRebuild(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
) (retError error) {
	context, releaseFn, err := r.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespace.ID(workflowKey.NamespaceID),
		commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
		workflow.CallerTypeAPI,
	)
	if err != nil {
		return err
	}
	defer func() { releaseFn(retError) }()
	context.Clear()

	msRecord, dbRecordVersion, err := r.getMutableState(workflowKey)
	if err != nil {
		return err
	}
	requestID := msRecord.ExecutionState.CreateRequestId
	versionHistories := msRecord.ExecutionInfo.VersionHistories
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	if err != nil {
		return err
	}
	branchToken := currentVersionHistory.BranchToken

	rebuildMutableState, rebuildHistorySize, err := r.replayResetWorkflow(
		ctx,
		workflowKey,
		branchToken,
		dbRecordVersion,
		requestID,
	)
	if err != nil {
		return err
	}
	return r.persistToDB(rebuildMutableState, rebuildHistorySize)
}

func (r *workflowRebuilderImpl) replayResetWorkflow(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	branchToken []byte,
	dbRecordVersion int64,
	requestID string,
) (workflow.MutableState, int64, error) {

	rebuildMutableState, rebuildHistorySize, err := r.stateRebuilder().rebuild(
		ctx,
		r.shard.GetTimeSource().Now(),
		workflowKey,
		branchToken,
		math.MaxInt64,
		nil, // skip event ID & version check
		workflowKey,
		branchToken,
		requestID,
	)
	if err != nil {
		return nil, 0, err
	}
	rebuildMutableState.SetUpdateCondition(rebuildMutableState.GetNextEventID(), dbRecordVersion)
	return rebuildMutableState, rebuildHistorySize, nil
}

func (r *workflowRebuilderImpl) persistToDB(
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
		resetWorkflowSnapshot,
		mutableState.GetNamespaceEntry().ActiveClusterName(),
	); err != nil {
		return err
	}
	return nil
}

func (r *workflowRebuilderImpl) getMutableState(
	workflowKey definition.WorkflowKey,
) (*persistencespb.WorkflowMutableState, int64, error) {
	record, err := r.executionMgr.GetWorkflowExecution(&persistence.GetWorkflowExecutionRequest{
		ShardID:     r.shard.GetShardID(),
		NamespaceID: workflowKey.NamespaceID,
		WorkflowID:  workflowKey.WorkflowID,
		RunID:       workflowKey.RunID,
	})
	if _, ok := err.(*serviceerror.NotFound); ok {
		return nil, 0, err
	}
	// only check whether the execution is nil, do as much as we can
	if record == nil {
		return nil, 0, serviceerror.NewUnavailable(fmt.Sprintf("workflowRebuilder encountered error when loading execution record: %v", err))
	}

	return record.State, record.DBRecordVersion, nil
}
