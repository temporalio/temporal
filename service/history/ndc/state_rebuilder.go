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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination state_rebuilder_mock.go

package ndc

import (
	"context"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	StateRebuilder interface {
		Rebuild(
			ctx context.Context,
			now time.Time,
			baseWorkflowIdentifier definition.WorkflowKey,
			baseBranchToken []byte,
			baseLastEventID int64,
			baseLastEventVersion *int64,
			targetWorkflowIdentifier definition.WorkflowKey,
			targetBranchToken []byte,
			requestID string,
		) (workflow.MutableState, int64, error)
	}

	StateRebuilderImpl struct {
		shard             shard.Context
		namespaceRegistry namespace.Registry
		eventsCache       events.Cache
		clusterMetadata   cluster.Metadata
		executionMgr      persistence.ExecutionManager
		taskRefresher     workflow.TaskRefresher

		rebuiltHistorySize int64
		logger             log.Logger
	}

	HistoryBlobsPaginationItem struct {
		History       *historypb.History
		TransactionID int64
	}
)

var _ StateRebuilder = (*StateRebuilderImpl)(nil)

func NewStateRebuilder(
	shard shard.Context,
	logger log.Logger,
) *StateRebuilderImpl {

	return &StateRebuilderImpl{
		shard:             shard,
		namespaceRegistry: shard.GetNamespaceRegistry(),
		eventsCache:       shard.GetEventsCache(),
		clusterMetadata:   shard.GetClusterMetadata(),
		executionMgr:      shard.GetExecutionManager(),
		taskRefresher: workflow.NewTaskRefresher(
			shard,
			logger,
		),
		rebuiltHistorySize: 0,
		logger:             logger,
	}
}

func (r *StateRebuilderImpl) Rebuild(
	ctx context.Context,
	now time.Time,
	baseWorkflowIdentifier definition.WorkflowKey,
	baseBranchToken []byte,
	baseLastEventID int64,
	baseLastEventVersion *int64,
	targetWorkflowIdentifier definition.WorkflowKey,
	targetBranchToken []byte,
	requestID string,
) (workflow.MutableState, int64, error) {
	iter := collection.NewPagingIterator(r.getPaginationFn(
		ctx,
		common.FirstEventID,
		baseLastEventID+1,
		baseBranchToken,
	))

	namespaceEntry, err := r.namespaceRegistry.GetNamespaceByID(namespace.ID(targetWorkflowIdentifier.NamespaceID))
	if err != nil {
		return nil, 0, err
	}

	rebuiltMutableState, stateBuilder := r.initializeBuilders(
		namespaceEntry,
		targetWorkflowIdentifier,
		now,
	)

	var lastTxnId int64
	for iter.HasNext() {
		history, err := iter.Next()
		switch err.(type) {
		case nil:
			// noop
		case *serviceerror.DataLoss:
			r.logger.Error("encountered data loss event", tag.WorkflowNamespaceID(baseWorkflowIdentifier.NamespaceID), tag.WorkflowID(baseWorkflowIdentifier.WorkflowID), tag.WorkflowRunID(baseWorkflowIdentifier.RunID))
			return nil, 0, err
		default:
			return nil, 0, err
		}

		if err := r.applyEvents(
			ctx,
			targetWorkflowIdentifier,
			stateBuilder,
			history.History.Events,
			requestID,
		); err != nil {
			return nil, 0, err
		}

		lastTxnId = history.TransactionID
	}

	if err := rebuiltMutableState.SetCurrentBranchToken(targetBranchToken); err != nil {
		return nil, 0, err
	}
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(rebuiltMutableState.GetExecutionInfo().GetVersionHistories())
	if err != nil {
		return nil, 0, err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
	if err != nil {
		return nil, 0, err
	}

	if baseLastEventVersion != nil {
		if !lastItem.Equal(versionhistory.NewVersionHistoryItem(
			baseLastEventID,
			*baseLastEventVersion,
		)) {
			return nil, 0, serviceerror.NewInvalidArgument(fmt.Sprintf(
				"StateRebuilder unable to Rebuild mutable state to event ID: %v, version: %v, this event must be at the boundary",
				baseLastEventID,
				*baseLastEventVersion,
			))
		}
	}

	// close rebuilt mutable state transaction clearing all generated tasks, etc.
	_, _, err = rebuiltMutableState.CloseTransactionAsSnapshot(workflow.TransactionPolicyPassive)
	if err != nil {
		return nil, 0, err
	}

	rebuiltMutableState.GetExecutionInfo().LastFirstEventTxnId = lastTxnId

	// refresh tasks to be generated
	// TODO: ideally the executionTimeoutTimerTaskStatus field should be carried over
	// from the base run. However, RefreshTasks always resets that field and
	// force regenerates the execution timeout timer task.
	if err := r.taskRefresher.RefreshTasks(ctx, rebuiltMutableState); err != nil {
		return nil, 0, err
	}

	return rebuiltMutableState, r.rebuiltHistorySize, nil
}

func (r *StateRebuilderImpl) initializeBuilders(
	namespaceEntry *namespace.Namespace,
	workflowIdentifier definition.WorkflowKey,
	now time.Time,
) (workflow.MutableState, workflow.MutableStateRebuilder) {
	resetMutableState := workflow.NewMutableState(
		r.shard,
		r.shard.GetEventsCache(),
		r.logger,
		namespaceEntry,
		workflowIdentifier.GetWorkflowID(),
		workflowIdentifier.GetRunID(),
		now,
	)
	stateBuilder := workflow.NewMutableStateRebuilder(
		r.shard,
		r.logger,
		resetMutableState,
	)
	return resetMutableState, stateBuilder
}

func (r *StateRebuilderImpl) applyEvents(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	stateBuilder workflow.MutableStateRebuilder,
	events []*historypb.HistoryEvent,
	requestID string,
) error {

	_, err := stateBuilder.ApplyEvents(
		ctx,
		namespace.ID(workflowKey.NamespaceID),
		requestID,
		&commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
		[][]*historypb.HistoryEvent{events},
		nil, // no new run history when rebuilding mutable state
		"",
	)
	if err != nil {
		r.logger.Error("StateRebuilder unable to Rebuild mutable state.", tag.Error(err))
		return err
	}
	return nil
}

func (r *StateRebuilderImpl) getPaginationFn(
	ctx context.Context,
	firstEventID int64,
	nextEventID int64,
	branchToken []byte,
) collection.PaginationFn[HistoryBlobsPaginationItem] {
	return func(paginationToken []byte) ([]HistoryBlobsPaginationItem, []byte, error) {
		resp, err := r.executionMgr.ReadHistoryBranchByBatch(ctx, &persistence.ReadHistoryBranchRequest{
			BranchToken:   branchToken,
			MinEventID:    firstEventID,
			MaxEventID:    nextEventID,
			PageSize:      defaultPageSize,
			NextPageToken: paginationToken,
			ShardID:       r.shard.GetShardID(),
		})
		if err != nil {
			return nil, nil, err
		}

		r.rebuiltHistorySize += int64(resp.Size)
		paginateItems := make([]HistoryBlobsPaginationItem, 0, len(resp.History))
		for i, history := range resp.History {
			nextBatch := HistoryBlobsPaginationItem{
				History:       history,
				TransactionID: resp.TransactionIDs[i],
			}
			paginateItems = append(paginateItems, nextBatch)
		}
		return paginateItems, resp.NextPageToken, nil
	}
}
