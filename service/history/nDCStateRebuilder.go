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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCStateRebuilder_mock.go

package history

import (
	"context"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	nDCStateRebuilder interface {
		rebuild(
			ctx context.Context,
			now *time.Time,
			baseWorkflowIdentifier definition.WorkflowIdentifier,
			baseBranchToken []byte,
			baseLastEventID int64,
			baseLastEventVersion int64,
			targetWorkflowIdentifier definition.WorkflowIdentifier,
			targetBranchToken []byte,
			requestID string,
		) (mutableState, int64, error)
	}

	nDCStateRebuilderImpl struct {
		shard           ShardContext
		namespaceCache  cache.NamespaceCache
		eventsCache     eventsCache
		clusterMetadata cluster.Metadata
		historyV2Mgr    persistence.HistoryManager
		taskRefresher   mutableStateTaskRefresher

		rebuiltHistorySize int64
		logger             log.Logger
	}
)

var _ nDCStateRebuilder = (*nDCStateRebuilderImpl)(nil)

func newNDCStateRebuilder(
	shard ShardContext,
	logger log.Logger,
) *nDCStateRebuilderImpl {

	return &nDCStateRebuilderImpl{
		shard:           shard,
		namespaceCache:  shard.GetNamespaceCache(),
		eventsCache:     shard.GetEventsCache(),
		clusterMetadata: shard.GetService().GetClusterMetadata(),
		historyV2Mgr:    shard.GetHistoryManager(),
		taskRefresher: newMutableStateTaskRefresher(
			shard.GetConfig(),
			shard.GetNamespaceCache(),
			shard.GetEventsCache(),
			logger,
		),
		rebuiltHistorySize: 0,
		logger:             logger,
	}
}

func (r *nDCStateRebuilderImpl) rebuild(
	ctx context.Context,
	now *time.Time,
	baseWorkflowIdentifier definition.WorkflowIdentifier,
	baseBranchToken []byte,
	baseLastEventID int64,
	baseLastEventVersion int64,
	targetWorkflowIdentifier definition.WorkflowIdentifier,
	targetBranchToken []byte,
	requestID string,
) (mutableState, int64, error) {

	iter := collection.NewPagingIterator(r.getPaginationFn(
		baseWorkflowIdentifier,
		common.FirstEventID,
		baseLastEventID+1,
		baseBranchToken,
	))

	namespaceEntry, err := r.namespaceCache.GetNamespaceByID(targetWorkflowIdentifier.NamespaceID)
	if err != nil {
		return nil, 0, err
	}

	// need to specially handling the first batch, to initialize mutable state & state builder
	batch, err := iter.Next()
	if err != nil {
		return nil, 0, err
	}
	firstEventBatch := batch.(*historypb.History).Events
	rebuiltMutableState, stateBuilder := r.initializeBuilders(
		namespaceEntry,
	)
	if err := r.applyEvents(targetWorkflowIdentifier, stateBuilder, firstEventBatch, requestID); err != nil {
		return nil, 0, err
	}

	for iter.HasNext() {
		batch, err := iter.Next()
		if err != nil {
			return nil, 0, err
		}
		events := batch.(*historypb.History).Events
		if err := r.applyEvents(targetWorkflowIdentifier, stateBuilder, events, requestID); err != nil {
			return nil, 0, err
		}
	}

	if err := rebuiltMutableState.SetCurrentBranchToken(targetBranchToken); err != nil {
		return nil, 0, err
	}
	currentVersionHistory, err := rebuiltMutableState.GetVersionHistories().GetCurrentVersionHistory()
	if err != nil {
		return nil, 0, err
	}
	lastItem, err := currentVersionHistory.GetLastItem()
	if err != nil {
		return nil, 0, err
	}
	if !lastItem.Equals(persistence.NewVersionHistoryItem(
		baseLastEventID,
		baseLastEventVersion,
	)) {
		return nil, 0, serviceerror.NewInternal(fmt.Sprintf("nDCStateRebuilder unable to rebuild mutable state to event ID: %v, version: %v", baseLastEventID, baseLastEventVersion))
	}

	nowValue := timestamp.TimeValue(now)
	// close rebuilt mutable state transaction clearing all generated tasks, etc.
	_, _, err = rebuiltMutableState.CloseTransactionAsSnapshot(nowValue, transactionPolicyPassive)
	if err != nil {
		return nil, 0, err
	}

	// refresh tasks to be generated
	if err := r.taskRefresher.refreshTasks(nowValue, rebuiltMutableState); err != nil {
		return nil, 0, err
	}

	// mutable state rebuild should use the same time stamp
	rebuiltMutableState.GetExecutionInfo().StartTime = &nowValue
	return rebuiltMutableState, r.rebuiltHistorySize, nil
}

func (r *nDCStateRebuilderImpl) initializeBuilders(
	namespaceEntry *cache.NamespaceCacheEntry,
) (mutableState, stateBuilder) {
	resetMutableStateBuilder := newMutableStateBuilderWithVersionHistories(
		r.shard,
		r.shard.GetEventsCache(),
		r.logger,
		namespaceEntry,
	)
	stateBuilder := newStateBuilder(
		r.shard,
		r.logger,
		resetMutableStateBuilder,
		func(mutableState mutableState) mutableStateTaskGenerator {
			return newMutableStateTaskGenerator(r.shard.GetNamespaceCache(), r.logger, mutableState)
		},
	)
	return resetMutableStateBuilder, stateBuilder
}

func (r *nDCStateRebuilderImpl) applyEvents(
	workflowIdentifier definition.WorkflowIdentifier,
	stateBuilder stateBuilder,
	events []*historypb.HistoryEvent,
	requestID string,
) error {

	_, err := stateBuilder.applyEvents(
		workflowIdentifier.NamespaceID,
		requestID,
		commonpb.WorkflowExecution{
			WorkflowId: workflowIdentifier.WorkflowID,
			RunId:      workflowIdentifier.RunID,
		},
		events,
		nil, // no new run history when rebuilding mutable state
	)
	if err != nil {
		r.logger.Error("nDCStateRebuilder unable to rebuild mutable state.", tag.Error(err))
		return err
	}
	return nil
}

func (r *nDCStateRebuilderImpl) getPaginationFn(
	workflowIdentifier definition.WorkflowIdentifier,
	firstEventID int64,
	nextEventID int64,
	branchToken []byte,
) collection.PaginationFn {

	return func(paginationToken []byte) ([]interface{}, []byte, error) {

		_, historyBatches, token, size, err := PaginateHistory(
			r.historyV2Mgr,
			true,
			branchToken,
			firstEventID,
			nextEventID,
			paginationToken,
			nDCDefaultPageSize,
			convert.Int32Ptr(r.shard.GetShardID()),
		)
		if err != nil {
			return nil, nil, err
		}
		r.rebuiltHistorySize += int64(size)

		var paginateItems []interface{}
		for _, history := range historyBatches {
			paginateItems = append(paginateItems, history)
		}
		return paginateItems, token, nil
	}
}
