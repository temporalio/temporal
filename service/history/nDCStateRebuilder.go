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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCStateRebuilder_mock.go

package history

import (
	ctx "context"
	"fmt"
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
)

type (
	nDCStateRebuilder interface {
		rebuild(
			ctx ctx.Context,
			now time.Time,
			baseWorkflowIdentifier definition.WorkflowIdentifier,
			baseBranchToken []byte,
			baseNextEventID int64,
			targetWorkflowIdentifier definition.WorkflowIdentifier,
			targetBranchToken []byte,
			requestID string,
		) (mutableState, int64, error)
	}

	nDCStateRebuilderImpl struct {
		shard           ShardContext
		domainCache     cache.DomainCache
		eventsCache     eventsCache
		clusterMetadata cluster.Metadata
		historyV2Mgr    persistence.HistoryV2Manager
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
		domainCache:     shard.GetDomainCache(),
		eventsCache:     shard.GetEventsCache(),
		clusterMetadata: shard.GetService().GetClusterMetadata(),
		historyV2Mgr:    shard.GetHistoryV2Manager(),
		taskRefresher: newMutableStateTaskRefresher(
			shard.GetConfig(),
			shard.GetDomainCache(),
			shard.GetEventsCache(),
			logger,
		),
		rebuiltHistorySize: 0,
		logger:             logger,
	}
}

func (r *nDCStateRebuilderImpl) rebuild(
	ctx ctx.Context,
	now time.Time,
	baseWorkflowIdentifier definition.WorkflowIdentifier,
	baseBranchToken []byte,
	baseNextEventID int64,
	targetWorkflowIdentifier definition.WorkflowIdentifier,
	targetBranchToken []byte,
	requestID string,
) (mutableState, int64, error) {

	iter := collection.NewPagingIterator(r.getPaginationFn(
		baseWorkflowIdentifier,
		common.FirstEventID,
		baseNextEventID,
		baseBranchToken,
	))

	domainEntry, err := r.domainCache.GetDomainByID(targetWorkflowIdentifier.DomainID)
	if err != nil {
		return nil, 0, err
	}

	// need to specially handling the first batch, to initialize mutable state & state builder
	batch, err := iter.Next()
	if err != nil {
		return nil, 0, err
	}
	firstEventBatch := batch.(*shared.History).Events
	rebuiltMutableState, stateBuilder := r.initializeBuilders(
		domainEntry,
	)
	if err := r.applyEvents(targetWorkflowIdentifier, stateBuilder, firstEventBatch, requestID); err != nil {
		return nil, 0, err
	}

	for iter.HasNext() {
		batch, err := iter.Next()
		if err != nil {
			return nil, 0, err
		}
		events := batch.(*shared.History).Events
		if err := r.applyEvents(targetWorkflowIdentifier, stateBuilder, events, requestID); err != nil {
			return nil, 0, err
		}
	}

	if rebuiltMutableState.GetNextEventID() != baseNextEventID {
		return nil, 0, &shared.InternalServiceError{
			Message: fmt.Sprintf("nDCStateRebuilder unable to rebuild mutable state to event ID: %v", baseNextEventID),
		}
	}
	if err := rebuiltMutableState.SetCurrentBranchToken(targetBranchToken); err != nil {
		return nil, 0, err
	}

	// close rebuilt mutable state transaction clearing all generated tasks, etc.
	_, _, err = rebuiltMutableState.CloseTransactionAsSnapshot(now, transactionPolicyPassive)
	if err != nil {
		return nil, 0, err
	}

	// refresh tasks to be generated
	if err := r.taskRefresher.refreshTasks(now, rebuiltMutableState); err != nil {
		return nil, 0, err
	}

	return rebuiltMutableState, r.rebuiltHistorySize, nil
}

func (r *nDCStateRebuilderImpl) initializeBuilders(
	domainEntry *cache.DomainCacheEntry,
) (mutableState, stateBuilder) {
	resetMutableStateBuilder := newMutableStateBuilderWithVersionHistories(
		r.shard,
		r.shard.GetEventsCache(),
		r.logger,
		domainEntry,
	)
	resetMutableStateBuilder.executionInfo.EventStoreVersion = nDCProtocolVersion
	stateBuilder := newStateBuilder(r.shard, resetMutableStateBuilder, r.logger)
	return resetMutableStateBuilder, stateBuilder
}

func (r *nDCStateRebuilderImpl) applyEvents(
	workflowIdentifier definition.WorkflowIdentifier,
	stateBuilder stateBuilder,
	events []*shared.HistoryEvent,
	requestID string,
) error {

	_, _, _, err := stateBuilder.applyEvents(
		workflowIdentifier.DomainID,
		requestID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowIdentifier.WorkflowID),
			RunId:      common.StringPtr(workflowIdentifier.RunID),
		},
		events,
		nil, // no new run history when rebuilding mutable state
		nDCProtocolVersion,
		nDCProtocolVersion,
		true,
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
			nil,
			r.historyV2Mgr,
			true,
			workflowIdentifier.DomainID,
			workflowIdentifier.WorkflowID,
			workflowIdentifier.RunID,
			persistence.EventStoreVersionV2,
			branchToken,
			firstEventID,
			nextEventID,
			paginationToken,
			nDCDefaultPageSize,
			common.IntPtr(r.shard.GetShardID()),
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
