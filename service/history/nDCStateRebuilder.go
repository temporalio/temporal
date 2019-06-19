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
	"fmt"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/persistence"
)

type (
	nDCStateRebuilder interface {
		rebuild(
			ctx ctx.Context,
			requestID string,
		) (mutableState, error)
	}

	nDCStateRebuilderImpl struct {
		mutableState mutableState
		branchIndex  int

		shard           ShardContext
		clusterMetadata cluster.Metadata
		historyV2Mgr    persistence.HistoryV2Manager
		logger          log.Logger
	}
)

var _ nDCStateRebuilder = (*nDCStateRebuilderImpl)(nil)

func newNDCStateRebuilder(
	mutableState mutableState,
	branchIndex int,

	shard ShardContext,
	historyV2Mgr persistence.HistoryV2Manager,
	logger log.Logger,
) *nDCStateRebuilderImpl {

	return &nDCStateRebuilderImpl{
		mutableState: mutableState,
		branchIndex:  branchIndex,

		shard:           shard,
		clusterMetadata: shard.GetService().GetClusterMetadata(),
		historyV2Mgr:    historyV2Mgr,
		logger:          logger,
	}
}

func (r *nDCStateRebuilderImpl) rebuild(
	ctx ctx.Context,
	requestID string,
) (mutableState, error) {

	versionHistories := r.mutableState.GetVersionHistories()
	replayVersionHistory, err := versionHistories.GetVersionHistory(r.branchIndex)
	if err != nil {
		return nil, err
	}
	lastItem, err := replayVersionHistory.GetLastItem()
	if err != nil {
		return nil, err
	}

	iter := collection.NewPagingIterator(r.getPaginationFn(
		common.FirstEventID,
		lastItem.GetEventID()+1,
		replayVersionHistory.GetBranchToken(),
	))

	// need to specially handling the first batch, to initialize mutable state & state builder
	batch, err := iter.Next()
	if err != nil {
		return nil, err
	}
	firstEventBatch := batch.(*shared.History).Events
	rebuildMutableState, stateBuilder := r.initializeBuilders(firstEventBatch[0].GetVersion())
	if err := r.applyEvents(stateBuilder, firstEventBatch, requestID); err != nil {
		return nil, err
	}

	for iter.HasNext() {
		batch, err := iter.Next()
		if err != nil {
			return nil, err
		}
		events := batch.(*shared.History).Events
		if err := r.applyEvents(stateBuilder, events, requestID); err != nil {
			return nil, err
		}
	}

	rebuildVersionHistories := rebuildMutableState.GetVersionHistories()
	rebuildVersionHistory, err := rebuildVersionHistories.GetVersionHistory(
		rebuildVersionHistories.GetCurrentBranchIndex(),
	)
	if err != nil {
		return nil, err
	}
	err = rebuildVersionHistory.SetBranchToken(replayVersionHistory.GetBranchToken())
	if err != nil {
		return nil, err
	}

	// TODO also double check get next event ID
	//Sanity check on last event id of the last history batch
	if !rebuildVersionHistory.Equals(replayVersionHistory) {
		return nil, &shared.BadRequestError{Message: fmt.Sprintf(
			"failed to rebuild mutable state as the version history before / after are not matched",
		)}
	}

	// set the original version history back
	if err = rebuildMutableState.SetVersionHistories(versionHistories); err != nil {
		return nil, err
	}
	return rebuildMutableState, nil
}

func (r *nDCStateRebuilderImpl) initializeBuilders(
	version int64,
) (mutableState, stateBuilder) {
	resetMutableStateBuilder := newMutableStateBuilderWithVersionHistories(
		r.clusterMetadata.GetCurrentClusterName(),
		r.shard,
		r.shard.GetEventsCache(),
		r.logger,
		version,
	)
	resetMutableStateBuilder.executionInfo.EventStoreVersion = nDCMutableStateEventStoreVersion
	stateBuilder := newStateBuilder(r.shard, resetMutableStateBuilder, r.logger)
	return resetMutableStateBuilder, stateBuilder
}

func (r *nDCStateRebuilderImpl) applyEvents(
	stateBuilder stateBuilder,
	events []*shared.HistoryEvent,
	requestID string,
) error {

	executionInfo := r.mutableState.GetExecutionInfo()
	_, _, _, err := stateBuilder.applyEvents(
		executionInfo.DomainID,
		requestID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(executionInfo.WorkflowID),
			RunId:      common.StringPtr(executionInfo.RunID),
		},
		events,
		nil, // no new run history when rebuilding mutable state
		nDCMutableStateEventStoreVersion,
		nDCMutableStateEventStoreVersion,
	)
	if err != nil {
		r.logger.Error("error rebuild mutable state.", tag.Error(err))
		return err
	}
	return nil
}

func (r *nDCStateRebuilderImpl) getPaginationFn(
	firstEventID int64,
	nextEventID int64,
	branchToken []byte,
) collection.PaginationFn {

	executionInfo := r.mutableState.GetExecutionInfo()
	return func(paginationToken []byte) ([]interface{}, []byte, error) {

		_, historyBatches, token, _, err := PaginateHistory(
			nil,
			r.historyV2Mgr,
			nil,
			r.logger,
			true,
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			firstEventID,
			nextEventID,
			paginationToken,
			nDCMutableStateEventStoreVersion,
			branchToken,
			nDCDefaultPageSize,
			common.IntPtr(r.shard.GetShardID()),
		)
		if err != nil {
			return nil, nil, err
		}

		var paginateItems []interface{}
		for _, history := range historyBatches {
			paginateItems = append(paginateItems, history)
		}
		return paginateItems, token, nil
	}
}
