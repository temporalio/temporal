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

package ndc

import (
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	HistoryBackfiller interface {
		BackfillWorkflow(
			ctx context.Context,
			workflowKey definition.WorkflowKey,
			versionHistoryItems []*historyspb.VersionHistoryItem,
			events [][]*historypb.HistoryEvent,
			token []byte,
		) ([]byte, error)
	}

	HistoryBackfillerImpl struct {
		shard          shard.Context
		namespaceCache namespace.Registry
		logger         log.Logger
	}
)

func NewHistoryBackfiller(
	shard shard.Context,
	logger log.Logger,
) *HistoryBackfillerImpl {
	backfiller := &HistoryBackfillerImpl{
		shard:  shard,
		logger: logger,
	}
	return backfiller
}

func (r *HistoryBackfillerImpl) BackfillWorkflow(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	events [][]*historypb.HistoryEvent,
	token []byte,
) ([]byte, error) {
	namespaceEntry, err := r.namespaceCache.GetNamespaceByID(namespace.ID(workflowKey.NamespaceID))
	if err != nil {
		return nil, err
	}
	var mutableState workflow.MutableState
	if len(token) == 0 {
		mutableState = workflow.NewMutableState(
			r.shard,
			r.shard.GetEventsCache(),
			r.logger,
			namespaceEntry,
			time.Now().UTC(),
		)
	} else {
		mutableStateRow, err := r.deserializeBackfillToken(token)
		if err != nil {
			return nil, err
		}
		mutableState, err = workflow.NewMutableStateFromDB(
			r.shard,
			r.shard.GetEventsCache(),
			r.logger,
			namespaceEntry,
			mutableStateRow,
			1,
		)
		if err != nil {
			return nil, err
		}
	}
	wfContext := workflow.NewContext(
		r.shard,
		definition.NewWorkflowKey(
			workflowKey.NamespaceID,
			workflowKey.WorkflowID,
			workflowKey.RunID,
		),
		r.logger,
	)
	wfContext.MutableState = mutableState

	// TODO do validation of version history

	requestID := uuid.New()
	stateBuilder := workflow.NewMutableStateRebuilder(
		r.shard,
		r.logger,
		mutableState,
	)

	// use state builder for workflow mutable state mutation
	_, err = stateBuilder.ApplyEvents(
		ctx,
		namespace.ID(workflowKey.NamespaceID),
		requestID,
		commonpb.WorkflowExecution{WorkflowId: workflowKey.WorkflowID, RunId: workflowKey.RunID},
		events,
		nil,
	)
	if err != nil {
		r.logger.Error(
			"HistoryBackfiller unable to apply events",
			tag.Error(err),
		)
		return nil, err
	}

	// TODO do serialization of mutable state
	// TODO when all events are applied, do refresh of tasks & write to DB
	err = r.transactionMgr.createWorkflow(
		ctx,
		NewWorkflow(
			ctx,
			r.namespaceRegistry,
			r.clusterMetadata,
			context,
			mutableState,
			releaseFn,
		),
	)
	if err != nil {
		return nil, err
	}
	// serialization?
	return nil, nil
}

func (r *HistoryBackfillerImpl) serializeBackfillToken(
	mutableState *persistencespb.WorkflowMutableState,
) ([]byte, error) {
	return mutableState.Marshal()
}

func (r *HistoryBackfillerImpl) deserializeBackfillToken(
	token []byte,
) (*persistencespb.WorkflowMutableState, error) {
	mutableState := &persistencespb.WorkflowMutableState{
		ActivityInfos:       make(map[int64]*persistencespb.ActivityInfo),
		TimerInfos:          make(map[string]*persistencespb.TimerInfo),
		ChildExecutionInfos: make(map[int64]*persistencespb.ChildExecutionInfo),
		RequestCancelInfos:  make(map[int64]*persistencespb.RequestCancelInfo),
		SignalInfos:         make(map[int64]*persistencespb.SignalInfo),
		SignalRequestedIds:  make([]string, 0),

		ExecutionInfo:  nil,
		ExecutionState: nil,
		NextEventId:    0,
		BufferedEvents: make([]*historypb.HistoryEvent, 0),
		Checksum:       nil,
	}
	if err := proto.Unmarshal(token, mutableState); err != nil {
		return nil, err
	}
	return mutableState, nil
}
