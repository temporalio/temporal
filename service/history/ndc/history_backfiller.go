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

	"github.com/gogo/protobuf/proto"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/cache"
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
		shard             shard.Context
		historyReplicator HistoryReplicator
		logger            log.Logger
	}
)

func NewHistoryBackfiller(
	shard shard.Context,
	eventsReapplier EventsReapplier,
	eventSerializer serialization.Serializer,
	logger log.Logger,
) *HistoryBackfillerImpl {
	backfiller := &HistoryBackfillerImpl{
		shard:             shard,
		historyReplicator: nil,
		logger:            logger,
	}
	historyReplicator := NewHistoryReplicator(
		shard,
		backfiller,
		eventsReapplier,
		eventSerializer,
		logger,
	)
	backfiller.historyReplicator = historyReplicator
	return backfiller
}

func (r *HistoryBackfillerImpl) BackfillWorkflow(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	events [][]*historypb.HistoryEvent,
	token []byte,
) ([]byte, error) {
	panic("i++")
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

func (r *HistoryBackfillerImpl) GetOrCreateCurrentWorkflowExecution(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	lockPriority workflow.LockPriority,
) (workflow.Context, cache.ReleaseCacheFunc, error) {
	return nil, nil, serviceerror.NewUnimplemented(
		"HistoryBackfiller does not support GetOrCreateCurrentWorkflowExecution operation",
	)
}

func (r *HistoryBackfillerImpl) GetOrCreateWorkflowExecution(
	ctx context.Context,
	namespaceID namespace.ID,
	execution commonpb.WorkflowExecution,
	lockPriority workflow.LockPriority,
) (workflow.Context, cache.ReleaseCacheFunc, error) {
	return workflow.NewContext(
		r.shard,
		definition.NewWorkflowKey(namespaceID.String(), execution.WorkflowId, execution.RunId),
		r.logger,
	), func(err error) {}, nil
}
