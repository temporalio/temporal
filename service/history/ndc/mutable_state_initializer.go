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
	"encoding/json"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/proto"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/utf8validator"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	MutableStateToken struct {
		ExistsInDB      bool
		DBRecordVersion int64
		DBHistorySize   int64
		MutableStateRow []byte
	}

	MutableStateInitializationSpec struct {
		ExistsInDB      bool
		IsBrandNew      bool
		DBRecordVersion int64
		DBHistorySize   int64
	}

	MutableStateInitializer interface {
		Initialize(
			ctx context.Context,
			workflowKey definition.WorkflowKey,
			token []byte,
		) (Workflow, bool, error)
	}

	MutableStateInitializerImpl struct {
		shardContext   shard.Context
		namespaceCache namespace.Registry
		workflowCache  wcache.Cache
		logger         log.Logger
	}
)

func NewMutableStateInitializer(
	shardContext shard.Context,
	workflowCache wcache.Cache,
	logger log.Logger,
) *MutableStateInitializerImpl {
	return &MutableStateInitializerImpl{
		shardContext:   shardContext,
		namespaceCache: shardContext.GetNamespaceRegistry(),
		workflowCache:  workflowCache,
		logger:         logger,
	}
}

func (r *MutableStateInitializerImpl) Initialize(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	token []byte,
) (Workflow, MutableStateInitializationSpec, error) {
	namespaceEntry, err := r.namespaceCache.GetNamespaceByID(namespace.ID(workflowKey.NamespaceID))
	if err != nil {
		return nil, MutableStateInitializationSpec{}, err
	}
	if len(token) == 0 {
		return r.InitializeFromDB(ctx, namespaceEntry, workflowKey)
	}
	return r.InitializeFromToken(ctx, namespaceEntry, workflowKey, token)
}

func (r *MutableStateInitializerImpl) InitializeFromDB(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	workflowKey definition.WorkflowKey,
) (Workflow, MutableStateInitializationSpec, error) {
	wfContext, releaseFn, err := r.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		r.shardContext,
		namespace.ID(workflowKey.NamespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
		workflow.LockPriorityHigh,
	)
	if err != nil {
		return nil, MutableStateInitializationSpec{}, err
	}

	mutableState, err := wfContext.LoadMutableState(ctx, r.shardContext)
	switch err.(type) {
	case nil:
		mutableState, err = r.flushBufferEvents(ctx, wfContext, mutableState)
		if err != nil {
			releaseFn(err)
			return nil, MutableStateInitializationSpec{}, err
		}
		_, dbRecordVersion := mutableState.GetUpdateCondition()
		dbHistorySize := mutableState.GetHistorySize()
		return NewWorkflow(
				r.shardContext.GetClusterMetadata(),
				wfContext,
				mutableState,
				releaseFn,
			), MutableStateInitializationSpec{
				ExistsInDB:      true,
				IsBrandNew:      false,
				DBRecordVersion: dbRecordVersion,
				DBHistorySize:   dbHistorySize,
			}, nil
	case *serviceerror.NotFound:
		return NewWorkflow(
				r.shardContext.GetClusterMetadata(),
				wfContext,
				workflow.NewMutableState(
					r.shardContext,
					r.shardContext.GetEventsCache(),
					r.logger,
					namespaceEntry,
					workflowKey.WorkflowID,
					workflowKey.RunID,
					time.Now().UTC(),
				),
				releaseFn,
			), MutableStateInitializationSpec{
				ExistsInDB:      false,
				IsBrandNew:      true,
				DBRecordVersion: 1,
				DBHistorySize:   0,
			}, nil
	default:
		releaseFn(err)
		return nil, MutableStateInitializationSpec{}, err
	}
}

func (r *MutableStateInitializerImpl) InitializeFromToken(
	_ context.Context,
	namespaceEntry *namespace.Namespace,
	workflowKey definition.WorkflowKey,
	token []byte,
) (Workflow, MutableStateInitializationSpec, error) {
	wfContext := workflow.NewContext(
		r.shardContext.GetConfig(),
		workflowKey,
		r.logger,
		r.shardContext.GetThrottledLogger(),
		r.shardContext.GetMetricsHandler(),
	)
	mutableStateRow, dbRecordVersion, dbHistorySize, existsInDB, err := r.deserializeBackfillToken(token)
	if err != nil {
		return nil, MutableStateInitializationSpec{}, err
	}
	mutableState, err := workflow.NewMutableStateFromDB(
		r.shardContext,
		r.shardContext.GetEventsCache(),
		r.logger,
		namespaceEntry,
		mutableStateRow,
		dbRecordVersion,
	)
	if err != nil {
		return nil, MutableStateInitializationSpec{}, err
	}
	return NewWorkflow(
			r.shardContext.GetClusterMetadata(),
			wfContext,
			mutableState,
			wcache.NoopReleaseFn,
		), MutableStateInitializationSpec{
			ExistsInDB:      existsInDB,
			IsBrandNew:      false,
			DBRecordVersion: dbRecordVersion,
			DBHistorySize:   dbHistorySize,
		}, nil
}

func (r *MutableStateInitializerImpl) flushBufferEvents(
	ctx context.Context,
	wfContext workflow.Context,
	mutableState workflow.MutableState,
) (workflow.MutableState, error) {
	flusher := NewBufferEventFlusher(r.shardContext, wfContext, mutableState, r.logger)
	_, mutableState, err := flusher.flush(ctx)
	if err != nil {
		r.logger.Error(
			"MutableStateMapping::FlushBufferEvents unable to flush buffer events",
			tag.Error(err),
		)
		return nil, err
	}
	return mutableState, err
}

func (r *MutableStateInitializerImpl) serializeBackfillToken(
	mutableState *persistencespb.WorkflowMutableState,
	dbRecordVersion int64,
	dbHistorySize int64,
	existsInDB bool,
) ([]byte, error) {
	// This is ultimately for the replication rpc stream, so it's not really a request or
	// response, but use SourceRPCResponse here since it's outgoing data.
	if err := utf8validator.Validate(mutableState, utf8validator.SourceRPCResponse); err != nil {
		return nil, err
	}
	mutableStateRow, err := mutableState.Marshal()
	if err != nil {
		return nil, err
	}
	return json.Marshal(MutableStateToken{
		MutableStateRow: mutableStateRow,
		DBRecordVersion: dbRecordVersion,
		DBHistorySize:   dbHistorySize,
		ExistsInDB:      existsInDB,
	})
}

func (r *MutableStateInitializerImpl) deserializeBackfillToken(
	token []byte,
) (_ *persistencespb.WorkflowMutableState, _ int64, _ int64, _ bool, _ error) {
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

	historyBackfillToken := &MutableStateToken{}
	if err := json.Unmarshal(token, historyBackfillToken); err != nil {
		return nil, 0, 0, false, serialization.NewDeserializationError(enums.ENCODING_TYPE_JSON, err)
	}
	err := proto.Unmarshal(historyBackfillToken.MutableStateRow, mutableState)
	if err == nil {
		// This is ultimately from the replication rpc stream, so it's not really a request or
		// response, but use SourceRPCRequest here since it's incoming data.
		err = utf8validator.Validate(mutableState, utf8validator.SourceRPCRequest)
	}
	if err != nil {
		return nil, 0, 0, false, serialization.NewDeserializationError(enums.ENCODING_TYPE_PROTO3, err)
	}
	return mutableState, historyBackfillToken.DBRecordVersion, historyBackfillToken.DBHistorySize, historyBackfillToken.ExistsInDB, nil
}
