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
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
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
		shard.Context
		historyReplicator HistoryReplicator
		logger            log.Logger

		sync.Mutex
		workflowCache map[definition.WorkflowKey]workflow.MutableState
	}
)

func NewHistoryBackfiller(
	shard shard.Context,
	eventsReapplier EventsReapplier,
	eventSerializer serialization.Serializer,
	logger log.Logger,
) *HistoryBackfillerImpl {
	backfiller := &HistoryBackfillerImpl{
		Context:           shard,
		historyReplicator: nil,
		logger:            logger,
	}
	historyReplicator := NewHistoryReplicator(
		backfiller,
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
	mutableState, err := r.deserializeBackfillToken(token)
	if err != nil {
		return nil, err
	}
	if err := r.addToCache(workflowKey, mutableState); err != nil {
		return nil, err
	}
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

func (r *HistoryBackfillerImpl) addToCache(
	workflowKey definition.WorkflowKey,
	mutableState workflow.MutableState,
) error {
	r.Lock()
	defer r.Unlock()
	if _, ok := r.workflowCache[workflowKey]; ok {
		return serviceerror.NewUnimplemented(
			"HistoryBackfiller does not support concurrent backfill of workflow executions",
		)
	}
	r.workflowCache[workflowKey] = mutableState
	return nil
}

func (r *HistoryBackfillerImpl) removeFromCache(
	workflowKey definition.WorkflowKey,
) {
	r.Lock()
	defer r.Unlock()
	delete(r.workflowCache, workflowKey)
}

// GetOrCreateCurrentWorkflowExecution override corresponding function defined cache.Cache
// so half backfilled workflow will not pollute workflow cache
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

// GetOrCreateWorkflowExecution override corresponding function defined cache.Cache
// so half backfilled workflow will not pollute workflow cache
func (r *HistoryBackfillerImpl) GetOrCreateWorkflowExecution(
	ctx context.Context,
	namespaceID namespace.ID,
	execution commonpb.WorkflowExecution,
	lockPriority workflow.LockPriority,
) (workflow.Context, cache.ReleaseCacheFunc, error) {
	wfContext := workflow.NewContext(
		r, // use self to override get workflow & creation workflow
		definition.NewWorkflowKey(namespaceID.String(), execution.WorkflowId, execution.RunId),
		r.logger,
	)
	mutableState, ok := r.workflowCache[definition.NewWorkflowKey(
		namespaceID.String(),
		execution.GetWorkflowId(),
		execution.GetRunId(),
	)]
	if !ok {
		return nil, nil, serviceerror.NewInternal(
			"HistoryBackfiller does not have target mutable state in cache",
		)
	}
	wfContext.MutableState = mutableState
	return wfContext, func(err error) {}, nil
}

// GetCurrentExecution override corresponding function defined shard.Context
// so half backfilled workflow will not be pollute database
func (r *HistoryBackfillerImpl) GetCurrentExecution(
	ctx context.Context,
	request *persistence.GetCurrentExecutionRequest,
) (*persistence.GetCurrentExecutionResponse, error) {
	return nil, serviceerror.NewInternal(
		"HistoryBackfiller GetCurrentExecution should not be invoked",
	)
}

// GetWorkflowExecution override corresponding function defined shard.Context
// so half backfilled workflow will not be pollute database
func (r *HistoryBackfillerImpl) GetWorkflowExecution(
	ctx context.Context,
	request *persistence.GetWorkflowExecutionRequest,
) (*persistence.GetWorkflowExecutionResponse, error) {
	workflowKey := definition.NewWorkflowKey(
		request.NamespaceID,
		request.WorkflowID,
		request.RunID,
	)
	r.Lock()
	defer r.Unlock()
	if _, ok := r.workflowCache[workflowKey]; !ok {
		return nil, serviceerror.NewInternal(
			"HistoryBackfiller GetWorkflowExecution encountered workflow key mismatch",
		)
	}
	return nil, serviceerror.NewNotFound(fmt.Sprintf("workflow %v not found", workflowKey))
}

// CreateWorkflowExecution override corresponding function defined shard.Context
// so half backfilled workflow will not be pollute database
func (r *HistoryBackfillerImpl) CreateWorkflowExecution(
	ctx context.Context,
	request *persistence.CreateWorkflowExecutionRequest,
) (*persistence.CreateWorkflowExecutionResponse, error) {
	branchToken := request.NewWorkflowEvents[0].BranchToken
	for i := 1; i < len(request.NewWorkflowEvents); i++ {
		if !bytes.Equal(branchToken, request.NewWorkflowEvents[i].BranchToken) {
			return nil, serviceerror.NewInternal(
				"HistoryBackfiller CreateWorkflowExecution encountered branch token mismatch",
			)
		}
	}
	for i, events := range request.NewWorkflowEvents {
		req := &persistence.AppendHistoryNodesRequest{
			ShardID:           r.GetShardID(),
			IsNewBranch:       i == 0,
			Info:              persistence.BuildHistoryGarbageCleanupInfo(events.NamespaceID, events.WorkflowID, events.RunID),
			BranchToken:       branchToken,
			Events:            events.Events,
			PrevTransactionID: events.PrevTxnID,
			TransactionID:     events.TxnID,
		}
		if _, err := r.AppendHistoryEvents(
			ctx,
			req,
			namespace.ID(events.NamespaceID),
			commonpb.WorkflowExecution{WorkflowId: events.WorkflowID, RunId: events.RunID},
		); err != nil {
			return nil, err
		}
	}
	return &persistence.CreateWorkflowExecutionResponse{}, nil
}

// UpdateWorkflowExecution override corresponding function defined shard.Context
// so half backfilled workflow will not be pollute database
func (r *HistoryBackfillerImpl) UpdateWorkflowExecution(
	ctx context.Context,
	request *persistence.UpdateWorkflowExecutionRequest,
) (*persistence.UpdateWorkflowExecutionResponse, error) {
	branchToken := request.NewWorkflowEvents[0].BranchToken
	for i := 1; i < len(request.NewWorkflowEvents); i++ {
		if !bytes.Equal(branchToken, request.NewWorkflowEvents[i].BranchToken) {
			return nil, serviceerror.NewInternal(
				"HistoryBackfiller UpdateWorkflowExecution encountered branch token mismatch",
			)
		}
	}
	for _, events := range request.NewWorkflowEvents {
		req := &persistence.AppendHistoryNodesRequest{
			ShardID:           r.GetShardID(),
			IsNewBranch:       false,
			Info:              "",
			BranchToken:       branchToken,
			Events:            events.Events,
			PrevTransactionID: events.PrevTxnID,
			TransactionID:     events.TxnID,
		}
		if _, err := r.AppendHistoryEvents(
			ctx,
			req,
			namespace.ID(events.NamespaceID),
			commonpb.WorkflowExecution{WorkflowId: events.WorkflowID, RunId: events.RunID},
		); err != nil {
			return nil, err
		}
	}
	return &persistence.UpdateWorkflowExecutionResponse{}, nil
}

// ConflictResolveWorkflowExecution override corresponding function defined shard.Context
// so half backfilled workflow will not be pollute database
func (r *HistoryBackfillerImpl) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *persistence.ConflictResolveWorkflowExecutionRequest,
) (*persistence.ConflictResolveWorkflowExecutionResponse, error) {
	return nil, serviceerror.NewInternal(
		"HistoryBackfiller ConflictResolveWorkflowExecution should not be invoked",
	)
}

// SetWorkflowExecution override corresponding function defined shard.Context
// so half backfilled workflow will not be pollute database
func (r *HistoryBackfillerImpl) SetWorkflowExecution(
	ctx context.Context,
	request *persistence.SetWorkflowExecutionRequest,
) (*persistence.SetWorkflowExecutionResponse, error) {
	return nil, serviceerror.NewInternal(
		"HistoryBackfiller SetWorkflowExecution should not be invoked",
	)
}

// DeleteWorkflowExecution override corresponding function defined shard.Context
// so half backfilled workflow will not be pollute database
func (r *HistoryBackfillerImpl) DeleteWorkflowExecution(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	branchToken []byte,
	startTime *time.Time,
	closeTime *time.Time,
	closeExecutionVisibilityTaskID int64,
	stage *tasks.DeleteWorkflowExecutionStage,
) error {
	return serviceerror.NewInternal(
		"HistoryBackfiller DeleteWorkflowExecution should not be invoked",
	)
}
