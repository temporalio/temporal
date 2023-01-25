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

package startworkflow

import (
	"context"

	"github.com/google/uuid"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"

	"go.temporal.io/server/api/historyservice/v1"
	serverCommon "go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type Starter struct {
	shardCtx                   shard.Context
	workflowConsistencyChecker api.WorkflowConsistencyChecker
	tokenSerializer            serverCommon.TaskTokenSerializer
	request                    *historyservice.StartWorkflowExecutionRequest
	namespace                  *namespace.Namespace
}

func NewStarter(
	shardCtx shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	tokenSerializer serverCommon.TaskTokenSerializer,
	request *historyservice.StartWorkflowExecutionRequest,
) (*Starter, error) {
	namespaceEntry, err := api.GetActiveNamespace(shardCtx, namespace.ID(request.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	return &Starter{
		shardCtx:                   shardCtx,
		workflowConsistencyChecker: workflowConsistencyChecker,
		tokenSerializer:            tokenSerializer,
		request:                    request,
		namespace:                  namespaceEntry,
	}, nil
}

func (s *Starter) Invoke(
	ctx context.Context,
) (resp *historyservice.StartWorkflowExecutionResponse, retError error) {
	shardCtx := s.shardCtx
	workflowConsistencyChecker := s.workflowConsistencyChecker
	startRequest := s.request

	request := startRequest.StartRequest
	metricsHandler := shardCtx.GetMetricsHandler()

	api.OverrideStartWorkflowExecutionRequest(request, metrics.HistoryStartWorkflowExecutionScope, shardCtx, metricsHandler)
	err := api.ValidateStartWorkflowExecutionRequest(ctx, request, shardCtx, s.namespace, "StartWorkflowExecution")
	if err != nil {
		return nil, err
	}

	if request.GetRequestEagerExecution() {
		metricsHandler.Counter(metrics.WorkflowEagerExecutionCounter.GetMetricName()).Record(
			1,
			metrics.NamespaceTag(s.namespace.Name().String()),
			metrics.TaskQueueTag(request.TaskQueue.Name),
		)
	}

	workflowID := request.GetWorkflowId()
	runID := uuid.NewString()
	workflowContext, err := api.NewWorkflowWithSignal(
		ctx,
		shardCtx,
		s.namespace,
		workflowID,
		runID,
		startRequest,
		nil,
	)
	if err != nil {
		return nil, err
	}

	now := shardCtx.GetTimeSource().Now()
	mutableState := workflowContext.GetMutableState()
	workflowTaskInfo, hasInflight := mutableState.GetInFlightWorkflowTask()
	if !hasInflight {
		return nil, serviceerror.NewInternal("unexpected error: mutable state did not have an inflight workflow task")
	}
	newWorkflow, newWorkflowEventsSeq, err := workflowContext.GetMutableState().CloseTransactionAsSnapshot(
		now,
		workflow.TransactionPolicyActive,
	)
	if err != nil {
		return nil, err
	}
	if len(newWorkflowEventsSeq) != 1 {
		return nil, serviceerror.NewInternal("unable to create 1st event batch")
	}

	// create as brand new
	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	err = workflowContext.GetContext().CreateWorkflowExecution(
		ctx,
		now,
		createMode,
		prevRunID,
		prevLastWriteVersion,
		workflowContext.GetMutableState(),
		newWorkflow,
		newWorkflowEventsSeq,
	)
	if err == nil {
		return s.generateResponse(runID, workflowTaskInfo, extractHistoryEvents(newWorkflowEventsSeq))
	}

	t, ok := err.(*persistence.CurrentWorkflowConditionFailedError)
	if !ok {
		return nil, err
	}

	// Handle CurrentWorkflowConditionFailedError where a previous request with the same request ID already created a
	// workflow execution with a different run ID.
	// The history we generated above should be deleted by a background process.
	if t.RequestID == request.GetRequestId() {
		return s.respondToRetriedRequest(ctx, t.RunID)
	}

	// create as ID reuse
	prevRunID = t.RunID
	prevLastWriteVersion = t.LastWriteVersion
	if workflowContext.GetMutableState().GetCurrentVersion() < prevLastWriteVersion {
		clusterMetadata := shardCtx.GetClusterMetadata()
		clusterName := clusterMetadata.ClusterNameForFailoverVersion(s.namespace.IsGlobalNamespace(), prevLastWriteVersion)
		return nil, serviceerror.NewNamespaceNotActive(
			request.GetNamespace(),
			clusterMetadata.GetCurrentClusterName(),
			clusterName,
		)
	}

	prevExecutionUpdateAction, err := api.ApplyWorkflowIDReusePolicy(
		t.RequestID,
		prevRunID,
		t.State,
		t.Status,
		workflowID,
		runID,
		startRequest.StartRequest.GetWorkflowIdReusePolicy(),
	)
	if err != nil {
		return nil, err
	}

	if prevExecutionUpdateAction != nil {
		var mutableState workflow.MutableState
		// update prev execution and create new execution in one transaction
		err := api.GetAndUpdateWorkflowWithNew(
			ctx,
			nil,
			api.BypassMutableStateConsistencyPredicate,
			definition.NewWorkflowKey(
				s.namespace.ID().String(),
				workflowID,
				prevRunID,
			),
			prevExecutionUpdateAction,
			func() (workflow.Context, workflow.MutableState, error) {
				workflowContext, err := api.NewWorkflowWithSignal(
					ctx,
					shardCtx,
					s.namespace,
					workflowID,
					runID,
					startRequest,
					nil)
				if err != nil {
					return nil, nil, err
				}
				mutableState = workflowContext.GetMutableState()
				return workflowContext.GetContext(), mutableState, nil
			},
			shardCtx,
			workflowConsistencyChecker,
		)
		switch err {
		case nil:
			if !request.GetRequestEagerExecution() {
				return &historyservice.StartWorkflowExecutionResponse{
					RunId: runID,
				}, nil
			}
			workflowTaskInfo, _ := mutableState.GetInFlightWorkflowTask()
			events, err := s.getWorkflowHistory(ctx, mutableState)
			if err != nil {
				return nil, err
			}
			return s.generateResponse(runID, workflowTaskInfo, events)
		case consts.ErrWorkflowCompleted:
			// previous workflow already closed
			// fallthough to the logic for only creating the new workflow below
		default:
			return nil, err
		}
	}

	if err = workflowContext.GetContext().CreateWorkflowExecution(
		ctx,
		now,
		persistence.CreateWorkflowModeUpdateCurrent,
		prevRunID,
		prevLastWriteVersion,
		workflowContext.GetMutableState(),
		newWorkflow,
		newWorkflowEventsSeq,
	); err != nil {
		return nil, err
	}
	return s.generateResponse(runID, workflowTaskInfo, extractHistoryEvents(newWorkflowEventsSeq))
}

// respondToRetriedRequest provides a response in case a start request is retried.
func (s *Starter) respondToRetriedRequest(
	ctx context.Context,
	runID string,
) (*historyservice.StartWorkflowExecutionResponse, error) {
	request := s.request.StartRequest

	if !request.GetRequestEagerExecution() {
		return &historyservice.StartWorkflowExecutionResponse{
			RunId: runID,
		}, nil
	}

	// For eager workflow execution, we need to get the task info and history events in order to construct a poll response.
	// We techincally never want to create a new execution but in practice this should not happen.
	workflowContext, releaseFn, err := s.workflowConsistencyChecker.GetWorkflowCache().GetOrCreateWorkflowExecution(
		ctx,
		s.namespace.ID(),
		common.WorkflowExecution{WorkflowId: request.WorkflowId, RunId: runID},
		workflow.CallerTypeAPI,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		// TODO: figure out which err to pass here
		releaseFn(err)
	}()
	mutableState, err := workflowContext.LoadMutableState(ctx)
	if err != nil {
		return nil, err
	}

	// Future work extend the task timeout (by failing / timing out the current task).
	workflowTaskInfo, hasInflight := mutableState.GetInFlightWorkflowTask()

	// The current workflow task is not inflight or not the first task or we exceeded the first attempt and fell back to
	// matching based dispatch.
	if !hasInflight || workflowTaskInfo.StartedEventID != 3 || workflowTaskInfo.Attempt > 1 {
		return nil, serviceerror.NewWorkflowExecutionAlreadyStarted(
			"First workflow task is no longer inflight, retried request came in too late",
			request.RequestId,
			runID,
		)
	}

	events, err := s.getWorkflowHistory(ctx, mutableState)
	if err != nil {
		return nil, err
	}

	return s.generateResponse(runID, workflowTaskInfo, events)
}

func (s *Starter) getWorkflowHistory(ctx context.Context, mutableState workflow.MutableState) ([]*history.HistoryEvent, error) {
	branchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}

	var events []*history.HistoryEvent
	// Future optimization: generate the task from mutable state to save the extra DB read.
	// NOTE: While unlikely that there'll be more than one page, it's safer to make less assumptions
	for {
		response, err := s.shardCtx.GetExecutionManager().ReadHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
			ShardID:     s.shardCtx.GetShardID(),
			BranchToken: branchToken,
			MinEventID:  1,
			MaxEventID:  mutableState.GetNextEventID() - 1,
			PageSize:    1024,
		})
		if err != nil {
			return nil, err
		}
		events = append(events, response.HistoryEvents...)
		if response.NextPageToken == nil || len(response.NextPageToken) == 0 {
			break
		}
	}

	return events, nil
}

// extractHistoryEvents extracts all history events from a batch of events sent to persistence.
// It's unlikely that persistence events would span multiple batches but better safe than sorry.
func extractHistoryEvents(persistenceEvents []*persistence.WorkflowEvents) []*history.HistoryEvent {
	if len(persistenceEvents) == 1 {
		return persistenceEvents[0].Events
	}
	var events []*history.HistoryEvent
	for _, page := range persistenceEvents {
		events = append(events, page.Events...)
	}
	return events
}

func (s *Starter) generateResponse(
	runID string,
	workflowTaskInfo *workflow.WorkflowTaskInfo,
	historyEvents []*history.HistoryEvent,
) (*historyservice.StartWorkflowExecutionResponse, error) {
	shardCtx := s.shardCtx
	tokenSerializer := s.tokenSerializer
	request := s.request.StartRequest
	workflowID := request.WorkflowId

	if !request.GetRequestEagerExecution() {
		return &historyservice.StartWorkflowExecutionResponse{
			RunId: runID,
		}, nil
	}
	clock, err := shardCtx.NewVectorClock()
	if err != nil {
		return nil, err
	}
	taskToken := &tokenspb.Task{
		NamespaceId:      s.namespace.ID().String(),
		WorkflowId:       workflowID,
		RunId:            runID,
		ScheduledEventId: workflowTaskInfo.ScheduledEventID,
		Attempt:          1,
		Clock:            clock,
	}
	serializedToken, err := tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, err
	}
	return &historyservice.StartWorkflowExecutionResponse{
		RunId: runID,
		Clock: clock,
		EagerWorkflowTask: &workflowservice.PollWorkflowTaskQueueResponse{
			TaskToken:                  serializedToken,
			WorkflowExecution:          &common.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			WorkflowType:               request.GetWorkflowType(),
			PreviousStartedEventId:     0,
			StartedEventId:             workflowTaskInfo.StartedEventID,
			Attempt:                    1,
			History:                    &history.History{Events: historyEvents},
			NextPageToken:              nil,
			WorkflowExecutionTaskQueue: workflowTaskInfo.TaskQueue,
			ScheduledTime:              workflowTaskInfo.ScheduledTime,
			StartedTime:                workflowTaskInfo.StartedTime,
		},
	}, nil
}
