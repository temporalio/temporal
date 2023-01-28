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
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

// Starter starts a new workflow execution.
type Starter struct {
	shardCtx                   shard.Context
	workflowConsistencyChecker api.WorkflowConsistencyChecker
	tokenSerializer            common.TaskTokenSerializer
	request                    *historyservice.StartWorkflowExecutionRequest
	namespace                  *namespace.Namespace
}

// creationContext is a container for all information obtained from creating the uncommitted execution.
// The information is later used to create a new execution and handle conflicts.
type creationContext struct {
	workflowID       string
	runID            string
	workflowContext  api.WorkflowContext
	workflowTaskInfo *workflow.WorkflowTaskInfo
	workflowSnapshot *persistence.WorkflowSnapshot
	workflowEvents   []*persistence.WorkflowEvents
}

// mutableStateInfo is a container for the relevant mutable state information to generate a start response with an eager
// workflow task.
type mutableStateInfo struct {
	branchToken      []byte
	lastEventID      int64
	workflowTaskInfo *workflow.WorkflowTaskInfo
	hasInflight      bool
}

// NewStarter creates a new starter, fails if getting the active namespace fails.
func NewStarter(
	shardCtx shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	tokenSerializer common.TaskTokenSerializer,
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

// prepare applies request overrides, validates the request, and records eager execution metrics.
func (s *Starter) prepare(ctx context.Context) error {
	request := s.request.StartRequest
	metricsHandler := s.shardCtx.GetMetricsHandler()

	api.OverrideStartWorkflowExecutionRequest(request, metrics.HistoryStartWorkflowExecutionScope, s.shardCtx, metricsHandler)
	err := api.ValidateStartWorkflowExecutionRequest(ctx, request, s.shardCtx, s.namespace, "StartWorkflowExecution")
	if err != nil {
		return err
	}

	// Override to false to avoid having to look up the dynamic config throughout the diffrent code paths.
	if !s.shardCtx.GetConfig().EnableEagerWorkflowStart(s.namespace.Name().String()) {
		request.RequestEagerExecution = false
	}
	if s.requestEagerStart() {
		metricsHandler.Counter(metrics.WorkflowEagerExecutionCounter.GetMetricName()).Record(
			1,
			metrics.NamespaceTag(s.namespace.Name().String()),
			metrics.TaskQueueTag(request.TaskQueue.Name),
		)
	}
	return nil
}

func (s *Starter) requestEagerStart() bool {
	return s.request.StartRequest.GetRequestEagerExecution() && *s.request.FirstWorkflowTaskBackoff == 0
}

// Invoke starts a new workflow execution
func (s *Starter) Invoke(
	ctx context.Context,
) (resp *historyservice.StartWorkflowExecutionResponse, retError error) {
	request := s.request.StartRequest
	if err := s.prepare(ctx); err != nil {
		return nil, err
	}

	runID := uuid.NewString()

	creationCtx, err := s.createNewMutableState(ctx, request.GetWorkflowId(), runID)
	if err != nil {
		return nil, err
	}
	err = s.createBrandNew(ctx, creationCtx)
	if err == nil {
		return s.generateResponse(creationCtx.runID, creationCtx.workflowTaskInfo, extractHistoryEvents(creationCtx.workflowEvents))
	}
	t, ok := err.(*persistence.CurrentWorkflowConditionFailedError)
	if !ok {
		return nil, err
	}
	return s.handleConflict(ctx, creationCtx, t)
}

// createNewMutableState creates a new workflow context, and closes its mutable state transaction as snapshot.
// It returns the creationContext which can later be used to insert into the executions table.
func (s *Starter) createNewMutableState(ctx context.Context, workflowID string, runID string) (*creationContext, error) {
	workflowContext, err := api.NewWorkflowWithSignal(
		ctx,
		s.shardCtx,
		s.namespace,
		workflowID,
		runID,
		s.request,
		nil,
	)
	if err != nil {
		return nil, err
	}

	now := s.shardCtx.GetTimeSource().Now()
	mutableState := workflowContext.GetMutableState()
	workflowTaskInfo, hasInflight := mutableState.GetInFlightWorkflowTask()
	if s.requestEagerStart() && !hasInflight {
		return nil, serviceerror.NewInternal("unexpected error: mutable state did not have an inflight workflow task")
	}
	workflowSnapshot, workflowEvents, err := mutableState.CloseTransactionAsSnapshot(
		now,
		workflow.TransactionPolicyActive,
	)
	if err != nil {
		return nil, err
	}
	if len(workflowEvents) != 1 {
		return nil, serviceerror.NewInternal("unable to create 1st event batch")
	}

	return &creationContext{
		workflowID:       workflowID,
		runID:            runID,
		workflowContext:  workflowContext,
		workflowTaskInfo: workflowTaskInfo,
		workflowSnapshot: workflowSnapshot,
		workflowEvents:   workflowEvents,
	}, nil
}

// createBrandNew creates a new "brand new" execution in the executions table.
func (s *Starter) createBrandNew(ctx context.Context, creationCtx *creationContext) error {
	now := s.shardCtx.GetTimeSource().Now()
	return creationCtx.workflowContext.GetContext().CreateWorkflowExecution(
		ctx,
		now,
		persistence.CreateWorkflowModeBrandNew,
		"", // prevRunID
		0,  // prevLastWriteVersion
		creationCtx.workflowContext.GetMutableState(),
		creationCtx.workflowSnapshot,
		creationCtx.workflowEvents,
	)
}

// handleConflict handles CurrentWorkflowConditionFailedError where a previous request with the same request ID already
// created a workflow execution with a different run ID.
// The history we generated above should be deleted by a background process.
func (s *Starter) handleConflict(
	ctx context.Context,
	creationCtx *creationContext,
	currentWorkflowConditionFailed *persistence.CurrentWorkflowConditionFailedError,
) (*historyservice.StartWorkflowExecutionResponse, error) {
	request := s.request.StartRequest
	if currentWorkflowConditionFailed.RequestID == request.GetRequestId() {
		return s.respondToRetriedRequest(ctx, currentWorkflowConditionFailed.RunID)
	}
	if err := s.verifyNamespaceActive(creationCtx, currentWorkflowConditionFailed); err != nil {
		return nil, err
	}
	response, err := s.applyWorkflowIDReusePolicy(ctx, currentWorkflowConditionFailed, creationCtx)
	if err != nil {
		return nil, err
	} else if response != nil {
		return response, nil
	}
	if err := s.createAsCurrent(ctx, creationCtx, currentWorkflowConditionFailed); err != nil {
		return nil, err
	}
	return s.generateResponse(creationCtx.runID, creationCtx.workflowTaskInfo, extractHistoryEvents(creationCtx.workflowEvents))
}

// createAsCurrent creates a new workflow execution and sets it to "current".
func (s *Starter) createAsCurrent(
	ctx context.Context,
	creationCtx *creationContext,
	currentWorkflowConditionFailed *persistence.CurrentWorkflowConditionFailedError,
) error {
	now := s.shardCtx.GetTimeSource().Now()
	return creationCtx.workflowContext.GetContext().CreateWorkflowExecution(
		ctx,
		now,
		persistence.CreateWorkflowModeUpdateCurrent,
		currentWorkflowConditionFailed.RunID,
		currentWorkflowConditionFailed.LastWriteVersion,
		creationCtx.workflowContext.GetMutableState(),
		creationCtx.workflowSnapshot,
		creationCtx.workflowEvents,
	)
}

func (s *Starter) verifyNamespaceActive(creationCtx *creationContext, currentWorkflowConditionFailed *persistence.CurrentWorkflowConditionFailedError) error {
	if creationCtx.workflowContext.GetMutableState().GetCurrentVersion() < currentWorkflowConditionFailed.LastWriteVersion {
		clusterMetadata := s.shardCtx.GetClusterMetadata()
		clusterName := clusterMetadata.ClusterNameForFailoverVersion(s.namespace.IsGlobalNamespace(), currentWorkflowConditionFailed.LastWriteVersion)
		return serviceerror.NewNamespaceNotActive(
			s.namespace.Name().String(),
			clusterMetadata.GetCurrentClusterName(),
			clusterName,
		)
	}
	return nil
}

// applyWorkflowIDReusePolicy applies the workflow ID reuse policy in case a workflow start requests fails with a
// duplicate execution.
// At the time of this writing, the only possible action here is to terminate the current execution in case the start
// request's ID reuse policy is TERMINATE_IF_RUNNING.
// Returns non-nil response if an action was required and completed successfully resulting in a newly created execution.
func (s *Starter) applyWorkflowIDReusePolicy(
	ctx context.Context,
	currentWorkflowConditionFailed *persistence.CurrentWorkflowConditionFailedError,
	creationCtx *creationContext,
) (*historyservice.StartWorkflowExecutionResponse, error) {
	workflowID := s.request.StartRequest.WorkflowId
	prevExecutionUpdateAction, err := api.ApplyWorkflowIDReusePolicy(
		currentWorkflowConditionFailed.RequestID,
		currentWorkflowConditionFailed.RunID,
		currentWorkflowConditionFailed.State,
		currentWorkflowConditionFailed.Status,
		workflowID,
		creationCtx.runID,
		s.request.StartRequest.GetWorkflowIdReusePolicy(),
	)
	if err != nil {
		return nil, err
	}

	if prevExecutionUpdateAction == nil {
		return nil, nil
	}
	var mutableStateInfo *mutableStateInfo
	// update prev execution and create new execution in one transaction
	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			s.namespace.ID().String(),
			workflowID,
			currentWorkflowConditionFailed.RunID,
		),
		prevExecutionUpdateAction,
		func() (workflow.Context, workflow.MutableState, error) {
			workflowContext, err := api.NewWorkflowWithSignal(
				ctx,
				s.shardCtx,
				s.namespace,
				workflowID,
				creationCtx.runID,
				s.request,
				nil)
			if err != nil {
				return nil, nil, err
			}
			mutableState := workflowContext.GetMutableState()
			mutableStateInfo, err = extractMutableStateInfo(mutableState)
			if err != nil {
				return nil, nil, err
			}
			return workflowContext.GetContext(), mutableState, nil
		},
		s.shardCtx,
		s.workflowConsistencyChecker,
	)
	switch err {
	case nil:
		if !s.requestEagerStart() {
			return &historyservice.StartWorkflowExecutionResponse{
				RunId: creationCtx.runID,
			}, nil
		}
		events, err := s.getWorkflowHistory(ctx, mutableStateInfo)
		if err != nil {
			return nil, err
		}
		return s.generateResponse(creationCtx.runID, mutableStateInfo.workflowTaskInfo, events)
	case consts.ErrWorkflowCompleted:
		// previous workflow already closed
		// fallthough to the logic for only creating the new workflow below
		return nil, nil
	default:
		return nil, err
	}
}

// respondToRetriedRequest provides a response in case a start request is retried.
func (s *Starter) respondToRetriedRequest(
	ctx context.Context,
	runID string,
) (*historyservice.StartWorkflowExecutionResponse, error) {
	request := s.request.StartRequest

	if !s.requestEagerStart() {
		return &historyservice.StartWorkflowExecutionResponse{
			RunId: runID,
		}, nil
	}

	// For eager workflow execution, we need to get the task info and history events in order to construct a poll response.
	mutableStateInfo, err := s.getMutableStateInfo(ctx, runID)
	if err != nil {
		return nil, err
	}

	// The current workflow task is not inflight or not the first task or we exceeded the first attempt and fell back to
	// matching based dispatch.
	if !mutableStateInfo.hasInflight || mutableStateInfo.workflowTaskInfo.StartedEventID != 3 || mutableStateInfo.workflowTaskInfo.Attempt > 1 {
		return nil, serviceerror.NewWorkflowExecutionAlreadyStarted(
			"First workflow task is no longer inflight, retried request came in too late",
			request.RequestId,
			runID,
		)
	}

	events, err := s.getWorkflowHistory(ctx, mutableStateInfo)
	if err != nil {
		return nil, err
	}

	return s.generateResponse(runID, mutableStateInfo.workflowTaskInfo, events)
}

// getMutableStateInfo gets the relevant mutable state information while getting the state for the given run from the
// workflow cache and managing the cache lease.
func (s *Starter) getMutableStateInfo(ctx context.Context, runID string) (*mutableStateInfo, error) {
	// We techincally never want to create a new execution but in practice this should not happen.
	workflowContext, releaseFn, err := s.workflowConsistencyChecker.GetWorkflowCache().GetOrCreateWorkflowExecution(
		ctx,
		s.namespace.ID(),
		commonpb.WorkflowExecution{WorkflowId: s.request.StartRequest.WorkflowId, RunId: runID},
		workflow.CallerTypeAPI,
	)
	if err != nil {
		return nil, err
	}

	var releaseErr error
	defer func() {
		releaseFn(releaseErr)
	}()

	var mutableState workflow.MutableState
	mutableState, releaseErr = workflowContext.LoadMutableState(ctx)
	return extractMutableStateInfo(mutableState)
}

// extractMutableStateInfo extracts the relevant information to generate a start response with an eager workflow task.
func extractMutableStateInfo(mutableState workflow.MutableState) (*mutableStateInfo, error) {
	branchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}

	// Future work for the request retry path: extend the task timeout (by failing / timing out the current task).
	workflowTaskInfoSource, hasInflight := mutableState.GetInFlightWorkflowTask()
	// The workflowTaskInfo returned from the mutable state call is generated on the fly and technically doesn't require
	// cloning. We clone here just in case that changes.
	var workflowTaskInfo workflow.WorkflowTaskInfo
	if hasInflight {
		workflowTaskInfo = *workflowTaskInfoSource
	}

	return &mutableStateInfo{
		branchToken:      branchToken,
		lastEventID:      mutableState.GetNextEventID() - 1,
		workflowTaskInfo: &workflowTaskInfo,
		hasInflight:      hasInflight,
	}, nil
}

// getWorkflowHistory loads the workflow history based on given mutable state information from the DB.
func (s *Starter) getWorkflowHistory(ctx context.Context, mutableState *mutableStateInfo) ([]*historypb.HistoryEvent, error) {
	var events []*historypb.HistoryEvent
	// Future optimization: generate the task from mutable state to save the extra DB read.
	// NOTE: While unlikely that there'll be more than one page, it's safer to make less assumptions.
	// TODO: Frontend also supports returning raw history and it's controlled by a feature flag (yycptt thinks).
	for {
		response, err := s.shardCtx.GetExecutionManager().ReadHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
			ShardID:     s.shardCtx.GetShardID(),
			BranchToken: mutableState.branchToken,
			MinEventID:  1,
			MaxEventID:  mutableState.lastEventID,
			PageSize:    1024,
		})
		if err != nil {
			return nil, err
		}
		events = append(events, response.HistoryEvents...)
		if len(response.NextPageToken) == 0 {
			break
		}
	}

	return events, nil
}

// extractHistoryEvents extracts all history events from a batch of events sent to persistence.
// It's unlikely that persistence events would span multiple batches but better safe than sorry.
func extractHistoryEvents(persistenceEvents []*persistence.WorkflowEvents) []*historypb.HistoryEvent {
	if len(persistenceEvents) == 1 {
		return persistenceEvents[0].Events
	}
	var events []*historypb.HistoryEvent
	for _, page := range persistenceEvents {
		events = append(events, page.Events...)
	}
	return events
}

// generateResponse is a helper for generating StartWorkflowExecutionResponse for eager and non eager workflow start
// requests.
func (s *Starter) generateResponse(
	runID string,
	workflowTaskInfo *workflow.WorkflowTaskInfo,
	historyEvents []*historypb.HistoryEvent,
) (*historyservice.StartWorkflowExecutionResponse, error) {
	shardCtx := s.shardCtx
	tokenSerializer := s.tokenSerializer
	request := s.request.StartRequest
	workflowID := request.WorkflowId

	if !s.requestEagerStart() {
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
			WorkflowExecution:          &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			WorkflowType:               request.GetWorkflowType(),
			PreviousStartedEventId:     0,
			StartedEventId:             workflowTaskInfo.StartedEventID,
			Attempt:                    1,
			History:                    &historypb.History{Events: historyEvents},
			NextPageToken:              nil,
			WorkflowExecutionTaskQueue: workflowTaskInfo.TaskQueue,
			ScheduledTime:              workflowTaskInfo.ScheduledTime,
			StartedTime:                workflowTaskInfo.StartedTime,
		},
	}, nil
}
