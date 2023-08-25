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
	"errors"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"

	"go.temporal.io/server/common/tasktoken"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/cache"
)

type eagerStartDeniedReason metrics.ReasonString

const (
	eagerStartDeniedReasonDynamicConfigDisabled    eagerStartDeniedReason = "dynamic_config_disabled"
	eagerStartDeniedReasonFirstWorkflowTaskBackoff eagerStartDeniedReason = "first_workflow_task_backoff"
	eagerStartDeniedReasonTaskAlreadyDispatched    eagerStartDeniedReason = "task_already_dispatched"
)

// Starter starts a new workflow execution.
type Starter struct {
	shardCtx                   shard.Context
	workflowConsistencyChecker api.WorkflowConsistencyChecker
	tokenSerializer            common.TaskTokenSerializer
	request                    *historyservice.StartWorkflowExecutionRequest
	namespace                  *namespace.Namespace
}

// creationParams is a container for all information obtained from creating the uncommitted execution.
// The information is later used to create a new execution and handle conflicts.
type creationParams struct {
	workflowID           string
	runID                string
	workflowContext      api.WorkflowContext
	workflowTaskInfo     *workflow.WorkflowTaskInfo
	workflowSnapshot     *persistence.WorkflowSnapshot
	workflowEventBatches []*persistence.WorkflowEvents
}

// mutableStateInfo is a container for the relevant mutable state information to generate a start response with an eager
// workflow task.
type mutableStateInfo struct {
	branchToken  []byte
	lastEventID  int64
	workflowTask *workflow.WorkflowTaskInfo
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

	if request.RequestEagerExecution {
		metricsHandler.Counter(metrics.WorkflowEagerExecutionCounter.GetMetricName()).Record(
			1,
			metrics.NamespaceTag(s.namespace.Name().String()),
			metrics.TaskQueueTag(request.TaskQueue.Name),
			metrics.WorkflowTypeTag(request.WorkflowType.Name),
		)
	}

	// Override to false to avoid having to look up the dynamic config throughout the diffrent code paths.
	if !s.shardCtx.GetConfig().EnableEagerWorkflowStart(s.namespace.Name().String()) {
		s.recordEagerDenied(eagerStartDeniedReasonDynamicConfigDisabled)
		request.RequestEagerExecution = false
	}
	if s.request.FirstWorkflowTaskBackoff != nil && *s.request.FirstWorkflowTaskBackoff > 0 {
		s.recordEagerDenied(eagerStartDeniedReasonFirstWorkflowTaskBackoff)
		request.RequestEagerExecution = false
	}
	return nil
}

func (s *Starter) recordEagerDenied(reason eagerStartDeniedReason) {
	metricsHandler := s.shardCtx.GetMetricsHandler()
	metricsHandler.Counter(metrics.WorkflowEagerExecutionDeniedCounter.GetMetricName()).Record(
		1,
		metrics.NamespaceTag(s.namespace.Name().String()),
		metrics.TaskQueueTag(s.request.StartRequest.TaskQueue.Name),
		metrics.WorkflowTypeTag(s.request.StartRequest.WorkflowType.Name),
		metrics.ReasonTag(metrics.ReasonString(reason)),
	)
}

func (s *Starter) requestEagerStart() bool {
	return s.request.StartRequest.GetRequestEagerExecution()
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

	creationParams, err := s.createNewMutableState(ctx, request.GetWorkflowId(), runID)
	if err != nil {
		return nil, err
	}

	// grab current workflow context as a lock so that user latency can be computed
	currentRelease, err := s.lockCurrentWorkflowExecution(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { currentRelease(retError) }()

	err = s.createBrandNew(ctx, creationParams)
	if err == nil {
		return s.generateResponse(creationParams.runID, creationParams.workflowTaskInfo, extractHistoryEvents(creationParams.workflowEventBatches))
	}
	var currentWorkflowConditionFailedError *persistence.CurrentWorkflowConditionFailedError
	if !errors.As(err, &currentWorkflowConditionFailedError) {
		return nil, err
	}
	// The history and mutable state we generated above should be deleted by a background process.
	return s.handleConflict(ctx, creationParams, currentWorkflowConditionFailedError)
}

func (s *Starter) lockCurrentWorkflowExecution(
	ctx context.Context,
) (cache.ReleaseCacheFunc, error) {
	_, currentRelease, err := s.workflowConsistencyChecker.GetWorkflowCache().GetOrCreateCurrentWorkflowExecution(
		ctx,
		s.namespace.ID(),
		s.request.StartRequest.WorkflowId,
		workflow.LockPriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	return currentRelease, nil
}

// createNewMutableState creates a new workflow context, and closes its mutable state transaction as snapshot.
// It returns the creationContext which can later be used to insert into the executions table.
func (s *Starter) createNewMutableState(ctx context.Context, workflowID string, runID string) (*creationParams, error) {
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

	mutableState := workflowContext.GetMutableState()
	workflowTaskInfo := mutableState.GetStartedWorkflowTask()
	if s.requestEagerStart() && workflowTaskInfo == nil {
		return nil, serviceerror.NewInternal("unexpected error: mutable state did not have a started workflow task")
	}
	workflowSnapshot, eventBatches, err := mutableState.CloseTransactionAsSnapshot(
		workflow.TransactionPolicyActive,
	)
	if err != nil {
		return nil, err
	}
	if len(eventBatches) != 1 {
		return nil, serviceerror.NewInternal("unable to create 1st event batch")
	}

	return &creationParams{
		workflowID:           workflowID,
		runID:                runID,
		workflowContext:      workflowContext,
		workflowTaskInfo:     workflowTaskInfo,
		workflowSnapshot:     workflowSnapshot,
		workflowEventBatches: eventBatches,
	}, nil
}

// createBrandNew creates a new "brand new" execution in the executions table.
func (s *Starter) createBrandNew(ctx context.Context, creationParams *creationParams) error {
	return creationParams.workflowContext.GetContext().CreateWorkflowExecution(
		ctx,
		persistence.CreateWorkflowModeBrandNew,
		"", // prevRunID
		0,  // prevLastWriteVersion
		creationParams.workflowContext.GetMutableState(),
		creationParams.workflowSnapshot,
		creationParams.workflowEventBatches,
	)
}

// handleConflict handles CurrentWorkflowConditionFailedError where there's a workflow with the same workflowID.
// This may happen either when the currently handled request is a retry of a previous attempt (identified by the
// RequestID) or simply because a different run exists for the same workflow.
func (s *Starter) handleConflict(
	ctx context.Context,
	creationParams *creationParams,
	currentWorkflowConditionFailed *persistence.CurrentWorkflowConditionFailedError,
) (*historyservice.StartWorkflowExecutionResponse, error) {
	request := s.request.StartRequest
	if currentWorkflowConditionFailed.RequestID == request.GetRequestId() {
		return s.respondToRetriedRequest(ctx, currentWorkflowConditionFailed.RunID)
	}
	if err := s.verifyNamespaceActive(creationParams, currentWorkflowConditionFailed); err != nil {
		return nil, err
	}
	response, err := s.applyWorkflowIDReusePolicy(ctx, currentWorkflowConditionFailed, creationParams)
	if err != nil {
		return nil, err
	} else if response != nil {
		return response, nil
	}
	if err := s.createAsCurrent(ctx, creationParams, currentWorkflowConditionFailed); err != nil {
		return nil, err
	}
	return s.generateResponse(creationParams.runID, creationParams.workflowTaskInfo, extractHistoryEvents(creationParams.workflowEventBatches))
}

// createAsCurrent creates a new workflow execution and sets it to "current".
func (s *Starter) createAsCurrent(
	ctx context.Context,
	creationParams *creationParams,
	currentWorkflowConditionFailed *persistence.CurrentWorkflowConditionFailedError,
) error {
	return creationParams.workflowContext.GetContext().CreateWorkflowExecution(
		ctx,
		persistence.CreateWorkflowModeUpdateCurrent,
		currentWorkflowConditionFailed.RunID,
		currentWorkflowConditionFailed.LastWriteVersion,
		creationParams.workflowContext.GetMutableState(),
		creationParams.workflowSnapshot,
		creationParams.workflowEventBatches,
	)
}

func (s *Starter) verifyNamespaceActive(creationParams *creationParams, currentWorkflowConditionFailed *persistence.CurrentWorkflowConditionFailedError) error {
	if creationParams.workflowContext.GetMutableState().GetCurrentVersion() < currentWorkflowConditionFailed.LastWriteVersion {
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
	creationParams *creationParams,
) (*historyservice.StartWorkflowExecutionResponse, error) {
	workflowID := s.request.StartRequest.WorkflowId
	prevExecutionUpdateAction, err := api.ApplyWorkflowIDReusePolicy(
		currentWorkflowConditionFailed.RequestID,
		currentWorkflowConditionFailed.RunID,
		currentWorkflowConditionFailed.State,
		currentWorkflowConditionFailed.Status,
		workflowID,
		creationParams.runID,
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
				creationParams.runID,
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
				RunId: creationParams.runID,
			}, nil
		}
		events, err := s.getWorkflowHistory(ctx, mutableStateInfo)
		if err != nil {
			return nil, err
		}
		return s.generateResponse(creationParams.runID, mutableStateInfo.workflowTask, events)
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

	// The current workflow task is not started or not the first task or we exceeded the first attempt and fell back to
	// matching based dispatch.
	if mutableStateInfo.workflowTask == nil || mutableStateInfo.workflowTask.StartedEventID != 3 || mutableStateInfo.workflowTask.Attempt > 1 {
		s.recordEagerDenied(eagerStartDeniedReasonTaskAlreadyDispatched)
		return &historyservice.StartWorkflowExecutionResponse{
			RunId: runID,
		}, nil
	}

	events, err := s.getWorkflowHistory(ctx, mutableStateInfo)
	if err != nil {
		return nil, err
	}

	return s.generateResponse(runID, mutableStateInfo.workflowTask, events)
}

// getMutableStateInfo gets the relevant mutable state information while getting the state for the given run from the
// workflow cache and managing the cache lease.
func (s *Starter) getMutableStateInfo(ctx context.Context, runID string) (*mutableStateInfo, error) {
	// We techincally never want to create a new execution but in practice this should not happen.
	workflowContext, releaseFn, err := s.workflowConsistencyChecker.GetWorkflowCache().GetOrCreateWorkflowExecution(
		ctx,
		s.namespace.ID(),
		commonpb.WorkflowExecution{WorkflowId: s.request.StartRequest.WorkflowId, RunId: runID},
		workflow.LockPriorityHigh,
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
	workflowTaskSource := mutableState.GetStartedWorkflowTask()
	// The workflowTask returned from the mutable state call is generated on the fly and technically doesn't require
	// cloning. We clone here just in case that changes.
	var workflowTask workflow.WorkflowTaskInfo
	if workflowTaskSource != nil {
		workflowTask = *workflowTaskSource
	}

	return &mutableStateInfo{
		branchToken:  branchToken,
		lastEventID:  mutableState.GetNextEventID() - 1,
		workflowTask: &workflowTask,
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

	taskToken := tasktoken.NewWorkflowTaskToken(
		s.namespace.ID().String(),
		workflowID,
		runID,
		workflowTaskInfo.ScheduledEventID,
		workflowTaskInfo.StartedEventID,
		workflowTaskInfo.StartedTime,
		workflowTaskInfo.Attempt,
		clock,
		workflowTaskInfo.Version,
	)
	serializedToken, err := tokenSerializer.Serialize(taskToken)
	if err != nil {
		return nil, err
	}
	return &historyservice.StartWorkflowExecutionResponse{
		RunId: runID,
		Clock: clock,
		EagerWorkflowTask: &workflowservice.PollWorkflowTaskQueueResponse{
			TaskToken:         serializedToken,
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			WorkflowType:      request.GetWorkflowType(),
			// TODO: consider getting the ID from mutable state, this was not done to avoid adding more complexity to
			// the code to plumb that value through.
			PreviousStartedEventId:     0,
			StartedEventId:             workflowTaskInfo.StartedEventID,
			Attempt:                    workflowTaskInfo.Attempt,
			History:                    &historypb.History{Events: historyEvents},
			NextPageToken:              nil,
			WorkflowExecutionTaskQueue: workflowTaskInfo.TaskQueue,
			ScheduledTime:              workflowTaskInfo.ScheduledTime,
			StartedTime:                workflowTaskInfo.StartedTime,
		},
	}, nil
}
