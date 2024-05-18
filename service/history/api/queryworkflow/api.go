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

package queryworkflow

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/worker_versioning"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/resetstickytaskqueue"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

// Fail query fast if workflow task keeps failing (attempt >= 3).
const failQueryWorkflowTaskAttemptCount = 3

func Invoke(
	ctx context.Context,
	request *historyservice.QueryWorkflowRequest,
	shardContext shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	rawMatchingClient matchingservice.MatchingServiceClient,
	matchingClient matchingservice.MatchingServiceClient,
) (_ *historyservice.QueryWorkflowResponse, retError error) {
	scope := shardContext.GetMetricsHandler().WithTags(metrics.OperationTag(metrics.HistoryQueryWorkflowScope))
	namespaceID := namespace.ID(request.GetNamespaceId())
	err := api.ValidateNamespaceUUID(namespaceID)
	if err != nil {
		return nil, err
	}
	nsEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	if len(request.Request.Execution.RunId) == 0 {
		request.Request.Execution.RunId, err = workflowConsistencyChecker.GetCurrentRunID(
			ctx,
			request.NamespaceId,
			request.Request.Execution.WorkflowId,
			workflow.LockPriorityHigh,
		)
		if err != nil {
			return nil, err
		}
	}
	workflowKey := definition.NewWorkflowKey(
		request.NamespaceId,
		request.Request.Execution.WorkflowId,
		request.Request.Execution.RunId,
	)
	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		workflowKey,
		workflow.LockPriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	req := request.GetRequest()
	_, mutableStateStatus := workflowLease.GetMutableState().GetWorkflowStateStatus()
	if mutableStateStatus != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && req.QueryRejectCondition != enumspb.QUERY_REJECT_CONDITION_NONE {
		notOpenReject := req.GetQueryRejectCondition() == enumspb.QUERY_REJECT_CONDITION_NOT_OPEN
		notCompletedCleanlyReject := req.GetQueryRejectCondition() == enumspb.QUERY_REJECT_CONDITION_NOT_COMPLETED_CLEANLY && mutableStateStatus != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		if notOpenReject || notCompletedCleanlyReject {
			return &historyservice.QueryWorkflowResponse{
				Response: &workflowservice.QueryWorkflowResponse{
					QueryRejected: &querypb.QueryRejected{
						Status: mutableStateStatus,
					},
				},
			}, nil
		}
	}

	mutableState := workflowLease.GetMutableState()
	if !mutableState.IsWorkflowExecutionRunning() && !mutableState.HasCompletedAnyWorkflowTask() {
		// Workflow was closed before WorkflowTaskStarted event. In this case query will fail.
		return nil, consts.ErrWorkflowClosedBeforeWorkflowTaskStarted
	}

	if !mutableState.HadOrHasWorkflowTask() {
		// workflow has no workflow task ever scheduled, this usually is due to firstWorkflowTaskBackoff (cron / retry)
		// in this case, don't buffer the query, because it is almost certain the query will time out.
		return nil, consts.ErrWorkflowTaskNotScheduled
	}

	if mutableState.GetExecutionInfo().WorkflowTaskAttempt >= failQueryWorkflowTaskAttemptCount {
		// while workflow task is failing, the query to that workflow will also fail. Failing fast here to prevent wasting
		// resources to load history for a query that will fail.
		shardContext.GetLogger().Info("Fail query fast due to WorkflowTask in failed state.",
			tag.WorkflowNamespace(request.Request.Namespace),
			tag.WorkflowNamespaceID(workflowKey.NamespaceID),
			tag.WorkflowID(workflowKey.WorkflowID),
			tag.WorkflowRunID(workflowKey.RunID))
		return nil, serviceerror.NewWorkflowNotReady("Unable to query workflow due to Workflow Task in failed state.")
	}

	// There are two ways in which queries get dispatched to workflow worker. First, queries can be dispatched on workflow tasks.
	// These workflow tasks potentially contain new events and queries. The events are treated as coming before the query in time.
	// The second way in which queries are dispatched to workflow worker is directly through matching; in this approach queries can be
	// dispatched to workflow worker immediately even if there are outstanding events that came before the query. The following logic
	// is used to determine if a query can be safely dispatched directly through matching or must be dispatched on a workflow task.
	//
	// Precondition to dispatch query directly to matching is workflow has at least one WorkflowTaskStarted event. Otherwise, sdk would panic.
	if mutableState.HasCompletedAnyWorkflowTask() {
		// There are three cases in which a query can be dispatched directly through matching safely, without violating strong consistency level:
		// 1. the namespace is not active, in this case history is immutable so a query dispatched at any time is consistent
		// 2. the workflow is not running, whenever a workflow is not running dispatching query directly is consistent
		// 3. if there is no pending or started workflow tasks it means no events came before query arrived, so its safe to dispatch directly
		safeToDispatchDirectly := !nsEntry.ActiveInCluster(shardContext.GetClusterMetadata().GetCurrentClusterName()) ||
			!mutableState.IsWorkflowExecutionRunning() ||
			(!mutableState.HasPendingWorkflowTask() && !mutableState.HasStartedWorkflowTask())
		if safeToDispatchDirectly {
			msResp, err := api.MutableStateToGetResponse(mutableState)
			if err != nil {
				return nil, err
			}
			workflowLease.GetReleaseFn()(nil)
			req.Execution.RunId = msResp.Execution.RunId
			return queryDirectlyThroughMatching(
				ctx,
				msResp,
				request.GetNamespaceId(),
				req,
				shardContext,
				workflowConsistencyChecker,
				rawMatchingClient,
				matchingClient,
				scope,
			)
		}
	}

	// If we get here it means query could not be dispatched through matching directly, so it must block
	// until either a result has been obtained on a workflow task response or until it is safe to dispatch directly through matching.
	startTime := time.Now().UTC()
	defer func() { metrics.WorkflowTaskQueryLatency.With(scope).Record(time.Since(startTime)) }()

	queryReg := mutableState.GetQueryRegistry()
	if len(queryReg.GetBufferedIDs()) >= shardContext.GetConfig().MaxBufferedQueryCount() {
		metrics.QueryBufferExceededCount.With(scope).Record(1)
		return nil, consts.ErrConsistentQueryBufferExceeded
	}
	queryID, completionCh := queryReg.BufferQuery(req.GetQuery())
	defer queryReg.RemoveQuery(queryID)
	workflowLease.GetReleaseFn()(nil)
	select {
	case <-completionCh:
		completionState, err := queryReg.GetCompletionState(queryID)
		if err != nil {
			metrics.QueryRegistryInvalidStateCount.With(scope).Record(1)
			return nil, err
		}
		switch completionState.Type {
		case workflow.QueryCompletionTypeSucceeded:
			result := completionState.Result
			switch result.GetResultType() {
			case enumspb.QUERY_RESULT_TYPE_ANSWERED:
				return &historyservice.QueryWorkflowResponse{
					Response: &workflowservice.QueryWorkflowResponse{
						QueryResult: result.GetAnswer(),
					},
				}, nil
			case enumspb.QUERY_RESULT_TYPE_FAILED:
				return nil, serviceerror.NewQueryFailed(result.GetErrorMessage())
			default:
				metrics.QueryRegistryInvalidStateCount.With(scope).Record(1)
				return nil, consts.ErrQueryEnteredInvalidState
			}
		case workflow.QueryCompletionTypeUnblocked:
			msResp, err := api.GetMutableState(ctx, shardContext, workflowKey, workflowConsistencyChecker)
			if err != nil {
				return nil, err
			}
			req.Execution.RunId = msResp.Execution.RunId
			return queryDirectlyThroughMatching(
				ctx,
				msResp,
				request.GetNamespaceId(),
				req,
				shardContext,
				workflowConsistencyChecker,
				rawMatchingClient,
				matchingClient,
				scope,
			)
		case workflow.QueryCompletionTypeFailed:
			return nil, completionState.Err
		default:
			metrics.QueryRegistryInvalidStateCount.With(scope).Record(1)
			return nil, consts.ErrQueryEnteredInvalidState
		}
	case <-ctx.Done():
		metrics.ConsistentQueryTimeoutCount.With(scope).Record(1)
		return nil, ctx.Err()
	}
}

func queryDirectlyThroughMatching(
	ctx context.Context,
	msResp *historyservice.GetMutableStateResponse,
	namespaceID string,
	queryRequest *workflowservice.QueryWorkflowRequest,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	rawMatchingClient matchingservice.MatchingServiceClient,
	matchingClient matchingservice.MatchingServiceClient,
	metricsHandler metrics.Handler,
) (*historyservice.QueryWorkflowResponse, error) {

	startTime := time.Now().UTC()
	defer func() {
		metrics.DirectQueryDispatchLatency.With(metricsHandler).Record(time.Since(startTime))
	}()

	directive := worker_versioning.MakeDirectiveForWorkflowTask(
		msResp.GetInheritedBuildId(),
		msResp.GetAssignedBuildId(),
		msResp.GetMostRecentWorkerVersionStamp(),
		msResp.GetPreviousStartedEventId() != common.EmptyEventID,
	)

	if msResp.GetIsStickyTaskQueueEnabled() &&
		len(msResp.GetStickyTaskQueue().GetName()) != 0 &&
		shard.GetConfig().EnableStickyQuery(queryRequest.GetNamespace()) {

		stickyMatchingRequest := &matchingservice.QueryWorkflowRequest{
			NamespaceId:      namespaceID,
			QueryRequest:     queryRequest,
			TaskQueue:        msResp.GetStickyTaskQueue(),
			VersionDirective: directive,
		}

		// using a clean new context in case customer provide a context which has
		// a really short deadline, causing we clear the stickiness
		stickyContext, cancel := rpc.ResetContextTimeout(ctx, timestamp.DurationValue(msResp.GetStickyTaskQueueScheduleToStartTimeout()))
		stickyStartTime := time.Now().UTC()
		matchingResp, err := rawMatchingClient.QueryWorkflow(stickyContext, stickyMatchingRequest)
		metrics.DirectQueryDispatchStickyLatency.With(metricsHandler).Record(time.Since(stickyStartTime))
		cancel()
		if err == nil {
			metrics.DirectQueryDispatchStickySuccessCount.With(metricsHandler).Record(1)
			return &historyservice.QueryWorkflowResponse{
				Response: &workflowservice.QueryWorkflowResponse{
					QueryResult:   matchingResp.GetQueryResult(),
					QueryRejected: matchingResp.GetQueryRejected(),
				}}, nil
		}
		if !common.IsContextDeadlineExceededErr(err) && !common.IsContextCanceledErr(err) && !common.IsStickyWorkerUnavailable(err) {
			return nil, err
		}
		if msResp.GetWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			resetContext, cancel := rpc.ResetContextTimeout(ctx, 5*time.Second)
			clearStickinessStartTime := time.Now().UTC()
			_, err := resetstickytaskqueue.Invoke(resetContext, &historyservice.ResetStickyTaskQueueRequest{
				NamespaceId: namespaceID,
				Execution:   queryRequest.GetExecution(),
			}, shard, workflowConsistencyChecker)
			metrics.DirectQueryDispatchClearStickinessLatency.With(metricsHandler).Record(time.Since(clearStickinessStartTime))
			cancel()
			if err != nil && err != consts.ErrWorkflowCompleted {
				return nil, err
			}
			metrics.DirectQueryDispatchClearStickinessSuccessCount.With(metricsHandler).Record(1)
		}
	}

	if err := common.IsValidContext(ctx); err != nil {
		metrics.DirectQueryDispatchTimeoutBeforeNonStickyCount.With(metricsHandler).Record(1)
		return nil, err
	}

	nonStickyMatchingRequest := &matchingservice.QueryWorkflowRequest{
		NamespaceId:      namespaceID,
		QueryRequest:     queryRequest,
		TaskQueue:        msResp.TaskQueue,
		VersionDirective: directive,
	}

	nonStickyStartTime := time.Now().UTC()
	matchingResp, err := matchingClient.QueryWorkflow(ctx, nonStickyMatchingRequest)
	metrics.DirectQueryDispatchNonStickyLatency.With(metricsHandler).Record(time.Since(nonStickyStartTime))
	if err != nil {
		return nil, err
	}
	metrics.DirectQueryDispatchNonStickySuccessCount.With(metricsHandler).Record(1)
	return &historyservice.QueryWorkflowResponse{
		Response: &workflowservice.QueryWorkflowResponse{
			QueryResult:   matchingResp.GetQueryResult(),
			QueryRejected: matchingResp.GetQueryRejected(),
		}}, err
}
