package queryworkflow

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/resetstickytaskqueue"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/fx"
)

// Fail query fast if workflow task keeps failing (attempt >= 3).
const failQueryWorkflowTaskAttemptCount = 3

type Deps struct {
	fx.In

	ShardContext               historyi.ShardContext
	MetricsHandler             metrics.Handler
	NamespaceRegistry          namespace.Registry
	Logger                     log.Logger
	ClusterMetadata            cluster.Metadata
	Config                     *configs.Config
	WorkflowConsistencyChecker api.WorkflowConsistencyChecker
	RawMatchingClient          resource.MatchingRawClient
	MatchingClient             resource.MatchingClient
}

func (deps *Deps) Invoke(
	ctx context.Context,
	request *historyservice.QueryWorkflowRequest,
) (_ *historyservice.QueryWorkflowResponse, retError error) {
	scope := deps.MetricsHandler.WithTags(metrics.OperationTag(metrics.HistoryQueryWorkflowScope))
	namespaceID := namespace.ID(request.GetNamespaceId())
	err := api.ValidateNamespaceUUID(namespaceID)
	if err != nil {
		return nil, err
	}
	nsEntry, err := deps.NamespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	if len(request.Request.Execution.RunId) == 0 {
		request.Request.Execution.RunId, err = deps.WorkflowConsistencyChecker.GetCurrentRunID(
			ctx,
			request.NamespaceId,
			request.Request.Execution.WorkflowId,
			locks.PriorityHigh,
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
	workflowLease, err := deps.WorkflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		workflowKey,
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		// Do not clear mutable state when query failed. Clear mutable state will fail other buffered pending queries.
		// Note: QueryWorkflow should not alter mutable state, so it is safe to ignore error and not clear ms.
		workflowLease.GetReleaseFn()(nil)
	}()

	req := request.GetRequest()
	_, mutableStateStatus := workflowLease.GetMutableState().GetWorkflowStateStatus()
	scope = scope.WithTags(metrics.StringTag("workflow_status", mutableStateStatus.String()))
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
		// Workflow has no workflow task scheduled.
		// This can be due to firstWorkflowTaskBackoff (cron / retry)
		// In this case, check if query can wait.
		queryWillTimeout, err := queryWillTimeoutsBeforeFirstWorkflowTaskStart(ctx, mutableState)
		if err != nil {
			return nil, err
		}
		if queryWillTimeout {
			return nil, consts.ErrWorkflowTaskNotScheduled
		}
	}

	if mutableState.GetExecutionInfo().WorkflowTaskAttempt >= failQueryWorkflowTaskAttemptCount {
		// while workflow task is failing, the query to that workflow will also fail. Failing fast here to prevent wasting
		// resources to load history for a query that will fail.
		deps.Logger.Info("Fail query fast due to WorkflowTask in failed state.",
			tag.WorkflowNamespace(request.Request.Namespace),
			tag.WorkflowNamespaceID(workflowKey.NamespaceID),
			tag.WorkflowID(workflowKey.WorkflowID),
			tag.WorkflowRunID(workflowKey.RunID))
		return nil, serviceerror.NewWorkflowNotReady("Unable to query workflow due to Workflow Task in failed state.")
	}

	priority := mutableState.GetExecutionInfo().Priority

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
		safeToDispatchDirectly := !nsEntry.ActiveInCluster(deps.ClusterMetadata.GetCurrentClusterName()) ||
			!mutableState.IsWorkflowExecutionRunning() ||
			(!mutableState.HasPendingWorkflowTask() && !mutableState.HasStartedWorkflowTask())
		if safeToDispatchDirectly {
			msResp, err := api.MutableStateToGetResponse(mutableState)
			if err != nil {
				return nil, err
			}
			workflowLease.GetReleaseFn()(nil) // release the lock - no access to mutable state beyond this point!
			req.Execution.RunId = msResp.Execution.RunId
			return deps.queryDirectlyThroughMatching(
				ctx,
				msResp,
				nsEntry,
				request.GetNamespaceId(),
				req,
				scope,
				priority,
			)
		}
	}

	// If we get here it means query could not be dispatched through matching directly, so it must block
	// until either a result has been obtained on a workflow task response or until it is safe to dispatch directly through matching.
	startTime := time.Now().UTC()
	defer func() { metrics.WorkflowTaskQueryLatency.With(scope).Record(time.Since(startTime)) }()

	queryReg := mutableState.GetQueryRegistry()
	if len(queryReg.GetBufferedIDs()) >= deps.Config.MaxBufferedQueryCount() {
		metrics.QueryBufferExceededCount.With(scope).Record(1)
		return nil, consts.ErrConsistentQueryBufferExceeded
	}
	queryID, completionCh := queryReg.BufferQuery(req.GetQuery())
	defer queryReg.RemoveQuery(queryID)

	msResp, err := api.MutableStateToGetResponse(mutableState)
	if err != nil {
		return nil, err
	}

	workflowLease.GetReleaseFn()(nil) // release the lock - no access to mutable state beyond this point!
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
				emitWorkflowQueryMetrics(
					scope,
					nsEntry,
					msResp,
					req.GetQuery().GetQueryType(),
					nil,
				)
				return &historyservice.QueryWorkflowResponse{
					Response: &workflowservice.QueryWorkflowResponse{
						QueryResult: result.GetAnswer(),
					},
				}, nil
			case enumspb.QUERY_RESULT_TYPE_FAILED:
				err := serviceerror.NewQueryFailedWithFailure(result.GetErrorMessage(), result.GetFailure())
				emitWorkflowQueryMetrics(
					scope,
					nsEntry,
					msResp,
					req.GetQuery().GetQueryType(),
					err,
				)
				return nil, err
			default:
				metrics.QueryRegistryInvalidStateCount.With(scope).Record(1)
				return nil, consts.ErrQueryEnteredInvalidState
			}
		case workflow.QueryCompletionTypeUnblocked:
			msResp, err := api.GetMutableState(ctx, deps.ShardContext, workflowKey, deps.WorkflowConsistencyChecker)
			if err != nil {
				return nil, err
			}
			req.Execution.RunId = msResp.Execution.RunId
			return deps.queryDirectlyThroughMatching(
				ctx,
				msResp,
				nsEntry,
				request.GetNamespaceId(),
				req,
				scope,
				priority,
			)
		case workflow.QueryCompletionTypeFailed:
			err = completionState.Err
			emitWorkflowQueryMetrics(
				scope,
				nsEntry,
				msResp,
				req.GetQuery().GetQueryType(),
				err,
			)
			return nil, err
		default:
			metrics.QueryRegistryInvalidStateCount.With(scope).Record(1)
			return nil, consts.ErrQueryEnteredInvalidState
		}
	case <-ctx.Done():
		emitWorkflowQueryMetrics(
			scope,
			nsEntry,
			msResp,
			req.GetQuery().GetQueryType(),
			ctx.Err(),
		)
		metrics.ConsistentQueryTimeoutCount.With(scope).Record(1)
		return nil, ctx.Err()
	}
}

func queryWillTimeoutsBeforeFirstWorkflowTaskStart(
	ctx context.Context, mutableState historyi.MutableState,
) (bool, error) {
	startEvent, err := mutableState.GetStartEvent(ctx)
	if err != nil {
		return false, err
	}
	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()
	workflowTaskBackoffDuration := timestamp.DurationValue(startAttr.GetFirstWorkflowTaskBackoff())

	workflowStart := mutableState.GetExecutionState().StartTime.AsTime().UTC()
	workflowTaskStart := workflowStart.Add(workflowTaskBackoffDuration)

	deadline, ok := ctx.Deadline()
	if !ok {
		return true, nil
	}
	deadline = deadline.UTC()
	if workflowTaskStart.After(deadline) {
		return true, nil
	}
	return false, nil
}

func (deps *Deps) queryDirectlyThroughMatching(
	ctx context.Context,
	msResp *historyservice.GetMutableStateResponse,
	nsEntry *namespace.Namespace,
	namespaceID string,
	queryRequest *workflowservice.QueryWorkflowRequest,
	scope metrics.Handler,
	priority *commonpb.Priority,
) (resp *historyservice.QueryWorkflowResponse, retError error) {

	startTime := time.Now().UTC()
	defer func() {
		metrics.DirectQueryDispatchLatency.With(scope).Record(time.Since(startTime))
		emitWorkflowQueryMetrics(
			scope,
			nsEntry,
			msResp,
			queryRequest.GetQuery().GetQueryType(),
			retError,
		)
	}()

	directive := worker_versioning.MakeDirectiveForWorkflowTask(
		msResp.GetInheritedBuildId(),
		msResp.GetAssignedBuildId(),
		msResp.GetMostRecentWorkerVersionStamp(),
		msResp.GetPreviousStartedEventId() != common.EmptyEventID,
		workflow.GetEffectiveVersioningBehavior(msResp.GetVersioningInfo()),
		workflow.GetEffectiveDeployment(msResp.GetVersioningInfo()),
	)

	if msResp.GetIsStickyTaskQueueEnabled() &&
		len(msResp.GetStickyTaskQueue().GetName()) != 0 &&
		deps.Config.EnableStickyQuery(queryRequest.GetNamespace()) {

		stickyMatchingRequest := &matchingservice.QueryWorkflowRequest{
			NamespaceId:      namespaceID,
			QueryRequest:     queryRequest,
			TaskQueue:        msResp.GetStickyTaskQueue(),
			VersionDirective: directive,
			Priority:         priority,
		}

		// using a clean new context in case customer provide a context which has
		// a really short deadline, causing we clear the stickiness
		stickyContext, cancel := rpc.ResetContextTimeout(ctx, timestamp.DurationValue(msResp.GetStickyTaskQueueScheduleToStartTimeout()))
		stickyStartTime := time.Now().UTC()
		matchingResp, err := deps.RawMatchingClient.QueryWorkflow(stickyContext, stickyMatchingRequest)
		metrics.DirectQueryDispatchStickyLatency.With(scope).Record(time.Since(stickyStartTime))
		cancel()
		if err == nil {
			metrics.DirectQueryDispatchStickySuccessCount.With(scope).Record(1)
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
			}, deps.ShardContext, deps.WorkflowConsistencyChecker)
			metrics.DirectQueryDispatchClearStickinessLatency.With(scope).Record(time.Since(clearStickinessStartTime))
			cancel()
			if err != nil && err != consts.ErrWorkflowCompleted {
				return nil, err
			}
			metrics.DirectQueryDispatchClearStickinessSuccessCount.With(scope).Record(1)
		}
	}

	if err := common.IsValidContext(ctx); err != nil {
		metrics.DirectQueryDispatchTimeoutBeforeNonStickyCount.With(scope).Record(1)
		return nil, err
	}

	nonStickyMatchingRequest := &matchingservice.QueryWorkflowRequest{
		NamespaceId:      namespaceID,
		QueryRequest:     queryRequest,
		TaskQueue:        msResp.TaskQueue,
		VersionDirective: directive,
		Priority:         priority,
	}

	nonStickyStartTime := time.Now().UTC()
	matchingResp, err := deps.MatchingClient.QueryWorkflow(ctx, nonStickyMatchingRequest)
	metrics.DirectQueryDispatchNonStickyLatency.With(scope).Record(time.Since(nonStickyStartTime))
	if err != nil {
		return nil, err
	}
	metrics.DirectQueryDispatchNonStickySuccessCount.With(scope).Record(1)
	return &historyservice.QueryWorkflowResponse{
		Response: &workflowservice.QueryWorkflowResponse{
			QueryResult:   matchingResp.GetQueryResult(),
			QueryRejected: matchingResp.GetQueryRejected(),
		}}, err
}

func emitWorkflowQueryMetrics(
	metricsHandler metrics.Handler,
	nsEntry *namespace.Namespace,
	msResp *historyservice.GetMutableStateResponse,
	queryType string,
	err error,
) {
	commonTags := []metrics.Tag{
		metrics.OperationTag(metrics.HistoryQueryWorkflowScope),
		metrics.NamespaceTag(nsEntry.Name().String()),
		metrics.VersioningBehaviorTag(workflow.GetEffectiveVersioningBehavior(msResp.GetVersioningInfo())),
		metrics.WorkflowStatusTag(msResp.GetWorkflowStatus().String()),
		metrics.QueryTypeTag(queryType),
	}

	if err == nil {
		metrics.WorkflowQuerySuccessCount.With(metricsHandler).Record(1, commonTags...)
	} else if common.IsContextDeadlineExceededErr(err) {
		metrics.WorkflowQueryTimeoutCount.With(metricsHandler).Record(1, commonTags...)
	} else {
		metrics.WorkflowQueryFailureCount.With(metricsHandler).Record(1, commonTags...)
	}
}
