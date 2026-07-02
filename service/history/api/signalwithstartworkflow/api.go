package signalwithstartworkflow

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	signalWithStartRequest *historyservice.SignalWithStartWorkflowExecutionRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	matchingClient matchingservice.MatchingServiceClient,
	versionCache worker_versioning.VersionMembershipAndReactivationStatusCache,
	reactivationSignaler api.VersionReactivationSignalerFn,
) (_ *historyservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(signalWithStartRequest.GetNamespaceId()), signalWithStartRequest.SignalWithStartRequest.WorkflowId)
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	var currentWorkflowLease api.WorkflowLease
	currentWorkflowLease, err = workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(
			string(namespaceID),
			signalWithStartRequest.SignalWithStartRequest.WorkflowId,
			"",
		),
		locks.PriorityHigh,
	)
	switch err.(type) {
	case nil:
		defer func() { currentWorkflowLease.GetReleaseFn()(retError) }()
	case *serviceerror.NotFound:
		currentWorkflowLease = nil
	default:
		return nil, err
	}

	startRequest := ConvertToStartRequest(
		namespaceID,
		signalWithStartRequest.SignalWithStartRequest,
		shard.GetTimeSource().Now(),
	)
	request := startRequest.StartRequest

	api.MigrateWorkflowIDReusePolicyForRunningWorkflow(
		&signalWithStartRequest.SignalWithStartRequest.WorkflowIdReusePolicy,
		&signalWithStartRequest.SignalWithStartRequest.WorkflowIdConflictPolicy)

	api.OverrideStartWorkflowExecutionRequest(request, metrics.HistorySignalWithStartWorkflowExecutionScope, shard, shard.GetMetricsHandler())

	err = api.ValidateStartWorkflowExecutionRequest(ctx, request, shard, namespaceEntry, "SignalWithStartWorkflowExecution")
	if err != nil {
		return nil, err
	}

	// Validation for versioning override, if any.
	shouldSkipReactivation, revisionNumber, err := worker_versioning.ValidateVersioningOverrideAndGetReactivationEligibility(ctx, request.GetVersioningOverride(), matchingClient, versionCache, request.GetTaskQueue().GetName(), enumspb.TASK_QUEUE_TYPE_WORKFLOW, namespaceID.String())
	if err != nil {
		return nil, err
	}

	runID, firstExecutionRunID, started, err := SignalWithStartWorkflow(
		ctx,
		shard,
		namespaceEntry,
		currentWorkflowLease,
		startRequest,
		signalWithStartRequest.SignalWithStartRequest,
	)
	if err != nil {
		return nil, err
	}

	// CurrentWorkflowConditionFailedError carries the persisted WorkflowExecutionState blob, which
	// may not have first_execution_run_id populated on records written before that field existed.
	// Load mutable state once to recover the canonical head-of-chain run id in that case (mirrors
	// StartWorkflowExecution's behavior).
	if firstExecutionRunID == "" && runID != "" {
		if id, derr := getFirstExecutionRunID(ctx, shard, workflowConsistencyChecker, namespaceID, signalWithStartRequest.SignalWithStartRequest.GetWorkflowId(), runID); derr == nil {
			firstExecutionRunID = id
		}
	}

	// Notify version workflow if we're starting a new workflow pinned to a potentially drained version
	if started {
		api.ReactivateVersionWorkflowIfPinned(ctx, namespaceEntry, request.GetVersioningOverride(), reactivationSignaler, shard.GetConfig().EnableVersionReactivationSignals(), shouldSkipReactivation, revisionNumber)
	}

	swr := signalWithStartRequest.SignalWithStartRequest
	return &historyservice.SignalWithStartWorkflowExecutionResponse{
		RunId:               runID,
		FirstExecutionRunId: firstExecutionRunID,
		Started:             started,
		SignalLink: api.GenerateRequestIDRefLink(
			swr.GetNamespace(),
			swr.GetWorkflowId(),
			runID,
			swr.GetRequestId(),
			enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		),
	}, nil
}

// getFirstExecutionRunID loads mutable state for the given run and resolves the head-of-chain run id
// via MutableState.GetFirstRunID() (which checks ExecutionState, then the deprecated ExecutionInfo
// field, then falls back to reading the WorkflowExecutionStarted event). Used to recover the value
// for legacy workflows whose persisted WorkflowExecutionState predates first_execution_run_id.
func getFirstExecutionRunID(
	ctx context.Context,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
) (_ string, retErr error) {
	workflowContext, releaseFn, err := workflowConsistencyChecker.GetWorkflowCache().GetOrCreateWorkflowExecution(
		ctx,
		shard,
		namespaceID,
		&commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		locks.PriorityHigh,
	)
	if err != nil {
		return "", err
	}
	defer func() { releaseFn(retErr) }()

	ms, err := workflowContext.LoadMutableState(ctx, shard)
	if err != nil {
		return "", err
	}
	return ms.GetFirstRunID(ctx)
}
