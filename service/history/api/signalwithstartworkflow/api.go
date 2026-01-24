package signalwithstartworkflow

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/enums"
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
	versionMembershipCache worker_versioning.VersionMembershipCache,
) (_ *historyservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(signalWithStartRequest.GetNamespaceId()), signalWithStartRequest.GetSignalWithStartRequest().GetWorkflowId())
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
			signalWithStartRequest.GetSignalWithStartRequest().GetWorkflowId(),
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

	// TODO: remove this call in 1.25
	req := signalWithStartRequest.GetSignalWithStartRequest()
	req.SetWorkflowIdConflictPolicy(enums.DefaultWorkflowIdConflictPolicy(
		req.GetWorkflowIdConflictPolicy(),
		enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING))

	reusePolicy, conflictPolicy := api.MigrateWorkflowIdReusePolicyForRunningWorkflow(
		req.GetWorkflowIdReusePolicy(),
		req.GetWorkflowIdConflictPolicy())
	req.SetWorkflowIdReusePolicy(reusePolicy)
	req.SetWorkflowIdConflictPolicy(conflictPolicy)

	startRequest := ConvertToStartRequest(
		namespaceID,
		signalWithStartRequest.GetSignalWithStartRequest(),
		shard.GetTimeSource().Now(),
	)
	request := startRequest.GetStartRequest()

	api.OverrideStartWorkflowExecutionRequest(request, metrics.HistorySignalWithStartWorkflowExecutionScope, shard, shard.GetMetricsHandler())

	err = api.ValidateStartWorkflowExecutionRequest(ctx, request, shard, namespaceEntry, "SignalWithStartWorkflowExecution")
	if err != nil {
		return nil, err
	}

	// Validation for versioning override, if any.
	err = worker_versioning.ValidateVersioningOverride(ctx, request.GetVersioningOverride(), matchingClient, versionMembershipCache, request.GetTaskQueue().GetName(), enumspb.TASK_QUEUE_TYPE_WORKFLOW, namespaceID.String())
	if err != nil {
		return nil, err
	}

	runID, started, err := SignalWithStartWorkflow(
		ctx,
		shard,
		namespaceEntry,
		currentWorkflowLease,
		startRequest,
		signalWithStartRequest.GetSignalWithStartRequest(),
	)
	if err != nil {
		return nil, err
	}
	return historyservice.SignalWithStartWorkflowExecutionResponse_builder{
		RunId:   runID,
		Started: started,
	}.Build(), nil
}
