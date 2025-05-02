package signalwithstartworkflow

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/enums"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	signalWithStartRequest *historyservice.SignalWithStartWorkflowExecutionRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (_ *historyservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(signalWithStartRequest.GetNamespaceId()))
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

	// TODO: remove this call in 1.25
	enums.SetDefaultWorkflowIdConflictPolicy(
		&signalWithStartRequest.SignalWithStartRequest.WorkflowIdConflictPolicy,
		enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING)

	api.MigrateWorkflowIdReusePolicyForRunningWorkflow(
		&signalWithStartRequest.SignalWithStartRequest.WorkflowIdReusePolicy,
		&signalWithStartRequest.SignalWithStartRequest.WorkflowIdConflictPolicy)

	startRequest := ConvertToStartRequest(
		namespaceID,
		signalWithStartRequest.SignalWithStartRequest,
		shard.GetTimeSource().Now(),
	)
	request := startRequest.StartRequest

	api.OverrideStartWorkflowExecutionRequest(request, metrics.HistorySignalWithStartWorkflowExecutionScope, shard, shard.GetMetricsHandler())

	err = api.ValidateStartWorkflowExecutionRequest(ctx, request, shard, namespaceEntry, "SignalWithStartWorkflowExecution")
	if err != nil {
		return nil, err
	}

	runID, started, err := SignalWithStartWorkflow(
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
	return &historyservice.SignalWithStartWorkflowExecutionResponse{
		RunId:   runID,
		Started: started,
	}, nil
}
