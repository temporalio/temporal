package unbindworkflowfromsessionworker

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	request *historyservice.UnbindWorkflowFromSessionWorkerRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.UnbindWorkflowFromSessionWorkerResponse, error) {
	// A standby cluster must not change the workflow.
	if _, err := api.GetActiveNamespace(shardContext, namespace.ID(request.GetNamespaceId()), request.GetExecution().GetWorkflowId()); err != nil {
		return nil, err
	}

	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			request.GetNamespaceId(),
			request.GetExecution().GetWorkflowId(),
			request.GetExecution().GetRunId(),
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			return unbindWorkflowFromSessionWorker(workflowLease.GetMutableState(), request.GetWorkerQueue())
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.UnbindWorkflowFromSessionWorkerResponse{}, nil
}

func unbindWorkflowFromSessionWorker(mutableState historyi.MutableState, workerQueue string) (*api.UpdateWorkflowAction, error) {
	if !mutableState.IsWorkflowExecutionRunning() {
		return nil, consts.ErrWorkflowCompleted
	}
	if mutableState.SessionWorkerQueue() != workerQueue {
		return &api.UpdateWorkflowAction{Noop: true}, nil
	}
	if mutableState.HasExclusiveBinding() {
		// Only a multi-session worker's queue is drained; a single-session binding is never released.
		return nil, serviceerror.NewFailedPrecondition("workflow is bound to a single-session worker and cannot be unbound from it")
	}
	unbound, err := mutableState.UpdateNonExclusiveWorkerBinding(nil, "", workerQueue)
	if err != nil {
		return nil, err
	}
	return &api.UpdateWorkflowAction{Noop: !unbound}, nil
}
