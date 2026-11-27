package resetstickytaskqueue

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
	resetRequest *historyservice.ResetStickyTaskQueueRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.ResetStickyTaskQueueResponse, error) {
	namespaceID := namespace.ID(resetRequest.GetNamespaceId())
	err := api.ValidateNamespaceUUID(namespaceID)
	if err != nil {
		return nil, err
	}

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			resetRequest.NamespaceId,
			resetRequest.Execution.WorkflowId,
			resetRequest.Execution.RunId,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			return resetStickyTaskQueue(workflowLease.GetMutableState())
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)

	if err != nil {
		return nil, err
	}
	return &historyservice.ResetStickyTaskQueueResponse{}, nil
}

func resetStickyTaskQueue(mutableState historyi.MutableState) (*api.UpdateWorkflowAction, error) {
	if !mutableState.IsWorkflowExecutionRunning() {
		return nil, consts.ErrWorkflowCompleted
	}
	// Forgetting a bound worker unbinds the workflow; its session activities wait for the next binding.
	if workerQueue := mutableState.SessionWorkerQueue(); workerQueue != "" {
		if mutableState.HasExclusiveBinding() {
			// A single-session binding is never released, so there is no sticky queue to reset.
			return nil, serviceerror.NewFailedPrecondition("workflow is bound to a single-session worker; its sticky task queue cannot be reset")
		}
		unbound, err := mutableState.UpdateNonExclusiveWorkerBinding(nil, "", workerQueue)
		if err != nil {
			return nil, err
		}
		return &api.UpdateWorkflowAction{
			Noop: !unbound,
			// Session activities wait while the workflow is unbound; a workflow task lets a session worker bind it.
			CreateWorkflowTask: unbound && mutableState.HasPendingSessionActivities(),
		}, nil
	}
	mutableState.ClearStickyTaskQueue(historyi.StickyClearForResetStickyTaskQueue)
	return &api.UpdateWorkflowAction{
		Noop:               true,
		CreateWorkflowTask: false,
	}, nil
}
