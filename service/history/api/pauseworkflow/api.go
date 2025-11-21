package pauseworkflow

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
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
	req *historyservice.PauseWorkflowExecutionRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.PauseWorkflowExecutionResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	pauseRequest := req.GetPauseRequest()
	if pauseRequest == nil {
		return nil, serviceerror.NewInvalidArgument("pause request is nil.")
	}

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			namespaceID.String(),
			pauseRequest.GetWorkflowId(),
			pauseRequest.GetRunId(),
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()

			releaseFn := workflowLease.GetReleaseFn()
			// Make sure the workflow is not closed.
			if !mutableState.IsWorkflowExecutionRunning() {
				// in-memory mutable state is still clean, release the lock with nil error to prevent
				// clearing and reloading mutable state
				releaseFn(nil)
				return nil, consts.ErrWorkflowCompleted
			}

			// Check if workflow is already paused
			if mutableState.GetExecutionState().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
				// Already paused, nothing to do
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}

			// Add the workflow execution paused event
			_, err := mutableState.AddWorkflowExecutionPausedEvent(
				pauseRequest.GetIdentity(),
				pauseRequest.GetReason(),
				pauseRequest.GetRequestId(),
			)
			if err != nil {
				return nil, err
			}

			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: false, // Don't create workflow task when pausing
			}, nil
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}

	// Return response
	return &historyservice.PauseWorkflowExecutionResponse{}, nil
}
