package unpauseworkflow

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	req *historyservice.UnpauseWorkflowExecutionRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.UnpauseWorkflowExecutionResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()), req.GetUnpauseRequest().GetWorkflowId())
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	unpauseRequest := req.GetUnpauseRequest()
	if unpauseRequest == nil {
		return nil, serviceerror.NewInvalidArgument("unpause request is nil.")
	}

	if err := validateUnpauseRequest(unpauseRequest, shard); err != nil {
		return nil, err
	}

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			namespaceID.String(),
			unpauseRequest.GetWorkflowId(),
			unpauseRequest.GetRunId(),
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

			// Ensure that the workflow is already paused
			if mutableState.GetExecutionState().GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
				releaseFn(nil)
				return nil, serviceerror.NewFailedPrecondition("workflow is not paused.")
			}

			// Add the workflow execution unpaused event
			_, err := mutableState.AddWorkflowExecutionUnpausedEvent(
				unpauseRequest.GetIdentity(),
				unpauseRequest.GetReason(),
				unpauseRequest.GetRequestId(),
			)
			if err != nil {
				return nil, err
			}

			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: true, // Create workflow task when unpausing
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
	return &historyservice.UnpauseWorkflowExecutionResponse{}, nil
}

func validateUnpauseRequest(req *workflowservice.UnpauseWorkflowExecutionRequest, shard historyi.ShardContext) error {
	// verify size limits of reason, request id and identity.
	if len(req.GetReason()) > shard.GetConfig().MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgument("reason is too long.")
	}
	if len(req.GetRequestId()) > shard.GetConfig().MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgument("request id is too long.")
	}
	if len(req.GetIdentity()) > shard.GetConfig().MaxIDLengthLimit() {
		return serviceerror.NewInvalidArgument("identity is too long.")
	}

	return nil
}
