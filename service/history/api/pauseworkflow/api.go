package pauseworkflow

import (
	"context"
	"fmt"

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
	shard.GetLogger().Warn(fmt.Sprintf("Pausing workflow execution %s/%s by %s", req.GetPauseRequest().GetId(), req.GetPauseRequest().GetRunId(), req.GetPauseRequest().GetIdentity()))
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	pauseRequest := req.GetPauseRequest()
	if pauseRequest == nil {
		return nil, serviceerror.NewInvalidArgument("pause request is nil.")
	}

	if !shard.GetConfig().WorkflowPauseEnabled(namespaceID.String()) {
		return nil, serviceerror.NewUnimplementedf("workflow pause is not enabled for namespace: %s", namespaceID.String())
	}

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			namespaceID.String(),
			pauseRequest.GetId(),
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
			if mutableState.IsWorkflowExecutionStatusPaused() {
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
