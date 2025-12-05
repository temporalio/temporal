package signalworkflow

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	req *historyservice.SignalWorkflowExecutionRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.SignalWorkflowExecutionResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()), req.SignalRequest.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	request := req.SignalRequest
	externalWorkflowExecution := req.ExternalWorkflowExecution
	childWorkflowOnly := req.GetChildWorkflowOnly()

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			namespaceID.String(),
			request.WorkflowExecution.WorkflowId,
			request.WorkflowExecution.RunId,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			if request.GetRequestId() != "" && mutableState.IsSignalRequested(request.GetRequestId()) {
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}

			releaseFn := workflowLease.GetReleaseFn()
			if !mutableState.IsWorkflowExecutionRunning() {
				// in-memory mutable state is still clean, release the lock with nil error to prevent
				// clearing and reloading mutable state
				releaseFn(nil)
				return nil, consts.ErrWorkflowCompleted
			}

			if err := api.ValidateSignal(
				ctx,
				shard,
				mutableState,
				request.GetInput().Size(),
				request.GetHeader().Size(),
				"SignalWorkflowExecution",
			); err != nil {
				releaseFn(nil)
				return nil, err
			}

			executionInfo := mutableState.GetExecutionInfo()

			// Do not create workflow task when the workflow has first workflow task backoff and execution is not started yet
			createWorkflowTask := !mutableState.IsWorkflowPendingOnWorkflowTaskBackoff()

			if childWorkflowOnly {
				parentWorkflowID := executionInfo.ParentWorkflowId
				parentRunID := executionInfo.ParentRunId
				if externalWorkflowExecution.GetWorkflowId() != parentWorkflowID ||
					externalWorkflowExecution.GetRunId() != parentRunID {
					releaseFn(nil)
					return nil, consts.ErrWorkflowParent
				}
			}

			if request.GetRequestId() != "" {
				mutableState.AddSignalRequested(request.GetRequestId())
			}
			_, err := mutableState.AddWorkflowExecutionSignaledEvent(
				request.GetSignalName(),
				request.GetInput(),
				request.GetIdentity(),
				request.GetHeader(),
				externalWorkflowExecution,
				request.GetLinks(),
			)
			if err != nil {
				return nil, err
			}

			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: createWorkflowTask,
			}, nil
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.SignalWorkflowExecutionResponse{}, nil
}
