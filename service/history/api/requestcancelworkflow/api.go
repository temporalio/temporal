package requestcancelworkflow

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
	req *historyservice.RequestCancelWorkflowExecutionRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.RequestCancelWorkflowExecutionResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()), req.CancelRequest.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	request := req.CancelRequest
	parentExecution := req.ExternalWorkflowExecution
	childWorkflowOnly := req.GetChildWorkflowOnly()
	workflowID := request.WorkflowExecution.WorkflowId
	runID := request.WorkflowExecution.RunId
	firstExecutionRunID := request.FirstExecutionRunId
	if len(firstExecutionRunID) != 0 {
		runID = ""
	}

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			runID,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				// the request to cancel this workflow is a success even
				// if the target workflow has already finished
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}

			// There is a workflow execution currently running with the WorkflowID.
			// If user passed in a FirstExecutionRunID with the request to allow cancel to work across runs then
			// let's compare the FirstExecutionRunID on the request to make sure we cancel the correct workflow
			// execution.
			executionInfo := mutableState.GetExecutionInfo()
			if len(firstExecutionRunID) > 0 && executionInfo.FirstExecutionRunId != firstExecutionRunID {
				return nil, consts.ErrWorkflowExecutionNotFound
			}

			if childWorkflowOnly {
				parentWorkflowID := executionInfo.ParentWorkflowId
				parentRunID := executionInfo.ParentRunId
				if parentExecution.GetWorkflowId() != parentWorkflowID ||
					parentExecution.GetRunId() != parentRunID {
					return nil, consts.ErrWorkflowParent
				}
			}

			isCancelRequested := mutableState.IsCancelRequested()
			if isCancelRequested {
				// since cancellation is idempotent
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}

			if _, err := mutableState.AddWorkflowExecutionCancelRequestedEvent(req); err != nil {
				return nil, err
			}

			return api.UpdateWorkflowWithNewWorkflowTask, nil
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.RequestCancelWorkflowExecutionResponse{}, nil
}
