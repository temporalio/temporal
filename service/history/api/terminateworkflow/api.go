package terminateworkflow

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	req *historyservice.TerminateWorkflowExecutionRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.TerminateWorkflowExecutionResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shardContext, namespace.ID(req.GetNamespaceId()), req.TerminateRequest.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	request := req.TerminateRequest
	parentExecution := req.ExternalWorkflowExecution
	childWorkflowOnly := req.ChildWorkflowOnly
	workflowID := request.WorkflowExecution.WorkflowId
	runID := request.WorkflowExecution.RunId
	firstExecutionRunID := request.FirstExecutionRunId
	if len(request.FirstExecutionRunId) != 0 {
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
				return nil, consts.ErrWorkflowCompleted
			}

			// There is a workflow execution currently running with the WorkflowID.
			// If user passed in a FirstExecutionRunID with the request to allow terminate to work across runs then
			// let's compare the FirstExecutionRunID on the request to make sure we terminate the correct workflow
			// execution.
			executionInfo := mutableState.GetExecutionInfo()
			if len(firstExecutionRunID) > 0 && executionInfo.FirstExecutionRunId != firstExecutionRunID {
				return nil, consts.ErrWorkflowExecutionNotFound
			}

			if childWorkflowOnly {
				if parentExecution.GetWorkflowId() != executionInfo.ParentWorkflowId ||
					parentExecution.GetRunId() != executionInfo.ParentRunId {
					return nil, consts.ErrWorkflowParent
				}
			}

			return api.UpdateWorkflowTerminate, workflow.TerminateWorkflow(
				mutableState,
				request.GetReason(),
				request.GetDetails(),
				request.GetIdentity(),
				false,
				request.GetLinks(),
			)
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.TerminateWorkflowExecutionResponse{}, nil
}
