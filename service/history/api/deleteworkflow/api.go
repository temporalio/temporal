package deleteworkflow

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	request *historyservice.DeleteWorkflowExecutionRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	workflowDeleteManager deletemanager.DeleteManager,
) (_ *historyservice.DeleteWorkflowExecutionResponse, retError error) {
	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.WorkflowExecution.WorkflowId,
			request.WorkflowExecution.RunId,
		),
		locks.PriorityLow,
	)
	if err != nil {
		return nil, err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	// Open and Close workflow executions are deleted differently.
	// Open workflow execution is deleted by terminating with special flag `deleteAfterTerminate` set to true.
	// This flag will be carried over with CloseExecutionTask and workflow will be deleted as the last step while processing the task.
	//
	// Close workflow execution is deleted using DeleteExecutionTask.
	//
	// DeleteWorkflowExecution is not replicated automatically. Workflow executions must be deleted separately in each cluster.
	// Although running workflows in active cluster are terminated first and the termination event might be replicated.
	// In passive cluster, workflow executions are just deleted in regardless of its state.

	if workflowLease.GetMutableState().IsWorkflowExecutionRunning() {
		if request.GetClosedWorkflowOnly() {
			// skip delete open workflow
			return &historyservice.DeleteWorkflowExecutionResponse{}, nil
		}
		ns, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(request.GetNamespaceId()))
		if err != nil {
			return nil, err
		}
		if ns.ActiveInCluster(shardContext.GetClusterMetadata().GetCurrentClusterName()) {
			// If workflow execution is running and in active cluster.
			if err := api.UpdateWorkflowWithNew(
				shardContext,
				ctx,
				workflowLease,
				func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
					mutableState := workflowLease.GetMutableState()

					return api.UpdateWorkflowTerminate, workflow.TerminateWorkflow(
						mutableState,
						"Delete workflow execution",
						nil,
						consts.IdentityHistoryService,
						true,
						// TODO(bergundy): No links will be attached here for now, we may want to add support for this later though.
						nil,
					)
				},
				nil,
			); err != nil {
				return nil, err
			}
			return &historyservice.DeleteWorkflowExecutionResponse{}, nil
		}
	}

	// If workflow execution is closed or in passive cluster.
	if err := workflowDeleteManager.AddDeleteWorkflowExecutionTask(
		ctx,
		namespace.ID(request.GetNamespaceId()),
		&commonpb.WorkflowExecution{
			WorkflowId: request.GetWorkflowExecution().GetWorkflowId(),
			RunId:      request.GetWorkflowExecution().GetRunId(),
		},
		workflowLease.GetMutableState(),
	); err != nil {
		return nil, err
	}
	return &historyservice.DeleteWorkflowExecutionResponse{}, nil
}
