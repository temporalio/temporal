package scheduleworkflowtask

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
	req *historyservice.ScheduleWorkflowTaskRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) error {

	_, err := api.GetActiveNamespace(shardContext, namespace.ID(req.GetNamespaceId()), req.WorkflowExecution.WorkflowId)
	if err != nil {
		return err
	}

	return api.GetAndUpdateWorkflowWithNew(
		ctx,
		req.ChildClock,
		definition.NewWorkflowKey(
			req.NamespaceId,
			req.WorkflowExecution.WorkflowId,
			req.WorkflowExecution.RunId,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			if req.IsFirstWorkflowTask && mutableState.HadOrHasWorkflowTask() {
				return &api.UpdateWorkflowAction{
					Noop: true,
				}, nil
			}

			startEvent, err := mutableState.GetStartEvent(ctx)
			if err != nil {
				return nil, err
			}
			if _, err := mutableState.AddFirstWorkflowTaskScheduled(req.ParentClock, startEvent, false); err != nil {
				return nil, err
			}

			return &api.UpdateWorkflowAction{}, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)
}
