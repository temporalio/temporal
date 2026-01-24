package resetactivity

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	req *historyservice.ResetActivityRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.ResetActivityResponse, retError error) {
	request := req.GetFrontendRequest()
	workflowKey := definition.NewWorkflowKey(
		req.GetNamespaceId(),
		request.GetExecution().GetWorkflowId(),
		request.GetExecution().GetRunId(),
	)

	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		workflowKey,
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			var activityIDs []string
			switch request.WhichActivity() {
			case workflowservice.ResetActivityRequest_Id_case:
				activityIDs = append(activityIDs, request.GetId())
			case workflowservice.ResetActivityRequest_Type_case:
				activityType := request.GetType()
				for _, ai := range mutableState.GetPendingActivityInfos() {
					if ai.GetActivityType().GetName() == activityType {
						activityIDs = append(activityIDs, ai.GetActivityId())
					}
				}
			}

			if len(activityIDs) == 0 {
				return nil, consts.ErrActivityNotFound
			}

			for _, activityId := range activityIDs {
				if err := workflow.ResetActivity(
					ctx,
					shardContext, mutableState, activityId,
					request.GetResetHeartbeat(), request.GetKeepPaused(), request.GetRestoreOriginalOptions(),
					request.GetJitter().AsDuration(),
				); err != nil {
					return nil, err
				}
			}
			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: false,
			}, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)

	if err != nil {
		return nil, err
	}

	return &historyservice.ResetActivityResponse{}, nil
}
