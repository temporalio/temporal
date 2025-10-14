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
		req.NamespaceId,
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
			switch a := request.GetActivity().(type) {
			case *workflowservice.ResetActivityRequest_Id:
				activityIDs = append(activityIDs, a.Id)
			case *workflowservice.ResetActivityRequest_Type:
				activityType := a.Type
				for _, ai := range mutableState.GetPendingActivityInfos() {
					if ai.ActivityType.Name == activityType {
						activityIDs = append(activityIDs, ai.ActivityId)
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
					request.ResetHeartbeat, request.KeepPaused, request.RestoreOriginalOptions,
					request.Jitter.AsDuration(),
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
