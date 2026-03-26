package resetactivityexecution

import (
	"context"
	"fmt"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	req *historyservice.ResetActivityExecutionRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.ResetActivityExecutionResponse, retError error) {
	return nil, fmt.Errorf("not implemented")
	// request := req.GetFrontendRequest()
	// workflowKey := definition.NewWorkflowKey(
	// 	req.NamespaceId,
	// 	request.GetExecution().GetWorkflowId(),
	// 	request.GetExecution().GetRunId(),
	// )

	// err := api.GetAndUpdateWorkflowWithNew(
	// 	ctx,
	// 	nil,
	// 	workflowKey,
	// 	func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
	// 		mutableState := workflowLease.GetMutableState()
	// 		var activityIDs []string
	// 		switch a := request.GetActivity().(type) {
	// 		case *workflowservice.ResetActivityRequest_Id:
	// 			activityIDs = append(activityIDs, a.Id)
	// 		case *workflowservice.ResetActivityRequest_Type:
	// 			activityType := a.Type
	// 			for _, ai := range mutableState.GetPendingActivityInfos() {
	// 				if ai.ActivityType.Name == activityType {
	// 					activityIDs = append(activityIDs, ai.ActivityId)
	// 				}
	// 			}
	// 		}

	// 		if len(activityIDs) == 0 {
	// 			return nil, consts.ErrActivityNotFound
	// 		}

	// 		for _, activityId := range activityIDs {
	// 			if err := workflow.ResetActivity(
	// 				ctx,
	// 				shardContext, mutableState, activityId,
	// 				request.ResetHeartbeat, request.KeepPaused, request.RestoreOriginalOptions,
	// 				request.Jitter.AsDuration(),
	// 			); err != nil {
	// 				return nil, err
	// 			}
	// 		}
	// 		return &api.UpdateWorkflowAction{
	// 			Noop:               false,
	// 			CreateWorkflowTask: false,
	// 		}, nil
	// 	},
	// 	nil,
	// 	shardContext,
	// 	workflowConsistencyChecker,
	// )

	// if err != nil {
	// 	return nil, err
	// }

	// return &historyservice.ResetActivityResponse{}, nil
}
