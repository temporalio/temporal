package unpauseactivityexecution

import (
	"context"
	"fmt"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	request *historyservice.UnpauseActivityExecutionRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.UnpauseActivityExecutionResponse, retError error) {
	return nil, fmt.Errorf("not implemented")
	// var response *historyservice.UnpauseActivityExecutionResponse

	// err := api.GetAndUpdateWorkflowWithNew(
	// 	ctx,
	// 	nil,
	// 	definition.NewWorkflowKey(
	// 		request.NamespaceId,
	// 		request.GetFrontendRequest().GetExecution().GetWorkflowId(),
	// 		request.GetFrontendRequest().GetExecution().GetRunId(),
	// 	),
	// 	func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
	// 		mutableState := workflowLease.GetMutableState()
	// 		var err error
	// 		response, err = processUnpauseActivityRequest(shardContext, mutableState, request)
	// 		if err != nil {
	// 			return nil, err
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

	// return response, err
}

// func processUnpauseActivityRequest(
// 	shardContext historyi.ShardContext,
// 	mutableState historyi.MutableState,
// 	request *historyservice.UnpauseActivityRequest,
// ) (*historyservice.UnpauseActivityResponse, error) {

// 	if !mutableState.IsWorkflowExecutionRunning() {
// 		return nil, consts.ErrWorkflowCompleted
// 	}
// 	frontendRequest := request.GetFrontendRequest()
// 	var activityIDs []string
// 	switch a := frontendRequest.GetActivity().(type) {
// 	case *workflowservice.UnpauseActivityRequest_Id:
// 		activityIDs = append(activityIDs, a.Id)
// 	case *workflowservice.UnpauseActivityRequest_Type:
// 		activityType := a.Type
// 		for _, ai := range mutableState.GetPendingActivityInfos() {
// 			if ai.ActivityType.Name == activityType {
// 				activityIDs = append(activityIDs, ai.ActivityId)
// 			}
// 		}
// 	}

// 	if len(activityIDs) == 0 {
// 		return nil, consts.ErrActivityNotFound
// 	}

// 	for _, activityId := range activityIDs {

// 		ai, activityFound := mutableState.GetActivityByActivityID(activityId)

// 		if !activityFound {
// 			return nil, consts.ErrActivityNotFound
// 		}

// 		if !ai.Paused {
// 			// do nothing
// 			continue
// 		}

// 		if err := workflow.UnpauseActivity(
// 			shardContext, mutableState, ai,
// 			frontendRequest.GetResetAttempts(),
// 			frontendRequest.GetResetHeartbeat(),
// 			frontendRequest.GetJitter().AsDuration()); err != nil {
// 			return nil, err
// 		}

// 	}

// 	return &historyservice.UnpauseActivityResponse{}, nil
// }
