package unpauseactivity

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
	request *historyservice.UnpauseActivityRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.UnpauseActivityResponse, retError error) {
	var response *historyservice.UnpauseActivityResponse

	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.GetFrontendRequest().GetExecution().GetWorkflowId(),
			request.GetFrontendRequest().GetExecution().GetRunId(),
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			var err error
			response, err = processUnpauseActivityRequest(shardContext, mutableState, request)
			if err != nil {
				return nil, err
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

	return response, err
}

func processUnpauseActivityRequest(
	shardContext historyi.ShardContext,
	mutableState historyi.MutableState,
	request *historyservice.UnpauseActivityRequest,
) (*historyservice.UnpauseActivityResponse, error) {

	if !mutableState.IsWorkflowExecutionRunning() {
		return nil, consts.ErrWorkflowCompleted
	}
	frontendRequest := request.GetFrontendRequest()
	var activityIDs []string
	switch a := frontendRequest.GetActivity().(type) {
	case *workflowservice.UnpauseActivityRequest_Id:
		activityIDs = append(activityIDs, a.Id)
	case *workflowservice.UnpauseActivityRequest_Type:
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

		ai, activityFound := mutableState.GetActivityByActivityID(activityId)

		if !activityFound {
			return nil, consts.ErrActivityNotFound
		}

		if !ai.Paused {
			// do nothing
			continue
		}

		if err := workflow.UnpauseActivity(
			shardContext, mutableState, ai,
			frontendRequest.GetResetAttempts(),
			frontendRequest.GetResetHeartbeat(),
			frontendRequest.GetJitter().AsDuration()); err != nil {
			return nil, err
		}

	}

	return &historyservice.UnpauseActivityResponse{}, nil
}
