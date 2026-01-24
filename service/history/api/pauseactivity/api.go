package pauseactivity

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func Invoke(
	ctx context.Context,
	request *historyservice.PauseActivityRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.PauseActivityResponse, retError error) {
	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			request.GetNamespaceId(),
			request.GetFrontendRequest().GetExecution().GetWorkflowId(),
			request.GetFrontendRequest().GetExecution().GetRunId(),
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			frontendRequest := request.GetFrontendRequest()
			var activityIDs []string
			switch frontendRequest.WhichActivity() {
			case workflowservice.PauseActivityRequest_Id_case:
				activityIDs = append(activityIDs, frontendRequest.GetId())
			case workflowservice.PauseActivityRequest_Type_case:
				activityType := frontendRequest.GetType()
				for _, ai := range mutableState.GetPendingActivityInfos() {
					if ai.GetActivityType().GetName() == activityType {
						activityIDs = append(activityIDs, ai.GetActivityId())
					}
				}
			}

			if len(activityIDs) == 0 {
				return nil, consts.ErrActivityNotFound
			}

			pauseInfo := persistencespb.ActivityInfo_PauseInfo_builder{
				PauseTime: timestamppb.New(shardContext.GetTimeSource().Now()),
				Manual: persistencespb.ActivityInfo_PauseInfo_Manual_builder{
					Identity: frontendRequest.GetIdentity(),
					Reason:   frontendRequest.GetReason(),
				}.Build(),
			}.Build()

			for _, activityId := range activityIDs {
				err := workflow.PauseActivity(mutableState, activityId, pauseInfo)
				if err != nil {
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

	return &historyservice.PauseActivityResponse{}, nil
}
