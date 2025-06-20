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
			request.NamespaceId,
			request.GetFrontendRequest().GetExecution().GetWorkflowId(),
			request.GetFrontendRequest().GetExecution().GetRunId(),
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			frontendRequest := request.GetFrontendRequest()
			var activityIDs []string
			switch a := frontendRequest.GetActivity().(type) {
			case *workflowservice.PauseActivityRequest_Id:
				activityIDs = append(activityIDs, a.Id)
			case *workflowservice.PauseActivityRequest_Type:
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

			pauseInfo := &persistencespb.ActivityInfo_PauseInfo{
				PauseTime: timestamppb.New(shardContext.GetTimeSource().Now()),
				PausedBy: &persistencespb.ActivityInfo_PauseInfo_Manual_{
					Manual: &persistencespb.ActivityInfo_PauseInfo_Manual{
						Identity: frontendRequest.GetIdentity(),
						Reason:   frontendRequest.GetReason(),
					},
				},
			}

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
