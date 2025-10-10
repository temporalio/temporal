package activity

import (
	"context"
	"fmt"
	"os"

	"go.temporal.io/api/activity/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/clock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type handler struct {
	activitypb.UnimplementedActivityServiceServer
}

func newHandler() *handler {
	return &handler{}
}

func (h *handler) StartActivityExecution(ctx context.Context, req *activitypb.StartActivityExecutionRequest) (*activitypb.StartActivityExecutionResponse, error) {
	frontendRequest := req.GetFrontendRequest()

	o, entityKey, _, err := chasm.NewEntity(
		ctx,
		chasm.EntityKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  frontendRequest.ActivityId,
		},
		func(mutableContext chasm.MutableContext, _ any) (*Activity, any, error) {
			maxAttempts := int32(0)
			if frontendRequest.GetOptions() != nil &&
				frontendRequest.GetOptions().GetRetryPolicy() != nil {
				maxAttempts = frontendRequest.GetOptions().GetRetryPolicy().GetMaximumAttempts()
			}
			activityInfo := &activity.ActivityExecutionInfo{
				ActivityId:      frontendRequest.GetActivityId(),
				RunId:           "", // TODO
				ActivityType:    frontendRequest.GetActivityType(),
				Status:          enums.ACTIVITY_EXECUTION_STATUS_RUNNING,
				RunState:        enums.PENDING_ACTIVITY_STATE_SCHEDULED,
				Attempt:         1,
				MaximumAttempts: maxAttempts,
				// TODO(dan): is this the correct way to compute this timestamp?
				ScheduledTime:   timestamppb.New(clock.NewRealTimeSource().Now()),
				ExpirationTime:  nil, // TODO
				Input:           frontendRequest.GetInput(),
				ActivityOptions: frontendRequest.Options,
			}

			newActivity := NewActivity(frontendRequest.GetNamespace(), req.NamespaceId, frontendRequest.ActivityId, activityInfo)

			mutableContext.AddTask(newActivity, chasm.TaskAttributes{}, &activitypb.ActivityStartExecuteTask{}) // Move to component

			return newActivity, nil, nil
		},
		nil)

	if err != nil {
		return nil, err
	}

	fmt.Println(o)
	fmt.Println(entityKey)

	// Add task to matching

	return &activitypb.StartActivityExecutionResponse{
		FrontendResponse: &workflowservice.StartActivityExecutionResponse{
			RunId: entityKey.EntityID,
		},
	}, nil
}

func (h *handler) DescribeActivityExecution(ctx context.Context, req *activitypb.DescribeActivityExecutionRequest) (*activitypb.DescribeActivityExecutionResponse, error) {
	act, err := GetActivity(ctx, chasm.EntityKey{
		NamespaceID: req.NamespaceId,
		BusinessID:  req.GetFrontendRequest().GetActivityId(),
		EntityID:    req.GetFrontendRequest().GetRunId(),
	})
	if err != nil {
		return nil, err
	}
	return &activitypb.DescribeActivityExecutionResponse{
		FrontendResponse: &workflowservice.DescribeActivityExecutionResponse{
			Info: act.ActivityExecutionInfo,
		},
	}, nil
}

func (h *handler) GetActivityExecutionResult(ctx context.Context, req *activitypb.GetActivityExecutionResultRequest) (*activitypb.GetActivityExecutionResultResponse, error) {
	act, err := GetActivity(ctx, chasm.EntityKey{
		NamespaceID: req.NamespaceId,
		BusinessID:  req.GetFrontendRequest().GetActivityId(),
		EntityID:    req.GetFrontendRequest().GetRunId(),
	})
	if err != nil {
		return nil, err
	}

	if req.GetFrontendRequest().GetWait() {
		fmt.Fprintln(os.Stderr, "TODO: GetActivityExecutionResult long-polling is not implemented")
	}

	response := &workflowservice.GetActivityExecutionResultResponse{
		RunId: act.ActivityExecutionInfo.GetRunId(),
	}

	switch outcome := act.GetOutcome().(type) {
	case *activitypb.ActivityState_Result:
		response.Outcome = &workflowservice.GetActivityExecutionResultResponse_Result{
			Result: outcome.Result,
		}
	case *activitypb.ActivityState_Failure:
		response.Outcome = &workflowservice.GetActivityExecutionResultResponse_Failure{
			Failure: outcome.Failure,
		}
	}

	return &activitypb.GetActivityExecutionResultResponse{
		FrontendResponse: response,
	}, nil
}
