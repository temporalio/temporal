package activity

import (
	"context"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

type handler struct {
	activitypb.UnimplementedActivityServiceServer
}

func newHandler() *handler {
	return &handler{}
}

func (h *handler) StartActivityExecution(ctx context.Context, req *activitypb.StartActivityExecutionRequest) (*activitypb.StartActivityExecutionResponse, error) {
	request := req.GetFrontendRequest()
	_, key, _, err := chasm.NewEntity(
		ctx,
		chasm.EntityKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  request.GetActivityId(),
		},
		func(mutableContext chasm.MutableContext, _ any) (*Activity, any, error) {
			newActivity := NewActivity(request)
			err := TransitionScheduled.Apply(mutableContext, newActivity, nil)
			if err != nil {
				return nil, nil, err
			}
			mutableContext.AddTask(newActivity, chasm.TaskAttributes{}, &activitypb.ActivityStartExecuteTask{}) // Move to component
			return newActivity, nil, nil
		}, nil)

	if err != nil {
		return nil, err
	}
	return &activitypb.StartActivityExecutionResponse{
		FrontendResponse: &workflowservice.StartActivityExecutionResponse{
			// TODO: Started
			RunId: key.EntityID,
		},
	}, nil
}

func (h *handler) DescribeActivityExecution(ctx context.Context, req *activitypb.DescribeActivityExecutionRequest) (*activitypb.DescribeActivityExecutionResponse, error) {
	request := req.GetFrontendRequest()
	key := chasm.EntityKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  request.GetActivityId(),
		EntityID:    request.GetRunId(),
	}
	act, err := GetActivity(ctx, key)
	if err != nil {
		return nil, err
	}
	return &activitypb.DescribeActivityExecutionResponse{
		FrontendResponse: &workflowservice.DescribeActivityExecutionResponse{
			// TODO: LongPollToken
			Info: act.GetActivityExecutionInfo(key),
		},
	}, nil
}
