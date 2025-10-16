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
			newActivity, err := NewStandaloneActivity(mutableContext, request)
			if err != nil {
				return nil, nil, err
			}

			err = TransitionScheduled.Apply(newActivity, mutableContext, nil)
			if err != nil {
				return nil, nil, err
			}

			return newActivity, nil, nil
		}, nil)

	if err != nil {
		return nil, err
	}

	return &activitypb.StartActivityExecutionResponse{
		FrontendResponse: &workflowservice.StartActivityExecutionResponse{
			Started: true,
			RunId:   key.EntityID,
		},
	}, nil
}
