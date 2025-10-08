package activity

import (
	"context"
	"fmt"

	"go.temporal.io/api/activity/v1"
	"go.temporal.io/api/enums/v1"
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
	frontendRequest := req.GetFrontendRequest()

	activityInfo := &activity.ActivityExecutionInfo{
		ActivityType:    frontendRequest.GetActivityType(),
		ActivityOptions: frontendRequest.Options,
		Status:          enums.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED,
		// TODO set other fields
	}

	o, entityKey, _, err := chasm.NewEntity(
		ctx,
		chasm.EntityKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  frontendRequest.ActivityId,
		},
		func(mutableContext chasm.MutableContext, _ any) (*Activity, any, error) {
			newActivity := NewActivity(frontendRequest.GetNamespace(), req.NamespaceId, frontendRequest.ActivityId, activityInfo)
			return newActivity, nil, nil
		},
		nil)

	if err != nil {
		return nil, err
	}

	fmt.Println(o)
	fmt.Println(entityKey)

	//r, err := h.DescribeActivityExecution(ctx, &activitypb.DescribeActivityExecutionRequest{
	//	NamespaceId: req.GetNamespaceId(),
	//	FrontendRequest: &workflowservice.DescribeActivityExecutionRequest{
	//		Namespace: frontendRequest.GetNamespace(),
	//		Execution: &activity.ActivityExecution{
	//			ActivityId: entityKey.BusinessID,
	//			RunId:      entityKey.EntityID,
	//		},
	//	},
	//})
	//
	//fmt.Println(r)

	return &activitypb.StartActivityExecutionResponse{
		FrontendResponse: &workflowservice.StartActivityExecutionResponse{
			RunId: entityKey.EntityID,
		},
	}, nil
}

func (h *handler) DescribeActivityExecution(ctx context.Context, req *activitypb.DescribeActivityExecutionRequest) (*activitypb.DescribeActivityExecutionResponse, error) {

	act, err := GetActivity(ctx, req)
	if err != nil {
		return nil, err
	}

	fmt.Println(act)

	return nil, nil
}
