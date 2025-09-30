package activity

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

type Handler struct {
	activitypb.UnimplementedActivityServiceServer
}

func (h Handler) StartActivityExecution(
	_ context.Context,
	_ *activitypb.StartActivityExecutionRequest,
) (resp *activitypb.StartActivityExecutionResponse, retError error) {
	// TODO implement me
	return &activitypb.StartActivityExecutionResponse{
		FrontendResponse: &workflowservice.StartActivityExecutionResponse{
			Started: true,
		},
	}, nil
}
