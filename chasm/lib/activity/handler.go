package activity

import (
	"context"

	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

type handler struct {
	activitypb.UnimplementedActivityServiceServer
}

func newHandler() *handler {
	return &handler{}
}

func (h *handler) StartActivityExecution(ctx context.Context, req *activitypb.StartActivityExecutionRequest) (*activitypb.StartActivityExecutionResponse, error) {
	panic("implement me")
}

func (h *handler) DescribeActivityExecution(ctx context.Context, req *activitypb.DescribeActivityExecutionRequest) (*activitypb.DescribeActivityExecutionResponse, error) {
	panic("implement me")
}
