package activity

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

type FrontendHandler interface {
	StartActivityExecution(ctx context.Context, req *workflowservice.StartActivityExecutionRequest) (*workflowservice.StartActivityExecutionResponse, error)
	CountActivityExecutions(context.Context, *workflowservice.CountActivityExecutionsRequest) (*workflowservice.CountActivityExecutionsResponse, error)
	DeleteActivityExecution(context.Context, *workflowservice.DeleteActivityExecutionRequest) (*workflowservice.DeleteActivityExecutionResponse, error)
	DescribeActivityExecution(context.Context, *workflowservice.DescribeActivityExecutionRequest) (*workflowservice.DescribeActivityExecutionResponse, error)
	GetActivityResult(context.Context, *workflowservice.GetActivityResultRequest) (*workflowservice.GetActivityResultResponse, error)
	ListActivityExecutions(context.Context, *workflowservice.ListActivityExecutionsRequest) (*workflowservice.ListActivityExecutionsResponse, error)
	RequestCancelActivityExecution(context.Context, *workflowservice.RequestCancelActivityExecutionRequest) (*workflowservice.RequestCancelActivityExecutionResponse, error)
	TerminateActivityExecution(context.Context, *workflowservice.TerminateActivityExecutionRequest) (*workflowservice.TerminateActivityExecutionResponse, error)
}

type frontendHandler struct {
	FrontendHandler
	client activitypb.ActivityServiceClient
}

func NewFrontendHandler(client activitypb.ActivityServiceClient) FrontendHandler {
	return &frontendHandler{
		client: client,
	}
}

func (h *frontendHandler) StartActivityExecution(ctx context.Context, req *workflowservice.StartActivityExecutionRequest) (*workflowservice.StartActivityExecutionResponse, error) {
	resp, err := h.client.StartActivityExecution(ctx, &activitypb.StartActivityExecutionRequest{
		// NamespaceId
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}
