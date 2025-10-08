package activity

import (
	"context"
	"strings"

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
	//namespaceRegistry namespace.Registry
}

func NewFrontendHandler(client activitypb.ActivityServiceClient) FrontendHandler {
	return &frontendHandler{
		client: client,
		//namespaceRegistry: namespaceRegistry,
	}
}

func (h *frontendHandler) StartActivityExecution(ctx context.Context, req *workflowservice.StartActivityExecutionRequest) (*workflowservice.StartActivityExecutionResponse, error) {
	//namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	//if err != nil {
	//	return nil, err
	//}

	resp, err := h.client.StartActivityExecution(ctx, &activitypb.StartActivityExecutionRequest{
		NamespaceId:     req.Identity, // TODO unhack
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

func (h *frontendHandler) DescribeActivityExecution(ctx context.Context, req *workflowservice.DescribeActivityExecutionRequest) (*workflowservice.DescribeActivityExecutionResponse, error) {
	parts := strings.Split(req.Execution.RunId, "|") // TODO unhack
	req.Execution.RunId = parts[0]

	resp, err := h.client.DescribeActivityExecution(ctx, &activitypb.DescribeActivityExecutionRequest{
		NamespaceId:     parts[1],
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}
