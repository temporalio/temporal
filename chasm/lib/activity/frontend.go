package activity

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/durationpb"
)

type FrontendHandler interface {
	StartActivityExecution(ctx context.Context, req *workflowservice.StartActivityExecutionRequest) (*workflowservice.StartActivityExecutionResponse, error)
	CountActivityExecutions(context.Context, *workflowservice.CountActivityExecutionsRequest) (*workflowservice.CountActivityExecutionsResponse, error)
	DeleteActivityExecution(context.Context, *workflowservice.DeleteActivityExecutionRequest) (*workflowservice.DeleteActivityExecutionResponse, error)
	DescribeActivityExecution(context.Context, *workflowservice.DescribeActivityExecutionRequest) (*workflowservice.DescribeActivityExecutionResponse, error)
	GetActivityExecutionResult(context.Context, *workflowservice.GetActivityExecutionResultRequest) (*workflowservice.GetActivityExecutionResultResponse, error)
	ListActivityExecutions(context.Context, *workflowservice.ListActivityExecutionsRequest) (*workflowservice.ListActivityExecutionsResponse, error)
	RequestCancelActivityExecution(context.Context, *workflowservice.RequestCancelActivityExecutionRequest) (*workflowservice.RequestCancelActivityExecutionResponse, error)
	TerminateActivityExecution(context.Context, *workflowservice.TerminateActivityExecutionRequest) (*workflowservice.TerminateActivityExecutionResponse, error)
}

type frontendHandler struct {
	FrontendHandler
	client            activitypb.ActivityServiceClient
	namespaceRegistry namespace.Registry
	dc                *dynamicconfig.Collection
}

func NewFrontendHandler(client activitypb.ActivityServiceClient, namespaceRegistry namespace.Registry, dc *dynamicconfig.Collection) FrontendHandler {
	return &frontendHandler{
		client:            client,
		namespaceRegistry: namespaceRegistry,
		dc:                dc,
	}
}

func (h *frontendHandler) StartActivityExecution(ctx context.Context, req *workflowservice.StartActivityExecutionRequest) (*workflowservice.StartActivityExecutionResponse, error) {
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	activityType := ""
	if req.ActivityType != nil {
		activityType = req.ActivityType.GetName()
	}

	if req.Options.RetryPolicy == nil {
		req.Options.RetryPolicy = &commonpb.RetryPolicy{}
	}

	validator := NewRequestAttributesValidator(
		req.ActivityId,
		activityType,
		dynamicconfig.DefaultActivityRetryPolicy.Get(h.dc),
		dynamicconfig.MaxIDLengthLimit.Get(h.dc)(),
		namespaceID,
		req.Options,
		req.Priority,
	)

	err = validator.ValidateAndAdjustTimeouts(durationpb.New(0))
	if err != nil {
		return nil, err
	}

	resp, err := h.client.StartActivityExecution(ctx, &activitypb.StartActivityExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})

	return resp.GetFrontendResponse(), err
}
