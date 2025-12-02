package activity

import (
	"context"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/types/known/durationpb"
)

type FrontendHandler interface {
	StartActivityExecution(ctx context.Context, req *workflowservice.StartActivityExecutionRequest) (*workflowservice.StartActivityExecutionResponse, error)
	CountActivityExecutions(context.Context, *workflowservice.CountActivityExecutionsRequest) (*workflowservice.CountActivityExecutionsResponse, error)
	DeleteActivityExecution(context.Context, *workflowservice.DeleteActivityExecutionRequest) (*workflowservice.DeleteActivityExecutionResponse, error)
	PollActivityExecution(context.Context, *workflowservice.PollActivityExecutionRequest) (*workflowservice.PollActivityExecutionResponse, error)
	ListActivityExecutions(context.Context, *workflowservice.ListActivityExecutionsRequest) (*workflowservice.ListActivityExecutionsResponse, error)
	RequestCancelActivityExecution(context.Context, *workflowservice.RequestCancelActivityExecutionRequest) (*workflowservice.RequestCancelActivityExecutionResponse, error)
	TerminateActivityExecution(context.Context, *workflowservice.TerminateActivityExecutionRequest) (*workflowservice.TerminateActivityExecutionResponse, error)
}

type frontendHandler struct {
	FrontendHandler
	client            activitypb.ActivityServiceClient
	dc                *dynamicconfig.Collection
	logger            log.Logger
	metricsHandler    metrics.Handler
	namespaceRegistry namespace.Registry
	saMapperProvider  searchattribute.MapperProvider
	saValidator       *searchattribute.Validator
}

// NewFrontendHandler creates a new FrontendHandler instance for processing activity frontend requests.
func NewFrontendHandler(
	client activitypb.ActivityServiceClient,
	dc *dynamicconfig.Collection,
	logger log.Logger,
	metricsHandler metrics.Handler,
	namespaceRegistry namespace.Registry,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) FrontendHandler {
	return &frontendHandler{
		client:            client,
		dc:                dc,
		logger:            logger,
		metricsHandler:    metricsHandler,
		namespaceRegistry: namespaceRegistry,
		saMapperProvider:  saMapperProvider,
		saValidator:       saValidator,
	}
}

// StartActivityExecution initiates a standalone activity execution in the specified namespace.
// It validates the request, resolves the namespace ID, applies default configurations,
// and forwards the request to the activity service handler.
//
// The method performs the following steps:
// 1. Resolves the namespace name to its internal ID
// 2. Validates and populates request fields (timeouts, retry policies, search attributes). The request is cloned
// before mutation to preserve the original for retries.
// 3. Sends the request to the history activity service.
func (h *frontendHandler) StartActivityExecution(ctx context.Context, req *workflowservice.StartActivityExecutionRequest) (*workflowservice.StartActivityExecutionResponse, error) {
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	modifiedReq, err := h.validateAndPopulateStartRequest(req, namespaceID)
	if err != nil {
		return nil, err
	}

	resp, err := h.client.StartActivityExecution(ctx, &activitypb.StartActivityExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: modifiedReq,
	})

	return resp.GetFrontendResponse(), err
}

// PollActivityExecution handles PollActivityExecutionRequest. This method supports querying current
// activity state, optionally as a long-poll that waits for certain state changes. It is used by
// clients to poll for activity state and/or result.
func (h *frontendHandler) PollActivityExecution(
	ctx context.Context,
	req *workflowservice.PollActivityExecutionRequest,
) (*workflowservice.PollActivityExecutionResponse, error) {
	err := ValidatePollActivityExecutionRequest(
		req,
		dynamicconfig.MaxIDLengthLimit.Get(h.dc)(),
	)
	if err != nil {
		return nil, err
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}
	resp, err := h.client.PollActivityExecution(ctx, &activitypb.PollActivityExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

// TerminateActivityExecution terminates a standalone activity execution
func (h *frontendHandler) TerminateActivityExecution(
	ctx context.Context,
	req *workflowservice.TerminateActivityExecutionRequest,
) (*workflowservice.TerminateActivityExecutionResponse, error) {
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	_, err = h.client.TerminateActivityExecution(ctx, &activitypb.TerminateActivityExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.TerminateActivityExecutionResponse{}, nil
}

func (h *frontendHandler) RequestCancelActivityExecution(
	ctx context.Context,
	req *workflowservice.RequestCancelActivityExecutionRequest,
) (*workflowservice.RequestCancelActivityExecutionResponse, error) {
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	// Since validation potentially mutates the request, we clone it first so that any retries use the original request.
	req = common.CloneProto(req)

	maxIDLen := dynamicconfig.MaxIDLengthLimit.Get(h.dc)()

	if len(req.GetRequestId()) > maxIDLen {
		return nil, serviceerror.NewInvalidArgument("RequestID length exceeds limit.")
	}

	if req.GetRequestId() == "" {
		req.RequestId = uuid.NewString()
	}

	if len(req.GetReason()) > maxIDLen {
		return nil, serviceerror.NewInvalidArgument("Reason length exceeds limit.")
	}

	_, err = h.client.RequestCancelActivityExecution(ctx, &activitypb.RequestCancelActivityExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.RequestCancelActivityExecutionResponse{}, nil
}

func (h *frontendHandler) validateAndPopulateStartRequest(
	req *workflowservice.StartActivityExecutionRequest,
	namespaceID namespace.ID,
) (*workflowservice.StartActivityExecutionRequest, error) {
	// Since validation includes mutation of the request, we clone it first so that any retries use the original request.
	req = common.CloneProto(req)
	activityType := req.ActivityType.GetName()

	if req.Options.RetryPolicy == nil {
		req.Options.RetryPolicy = &commonpb.RetryPolicy{}
	}

	err := ValidateAndNormalizeActivityAttributes(
		req.ActivityId,
		activityType,
		dynamicconfig.DefaultActivityRetryPolicy.Get(h.dc),
		dynamicconfig.MaxIDLengthLimit.Get(h.dc)(),
		namespaceID,
		req.Options,
		req.Priority,
		durationpb.New(0),
	)
	if err != nil {
		return nil, err
	}

	err = validateAndNormalizeStartActivityExecutionRequest(
		req,
		dynamicconfig.BlobSizeLimitError.Get(h.dc),
		dynamicconfig.BlobSizeLimitWarn.Get(h.dc),
		h.logger,
		dynamicconfig.MaxIDLengthLimit.Get(h.dc)(),
		h.saValidator)
	if err != nil {
		return nil, err
	}

	return req, nil
}
