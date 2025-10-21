package activity

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/searchattribute"
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
	saMapperProvider  searchattribute.MapperProvider
	saValidator       *searchattribute.Validator
}

func NewFrontendHandler(
	client activitypb.ActivityServiceClient,
	namespaceRegistry namespace.Registry,
	dc *dynamicconfig.Collection,
	saMapperProvider searchattribute.MapperProvider,
	saProvider searchattribute.Provider,
	visibilityMgr manager.VisibilityManager) FrontendHandler {
	return &frontendHandler{
		client:            client,
		namespaceRegistry: namespaceRegistry,
		dc:                dc,
		saMapperProvider:  saMapperProvider,
		saValidator: searchattribute.NewValidator(
			saProvider,
			saMapperProvider,
			dynamicconfig.SearchAttributesNumberOfKeysLimit.Get(dc),
			dynamicconfig.SearchAttributesSizeOfValueLimit.Get(dc),
			dynamicconfig.SearchAttributesTotalSizeLimit.Get(dc),
			visibilityMgr,
			visibility.AllowListForValidation(
				visibilityMgr.GetStoreNames(),
				dynamicconfig.VisibilityAllowList.Get(dc),
			),
			dynamicconfig.SuppressErrorSetSystemSearchAttribute.Get(dc),
		),
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
		&StandaloneActivityAttributes{
			namespaceName:    req.GetNamespace(),
			requestID:        req.RequestId,
			searchAttributes: req.SearchAttributes,
			saMapperProvider: h.saMapperProvider,
			saValidator:      h.saValidator,
		},
	)

	err = validator.ValidateAndAdjustTimeouts(durationpb.New(0))
	if err != nil {
		return nil, err
	}

	modifiedAttributes, err := validator.ValidateStandaloneActivity()
	if err != nil {
		return nil, err
	}

	if modifiedAttributes.requestID != "" {
		req.RequestId = modifiedAttributes.requestID
	}

	if modifiedAttributes.searchAttributesUnaliased != nil && modifiedAttributes.searchAttributesUnaliased != req.SearchAttributes {
		// Since searchAttributesUnaliased is not idempotent, we need to clone the request so that in case of retries,
		// the field is set to the original value.
		req = common.CloneProto(req)
		req.SearchAttributes = modifiedAttributes.searchAttributesUnaliased
	}

	resp, err := h.client.StartActivityExecution(ctx, &activitypb.StartActivityExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})

	return resp.GetFrontendResponse(), err
}
