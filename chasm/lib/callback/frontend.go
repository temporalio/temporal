package callback

import (
	"context"

	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrStandaloneCallbacksDisabled = serviceerror.NewUnimplemented("standalone callback executions are not enabled")

// FrontendHandler defines the frontend interface for standalone callback execution RPCs,
// in which the Frontend microservice receives requests from the Temporal SDK and proxies
// them to the implementation of the CHASM component running in the History service.
type FrontendHandler interface {
	StartCallbackExecution(context.Context, *workflowservice.StartCallbackExecutionRequest) (*workflowservice.StartCallbackExecutionResponse, error)
	DescribeCallbackExecution(context.Context, *workflowservice.DescribeCallbackExecutionRequest) (*workflowservice.DescribeCallbackExecutionResponse, error)
	PollCallbackExecution(context.Context, *workflowservice.PollCallbackExecutionRequest) (*workflowservice.PollCallbackExecutionResponse, error)
	ListCallbackExecutions(context.Context, *workflowservice.ListCallbackExecutionsRequest) (*workflowservice.ListCallbackExecutionsResponse, error)
	CountCallbackExecutions(context.Context, *workflowservice.CountCallbackExecutionsRequest) (*workflowservice.CountCallbackExecutionsResponse, error)
	TerminateCallbackExecution(context.Context, *workflowservice.TerminateCallbackExecutionRequest) (*workflowservice.TerminateCallbackExecutionResponse, error)
	DeleteCallbackExecution(context.Context, *workflowservice.DeleteCallbackExecutionRequest) (*workflowservice.DeleteCallbackExecutionResponse, error)
}

type frontendHandler struct {
	logger            log.Logger
	namespaceRegistry namespace.Registry

	client       callbackspb.CallbackServiceClient
	config       *Config
	reqValidator *frontendRequestValidator
}

func NewFrontendHandler(
	logger log.Logger,
	namespaceRegistry namespace.Registry,
	client callbackspb.CallbackServiceClient,
	config *Config,
	callbackValidator Validator,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) FrontendHandler {
	return &frontendHandler{
		logger:            logger,
		namespaceRegistry: namespaceRegistry,
		client:            client,
		config:            config,
		reqValidator: &frontendRequestValidator{
			config:           config,
			cbValidator:      callbackValidator,
			logger:           logger,
			saMapperProvider: saMapperProvider,
			saValidator:      saValidator,
		},
	}
}

type Namespacer interface{ GetNamespace() string }

// Looks up the namespace ID from the user-supplied namespace name in the request proto.
func (h *frontendHandler) getTargetNamespace(requestProto Namespacer) (namespace.ID, error) {
	targetNamespaceName := namespace.Name(requestProto.GetNamespace())
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(targetNamespaceName)
	if err != nil {
		return "", err
	}
	return namespaceID, nil
}

// Checks if standalone callback executions are supported in the target namespace.
func (h *frontendHandler) checkFeatureEnabled(requestProto Namespacer) error {
	// Confirm CHASM is enabled.
	targetNamespaceName := requestProto.GetNamespace()
	if !h.config.CHASMEnabled(targetNamespaceName) || !h.config.CHASMCallbacksEnabled(targetNamespaceName) {
		return ErrStandaloneCallbacksDisabled
	}
	if !h.config.EnableStandaloneExecutions(targetNamespaceName) {
		return ErrStandaloneCallbacksDisabled
	}
	return nil
}

// StartCallbackExecution creates a new standalone callback execution that will deliver the
// provided Nexus completion payload to the target callback URL with retries.
func (h *frontendHandler) StartCallbackExecution(
	ctx context.Context,
	request *workflowservice.StartCallbackExecutionRequest,
) (*workflowservice.StartCallbackExecutionResponse, error) {
	// Validate
	if err := h.checkFeatureEnabled(request); err != nil {
		return nil, err
	}
	if err := h.reqValidator.ValidateStartCallbackExecution(request); err != nil {
		return nil, err
	}

	// Execute
	namespaceID, err := h.getTargetNamespace(request)
	if err != nil {
		return nil, err
	}
	resp, err := h.client.StartCallbackExecution(ctx, &callbackspb.StartCallbackExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: request,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetFrontendResponse(), nil
}

// DescribeCallbackExecution returns detailed information about a callback execution
// including its current state, delivery attempt history, and timing information.
// Optionally takes a long-poll token and waits for any change.
func (h *frontendHandler) DescribeCallbackExecution(
	ctx context.Context,
	request *workflowservice.DescribeCallbackExecutionRequest,
) (*workflowservice.DescribeCallbackExecutionResponse, error) {
	// Validate
	if err := h.checkFeatureEnabled(request); err != nil {
		return nil, err
	}
	if err := h.reqValidator.ValidateDescribeCallbackExecution(request); err != nil {
		return nil, err
	}

	// Execute
	namespaceID, err := h.getTargetNamespace(request)
	if err != nil {
		return nil, err
	}
	resp, err := h.client.DescribeCallbackExecution(ctx, &callbackspb.DescribeCallbackExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: request,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetFrontendResponse(), nil
}

// PollCallbackExecution blocks until the callback execution completes and returns its outcome.
func (h *frontendHandler) PollCallbackExecution(
	ctx context.Context,
	request *workflowservice.PollCallbackExecutionRequest,
) (*workflowservice.PollCallbackExecutionResponse, error) {
	// Validate
	if err := h.checkFeatureEnabled(request); err != nil {
		return nil, err
	}
	if err := h.reqValidator.ValidatePollCallbackExecution(request); err != nil {
		return nil, err
	}

	// Execute
	namespaceID, err := h.getTargetNamespace(request)
	if err != nil {
		return nil, err
	}
	resp, err := h.client.PollCallbackExecution(ctx, &callbackspb.PollCallbackExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: request,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetFrontendResponse(), nil
}

// TerminateCallbackExecution forcefully stops a running callback execution.
// No-op if already in a terminal state.
func (h *frontendHandler) TerminateCallbackExecution(
	ctx context.Context,
	request *workflowservice.TerminateCallbackExecutionRequest,
) (*workflowservice.TerminateCallbackExecutionResponse, error) {
	// Validate
	if err := h.checkFeatureEnabled(request); err != nil {
		return nil, err
	}
	if err := h.reqValidator.ValidateTerminateCallbackExecution(request); err != nil {
		return nil, err
	}

	// Execute
	namespaceID, err := h.getTargetNamespace(request)
	if err != nil {
		return nil, err
	}
	resp, err := h.client.TerminateCallbackExecution(ctx, &callbackspb.TerminateCallbackExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: request,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetFrontendResponse(), nil
}

// DeleteCallbackExecution terminates the callback if still running and marks it for cleanup.
func (h *frontendHandler) DeleteCallbackExecution(
	ctx context.Context,
	request *workflowservice.DeleteCallbackExecutionRequest,
) (*workflowservice.DeleteCallbackExecutionResponse, error) {
	// Validate
	if err := h.checkFeatureEnabled(request); err != nil {
		return nil, err
	}
	if err := h.reqValidator.ValidateDeleteCallbackExecution(request); err != nil {
		return nil, err
	}

	// Execute
	namespaceID, err := h.getTargetNamespace(request)
	if err != nil {
		return nil, err
	}
	resp, err := h.client.DeleteCallbackExecution(ctx, &callbackspb.DeleteCallbackExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: request,
	})
	if err != nil {
		return nil, err
	}
	return resp.GetFrontendResponse(), nil
}

// ListCallbackExecutions queries the visibility store for callback executions matching
// the provided filter. Supports the same query syntax as workflow list filters.
func (h *frontendHandler) ListCallbackExecutions(
	ctx context.Context,
	request *workflowservice.ListCallbackExecutionsRequest,
) (*workflowservice.ListCallbackExecutionsResponse, error) {
	// Validate
	if err := h.checkFeatureEnabled(request); err != nil {
		return nil, err
	}
	if err := h.reqValidator.ValidateListCallbackExecutions(request); err != nil {
		return nil, err
	}

	// Lookup the namespace by its name, to confirm it actually exists.
	namespaceName := namespace.Name(request.GetNamespace())
	if _, err := h.namespaceRegistry.GetNamespaceID(namespaceName); err != nil {
		return nil, err
	}

	resp, err := chasm.ListExecutions[*Callback, *callbackpb.CallbackExecutionListInfo](
		ctx,
		&chasm.ListExecutionsRequest{
			NamespaceName: namespaceName.String(),
			PageSize:      int(request.GetPageSize()),
			NextPageToken: request.GetNextPageToken(),
			Query:         request.GetQuery(),
		},
	)
	if err != nil {
		return nil, err
	}

	// Build the response object.
	executions := make([]*callbackpb.CallbackExecutionListInfo, 0, len(resp.Executions))
	for _, exec := range resp.Executions {

		statusStr, _ := chasm.SearchAttributeValue(exec.ChasmSearchAttributes, executionStatusSearchAttribute)
		status, _ := enumspb.CallbackExecutionStatusFromString(statusStr)

		info := callbackpb.CallbackExecutionListInfo{
			CallbackId:           exec.BusinessID,
			RunId:                exec.RunID,
			Status:               status,
			CreateTime:           timestamppb.New(exec.StartTime),
			CloseTime:            timestamppb.New(exec.CloseTime),
			SearchAttributes:     &commonpb.SearchAttributes{IndexedFields: exec.CustomSearchAttributes},
			StateTransitionCount: exec.StateTransitionCount,
		}
		executions = append(executions, &info)
	}
	return &workflowservice.ListCallbackExecutionsResponse{
		Executions:    executions,
		NextPageToken: resp.NextPageToken,
	}, nil
}

// CountCallbackExecutions returns the number of callback executions matching the query,
// with optional grouping by search attribute values.
func (h *frontendHandler) CountCallbackExecutions(
	ctx context.Context,
	request *workflowservice.CountCallbackExecutionsRequest,
) (*workflowservice.CountCallbackExecutionsResponse, error) {
	// Validate
	if err := h.checkFeatureEnabled(request); err != nil {
		return nil, err
	}
	if err := h.reqValidator.ValidateCountCallbackExecutions(request); err != nil {
		return nil, err
	}

	// Lookup the namespace by its name, to confirm it actually exists.
	namespaceName := namespace.Name(request.GetNamespace())
	if _, err := h.namespaceRegistry.GetNamespaceID(namespaceName); err != nil {
		return nil, err
	}
	resp, err := chasm.CountExecutions[*Callback](
		ctx,
		&chasm.CountExecutionsRequest{
			NamespaceName: namespaceName.String(),
			Query:         request.GetQuery(),
		},
	)
	if err != nil {
		return nil, err
	}

	// Build the response object.
	groups := make([]*workflowservice.CountCallbackExecutionsResponse_AggregationGroup, 0, len(resp.Groups))
	for _, g := range resp.Groups {
		groups = append(groups, &workflowservice.CountCallbackExecutionsResponse_AggregationGroup{
			GroupValues: g.Values,
			Count:       g.Count,
		})
	}
	return &workflowservice.CountCallbackExecutionsResponse{
		Count:  resp.Count,
		Groups: groups,
	}, nil
}
