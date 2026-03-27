package callback

import (
	"context"
	"fmt"

	callbackpb "go.temporal.io/api/callback/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
)

type (
	// CallbackExecutionFrontendHandler defines the frontend interface for standalone callback execution RPCs.
	CallbackExecutionFrontendHandler interface {
		StartCallbackExecution(context.Context, *workflowservice.StartCallbackExecutionRequest) (*workflowservice.StartCallbackExecutionResponse, error)
		DescribeCallbackExecution(context.Context, *workflowservice.DescribeCallbackExecutionRequest) (*workflowservice.DescribeCallbackExecutionResponse, error)
		PollCallbackExecution(context.Context, *workflowservice.PollCallbackExecutionRequest) (*workflowservice.PollCallbackExecutionResponse, error)
		TerminateCallbackExecution(context.Context, *workflowservice.TerminateCallbackExecutionRequest) (*workflowservice.TerminateCallbackExecutionResponse, error)
		DeleteCallbackExecution(context.Context, *workflowservice.DeleteCallbackExecutionRequest) (*workflowservice.DeleteCallbackExecutionResponse, error)
		ListCallbackExecutions(context.Context, *workflowservice.ListCallbackExecutionsRequest) (*workflowservice.ListCallbackExecutionsResponse, error)
		CountCallbackExecutions(context.Context, *workflowservice.CountCallbackExecutionsRequest) (*workflowservice.CountCallbackExecutionsResponse, error)
		IsStandaloneCallbackEnabled(namespaceName string) bool
	}

	callbackExecutionFrontendHandler struct {
		client            callbackspb.CallbackExecutionServiceClient
		config            *Config
		logger            log.Logger
		namespaceRegistry namespace.Registry
	}
)

var ErrStandaloneCallbackDisabled = serviceerror.NewUnimplemented("standalone callback executions are not enabled")

// NewCallbackExecutionFrontendHandler creates a new frontend handler for standalone callback executions.
func NewCallbackExecutionFrontendHandler(
	client callbackspb.CallbackExecutionServiceClient,
	config *Config,
	logger log.Logger,
	namespaceRegistry namespace.Registry,
) CallbackExecutionFrontendHandler {
	return &callbackExecutionFrontendHandler{
		client:            client,
		config:            config,
		logger:            logger,
		namespaceRegistry: namespaceRegistry,
	}
}

// validateStartCallbackExecutionRequest checks that required fields are set and structurally valid
// before the request reaches the CHASM handler.
func validateStartCallbackExecutionRequest(request *workflowservice.StartCallbackExecutionRequest, config *Config) error {
	// Validate callback is set and is the Nexus variant.
	cb := request.GetCallback()
	if cb == nil {
		return serviceerror.NewInvalidArgument("Callback is not set on request.")
	}
	nexusCb := cb.GetNexus()
	if nexusCb == nil {
		return serviceerror.NewInvalidArgument("Only Nexus callback variant is supported.")
	}
	if nexusCb.GetUrl() == "" {
		return serviceerror.NewInvalidArgument("Callback URL is not set.")
	}

	// Validate callback URL structure and length.
	if err := validateCallbackURL(nexusCb.GetUrl(), request.GetNamespace(), config); err != nil {
		return err
	}

	// Validate completion is set.
	completion := request.GetCompletion()
	if completion == nil {
		return serviceerror.NewInvalidArgument("Completion is not set on request.")
	}
	if completion.GetSuccess() == nil && completion.GetFailure() == nil {
		return serviceerror.NewInvalidArgument("Completion must have either success or failure set.")
	}
	if completion.GetSuccess() != nil && completion.GetFailure() != nil {
		return serviceerror.NewInvalidArgument("Completion must have exactly one of success or failure set, not both.")
	}

	// Validate schedule_to_close_timeout.
	if request.GetScheduleToCloseTimeout() == nil || request.GetScheduleToCloseTimeout().AsDuration() <= 0 {
		return serviceerror.NewInvalidArgument("ScheduleToCloseTimeout must be set and positive.")
	}

	return nil
}

// validateCallbackURL checks that the callback URL does not exceed the configured maximum length
// and passes the standard address match rules validation (scheme, host, allowlist).
func validateCallbackURL(rawURL string, namespaceName string, config *Config) error {
	maxLen := config.CallbackURLMaxLength(namespaceName)
	if len(rawURL) > maxLen {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("Callback URL length exceeds maximum allowed length of %d.", maxLen))
	}
	return config.AllowedAddresses(namespaceName).Validate(rawURL)
}

func (h *callbackExecutionFrontendHandler) IsStandaloneCallbackEnabled(namespaceName string) bool {
	return h.config.Enabled(namespaceName)
}

// StartCallbackExecution creates a new standalone callback execution that will deliver the
// provided Nexus completion payload to the target callback URL with retries.
func (h *callbackExecutionFrontendHandler) StartCallbackExecution(
	ctx context.Context,
	request *workflowservice.StartCallbackExecutionRequest,
) (*workflowservice.StartCallbackExecutionResponse, error) {
	if !h.config.Enabled(request.GetNamespace()) {
		return nil, ErrStandaloneCallbackDisabled
	}
	if request.GetCallbackId() == "" {
		return nil, serviceerror.NewInvalidArgument("CallbackId is not set on request.")
	}

	if err := validateStartCallbackExecutionRequest(request, h.config); err != nil {
		return nil, err
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
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
func (h *callbackExecutionFrontendHandler) DescribeCallbackExecution(
	ctx context.Context,
	request *workflowservice.DescribeCallbackExecutionRequest,
) (*workflowservice.DescribeCallbackExecutionResponse, error) {
	if !h.config.Enabled(request.GetNamespace()) {
		return nil, ErrStandaloneCallbackDisabled
	}
	if request.GetCallbackId() == "" {
		return nil, serviceerror.NewInvalidArgument("CallbackId is not set on request.")
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
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
func (h *callbackExecutionFrontendHandler) PollCallbackExecution(
	ctx context.Context,
	request *workflowservice.PollCallbackExecutionRequest,
) (*workflowservice.PollCallbackExecutionResponse, error) {
	if !h.config.Enabled(request.GetNamespace()) {
		return nil, ErrStandaloneCallbackDisabled
	}
	if request.GetCallbackId() == "" {
		return nil, serviceerror.NewInvalidArgument("CallbackId is not set on request.")
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
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
func (h *callbackExecutionFrontendHandler) TerminateCallbackExecution(
	ctx context.Context,
	request *workflowservice.TerminateCallbackExecutionRequest,
) (*workflowservice.TerminateCallbackExecutionResponse, error) {
	if !h.config.Enabled(request.GetNamespace()) {
		return nil, ErrStandaloneCallbackDisabled
	}
	if request.GetCallbackId() == "" {
		return nil, serviceerror.NewInvalidArgument("CallbackId is not set on request.")
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
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
func (h *callbackExecutionFrontendHandler) DeleteCallbackExecution(
	ctx context.Context,
	request *workflowservice.DeleteCallbackExecutionRequest,
) (*workflowservice.DeleteCallbackExecutionResponse, error) {
	if !h.config.Enabled(request.GetNamespace()) {
		return nil, ErrStandaloneCallbackDisabled
	}
	if request.GetCallbackId() == "" {
		return nil, serviceerror.NewInvalidArgument("CallbackId is not set on request.")
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
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
func (h *callbackExecutionFrontendHandler) ListCallbackExecutions(
	ctx context.Context,
	request *workflowservice.ListCallbackExecutionsRequest,
) (*workflowservice.ListCallbackExecutionsResponse, error) {
	if !h.config.Enabled(request.GetNamespace()) {
		return nil, ErrStandaloneCallbackDisabled
	}

	namespaceName := namespace.Name(request.GetNamespace())
	if _, err := h.namespaceRegistry.GetNamespaceID(namespaceName); err != nil {
		return nil, err
	}

	resp, err := chasm.ListExecutions[*CallbackExecution, *callbackpb.CallbackExecutionListInfo](
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

	executions := make([]*callbackpb.CallbackExecutionListInfo, len(resp.Executions))
	for i, ex := range resp.Executions {
		listInfo := ex.ChasmMemo
		if listInfo == nil {
			listInfo = &callbackpb.CallbackExecutionListInfo{
				CallbackId: ex.BusinessID,
			}
		}
		executions[i] = listInfo
	}

	return &workflowservice.ListCallbackExecutionsResponse{
		Executions:    executions,
		NextPageToken: resp.NextPageToken,
	}, nil
}

// CountCallbackExecutions returns the number of callback executions matching the query,
// with optional grouping by search attribute values.
func (h *callbackExecutionFrontendHandler) CountCallbackExecutions(
	ctx context.Context,
	request *workflowservice.CountCallbackExecutionsRequest,
) (*workflowservice.CountCallbackExecutionsResponse, error) {
	if !h.config.Enabled(request.GetNamespace()) {
		return nil, ErrStandaloneCallbackDisabled
	}

	namespaceName := namespace.Name(request.GetNamespace())
	if _, err := h.namespaceRegistry.GetNamespaceID(namespaceName); err != nil {
		return nil, err
	}

	resp, err := chasm.CountExecutions[*CallbackExecution](
		ctx,
		&chasm.CountExecutionsRequest{
			NamespaceName: namespaceName.String(),
			Query:         request.GetQuery(),
		},
	)
	if err != nil {
		return nil, err
	}

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
