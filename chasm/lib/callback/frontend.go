package callback

import (
	"context"
	"fmt"

	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/proto"
)

var ErrStandaloneCallbackDisabled = serviceerror.NewUnimplemented("standalone callback executions are not enabled")

// Returns a serviceerror.InvalidArgument error for a missing required field.
func missingRequiredFieldError(fieldName string) error {
	msg := fmt.Sprintf("%s is not set on request.", fieldName)
	return serviceerror.NewInvalidArgument(msg)
}

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

	client            callbackspb.CallbackExecutionServiceClient
	config            *Config
	callbackValidator Validator
}

// NewFrontendHandler creates a new FrontendHandler instance for standalone callback executions.
func NewFrontendHandler(
	logger log.Logger,
	namespaceRegistry namespace.Registry,
	client callbackspb.CallbackExecutionServiceClient,
	config *Config,
	callbackValidator Validator,
) FrontendHandler {
	return &frontendHandler{
		logger:            logger,
		namespaceRegistry: namespaceRegistry,
		client:            client,
		config:            config,
		callbackValidator: callbackValidator,
	}
}

// Duck type to simplify the code for fetching the target namespace from incomming protos.
type HasNamespace interface{ GetNamespace() string }

// Looks up the Namespace ID from the user-supplied namespace name in the request proto.
func (h *frontendHandler) getTargetNamespace(requestProto HasNamespace) (namespace.ID, error) {
	targetNamespaceName := namespace.Name(requestProto.GetNamespace())
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(targetNamespaceName)
	if err != nil {
		return "", err
	}
	return namespaceID, nil
}

// Checks if standalone callback executions are enabled in the request's target namesapce.
func (h *frontendHandler) checkFeatureEnabled(requestProto HasNamespace) error {
	targetNamespaceName := requestProto.GetNamespace()
	if !h.config.EnableStandaloneExecutions(targetNamespaceName) {
		return ErrStandaloneCallbackDisabled
	}
	return nil
}

func validateStartCallbackExecution(req *workflowservice.StartCallbackExecutionRequest, cbValidator Validator) error {
	// Required fields.
	requiredFields := map[string]string{
		"Namespace":  req.GetNamespace(),
		"Identity":   req.GetIdentity(),
		"RequestId":  req.GetRequestId(),
		"CallbackId": req.GetCallbackId(),
	}
	for k, v := range requiredFields {
		if v == "" {
			return missingRequiredFieldError(k)
		}
	}

	// Validate the callback to be invoked, and its parameters.
	if err := cbValidator.Validate(req.GetNamespace(), []*commonpb.Callback{req.Callback}); err != nil {
		return err
	}

	// ScheduleToCloseTimeout
	if req.GetScheduleToCloseTimeout() == nil || req.GetScheduleToCloseTimeout().AsDuration() <= 0 {
		return serviceerror.NewInvalidArgument("ScheduleToCloseTimeout must be set and positive.")
	}

	// Validate teh input data to deliver to the callback URL, currently only one kind is supported (Completion).
	completion := req.GetCompletion()
	if completion == nil {
		return serviceerror.NewInvalidArgument("Completion is not set on request.")
	}
	if completion.GetSuccess() == nil && completion.GetFailure() == nil {
		return serviceerror.NewInvalidArgument("Completion must have either success or failure set.")
	}
	if completion.GetSuccess() != nil && completion.GetFailure() != nil {
		return serviceerror.NewInvalidArgument("Completion must have exactly one of success or failure set, not both.")
	}
	// Success payloads are empty, but the Failure details may be excessively large.
	if failureInfo := completion.GetFailure(); failureInfo != nil {
		failureProtoSize := proto.Size(failureInfo)
		// TODO(chrsmith): Is there a standard config field to use here?
		if failureProtoSize > 2*1024*1024 {
			return serviceerror.NewInvalidArgument("Completion.Failure message is too large")
		}
	}

	// TODO(chrsmith): How can I validate the req.SearchAttributes (temporal.api.common.v1.SearchAttributes)?

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
	if err := validateStartCallbackExecution(request, h.callbackValidator); err != nil {
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

func validateDescribeCallbackExecution(req *workflowservice.DescribeCallbackExecutionRequest) error {
	// Required fields.
	requiredFields := map[string]string{
		"Namespace":  req.GetNamespace(),
		"CallbackId": req.GetCallbackId(),
	}
	for k, v := range requiredFields {
		if v == "" {
			return missingRequiredFieldError(k)
		}
	}

	return nil
}

// DescribeCallbackExecution returns detailed information about a callback execution
// including its current state, delivery attempt history, and timing information.
func (h *frontendHandler) DescribeCallbackExecution(
	ctx context.Context,
	request *workflowservice.DescribeCallbackExecutionRequest,
) (*workflowservice.DescribeCallbackExecutionResponse, error) {
	// Validate
	if err := h.checkFeatureEnabled(request); err != nil {
		return nil, err
	}
	if err := validateDescribeCallbackExecution(request); err != nil {
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

func validatePollCallbackExecution(req *workflowservice.PollCallbackExecutionRequest) error {
	// Required fields.
	requiredFields := map[string]string{
		"Namespace":  req.GetNamespace(),
		"CallbackId": req.GetCallbackId(),
	}
	for k, v := range requiredFields {
		if v == "" {
			return missingRequiredFieldError(k)
		}
	}

	return nil
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
	if err := validatePollCallbackExecution(request); err != nil {
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

func validateTerminateCallbackExecution(req *workflowservice.TerminateCallbackExecutionRequest) error {
	// Required fields.
	requiredFields := map[string]string{
		"Namespace":  req.GetNamespace(),
		"CallbackId": req.GetCallbackId(),

		// NOTE: We don't require the Identity or Reason fields to be set,
		// and just set reasonable defaults.
	}
	for k, v := range requiredFields {
		if v == "" {
			return missingRequiredFieldError(k)
		}
	}

	return nil
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
	if err := validateTerminateCallbackExecution(request); err != nil {
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

func validateDeleteCallbackExecution(req *workflowservice.DeleteCallbackExecutionRequest) error {
	if req.GetNamespace() == "" {
		return missingRequiredFieldError("Namespace")
	}
	if req.GetCallbackId() == "" {
		return missingRequiredFieldError("CallbackID")
	}
	return nil
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
	if err := validateDeleteCallbackExecution(request); err != nil {
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

func validateListCallbackExecutions(req *workflowservice.ListCallbackExecutionsRequest) error {
	if req.GetNamespace() == "" {
		return missingRequiredFieldError("Namespace")
	}
	return nil
}

// ListCallbackExecutions queries the visibility store for callback executions matching
// the provided filter. Supports the same query syntax as workflow list filters.
//
// TODO(chrsmith): Unresolved comments.
// > Roey: Follow chasm/lib/activity/frontend.go ListActivityExecutions please.
// > Quinn:  I did when I wrote this, sorry I don't understand the feedback here.
// > Roey: Read the fields either from search attributes or VisibilityExecutionInfo, same as ListActivityExecutions.
func (h *frontendHandler) ListCallbackExecutions(
	ctx context.Context,
	request *workflowservice.ListCallbackExecutionsRequest,
) (*workflowservice.ListCallbackExecutionsResponse, error) {
	// Validate
	if err := h.checkFeatureEnabled(request); err != nil {
		return nil, err
	}
	if err := validateListCallbackExecutions(request); err != nil {
		return nil, err
	}

	// Lookup the namespace by its name, to confirm it actually exists.
	// TODO(chrsmith): Does this leak info? e.g. is the existence of a namespace name considered sensitive?
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

	// Build the response object.
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

func validateCountCallbackExecutions(req *workflowservice.CountCallbackExecutionsRequest) error {
	if req.GetNamespace() == "" {
		return missingRequiredFieldError("Namespace")
	}
	return nil
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
	if err := validateCountCallbackExecutions(request); err != nil {
		return nil, err
	}

	// Lookup the namespace by its name, to confirm it actually exists.
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
