package nexusoperation

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// FrontendHandler provides the frontend-facing API for standalone Nexus operations.
type FrontendHandler interface {
	StartNexusOperationExecution(context.Context, *workflowservice.StartNexusOperationExecutionRequest) (*workflowservice.StartNexusOperationExecutionResponse, error)
	DescribeNexusOperationExecution(context.Context, *workflowservice.DescribeNexusOperationExecutionRequest) (*workflowservice.DescribeNexusOperationExecutionResponse, error)
	PollNexusOperationExecution(context.Context, *workflowservice.PollNexusOperationExecutionRequest) (*workflowservice.PollNexusOperationExecutionResponse, error)
	ListNexusOperationExecutions(context.Context, *workflowservice.ListNexusOperationExecutionsRequest) (*workflowservice.ListNexusOperationExecutionsResponse, error)
	CountNexusOperationExecutions(context.Context, *workflowservice.CountNexusOperationExecutionsRequest) (*workflowservice.CountNexusOperationExecutionsResponse, error)
	RequestCancelNexusOperationExecution(context.Context, *workflowservice.RequestCancelNexusOperationExecutionRequest) (*workflowservice.RequestCancelNexusOperationExecutionResponse, error)
	TerminateNexusOperationExecution(context.Context, *workflowservice.TerminateNexusOperationExecutionRequest) (*workflowservice.TerminateNexusOperationExecutionResponse, error)
	DeleteNexusOperationExecution(context.Context, *workflowservice.DeleteNexusOperationExecutionRequest) (*workflowservice.DeleteNexusOperationExecutionResponse, error)
}

var ErrStandaloneNexusOperationDisabled = serviceerror.NewUnimplemented("Standalone Nexus operation is disabled")

type frontendHandler struct {
	client            nexusoperationpb.NexusOperationServiceClient
	config            *Config
	logger            log.Logger
	namespaceRegistry namespace.Registry
	endpointRegistry  commonnexus.EndpointRegistry
	saMapperProvider  searchattribute.MapperProvider
	saValidator       *searchattribute.Validator
}

func NewFrontendHandler(
	client nexusoperationpb.NexusOperationServiceClient,
	config *Config,
	logger log.Logger,
	namespaceRegistry namespace.Registry,
	endpointRegistry commonnexus.EndpointRegistry,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) FrontendHandler {
	return &frontendHandler{
		client:            client,
		config:            config,
		logger:            logger,
		namespaceRegistry: namespaceRegistry,
		endpointRegistry:  endpointRegistry,
		saMapperProvider:  saMapperProvider,
		saValidator:       saValidator,
	}
}

func (h *frontendHandler) StartNexusOperationExecution(
	ctx context.Context,
	req *workflowservice.StartNexusOperationExecutionRequest,
) (*workflowservice.StartNexusOperationExecutionResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if err := validateAndNormalizeStartRequest(req, h.config, h.logger, h.saMapperProvider, h.saValidator); err != nil {
		return nil, err
	}

	// Verify the endpoint exists before creating the operation.
	endpointEntry, err := h.endpointRegistry.GetByName(ctx, namespaceID, req.GetEndpoint())
	if err != nil {
		return nil, err
	}

	resp, err := h.client.StartNexusOperation(ctx, &nexusoperationpb.StartNexusOperationRequest{
		EndpointId:      endpointEntry.GetId(),
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

func (h *frontendHandler) DescribeNexusOperationExecution(
	ctx context.Context,
	req *workflowservice.DescribeNexusOperationExecutionRequest,
) (*workflowservice.DescribeNexusOperationExecutionResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if err := validateAndNormalizeDescribeRequest(req, namespaceID.String(), h.config); err != nil {
		return nil, err
	}

	resp, err := h.client.DescribeNexusOperation(ctx, &nexusoperationpb.DescribeNexusOperationRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

// PollNexusOperationExecution long-polls for a Nexus operation to reach a specific stage.
func (h *frontendHandler) PollNexusOperationExecution(
	ctx context.Context,
	req *workflowservice.PollNexusOperationExecutionRequest,
) (*workflowservice.PollNexusOperationExecutionResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}

	if err := validateAndNormalizePollRequest(req, h.config); err != nil {
		return nil, err
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	resp, err := h.client.PollNexusOperation(ctx, &nexusoperationpb.PollNexusOperationRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

func (h *frontendHandler) ListNexusOperationExecutions(
	ctx context.Context,
	req *workflowservice.ListNexusOperationExecutionsRequest,
) (*workflowservice.ListNexusOperationExecutionsResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}

	pageSize := req.GetPageSize()
	maxPageSize := int32(h.config.VisibilityMaxPageSize(req.GetNamespace()))
	if pageSize <= 0 || pageSize > maxPageSize {
		pageSize = maxPageSize
	}

	resp, err := chasm.ListExecutions[*Operation, *emptypb.Empty](ctx, &chasm.ListExecutionsRequest{
		NamespaceName: req.GetNamespace(),
		PageSize:      int(pageSize),
		NextPageToken: req.GetNextPageToken(),
		Query:         req.GetQuery(),
	})
	if err != nil {
		return nil, err
	}

	operations := make([]*nexuspb.NexusOperationExecutionListInfo, 0, len(resp.Executions))
	for _, exec := range resp.Executions {
		endpoint, _ := chasm.SearchAttributeValue(exec.ChasmSearchAttributes, EndpointSearchAttribute)
		service, _ := chasm.SearchAttributeValue(exec.ChasmSearchAttributes, ServiceSearchAttribute)
		operation, _ := chasm.SearchAttributeValue(exec.ChasmSearchAttributes, OperationSearchAttribute)
		statusStr, _ := chasm.SearchAttributeValue(exec.ChasmSearchAttributes, StatusSearchAttribute)
		status, _ := enumspb.NexusOperationExecutionStatusFromString(statusStr)

		var closeTime *timestamppb.Timestamp
		var executionDuration *durationpb.Duration
		if !exec.CloseTime.IsZero() {
			closeTime = timestamppb.New(exec.CloseTime)
			if !exec.StartTime.IsZero() {
				executionDuration = durationpb.New(exec.CloseTime.Sub(exec.StartTime))
			}
		}

		operations = append(operations, &nexuspb.NexusOperationExecutionListInfo{
			OperationId:          exec.BusinessID,
			RunId:                exec.RunID,
			Endpoint:             endpoint,
			Service:              service,
			Operation:            operation,
			Status:               status,
			ScheduleTime:         timestamppb.New(exec.StartTime),
			CloseTime:            closeTime,
			ExecutionDuration:    executionDuration,
			StateTransitionCount: exec.StateTransitionCount,
			SearchAttributes:     &commonpb.SearchAttributes{IndexedFields: exec.CustomSearchAttributes},
		})
	}

	return &workflowservice.ListNexusOperationExecutionsResponse{
		Operations:    operations,
		NextPageToken: resp.NextPageToken,
	}, nil
}

func (h *frontendHandler) CountNexusOperationExecutions(
	ctx context.Context,
	req *workflowservice.CountNexusOperationExecutionsRequest,
) (*workflowservice.CountNexusOperationExecutionsResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}

	resp, err := chasm.CountExecutions[*Operation](ctx, &chasm.CountExecutionsRequest{
		NamespaceName: req.GetNamespace(),
		Query:         req.GetQuery(),
	})
	if err != nil {
		return nil, err
	}

	groups := make([]*workflowservice.CountNexusOperationExecutionsResponse_AggregationGroup, 0, len(resp.Groups))
	for _, g := range resp.Groups {
		groups = append(groups, &workflowservice.CountNexusOperationExecutionsResponse_AggregationGroup{
			GroupValues: g.Values,
			Count:       g.Count,
		})
	}

	return &workflowservice.CountNexusOperationExecutionsResponse{
		Count:  resp.Count,
		Groups: groups,
	}, nil
}

func (h *frontendHandler) RequestCancelNexusOperationExecution(
	ctx context.Context,
	req *workflowservice.RequestCancelNexusOperationExecutionRequest,
) (*workflowservice.RequestCancelNexusOperationExecutionResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if err := validateAndNormalizeCancelRequest(req, h.config); err != nil {
		return nil, err
	}

	_, err = h.client.RequestCancelNexusOperation(ctx, &nexusoperationpb.RequestCancelNexusOperationRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.RequestCancelNexusOperationExecutionResponse{}, nil
}

func (h *frontendHandler) TerminateNexusOperationExecution(
	ctx context.Context,
	req *workflowservice.TerminateNexusOperationExecutionRequest,
) (*workflowservice.TerminateNexusOperationExecutionResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if err := validateAndNormalizeTerminateRequest(req, h.config); err != nil {
		return nil, err
	}

	_, err = h.client.TerminateNexusOperation(ctx, &nexusoperationpb.TerminateNexusOperationRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.TerminateNexusOperationExecutionResponse{}, nil
}

func (h *frontendHandler) DeleteNexusOperationExecution(
	ctx context.Context,
	req *workflowservice.DeleteNexusOperationExecutionRequest,
) (*workflowservice.DeleteNexusOperationExecutionResponse, error) {
	if !h.isStandaloneNexusOperationEnabled(req.GetNamespace()) {
		return nil, ErrStandaloneNexusOperationDisabled
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	if err := validateAndNormalizeDeleteRequest(req, h.config); err != nil {
		return nil, err
	}

	_, err = h.client.DeleteNexusOperation(ctx, &nexusoperationpb.DeleteNexusOperationRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	if err != nil {
		return nil, err
	}

	return &workflowservice.DeleteNexusOperationExecutionResponse{}, nil
}

// isStandaloneNexusOperationEnabled checks if standalone Nexus operations are enabled for the given namespace.
func (h *frontendHandler) isStandaloneNexusOperationEnabled(namespaceName string) bool {
	return h.config.EnableChasm(namespaceName) && h.config.Enabled(namespaceName)
}
