package activity

import (
	"context"

	"github.com/google/uuid"
	apiactivitypb "go.temporal.io/api/activity/v1" //nolint:importas
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const StandaloneActivityDisabledError = "Standalone activity is disabled"

type FrontendHandler interface {
	StartActivityExecution(ctx context.Context, req *workflowservice.StartActivityExecutionRequest) (*workflowservice.StartActivityExecutionResponse, error)
	DescribeActivityExecution(ctx context.Context, req *workflowservice.DescribeActivityExecutionRequest) (*workflowservice.DescribeActivityExecutionResponse, error)
	PollActivityExecution(ctx context.Context, req *workflowservice.PollActivityExecutionRequest) (*workflowservice.PollActivityExecutionResponse, error)
	CountActivityExecutions(context.Context, *workflowservice.CountActivityExecutionsRequest) (*workflowservice.CountActivityExecutionsResponse, error)
	DeleteActivityExecution(context.Context, *workflowservice.DeleteActivityExecutionRequest) (*workflowservice.DeleteActivityExecutionResponse, error)
	ListActivityExecutions(context.Context, *workflowservice.ListActivityExecutionsRequest) (*workflowservice.ListActivityExecutionsResponse, error)
	RequestCancelActivityExecution(context.Context, *workflowservice.RequestCancelActivityExecutionRequest) (*workflowservice.RequestCancelActivityExecutionResponse, error)
	TerminateActivityExecution(context.Context, *workflowservice.TerminateActivityExecutionRequest) (*workflowservice.TerminateActivityExecutionResponse, error)
	IsStandaloneActivityEnabled(namespaceName string) bool
}

type frontendHandler struct {
	FrontendHandler
	client            activitypb.ActivityServiceClient
	config            *Config
	logger            log.Logger
	metricsHandler    metrics.Handler
	namespaceRegistry namespace.Registry
	saMapperProvider  searchattribute.MapperProvider
	saValidator       *searchattribute.Validator
	visibilityManager chasm.VisibilityManager
}

// NewFrontendHandler creates a new FrontendHandler instance for processing activity frontend requests.
func NewFrontendHandler(
	client activitypb.ActivityServiceClient,
	config *Config,
	logger log.Logger,
	metricsHandler metrics.Handler,
	namespaceRegistry namespace.Registry,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
	visibilityManager chasm.VisibilityManager,
) FrontendHandler {
	return &frontendHandler{
		client:            client,
		config:            config,
		logger:            logger,
		metricsHandler:    metricsHandler,
		namespaceRegistry: namespaceRegistry,
		saMapperProvider:  saMapperProvider,
		saValidator:       saValidator,
		visibilityManager: visibilityManager,
	}
}

// IsStandaloneActivityEnabled checks if standalone activities are enabled for the given namespace
func (h *frontendHandler) IsStandaloneActivityEnabled(namespaceName string) bool {
	return h.config.Enabled(namespaceName)
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
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, serviceerror.NewUnavailable(StandaloneActivityDisabledError)
	}

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

// DescribeActivityExecution queries current activity state, optionally as a long-poll that waits
// for any state change.
func (h *frontendHandler) DescribeActivityExecution(
	ctx context.Context,
	req *workflowservice.DescribeActivityExecutionRequest,
) (*workflowservice.DescribeActivityExecutionResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, serviceerror.NewUnavailable(StandaloneActivityDisabledError)
	}

	err := ValidateDescribeActivityExecutionRequest(
		req,
		h.config.MaxIDLengthLimit(),
	)
	if err != nil {
		return nil, err
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	resp, err := h.client.DescribeActivityExecution(ctx, &activitypb.DescribeActivityExecutionRequest{
		NamespaceId:     namespaceID.String(),
		FrontendRequest: req,
	})
	return resp.GetFrontendResponse(), err
}

// PollActivityExecution long-polls for activity outcome.
func (h *frontendHandler) PollActivityExecution(
	ctx context.Context,
	req *workflowservice.PollActivityExecutionRequest,
) (*workflowservice.PollActivityExecutionResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, serviceerror.NewUnavailable(StandaloneActivityDisabledError)
	}

	err := ValidatePollActivityExecutionRequest(
		req,
		h.config.MaxIDLengthLimit(),
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

// ListActivityExecutions lists activity executions matching the query in the request.
func (h *frontendHandler) ListActivityExecutions(
	ctx context.Context,
	req *workflowservice.ListActivityExecutionsRequest,
) (*workflowservice.ListActivityExecutionsResponse, error) {
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}
	ctx = chasm.NewVisibilityManagerContext(ctx, h.visibilityManager)

	resp, err := chasm.ListExecutions[*Activity, *activitypb.ActivityListMemo](ctx, &chasm.ListExecutionsRequest{
		NamespaceID:   namespaceID.String(),
		NamespaceName: req.GetNamespace(),
		PageSize:      int(req.GetPageSize()),
		NextPageToken: req.GetNextPageToken(),
		Query:         req.GetQuery(),
	})
	if err != nil {
		return nil, err
	}

	executions := make([]*apiactivitypb.ActivityExecutionListInfo, 0, len(resp.Executions))
	for _, exec := range resp.Executions {
		info := &apiactivitypb.ActivityExecutionListInfo{
			ActivityId:           exec.BusinessID,
			RunId:                exec.RunID,
			ScheduleTime:         timestamppb.New(exec.StartTime),
			CloseTime:            timestamppb.New(exec.CloseTime),
			StateTransitionCount: exec.StateTransitionCount,
			StateSizeBytes:       exec.HistorySizeBytes,
			// TODO(dan): exec.CustomSearchAttributes
			ActivityType: &commonpb.ActivityType{Name: exec.ChasmMemo.GetActivityType()},
			TaskQueue:    exec.ChasmMemo.GetTaskQueue(),
			Status:       InternalStatusToAPIStatus(exec.ChasmMemo.GetStatus()),
		}
		if !exec.CloseTime.IsZero() && !exec.StartTime.IsZero() {
			info.ExecutionDuration = durationpb.New(exec.CloseTime.Sub(exec.StartTime))
		}
		executions = append(executions, info)
	}

	return &workflowservice.ListActivityExecutionsResponse{
		Executions:    executions,
		NextPageToken: resp.NextPageToken,
	}, nil
}

// CountActivityExecutions counts activity executions matching the query in the request.
func (h *frontendHandler) CountActivityExecutions(
	ctx context.Context,
	req *workflowservice.CountActivityExecutionsRequest,
) (*workflowservice.CountActivityExecutionsResponse, error) {
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}
	ctx = chasm.NewVisibilityManagerContext(ctx, h.visibilityManager)

	resp, err := chasm.CountExecutions[*Activity](ctx, &chasm.CountExecutionsRequest{
		NamespaceID:   namespaceID.String(),
		NamespaceName: req.GetNamespace(),
		Query:         req.GetQuery(),
	})
	if err != nil {
		return nil, err
	}

	groups := make([]*workflowservice.CountActivityExecutionsResponse_AggregationGroup, 0, len(resp.Groups))
	for _, g := range resp.Groups {
		groups = append(groups, &workflowservice.CountActivityExecutionsResponse_AggregationGroup{
			GroupValues: g.Values,
			Count:       g.Count,
		})
	}

	return &workflowservice.CountActivityExecutionsResponse{
		Count:  resp.Count,
		Groups: groups,
	}, nil
}

// TerminateActivityExecution terminates a standalone activity execution
func (h *frontendHandler) TerminateActivityExecution(
	ctx context.Context,
	req *workflowservice.TerminateActivityExecutionRequest,
) (*workflowservice.TerminateActivityExecutionResponse, error) {
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, serviceerror.NewUnavailable(StandaloneActivityDisabledError)
	}

	namespaceName := req.GetNamespace()
	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(namespaceName))
	if err != nil {
		return nil, err
	}

	if err := validateInputSize(
		req.GetActivityId(),
		"activity-termination",
		h.config.BlobSizeLimitError,
		h.config.BlobSizeLimitWarn,
		len(req.GetReason()),
		h.logger,
		namespaceName); err != nil {
		return nil, err
	}

	// TODO add request ID validation when API updated

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
	if !h.config.Enabled(req.GetNamespace()) {
		return nil, serviceerror.NewUnavailable(StandaloneActivityDisabledError)
	}

	namespaceID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}

	// Since validation potentially mutates the request, we clone it first so that any retries use the original request.
	req = common.CloneProto(req)

	maxIDLen := h.config.MaxIDLengthLimit()

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

	if req.RetryPolicy == nil {
		req.RetryPolicy = &commonpb.RetryPolicy{}
	}

	opts := activityOptionsFromStartRequest(req)
	err := ValidateAndNormalizeActivityAttributes(
		req.ActivityId,
		activityType,
		h.config.DefaultActivityRetryPolicy,
		h.config.MaxIDLengthLimit(),
		namespaceID,
		opts,
		req.Priority,
		durationpb.New(0),
	)
	if err != nil {
		return nil, err
	}
	applyActivityOptionsToStartRequest(opts, req)

	// TODO: Unalias for validation, then restore aliased SA for CHASM visibility storage. The
	// validator requires unaliased format but CHASM visibility expects aliased format.
	originalSA := req.SearchAttributes
	if originalSA != nil {
		unaliasedSA, err := searchattribute.UnaliasFields(
			h.saMapperProvider,
			originalSA,
			req.GetNamespace(),
		)
		if err != nil {
			return nil, err
		}
		req.SearchAttributes = unaliasedSA
	}

	err = validateAndNormalizeStartActivityExecutionRequest(
		req,
		h.config.BlobSizeLimitError,
		h.config.BlobSizeLimitWarn,
		h.logger,
		h.config.MaxIDLengthLimit(),
		h.saValidator)
	if err != nil {
		return nil, err
	}

	req.SearchAttributes = originalSA

	return req, nil
}

// activityOptionsFromStartRequest builds an ActivityOptions from the inlined fields
// of a StartActivityExecutionRequest for use with shared validation logic.
func activityOptionsFromStartRequest(req *workflowservice.StartActivityExecutionRequest) *apiactivitypb.ActivityOptions {
	return &apiactivitypb.ActivityOptions{
		TaskQueue:              req.TaskQueue,
		ScheduleToCloseTimeout: req.ScheduleToCloseTimeout,
		ScheduleToStartTimeout: req.ScheduleToStartTimeout,
		StartToCloseTimeout:    req.StartToCloseTimeout,
		HeartbeatTimeout:       req.HeartbeatTimeout,
		RetryPolicy:            req.RetryPolicy,
	}
}

// applyActivityOptionsToStartRequest copies normalized values from ActivityOptions
// back to the StartActivityExecutionRequest.
func applyActivityOptionsToStartRequest(opts *apiactivitypb.ActivityOptions, req *workflowservice.StartActivityExecutionRequest) {
	req.TaskQueue = opts.TaskQueue
	req.ScheduleToCloseTimeout = opts.ScheduleToCloseTimeout
	req.ScheduleToStartTimeout = opts.ScheduleToStartTimeout
	req.StartToCloseTimeout = opts.StartToCloseTimeout
	req.HeartbeatTimeout = opts.HeartbeatTimeout
	req.RetryPolicy = opts.RetryPolicy
}
