package activity

import (
	"context"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	errordetailspb "go.temporal.io/api/errordetails/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	businessIDReusePolicyMap = map[enumspb.ActivityIdReusePolicy]chasm.BusinessIDReusePolicy{
		enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE:             chasm.BusinessIDReusePolicyAllowDuplicate,
		enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY: chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly,
		enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE:            chasm.BusinessIDReusePolicyRejectDuplicate,
	}

	businessIDConflictPolicyMap = map[enumspb.ActivityIdConflictPolicy]chasm.BusinessIDConflictPolicy{
		enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL:         chasm.BusinessIDConflictPolicyFail,
		enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING: chasm.BusinessIDConflictPolicyUseExisting,
	}
)

type handler struct {
	activitypb.UnimplementedActivityServiceServer
	config            *Config
	logger            log.Logger
	metricsHandler    metrics.Handler
	namespaceRegistry namespace.Registry
}

func newHandler(config *Config, metricsHandler metrics.Handler, logger log.Logger, namespaceRegistry namespace.Registry) *handler {
	return &handler{
		config:            config,
		logger:            logger,
		metricsHandler:    metricsHandler,
		namespaceRegistry: namespaceRegistry,
	}
}

// StartActivityExecution schedules an activity execution. Note that while external callers refer to
// this as "start", the start transition in fact happens later, in response to the activity task in
// matching being delivered to a worker poll request.
func (h *handler) StartActivityExecution(ctx context.Context, req *activitypb.StartActivityExecutionRequest) (*activitypb.StartActivityExecutionResponse, error) {
	frontendReq := req.GetFrontendRequest()

	reusePolicy, ok := businessIDReusePolicyMap[frontendReq.GetIdReusePolicy()]
	if !ok {
		return nil, serviceerror.NewFailedPrecondition(fmt.Sprintf("unsupported ID reuse policy: %v", frontendReq.GetIdReusePolicy()))
	}

	conflictPolicy, ok := businessIDConflictPolicyMap[frontendReq.GetIdConflictPolicy()]
	if !ok {
		return nil, serviceerror.NewFailedPrecondition(fmt.Sprintf("unsupported ID conflict policy: %v", frontendReq.GetIdConflictPolicy()))
	}

	result, err := chasm.NewExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  req.GetFrontendRequest().GetActivityId(),
		},
		func(mutableContext chasm.MutableContext, request *workflowservice.StartActivityExecutionRequest) (*Activity, *workflowservice.StartActivityExecutionResponse, error) {
			newActivity, err := NewStandaloneActivity(mutableContext, request)
			if err != nil {
				return nil, nil, err
			}

			err = TransitionScheduled.Apply(newActivity, mutableContext, nil)
			if err != nil {
				return nil, nil, err
			}

			return newActivity, &workflowservice.StartActivityExecutionResponse{
				// EagerTask: TODO when supported, need to call the same code that would handle the HandleStarted API
			}, nil
		},
		req.GetFrontendRequest(),
		chasm.WithRequestID(req.GetFrontendRequest().GetRequestId()),
		chasm.WithBusinessIDPolicy(reusePolicy, conflictPolicy),
	)

	if err != nil {
		var alreadyStartedErr *chasm.ExecutionAlreadyStartedError
		if errors.As(err, &alreadyStartedErr) {
			details := &errordetailspb.ActivityExecutionAlreadyStartedFailure{
				StartRequestId: alreadyStartedErr.CurrentRequestID,
				RunId:          alreadyStartedErr.CurrentRunID,
			}

			errStatus := status.New(codes.AlreadyExists, "activity execution already started")

			errStatusWithDetails, errDetail := status.New(codes.AlreadyExists, "activity execution already started").WithDetails(details)
			if errDetail != nil {
				h.logger.Error("Failed to add error details to ActivityExecutionAlreadyStartedFailure",
					tag.Error(errDetail), tag.ActivityID(frontendReq.GetActivityId()))
				return nil, errStatus.Err()
			}

			return nil, errStatusWithDetails.Err()
		}

		return nil, err
	}

	result.Output.RunId = result.ExecutionKey.RunID
	result.Output.Started = result.Created

	return &activitypb.StartActivityExecutionResponse{
		FrontendResponse: result.Output,
	}, nil
}

// DescribeActivityExecution queries current activity state, optionally as a long-poll that waits
// for any state change. When used to long-poll, it returns an empty non-error response on context
// deadline expiry, to indicate that the state being waited for was not reached. Callers should
// interpret this as an invitation to resubmit their long-poll request. This response is sent before
// the caller's deadline (see chasm.activity.longPollBuffer) so that it is likely that the caller
// does indeed receive the non-error response.
func (h *handler) DescribeActivityExecution(
	ctx context.Context,
	req *activitypb.DescribeActivityExecutionRequest,
) (response *activitypb.DescribeActivityExecutionResponse, err error) {
	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFrontendRequest().GetActivityId(),
		RunID:       req.GetFrontendRequest().GetRunId(),
	})
	defer func() {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			err = serviceerror.NewNotFound("activity execution not found")
		}
	}()

	// Below, we send an empty non-error response on context deadline expiry. Here we compute a
	// deadline that causes us to send that response before the caller's own deadline (see
	// chasm.activity.longPollBuffer). We also cap the caller's deadline at
	// chasm.activity.longPollTimeout.
	ns := req.GetFrontendRequest().GetNamespace()
	ctx, cancel := contextutil.WithDeadlineBuffer(
		ctx,
		h.config.LongPollTimeout(ns),
		h.config.LongPollBuffer(ns),
	)
	defer cancel()

	token := req.GetFrontendRequest().GetLongPollToken()
	if len(token) == 0 {
		return chasm.ReadComponent(ctx, ref, (*Activity).buildDescribeActivityExecutionResponse, req, nil)
	}
	response, _, err = chasm.PollComponent(ctx, ref, func(
		a *Activity,
		ctx chasm.Context,
		req *activitypb.DescribeActivityExecutionRequest,
	) (*activitypb.DescribeActivityExecutionResponse, bool, error) {
		changed, err := chasm.ExecutionStateChanged(a, ctx, token)
		if err != nil {
			if errors.Is(err, chasm.ErrMalformedComponentRef) {
				return nil, false, serviceerror.NewInvalidArgument("invalid long poll token")
			}
			if errors.Is(err, chasm.ErrInvalidComponentRef) {
				return nil, false, serviceerror.NewInvalidArgument("long poll token does not match execution")
			}
			return nil, false, err
		}
		if changed {
			response, err := a.buildDescribeActivityExecutionResponse(ctx, req)
			return response, true, err
		}
		return nil, false, nil
	}, req)

	if err != nil && ctx.Err() != nil {
		// Send empty non-error response on deadline expiry: caller should continue long-polling.
		return &activitypb.DescribeActivityExecutionResponse{
			FrontendResponse: &workflowservice.DescribeActivityExecutionResponse{},
		}, nil
	}
	return response, err
}

// PollActivityExecution long-polls for activity outcome. It returns an empty non-error response on
// context deadline expiry, to indicate that the state being waited for was not reached. Callers
// should interpret this as an invitation to resubmit their long-poll request. This response is sent
// before the caller's deadline (see chasm.activity.longPollBuffer) so that it is likely that the
// caller does indeed receive the non-error response.
func (h *handler) PollActivityExecution(
	ctx context.Context,
	req *activitypb.PollActivityExecutionRequest,
) (response *activitypb.PollActivityExecutionResponse, err error) {
	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFrontendRequest().GetActivityId(),
		RunID:       req.GetFrontendRequest().GetRunId(),
	})
	defer func() {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			err = serviceerror.NewNotFound("activity execution not found")
		}
	}()

	// Below, we send an empty non-error response on context deadline expiry. Here we compute a
	// deadline that causes us to send that response before the caller's own deadline (see
	// chasm.activity.longPollBuffer). We also cap the caller's deadline at
	// chasm.activity.longPollTimeout.
	ns := req.GetFrontendRequest().GetNamespace()
	ctx, cancel := contextutil.WithDeadlineBuffer(
		ctx,
		h.config.LongPollTimeout(ns),
		h.config.LongPollBuffer(ns),
	)
	defer cancel()

	response, _, err = chasm.PollComponent(ctx, ref, func(
		a *Activity,
		ctx chasm.Context,
		req *activitypb.PollActivityExecutionRequest,
	) (*activitypb.PollActivityExecutionResponse, bool, error) {
		if a.LifecycleState(ctx) != chasm.LifecycleStateRunning {
			response, err := a.buildPollActivityExecutionResponse(ctx)
			return response, true, err
		}
		return nil, false, nil
	}, req)

	if err != nil && ctx.Err() != nil {
		// Send an empty non-error response as an invitation to resubmit the long-poll.
		return &activitypb.PollActivityExecutionResponse{
			FrontendResponse: &workflowservice.PollActivityExecutionResponse{},
		}, nil
	}
	return response, err
}

// TerminateActivityExecution terminates an activity execution.
func (h *handler) TerminateActivityExecution(
	ctx context.Context,
	req *activitypb.TerminateActivityExecutionRequest,
) (response *activitypb.TerminateActivityExecutionResponse, err error) {
	frontendReq := req.GetFrontendRequest()

	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetActivityId(),
		RunID:       frontendReq.GetRunId(),
	})

	namespaceName, err := h.namespaceRegistry.GetNamespaceName(namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	response, _, err = chasm.UpdateComponent(
		ctx,
		ref,
		(*Activity).handleTerminated,
		terminateEvent{
			request: req,
			MetricsHandlerBuilderParams: MetricsHandlerBuilderParams{
				Handler:                     h.metricsHandler,
				NamespaceName:               namespaceName.String(),
				BreakdownMetricsByTaskQueue: h.config.BreakdownMetricsByTaskQueue,
			},
		},
	)

	if err != nil {
		return nil, err
	}

	return response, nil
}

// RequestCancelActivityExecution requests cancellation of an activity execution.
func (h *handler) RequestCancelActivityExecution(
	ctx context.Context,
	req *activitypb.RequestCancelActivityExecutionRequest,
) (response *activitypb.RequestCancelActivityExecutionResponse, err error) {
	frontendReq := req.GetFrontendRequest()

	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetActivityId(),
		RunID:       frontendReq.GetRunId(),
	})

	namespaceName, err := h.namespaceRegistry.GetNamespaceName(namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	response, _, err = chasm.UpdateComponent(
		ctx,
		ref,
		(*Activity).handleCancellationRequested,
		requestCancelEvent{
			request: req,
			MetricsHandlerBuilderParams: MetricsHandlerBuilderParams{
				Handler:                     h.metricsHandler,
				NamespaceName:               namespaceName.String(),
				BreakdownMetricsByTaskQueue: h.config.BreakdownMetricsByTaskQueue,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	return response, nil
}
