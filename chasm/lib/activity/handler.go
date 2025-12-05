package activity

import (
	"context"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/contextutil"
)

var (
	businessIDReusePolicyMap = map[enumspb.ActivityIdReusePolicy]chasm.BusinessIDReusePolicy{
		enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE:             chasm.BusinessIDReusePolicyAllowDuplicate,
		enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY: chasm.BusinessIDReusePolicyAllowDuplicateFailedOnly,
		enumspb.ACTIVITY_ID_REUSE_POLICY_REJECT_DUPLICATE:            chasm.BusinessIDReusePolicyRejectDuplicate,
	}

	// TODO this will change once we rebase on main
	businessIDConflictPolicyMap = map[enumspb.ActivityIdConflictPolicy]chasm.BusinessIDConflictPolicy{
		enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL: chasm.BusinessIDConflictPolicyFail,
	}
)

type handler struct {
	activitypb.UnimplementedActivityServiceServer
	config *Config
}

func newHandler(config *Config) *handler {
	return &handler{
		config: config,
	}
}

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

	response, key, _, err := chasm.NewExecution(
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
				Started: true,
				// EagerTask: TODO when supported, need to call the same code that would handle the HandleStarted API
			}, nil
		},
		req.GetFrontendRequest(),
		chasm.WithRequestID(req.GetFrontendRequest().GetRequestId()),
		chasm.WithBusinessIDPolicy(reusePolicy, conflictPolicy),
	)

	if err != nil {
		return nil, err
	}

	response.RunId = key.RunID

	return &activitypb.StartActivityExecutionResponse{
		FrontendResponse: response,
	}, nil
}

// PollActivityExecution handles PollActivityExecutionRequest from frontend. This method supports
// querying current activity state, optionally as a long-poll that waits for certain state changes.
// It is used by clients to poll for activity state and/or result. When used to long-poll, it
// returns an empty non-error response on context deadline expiry, to indicate that the state being
// waited for was not reached. Callers should interpret this as an invitation to resubmit their
// long-poll request. This response is sent before the caller's deadline (see
// chasm.activity.longPollBuffer) so that it is likely that the caller does indeed receive the
// non-error response.
//
//nolint:revive // cognitive complexity
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

	waitPolicy := req.GetFrontendRequest().GetWaitPolicy()

	if waitPolicy == nil {
		return chasm.ReadComponent(ctx, ref, (*Activity).buildPollActivityExecutionResponse, req, nil)
	}

	// Below, we send an empty non-error response on context deadline expiry. Here we compute a
	// deadline that causes us to send that response before the caller's own deadline (see
	// chasm.activity.longPollBuffer). We also cap the caller's deadline at
	// chasm.activity.longPollTimeout.
	namespace := req.GetFrontendRequest().GetNamespace()
	ctx, cancel := contextutil.WithDeadlineBuffer(
		ctx,
		h.config.LongPollTimeout(namespace),
		h.config.LongPollBuffer(namespace),
	)
	defer cancel()

	switch waitPolicyType := waitPolicy.(type) {
	case *workflowservice.PollActivityExecutionRequest_WaitAnyStateChange:
		token := waitPolicyType.WaitAnyStateChange.GetLongPollToken()
		if len(token) == 0 {
			return chasm.ReadComponent(ctx, ref, (*Activity).buildPollActivityExecutionResponse, req, nil)
		}
		response, _, err = chasm.PollComponent(ctx, ref, func(
			a *Activity,
			ctx chasm.Context,
			req *activitypb.PollActivityExecutionRequest,
		) (*activitypb.PollActivityExecutionResponse, bool, error) {
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
				response, err := a.buildPollActivityExecutionResponse(ctx, req)
				return response, true, err
			}
			return nil, false, nil
		}, req)
	case *workflowservice.PollActivityExecutionRequest_WaitCompletion:
		// TODO(dan): add functional test when RecordActivityTaskCompleted is implemented
		response, _, err = chasm.PollComponent(ctx, ref, func(
			a *Activity,
			ctx chasm.Context,
			req *activitypb.PollActivityExecutionRequest,
		) (*activitypb.PollActivityExecutionResponse, bool, error) {
			if a.LifecycleState(ctx) != chasm.LifecycleStateRunning {
				response, err := a.buildPollActivityExecutionResponse(ctx, req)
				if err != nil {
					return nil, true, err
				}

				return response, true, nil
			}

			return nil, false, nil
		}, req)
	default:
		return nil, serviceerror.NewInvalidArgumentf("unexpected wait policy type: %T", waitPolicy)
	}

	if ctx.Err() != nil {
		// We send an empty non-error response on deadline expiry as an invitation to the caller to
		// resubmit their long-poll.

		// TODO(dan): the definition of "empty" is unclear, since callers can currently choose to
		// exclude info, outcome, and input from the result. Currently, a caller can infer that the
		// long-poll timed out due to a server-imposed timeout from the absence of the long-poll
		// token. However, this is not a clear API. We are considering splitting the public API into
		// two methods: one that returns info (optionally with input), and one that returns result,
		// both with long-poll options. An empty response will then be more obvious to the caller.
		return &activitypb.PollActivityExecutionResponse{
			FrontendResponse: &workflowservice.PollActivityExecutionResponse{},
		}, nil
	}
	return response, err
}

// TerminateActivityExecution terminates a standalone activity execution
func (h *handler) TerminateActivityExecution(
	ctx context.Context,
	req *activitypb.TerminateActivityExecutionRequest,
) (response *activitypb.TerminateActivityExecutionResponse, err error) {
	frontendReq := req.GetFrontendRequest()

	ref := chasm.NewComponentRef[*Activity](chasm.EntityKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetActivityId(),
		EntityID:    frontendReq.GetRunId(),
	})

	response, _, err = chasm.UpdateComponent(
		ctx,
		ref,
		(*Activity).handleTerminated,
		req,
	)

	if err != nil {
		return nil, err
	}

	return response, nil
}

// RequestCancelActivityExecution requests cancellation on a standalone activity execution
func (h *handler) RequestCancelActivityExecution(
	ctx context.Context,
	req *activitypb.RequestCancelActivityExecutionRequest,
) (response *activitypb.RequestCancelActivityExecutionResponse, err error) {
	frontendReq := req.GetFrontendRequest()

	ref := chasm.NewComponentRef[*Activity](chasm.EntityKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  frontendReq.GetActivityId(),
		EntityID:    frontendReq.GetRunId(),
	})

	response, _, err = chasm.UpdateComponent(
		ctx,
		ref,
		(*Activity).handleCancellationRequested,
		req,
	)
	if err != nil {
		return nil, err
	}

	return response, nil
}
