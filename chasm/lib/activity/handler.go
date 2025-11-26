package activity

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

type handler struct {
	activitypb.UnimplementedActivityServiceServer
}

func newHandler() *handler {
	return &handler{}
}

func (h *handler) StartActivityExecution(ctx context.Context, req *activitypb.StartActivityExecutionRequest) (*activitypb.StartActivityExecutionResponse, error) {
	response, key, _, err := chasm.NewEntity(
		ctx,
		chasm.EntityKey{
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
		req.GetFrontendRequest())

	if err != nil {
		return nil, err
	}

	response.RunId = key.EntityID

	return &activitypb.StartActivityExecutionResponse{
		FrontendResponse: response,
	}, nil
}

// PollActivityExecution handles PollActivityExecutionRequest from frontend. This method supports
// querying current activity state, optionally as a long-poll that waits for certain state changes.
// It is used by clients to poll for activity state and/or result.
func (h *handler) PollActivityExecution(
	ctx context.Context,
	req *activitypb.PollActivityExecutionRequest,
) (response *activitypb.PollActivityExecutionResponse, err error) {
	ref := chasm.NewComponentRef[*Activity](chasm.EntityKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  req.GetFrontendRequest().GetActivityId(),
		EntityID:    req.GetFrontendRequest().GetRunId(),
	})
	waitPolicy := req.GetFrontendRequest().GetWaitPolicy()

	if waitPolicy == nil {
		return chasm.ReadComponent(ctx, ref, (*Activity).buildPollActivityExecutionResponse, req, nil)
	}

	// Do not allow long-poll to use all remaining time
	childCtx := ctx
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		childCtx, cancel = context.WithDeadline(ctx, deadline.Add(-time.Second))
		defer cancel()
	}

	switch waitPolicy.(type) {
	case *workflowservice.PollActivityExecutionRequest_WaitAnyStateChange:
		token := req.GetFrontendRequest().
			GetWaitPolicy().(*workflowservice.PollActivityExecutionRequest_WaitAnyStateChange).
			WaitAnyStateChange.GetLongPollToken()
		response, _, err = chasm.PollComponent(childCtx, ref, func(
			a *Activity,
			ctx chasm.Context,
			req *activitypb.PollActivityExecutionRequest,
		) (*activitypb.PollActivityExecutionResponse, bool, error) {
			changed, err := chasm.ExecutionStateChanged(a, ctx, token)
			if err != nil {
				if errors.Is(err, chasm.ErrInvalidComponentRefBytes) {
					return nil, false, serviceerror.NewInvalidArgument("invalid long poll token")
				}
				return nil, false, err
			}
			if changed {
				response, err := a.buildPollActivityExecutionResponse(ctx, req)
				return response, true, err
			} else {
				return nil, false, nil
			}
		}, req)
	case *workflowservice.PollActivityExecutionRequest_WaitCompletion:
		response, _, err = chasm.PollComponent(childCtx, ref, func(
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

	if childCtx.Err() != nil {
		// Caller deadline exceeded
		return nil, childCtx.Err()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		// Server-imposed long-poll deadline exceeded. Communicate this to callers by returning a
		// non-error empty response.

		// TODO(dan): the definition of "empty" is unclear, since callers can currently choose to
		// exclude info, outcome, and input from the result. Currently, a caller can infer that the
		// long-poll timed out due to a server-imposed timeout from the absence of the long-poll
		// token. However, this is not a clear API. We are considering splitting the public API into
		// two methods: one that returns info (optionally with input), and one that returns result,
		// both with long-poll options. An empty response will then be more obvious to the caller.
		// However, we may want to consider a more explicit way of saying to the caller "timed out
		// due to internal long-poll timeout; please resubmit your long-poll request".
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

// CancelActivityExecution requests cancellation on a standalone activity execution
func (h *handler) CancelActivityExecution(
	ctx context.Context,
	req *activitypb.CancelActivityExecutionRequest,
) (response *activitypb.CancelActivityExecutionResponse, err error) {
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
