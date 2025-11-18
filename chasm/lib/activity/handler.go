package activity

import (
	"context"

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
				// EagerTask: TODO when supported, need to call the same code that would handle the RecordStarted API
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

	switch waitPolicy.(type) {
	case *workflowservice.PollActivityExecutionRequest_WaitAnyStateChange:
		stateToken := req.GetFrontendRequest().
			GetWaitPolicy().(*workflowservice.PollActivityExecutionRequest_WaitAnyStateChange).
			WaitAnyStateChange.GetLongPollToken()
		response, _, err = chasm.PollComponent(ctx, ref, func(
			a *Activity,
			ctx chasm.Context,
			req *activitypb.PollActivityExecutionRequest,
		) (*activitypb.PollActivityExecutionResponse, bool, error) {
			_, advanced, err := chasm.ComponentStateChanged(a, ctx, stateToken)
			if err != nil {
				return nil, false, err
			}
			if advanced {
				// TODO(dan): pass ref returned by HasStateAdvanced into this?
				response, err := a.buildPollActivityExecutionResponse(ctx, req)
				if err != nil {
					return nil, true, err
				}
				return response, true, nil
			} else {
				return nil, false, nil
			}
		}, req)
	case *workflowservice.PollActivityExecutionRequest_WaitCompletion:
		response, _, err = chasm.PollComponent(ctx, ref, func(
			a *Activity,
			ctx chasm.Context,
			req *activitypb.PollActivityExecutionRequest,
		) (*activitypb.PollActivityExecutionResponse, bool, error) {

			switch a.Status {
			case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
				activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
				activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
				activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
				activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED:
				response, err := a.buildPollActivityExecutionResponse(ctx, req)
				if err != nil {
					return nil, true, err
				}

				return response, true, nil
			default:
				return nil, false, nil
			}
		}, req)
	default:
		return nil, serviceerror.NewInvalidArgumentf("unexpected wait policy type: %T", waitPolicy)
	}
	if err != nil {
		return nil, err
	}
	if response == nil {
		// nil response indicates server-imposed long-poll timeout. Communicate this to callers by
		// returning a non-error empty response.

		// TODO(dan): the definition of "empty" is unclear, since callers can currently choose to
		// exclude info, outcome, and input from the result. Currently, a caller can infer that the
		// long-poll timed out due to a server-imposed timeout from the absence of the long-poll
		// token. However, this is not a clear API. We are considering splitting the public API into
		// two methods: one that returns info (optionally with input), and one that returns result,
		// both with long-poll options. An empty response will then be more obvious to the caller.
		// However, we may want to consider a more explicit way of saying to the caller "timed out
		// due to internal long-poll timeout; please resubmit your long-poll request".
		response = &activitypb.PollActivityExecutionResponse{
			FrontendResponse: &workflowservice.PollActivityExecutionResponse{},
		}
	}
	return response, nil
}
