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
				// EagerTask: TODO when supported, need to call the same code that would handle the RecordActivityTaskStarted API
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
) (*activitypb.PollActivityExecutionResponse, error) {
	switch req.GetFrontendRequest().GetWaitPolicy().(type) {
	case nil:
		return chasm.ReadComponent(
			ctx,
			chasm.NewComponentRef[*Activity](chasm.EntityKey{
				NamespaceID: req.GetNamespaceId(),
				BusinessID:  req.GetFrontendRequest().GetActivityId(),
				EntityID:    req.GetFrontendRequest().GetRunId(),
			}),
			(*Activity).buildPollActivityExecutionResponse,
			req,
			nil,
		)
	case *workflowservice.PollActivityExecutionRequest_WaitAnyStateChange:
		return pollActivityExecutionWaitAnyStateChange(ctx, req)
	case *workflowservice.PollActivityExecutionRequest_WaitCompletion:
		return pollActivityExecutionWaitCompletion(ctx, req)
	default:
		return nil, serviceerror.NewInvalidArgumentf("unexpected wait policy type: %T", req.GetFrontendRequest().GetWaitPolicy())
	}
}

// pollActivityExecutionWaitAnyStateChange waits until the activity state has advanced beyond that
// specified by the submitted token. If no token was submitted, it returns the current state without
// waiting.
func pollActivityExecutionWaitAnyStateChange(
	ctx context.Context,
	req *activitypb.PollActivityExecutionRequest,
) (*activitypb.PollActivityExecutionResponse, error) {
	token := req.GetFrontendRequest().
		GetWaitPolicy().(*workflowservice.PollActivityExecutionRequest_WaitAnyStateChange).
		WaitAnyStateChange.GetLongPollToken()

	response, newRef, err := chasm.PollComponent(
		ctx,
		chasm.NewComponentRef[*Activity](chasm.EntityKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  req.GetFrontendRequest().GetActivityId(),
			EntityID:    req.GetFrontendRequest().GetRunId(),
		}),
		func(
			a *Activity,
			ctx chasm.Context,
			req *activitypb.PollActivityExecutionRequest,
		) (*activitypb.PollActivityExecutionResponse, bool, error) {
			_, advanced, err := chasm.HasStateAdvanced(a, ctx, token)
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
		},
		req,
	)
	if err != nil {
		return nil, err
	}
	if response == nil {
		// nil response indicates server-imposed long-poll timeout. Communicate this to callers by
		// returning a non-error empty response.
		response = &activitypb.PollActivityExecutionResponse{
			FrontendResponse: &workflowservice.PollActivityExecutionResponse{},
		}
	} else {
		token, err := chasm.EncodeStateToken(newRef)
		if err != nil {
			return nil, err
		}
		response.GetFrontendResponse().StateChangeLongPollToken = token
	}
	return response, nil
}

// pollActivityExecutionWaitCompletion waits until the activity is completed.
func pollActivityExecutionWaitCompletion(
	ctx context.Context,
	req *activitypb.PollActivityExecutionRequest,
) (*activitypb.PollActivityExecutionResponse, error) {
	// TODO(dan): implement functional test when RecordActivityTaskCompleted is implemented
	response, newRef, err := chasm.PollComponent(
		ctx,
		chasm.NewComponentRef[*Activity](chasm.EntityKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  req.GetFrontendRequest().GetActivityId(),
			EntityID:    req.GetFrontendRequest().GetRunId(),
		}),
		func(
			a *Activity,
			ctx chasm.Context,
			req *activitypb.PollActivityExecutionRequest,
		) (*activitypb.PollActivityExecutionResponse, bool, error) {
			panic("TODO(dan): pollActivityExecutionWaitCompletion is not implemented")
			completed := false
			if completed {
				response, err := a.buildPollActivityExecutionResponse(ctx, req)
				if err != nil {
					return nil, true, err
				}
				return response, true, nil
			}
			return nil, false, nil
		},
		req,
	)
	if err != nil {
		return nil, err
	}
	if response == nil {
		// nil response indicates server-imposed long-poll timeout. Communicate this to callers by
		// returning a non-error empty response.
		response = &activitypb.PollActivityExecutionResponse{
			FrontendResponse: &workflowservice.PollActivityExecutionResponse{},
		}
	} else {
		token, err := chasm.EncodeStateToken(newRef)
		if err != nil {
			return nil, err
		}
		response.GetFrontendResponse().StateChangeLongPollToken = token
	}
	return response, nil
}
