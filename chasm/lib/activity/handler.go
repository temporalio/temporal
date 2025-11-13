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

	// TODO(dan): do we want to guarantee that response data will differ from that received when the
	// token was obtained? It's potentially confusing for the server to say "there's been a change"
	// while returning data in which the change is not apparent.

	refBytesFromToken := req.GetFrontendRequest().
		GetWaitPolicy().(*workflowservice.PollActivityExecutionRequest_WaitAnyStateChange).
		WaitAnyStateChange.GetLongPollToken()

	var lastSeenRef chasm.ComponentRef
	if refBytesFromToken != nil {
		var err error
		lastSeenRef, err = chasm.DeserializeComponentRef(refBytesFromToken)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument("invalid long poll token")
		}
		if lastSeenRef.NamespaceID != req.GetNamespaceId() ||
			lastSeenRef.BusinessID != req.GetFrontendRequest().GetActivityId() ||
			lastSeenRef.EntityID != req.GetFrontendRequest().GetRunId() {
			// token is inconsistent with request
			return nil, serviceerror.NewInvalidArgument("invalid long poll token")
		}
	} else {
		// This ref will compare less than currentRef in the comparison below.
		lastSeenRef = chasm.NewComponentRef[*Activity](chasm.EntityKey{
			NamespaceID: req.GetNamespaceId(),
			BusinessID:  req.GetFrontendRequest().GetActivityId(),
			EntityID:    req.GetFrontendRequest().GetRunId(),
		})
	}

	// PollComponent will return an error if lastSeenRef is not consistent with the entity
	// transition history on this shard, or if the state on this shard is behind the ref after a
	// reload.
	// TODO(dan): retryability of these errors
	response, newRef, err := chasm.PollComponent(
		ctx,
		lastSeenRef,
		func(
			a *Activity,
			ctx chasm.Context,
			req *activitypb.PollActivityExecutionRequest,
		) (*activitypb.PollActivityExecutionResponse, bool, error) {
			// TODO(dan): we're walking the tree to construct a ref when all we want here is the
			// root/entity VT. Would it make sense for Context to provide access to root node?
			currentRefBytes, err := ctx.Ref(a)
			if err != nil {
				return nil, false, err
			}
			currentRef, err := chasm.DeserializeComponentRef(currentRefBytes)
			if err != nil {
				return nil, false, err
			}

			if lastSeenRef.EntityID != currentRef.EntityID {
				// The runID from the token doesn't match this shard's state. We return immediately,
				// on the basis that this constitutes a state change. If the runID from the token is
				// ahead of this shard's state then this will be detected by shard ownership or
				// staleness checks and the caller will receive an error.
				response, err := a.buildPollActivityExecutionResponse(ctx, req)
				if err != nil {
					return nil, true, err
				}
				return response, true, nil
			}

			refComparison, err := chasm.CompareComponentRefs(&lastSeenRef, &currentRef)
			if err != nil {
				return nil, false, err
			}
			switch refComparison {
			case -1:
				// state has advanced beyond last seen: this is what we're waiting for
				response, err := a.buildPollActivityExecutionResponse(ctx, req)
				if err != nil {
					return nil, true, err
				}
				return response, true, nil
			case 0:
				// state is same as last seen: keep waiting
				return nil, false, nil
			case 1:
				// Impossible: PollComponent guarantees that at this point, current VT >= lastSeen VT.
				return nil, false, serviceerror.NewFailedPrecondition("submitted long-poll token represents a state beyond current")
			default:
				// Impossible
				return nil, false, serviceerror.NewInternal("unexpected transition history comparison result")
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
		response.GetFrontendResponse().StateChangeLongPollToken = newRef
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
		response.GetFrontendResponse().StateChangeLongPollToken = newRef
	}
	return response, nil
}
