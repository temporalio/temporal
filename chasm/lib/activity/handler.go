package activity

import (
	"context"
	"fmt"
	"strconv"

	"go.temporal.io/api/activity/v1"
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

// PollActivityExecution handles PollActivityExecutionRequest from frontend. This method is used by
// clients to poll for activity info and/or result, optionally as a long-poll.
func (h *handler) PollActivityExecution(ctx context.Context, req *activitypb.PollActivityExecutionRequest) (*activitypb.PollActivityExecutionResponse, error) {
	request := req.GetFrontendRequest()
	info := &activity.ActivityExecutionInfo{
		ActivityId: request.GetActivityId(),
		RunId:      request.GetRunId(),
	}
	key := chasm.EntityKey{
		NamespaceID: req.GetNamespaceId(),
		BusinessID:  request.GetActivityId(),
		EntityID:    request.GetRunId(),
	}

	var stateChangeToken []byte

	waitPolicy := request.GetWaitPolicy()
	if waitPolicy != nil {
		switch waitPolicy := request.GetWaitPolicy().(type) {
		case *workflowservice.PollActivityExecutionRequest_WaitAnyStateChange:
			options := waitPolicy.WaitAnyStateChange
			var prevTransitionCount int64 = -1
			if options.GetLongPollToken() != nil {
				parsed, err := strconv.ParseInt(string(options.GetLongPollToken()), 10, 64)
				if err != nil {
					return nil, serviceerror.NewInvalidArgument("invalid long poll token")
				}
				prevTransitionCount = parsed
			}

			if prevTransitionCount == -1 {
				currentCount, err := h.getCurrentTransitionCount(ctx, key)
				if err != nil {
					return nil, err
				}
				stateChangeToken = []byte(strconv.FormatInt(currentCount, 10))
			} else {
				predicateFn := func(activity *Activity, ctx chasm.Context, _ any) (any, bool, error) {
					refBytes, err := ctx.Ref(activity)
					if err != nil {
						return nil, false, err
					}

					ref, err := chasm.DeserializeComponentRef(refBytes)
					if err != nil {
						return nil, false, err
					}

					currentCount := ref.GetEntityLastUpdateVersionedTransition().GetTransitionCount()

					if currentCount < prevTransitionCount {
						return nil, false, serviceerror.NewFailedPrecondition("stale activity state")
					}

					hasChanged := currentCount > prevTransitionCount
					if hasChanged {
						return []byte(strconv.FormatInt(currentCount, 10)), true, nil
					}
					return nil, false, nil
				}

				var operationFn func(_ *Activity, _ chasm.MutableContext, _ any, _ any) (any, error)

				_, _, err := chasm.PollComponent(
					ctx,
					chasm.NewComponentRef[*Activity](key),
					predicateFn,
					operationFn,
					nil,
				)
				if err != nil {
					return nil, err
				}
			}

		case *workflowservice.PollActivityExecutionRequest_WaitCompletion:
			predicateFn := func(activity *Activity, _ chasm.Context, _ any) (any, bool, error) {
				completed := activity.State() == activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED
				return nil, completed, nil
			}

			var operationFn func(_ *Activity, _ chasm.MutableContext, _ any, _ any) (any, error)

			_, _, err := chasm.PollComponent(
				ctx,
				chasm.NewComponentRef[*Activity](key),
				predicateFn,
				operationFn,
				nil,
			)
			if err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("unexpected wait policy type: %T", waitPolicy)
		}
	}

	return &activitypb.PollActivityExecutionResponse{
		FrontendResponse: &workflowservice.PollActivityExecutionResponse{
			Info:                     info,
			StateChangeLongPollToken: stateChangeToken,
		},
	}, nil
}

func (h *handler) getCurrentTransitionCount(ctx context.Context, key chasm.EntityKey) (int64, error) {
	count, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*Activity](key),
		func(activity *Activity, ctx chasm.Context, _ any) (int64, error) {
			refBytes, err := ctx.Ref(activity)
			if err != nil {
				return 0, err
			}

			ref, err := chasm.DeserializeComponentRef(refBytes)
			if err != nil {
				return 0, err
			}

			return ref.GetEntityLastUpdateVersionedTransition().GetTransitionCount(), nil
		},
		nil,
	)
	return count, err
}
