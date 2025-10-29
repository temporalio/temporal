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

	// Returned token representing state of component seen by caller.
	var newStateChangeToken []byte
	// Returned info for the activity
	var activityInfo *activity.ActivityExecutionInfo

	waitPolicy := request.GetWaitPolicy()
	if waitPolicy != nil {
		// Long-poll

		var predicateFn func(*Activity, chasm.Context, any) (any, bool, error)

		switch waitPolicy := request.GetWaitPolicy().(type) {
		case *workflowservice.PollActivityExecutionRequest_WaitAnyStateChange:
			// Two cases:
			//
			// 1. Request does not have a token
			//    Last state seen by caller is unknown, so return immediately.

			// 2. Request has a token
			//    Extract caller's last-seen component state transition count from token and compare
			//    to current component state. If the component has already evolved beyond the
			//    caller's last-seen count then return immediately. Otherwise, use PollComponent to
			//    wait for the component transition count to exceed last-seen.

			token := waitPolicy.WaitAnyStateChange.GetLongPollToken()
			var prevTransitionCount int64 = -1
			if token != nil {
				parsed, err := strconv.ParseInt(string(token), 10, 64)
				if err != nil {
					return nil, serviceerror.NewInvalidArgument("invalid long poll token")
				}
				prevTransitionCount = parsed
			}

			predicateFn = func(a *Activity, ctx chasm.Context, _ any) (any, bool, error) {
				refBytes, err := ctx.Ref(a)
				if err != nil {
					return nil, false, err
				}

				ref, err := chasm.DeserializeComponentRef(refBytes)
				if err != nil {
					return nil, false, err
				}

				newTransitionCount := ref.GetEntityLastUpdateVersionedTransition().GetTransitionCount()

				// we're waiting for new transition count to exceed last seen

				if prevTransitionCount == -1 || newTransitionCount > prevTransitionCount {
					// Prev count unknown or less than new - capture new state and return
					newStateChangeToken = []byte(strconv.FormatInt(newTransitionCount, 10))

					if !request.ExcludeInfo {
						activityInfo, err = a.buildActivityExecutionInfo(ctx, chasm.EntityKey{
							NamespaceID: req.GetNamespaceId(),
							BusinessID:  request.GetActivityId(),
							EntityID:    request.GetRunId(),
						})
						if err != nil {
							return nil, false, err
						}
					}
					return nil, true, nil
				} else if newTransitionCount < prevTransitionCount {
					// TODO: error code?
					return nil, false, serviceerror.NewFailedPrecondition(
						fmt.Sprintf("invalid activity state: last seen transition count (%d) exceeds current (%d)", prevTransitionCount, newTransitionCount))
				} else {
					// No change yet - keep waiting
					return nil, false, nil
				}
			}

		case *workflowservice.PollActivityExecutionRequest_WaitCompletion:
			predicateFn = func(a *Activity, ctx chasm.Context, _ any) (any, bool, error) {
				completed := a.State() == activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED

				if completed {
					refBytes, err := ctx.Ref(a)
					if err != nil {
						return nil, false, err
					}
					ref, err := chasm.DeserializeComponentRef(refBytes)
					if err != nil {
						return nil, false, err
					}
					currentCount := ref.GetEntityLastUpdateVersionedTransition().GetTransitionCount()
					newStateChangeToken = []byte(strconv.FormatInt(currentCount, 10))
					if !request.ExcludeInfo {
						activityInfo, err = a.buildActivityExecutionInfo(ctx, chasm.EntityKey{
							NamespaceID: req.GetNamespaceId(),
							BusinessID:  request.GetActivityId(),
							EntityID:    request.GetRunId(),
						})
						if err != nil {
							return nil, false, err
						}
					}
				}
				return nil, completed, nil
			}

		default:
			return nil, fmt.Errorf("unexpected wait policy type: %T", waitPolicy)
		}

		_, _, err := chasm.PollComponent[*Activity, chasm.ComponentRef, any, any, any](
			ctx,
			chasm.NewComponentRef[*Activity](chasm.EntityKey{
				NamespaceID: req.GetNamespaceId(),
				BusinessID:  request.GetActivityId(),
				EntityID:    request.GetRunId(),
			}),
			predicateFn,
			nil,
			nil,
		)
		if err != nil {
			return nil, err
		}
	}

	return &activitypb.PollActivityExecutionResponse{
		FrontendResponse: &workflowservice.PollActivityExecutionResponse{
			Info:                     activityInfo,
			StateChangeLongPollToken: newStateChangeToken,
		},
	}, nil
}
