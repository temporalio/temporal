package activity

import (
	"context"
	"fmt"

	"go.temporal.io/api/activity/v1"
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
	// TODO
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
	waitPolicy := request.GetWaitPolicy()
	if waitPolicy != nil {
		var waitPredicateFn func(*Activity, chasm.Context, any) (any, bool, error)
		switch waitPolicy := request.GetWaitPolicy().(type) {
		case *workflowservice.PollActivityExecutionRequest_WaitAnyStateChange:
			waitPredicateFn = func(_ *Activity, ctx chasm.Context, _ any) (any, bool, error) {
				// TODO
				return nil, true, nil
			}
		case *workflowservice.PollActivityExecutionRequest_WaitCompletion:
			waitPredicateFn = func(activity *Activity, _ chasm.Context, _ any) (any, bool, error) {
				// TODO
				completed := activity.State() == activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED
				return nil, completed, nil
			}
		default:
			return nil, fmt.Errorf("unexpected wait policy type: %T", waitPolicy)
		}

		var operationFn func(_ *Activity, _ chasm.MutableContext, _ any, _ any) (any, error)

		_, _, err := chasm.PollComponent(
			ctx,
			chasm.NewComponentRef[*Activity](key),
			waitPredicateFn,
			operationFn,
			nil,
		)
		if err != nil {
			return nil, err
		}
	}
	return &activitypb.PollActivityExecutionResponse{
		FrontendResponse: &workflowservice.PollActivityExecutionResponse{
			Info: info,
		},
	}, nil

}
