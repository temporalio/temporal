package activity

import (
	"context"
	"fmt"

	"go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
)

type Activity struct {
	chasm.UnimplementedComponent

	*activitypb.ActivityState
}

func (a Activity) LifecycleState(context chasm.Context) chasm.LifecycleState {
	switch a.ActivityExecutionInfo.Status {
	case enums.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		enums.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		enums.ACTIVITY_EXECUTION_STATUS_CANCELED:
		return chasm.LifecycleStateCompleted
	case enums.ACTIVITY_EXECUTION_STATUS_FAILED,
		enums.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

func NewActivity(namespace, namespaceID, activityId string,
	activityExecutionInfo *activity.ActivityExecutionInfo) *Activity {

	// Do task here

	return &Activity{
		ActivityState: &activitypb.ActivityState{
			Namespace:             namespace,
			NamespaceId:           namespaceID,
			ActivityId:            activityId,
			ActivityExecutionInfo: activityExecutionInfo,
		},
	}
}

func GetActivity(ctx context.Context, key chasm.EntityKey) (*Activity, error) {
	state, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*Activity](key),
		func(
			a *Activity,
			ctx chasm.Context,
			_ any,
		) (*activitypb.ActivityState, error) {
			fmt.Println("Reading activity state for", a.ActivityId)

			return common.CloneProto(a.ActivityState), nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &Activity{
		ActivityState: state,
	}, nil
}

// TODO(dan): Let's collect here the heuristics we use to classify different types of requests as
// being for chasm (i.e. standalone) activities.
func ShouldHandle(req any) bool {
	switch req := req.(type) {
	case *historyservice.RecordActivityTaskStartedRequest:
		return req.GetComponentRef() != nil
	}
	return false
}

// TODO(dan): This one takes token to avoid deserializing it twice.
func ShouldHandleRespondActivityTaskCompleted(req any, token *tokenspb.Task) bool {
	switch req.(type) {
	case *historyservice.RespondActivityTaskCompletedRequest:
		return token.WorkflowId == ""
	}
	return false

}

// An alternative design would be to allow the Handle* functions below to return a nil response to
// indicate that something else should handle it.

func HandleRecordActivityTaskStarted(
	ctx context.Context,
	req *historyservice.RecordActivityTaskStartedRequest,
) (*historyservice.RecordActivityTaskStartedResponse, error) {
	componentRefProto := req.GetComponentRef()
	if componentRefProto == nil {
		return nil, fmt.Errorf("component ref is required")
	}
	componentRef := chasm.ProtoRefToComponentRef(componentRefProto)

	var activityType *commonpb.ActivityType

	_, _, err := chasm.UpdateComponent(
		ctx,
		componentRef,
		func(a *Activity, ctx chasm.MutableContext, _ any) (struct{}, error) {
			a.ActivityExecutionInfo.Status = enums.ACTIVITY_EXECUTION_STATUS_RUNNING
			activityType = a.ActivityExecutionInfo.ActivityType
			return struct{}{}, nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &historyservice.RecordActivityTaskStartedResponse{
		ScheduledEvent: &history.HistoryEvent{
			EventType: enums.EVENT_TYPE_ACTIVITY_TASK_STARTED,
			Attributes: &history.HistoryEvent_ActivityTaskScheduledEventAttributes{
				ActivityTaskScheduledEventAttributes: &history.ActivityTaskScheduledEventAttributes{
					ActivityId:   componentRef.BusinessID,
					ActivityType: activityType,
				},
			},
		},
	}, nil
}

// This is a handler for a workflowservice method (as opposed to a method in the service owned by
// this chasm component).
// TODO(dan): What is the right place for this?
func HandleRespondActivityTaskCompleted(
	ctx context.Context,
	req *historyservice.RespondActivityTaskCompletedRequest,
	key chasm.EntityKey,
) (*historyservice.RespondActivityTaskCompletedResponse, error) {
	chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Activity](key),
		func(a *Activity, ctx chasm.MutableContext, _ any) (struct{}, error) {
			a.ActivityExecutionInfo.Status = enums.ACTIVITY_EXECUTION_STATUS_COMPLETED
			a.Outcome = &activitypb.ActivityState_Result{
				Result: req.CompleteRequest.Result,
			}
			return struct{}{}, nil
		}, nil)

	// TODO(dan): any response struct fields?
	return &historyservice.RespondActivityTaskCompletedResponse{}, nil
}
