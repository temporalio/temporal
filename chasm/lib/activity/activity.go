package activity

import (
	"context"
	"fmt"

	"go.temporal.io/api/activity/v1"
	"go.temporal.io/api/enums/v1"
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

	return &Activity{
		ActivityState: &activitypb.ActivityState{
			Namespace:             namespace,
			NamespaceId:           namespaceID,
			ActivityId:            activityId,
			ActivityExecutionInfo: activityExecutionInfo,
		},
	}
}

func GetActivity(ctx context.Context, req *activitypb.DescribeActivityExecutionRequest) (*Activity, error) {
	state, err := chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*Activity](
			chasm.EntityKey{
				NamespaceID: req.NamespaceId,
				BusinessID:  req.GetFrontendRequest().GetExecution().GetActivityId(),
				EntityID:    req.GetFrontendRequest().GetExecution().GetRunId(), // TODO this is hacked
			},
		),
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

//
//func (a *Activity) describe(
//	_ chasm.Context,
//	_ DescribePayloadStoreRequest,
//) (*testspb.TestPayloadStore, error) {
//	return common.CloneProto(s.State), nil
//}
