package activity

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

// Activity is a Chasm component that represents a single activity execution.
//
// An Activity encapsulates all the state and metadata associated with an activity execution,
// including:
//   - Namespace information (namespace name and ID)
//   - Activity ID
//   - Execution information (status, timeouts, result, etc.)
type Activity struct {
	chasm.UnimplementedComponent

	*activitypb.Identifier

	Config    chasm.Field[*Config]
	ExecInfo  chasm.Field[*ExecInfo]
	Heartbeat chasm.Field[*Heartbeat]
	Input     chasm.Field[*Input]
	Outcome   chasm.Field[*Outcome]
}

//func (a Activity) LifecycleState(_ chasm.Context) chasm.LifecycleState {
//	switch a.ActivityExecutionInfo.Status {
//	case enums.ACTIVITY_EXECUTION_STATUS_COMPLETED,
//		enums.ACTIVITY_EXECUTION_STATUS_TERMINATED,
//		enums.ACTIVITY_EXECUTION_STATUS_CANCELED:
//		return chasm.LifecycleStateCompleted
//	case enums.ACTIVITY_EXECUTION_STATUS_FAILED,
//		enums.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
//		return chasm.LifecycleStateFailed
//	default:
//		return chasm.LifecycleStateRunning
//	}
//}
//
//func NewActivity(
//	mutableContext chasm.MutableContext,
//	namespace, namespaceID, activityId string,
//	activityExecutionInfo *activity.ActivityExecutionInfo) *Activity {
//
//	newActivity := &Activity{
//		ActivityState: &activitypb.ActivityState{
//			Namespace:             namespace,
//			NamespaceId:           namespaceID,
//			ActivityId:            activityId,
//			ActivityExecutionInfo: activityExecutionInfo,
//		},
//	}
//
//	mutableContext.AddTask(newActivity, chasm.TaskAttributes{}, &activitypb.ActivityStartExecuteTask{})
//
//	return newActivity
//}
//
//func GetActivity(ctx context.Context, key chasm.EntityKey) (*Activity, error) {
//	state, err := chasm.ReadComponent(
//		ctx,
//		chasm.NewComponentRef[*Activity](key),
//		func(
//			a *Activity,
//			ctx chasm.Context,
//			_ any,
//		) (*activitypb.ActivityState, error) {
//			return common.CloneProto(a.ActivityState), nil
//		},
//		nil,
//	)
//	if err != nil {
//		return nil, err
//	}
//
//	return &Activity{
//		ActivityState: state,
//	}, nil
//}
//
//func UpdateActivityStarted(
//	ctx context.Context,
//	activityRef *chasm.ComponentRef,
//) error {
//	_, _, err := chasm.UpdateComponent(
//		ctx,
//		*activityRef,
//		func(a *Activity, ctx chasm.MutableContext, _ any) (struct{}, error) {
//			a.ActivityExecutionInfo.Status = enums.ACTIVITY_EXECUTION_STATUS_RUNNING
//			return struct{}{}, nil
//		},
//		nil,
//	)
//
//	if err != nil {
//		return err
//	}
//
//	return nil
//}
