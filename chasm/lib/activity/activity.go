package activity

import (
	"context"
	"fmt"

	"go.temporal.io/api/activity/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Activity struct {
	chasm.UnimplementedComponent

	*activitypb.ActivityState
}

func (a Activity) LifecycleState(context chasm.Context) chasm.LifecycleState {
	switch a.ActivityState.Status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		return chasm.LifecycleStateCompleted
	case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

// GetActivityExecutionInfo constructs an ActivityExecutionInfo from the CHASM activity component.
// namespace, activityID, and runID come from the CHASM component key, not from persisted data.
func (a *Activity) GetActivityExecutionInfo(key chasm.EntityKey) *activity.ActivityExecutionInfo {
	if a.ActivityState == nil {
		return nil
	}

	// Convert internal status to external status and run state
	var status enums.ActivityExecutionStatus
	var runState enums.PendingActivityState

	switch a.ActivityState.Status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED:
		status = enums.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
		runState = enums.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED:
		status = enums.ACTIVITY_EXECUTION_STATUS_RUNNING
		runState = enums.PENDING_ACTIVITY_STATE_SCHEDULED
	case activitypb.ACTIVITY_EXECUTION_STATUS_STARTED:
		status = enums.ACTIVITY_EXECUTION_STATUS_RUNNING
		runState = enums.PENDING_ACTIVITY_STATE_STARTED
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED:
		status = enums.ACTIVITY_EXECUTION_STATUS_RUNNING
		runState = enums.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
	// TODO: Add pause support
	// case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED:
	// 	status = enums.ACTIVITY_EXECUTION_STATUS_RUNNING
	// 	runState = enums.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED
	// case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED:
	// 	status = enums.ACTIVITY_EXECUTION_STATUS_RUNNING
	// 	runState = enums.PENDING_ACTIVITY_STATE_PAUSED
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
		status = enums.ACTIVITY_EXECUTION_STATUS_COMPLETED
		runState = enums.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED:
		status = enums.ACTIVITY_EXECUTION_STATUS_FAILED
		runState = enums.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		status = enums.ACTIVITY_EXECUTION_STATUS_CANCELED
		runState = enums.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED:
		status = enums.ACTIVITY_EXECUTION_STATUS_TERMINATED
		runState = enums.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		status = enums.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
		runState = enums.PENDING_ACTIVITY_STATE_UNSPECIFIED
	}

	info := &activity.ActivityExecutionInfo{
		ActivityType:    a.ActivityState.ActivityType,
		ActivityOptions: a.ActivityState.ActivityOptions,
		Status:          status,
		RunState:        runState,
		ScheduledTime:   a.ActivityState.ScheduledTime,
		Priority:        a.ActivityState.Priority,
		Input:           a.RequestData.Input,
		Header:          a.RequestData.Header,

		// TODO: These fields are left at zero value for now:
		// - StartedTime
		// - LastHeartbeatTime
		// - HeartbeatDetails
		// - RetryInfo
		// - AutoResetPoints
		// - ClockTime
	}

	return info
}

func NewActivity(request *workflowservice.StartActivityExecutionRequest) *Activity {
	return &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:    request.ActivityType,
			ActivityOptions: request.Options,
			// TODO(dan): is this the correct way to compute this timestamp?
			ScheduledTime: timestamppb.New(clock.NewRealTimeSource().Now()),
			RequestData: &activitypb.ActivityRequestData{
				Input:        request.Input,
				Header:       request.Header,
				UserMetadata: request.UserMetadata,
			},
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
			fmt.Println("Reading activity state for", key.BusinessID)

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
