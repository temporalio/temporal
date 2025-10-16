package activity

import (
	"go.temporal.io/server/chasm"
	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

// Ensure that Activity implements chasm.StateMachine interface
var _ chasm.StateMachine[activitypb.ActivityExecutionStatus] = (*Activity)(nil)

// State returns the current status of the activity.
func (a *Activity) State() activitypb.ActivityExecutionStatus {
	if a.ActivityState == nil {
		return activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
	}
	return a.ActivityState.Status
}

// SetState sets the status of the activity.
func (a *Activity) SetState(state activitypb.ActivityExecutionStatus) {
	a.ActivityState.Status = state
}

var (
	TransitionScheduled = chasm.NewTransition(
		[]activitypb.ActivityExecutionStatus{
			activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED,
		},
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		func(_ chasm.MutableContext, _ *Activity, _ any) error {
			return nil
		},
	)

	TransitionStarted = chasm.NewTransition(
		[]activitypb.ActivityExecutionStatus{
			activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		},
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		func(_ chasm.MutableContext, _ *Activity, _ any) error {
			return nil
		},
	)

	TransitionCompleted = chasm.NewTransition(
		[]activitypb.ActivityExecutionStatus{
			activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		func(_ chasm.MutableContext, _ *Activity, _ any) error {
			return nil
		},
	)

	TransitionFailed = chasm.NewTransition(
		[]activitypb.ActivityExecutionStatus{
			activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
		func(_ chasm.MutableContext, _ *Activity, _ any) error {
			return nil
		},
	)

	TransitionTerminated = chasm.NewTransition(
		[]activitypb.ActivityExecutionStatus{
			activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		func(_ chasm.MutableContext, _ *Activity, _ any) error {
			return nil
		},
	)

	TransitionTimedOut = chasm.NewTransition(
		[]activitypb.ActivityExecutionStatus{
			activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
			activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		},
		activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		func(_ chasm.MutableContext, _ *Activity, _ any) error {
			return nil
		},
	)
)
