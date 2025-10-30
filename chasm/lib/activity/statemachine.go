package activity

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
)

type scheduledEvent struct {
	TaskStartDelay time.Duration // Delay before the activity tasks should be run, e.g., for retries
}

type timeoutEvent struct {
	TimeoutType enumspb.TimeoutType
}

// Ensure that Activity implements chasm.StateMachine interface
var _ chasm.StateMachine[activitypb.ActivityExecutionStatus] = (*Activity)(nil)

// StateMachineState State returns the current status of the activity.
func (a *Activity) StateMachineState() activitypb.ActivityExecutionStatus {
	if a.ActivityState == nil {
		return activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
	}
	return a.Status
}

// SetStateMachineState SetState sets the status of the activity.
func (a *Activity) SetStateMachineState(state activitypb.ActivityExecutionStatus) {
	a.Status = state
}

// TransitionScheduled effects a transition to Scheduled status
var TransitionScheduled = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, // On retries for start-to-close timeouts
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	func(a *Activity, ctx chasm.MutableContext, scheduledEvent scheduledEvent) error {
		attempt, err := a.Attempt.Get(ctx)
		if err != nil {
			return err
		}

		currentTime := time.Now()

		if timeout := a.GetScheduleToStartTimeout().AsDuration(); timeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: currentTime.Add(timeout).Add(scheduledEvent.TaskStartDelay),
				},
				&activitypb.ScheduleToStartTimeoutTask{
					Attempt: attempt.GetCount(),
				})
		}

		if timeout := a.GetScheduleToCloseTimeout().AsDuration(); timeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: currentTime.Add(timeout).Add(scheduledEvent.TaskStartDelay),
				},
				&activitypb.ScheduleToCloseTimeoutTask{
					Attempt: attempt.GetCount(),
				})
		}

		ctx.AddTask(
			a,
			chasm.TaskAttributes{
				ScheduledTime: currentTime.Add(scheduledEvent.TaskStartDelay),
			},
			&activitypb.ActivityDispatchTask{
				Attempt: attempt.GetCount(),
			})

		return nil
	},
)

// TransitionStarted effects a transition to Started status
var TransitionStarted = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	func(a *Activity, ctx chasm.MutableContext, _ any) error {
		attempt, err := a.Attempt.Get(ctx)
		if err != nil {
			return err
		}

		ctx.AddTask(
			a,
			chasm.TaskAttributes{
				ScheduledTime: time.Now().Add(a.GetStartToCloseTimeout().AsDuration()),
			},
			&activitypb.StartToCloseTimeoutTask{
				Attempt: attempt.GetCount(),
			})

		return nil
	},
)

// TransitionCompleted effects a transition to Completed status
var TransitionCompleted = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
	func(_ *Activity, _ chasm.MutableContext, _ any) error {
		return nil
	},
)

// TransitionFailed effects a transition to Failed status
var TransitionFailed = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
	func(_ *Activity, _ chasm.MutableContext, _ any) error {
		return nil
	},
)

// TransitionTerminated effects a transition to Terminated status
var TransitionTerminated = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
	func(_ *Activity, _ chasm.MutableContext, _ any) error {
		return nil
	},
)

// TransitionTimedOut effects a transition to TimedOut status
var TransitionTimedOut = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
	func(a *Activity, ctx chasm.MutableContext, event timeoutEvent) error {
		return a.handleActivityTimedOut(ctx, event.TimeoutType)
	},
)
