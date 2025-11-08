package activity

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Ensure that Activity implements chasm.StateMachine interface
var _ chasm.StateMachine[activitypb.ActivityExecutionStatus] = (*Activity)(nil)

// StateMachineState returns the current status of the activity.
func (a *Activity) StateMachineState() activitypb.ActivityExecutionStatus {
	if a.ActivityState == nil {
		return activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
	}
	return a.Status
}

// SetStateMachineState sets the status of the activity.
func (a *Activity) SetStateMachineState(state activitypb.ActivityExecutionStatus) {
	a.Status = state
}

// TransitionScheduled affects a transition to Scheduled status. This is only called on the initial scheduling of the activity.
var TransitionScheduled = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	func(a *Activity, ctx chasm.MutableContext, _ any) error {
		attempt, err := a.Attempt.Get(ctx)
		if err != nil {
			return err
		}

		currentTime := ctx.Now(a)
		attempt.Count += 1

		if timeout := a.GetScheduleToStartTimeout().AsDuration(); timeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: currentTime.Add(timeout),
				},
				&activitypb.ScheduleToStartTimeoutTask{
					Attempt: attempt.GetCount(),
				})
		}

		if timeout := a.GetScheduleToCloseTimeout().AsDuration(); timeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: currentTime.Add(timeout),
				},
				&activitypb.ScheduleToCloseTimeoutTask{
					Attempt: attempt.GetCount(),
				})
		}

		ctx.AddTask(
			a,
			chasm.TaskAttributes{},
			&activitypb.ActivityDispatchTask{
				Attempt: attempt.GetCount(),
			})

		return nil
	},
)

// TransitionRescheduled affects a transition to Scheduled from Started, which happens on retries
var TransitionRescheduled = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, // On retries for start-to-close timeouts
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	func(a *Activity, ctx chasm.MutableContext, _ any) error {
		attempt, err := a.Attempt.Get(ctx)
		if err != nil {
			return err
		}

		currentTime := ctx.Now(a)
		attempt.Count += 1

		// If this is a retry, calculate the delay before scheduling tasks and update attempt fields
		// TODO: for activity failures it'll go through this retry path as well; we'll need to refactor the record timeout and retryInterval recording, probably passed as an event func
		retryInterval := backoff.CalculateExponentialRetryInterval(a.GetRetryPolicy(), attempt.GetCount())
		err = a.recordStartToCloseTimedOut(ctx, retryInterval, false)
		if err != nil {
			return err
		}

		if timeout := a.GetScheduleToStartTimeout().AsDuration(); timeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: currentTime.Add(timeout).Add(retryInterval),
				},
				&activitypb.ScheduleToStartTimeoutTask{
					Attempt: attempt.GetCount(),
				})
		}

		ctx.AddTask(
			a,
			chasm.TaskAttributes{
				ScheduledTime: currentTime.Add(retryInterval),
			},
			&activitypb.ActivityDispatchTask{
				Attempt: attempt.GetCount(),
			})

		return nil
	},
)

// TransitionStarted affects a transition to Started status
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
				ScheduledTime: ctx.Now(a).Add(a.GetStartToCloseTimeout().AsDuration()),
			},
			&activitypb.StartToCloseTimeoutTask{
				Attempt: attempt.GetCount(),
			})

		return nil
	},
)

// TransitionCompleted affects a transition to Completed status
var TransitionCompleted = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
	func(_ *Activity, _ chasm.MutableContext, _ any) error {
		return nil
	},
)

// TransitionFailed affects a transition to Failed status
var TransitionFailed = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
	func(_ *Activity, _ chasm.MutableContext, _ any) error {
		return nil
	},
)

// TransitionTerminated affects a transition to Terminated status
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

// TransitionTimedOut affects a transition to TimedOut status
var TransitionTimedOut = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
	func(a *Activity, ctx chasm.MutableContext, timeoutType enumspb.TimeoutType) error {
		store, err := a.Store.Get(ctx)
		if err != nil {
			return err
		}

		if store == nil {
			return a.RecordCompletion(ctx, func(ctx chasm.MutableContext) error {
				switch timeoutType {
				case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
					enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
					return a.recordFromScheduledTimeOut(ctx, timeoutType)
				case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
					return a.recordStartToCloseTimedOut(ctx, 0, true)
				default:
					return fmt.Errorf("unhandled activity timeout: %v", timeoutType)
				}
			})
		}

		return store.RecordCompletion(ctx, func(ctx chasm.MutableContext) error {
			// Implement workflow timeout handling here, including logic to rebuild the workflow state from history if needed.
			return status.Errorf(codes.Unimplemented, "workflow timeout handling is not implemented")
		})
	},
)
