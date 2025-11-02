package activity

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
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

// TransitionScheduled effects a transition to Scheduled status
var TransitionScheduled = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, // On retries for start-to-close timeouts
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	func(a *Activity, ctx chasm.MutableContext, _ any) error {
		attempt, err := a.Attempt.Get(ctx)
		if err != nil {
			return err
		}

		currentTime := ctx.Now(a)
		retryInterval := time.Duration(0)
		isRetry := attempt.GetCount() >= 1
		attempt.Count += 1

		// If this is a retry, calculate the delay before scheduling tasks and update attempt fields
		if isRetry {
			retryInterval = backoff.CalculateExponentialRetryInterval(a.GetRetryPolicy(), attempt.GetCount())
			attempt.CurrentRetryInterval = durationpb.New(retryInterval)
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

		if timeout := a.GetScheduleToCloseTimeout().AsDuration(); timeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: currentTime.Add(timeout).Add(retryInterval),
				},
				&activitypb.ScheduleToCloseTimeoutTask{
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
				ScheduledTime: ctx.Now(a).Add(a.GetStartToCloseTimeout().AsDuration()),
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
	func(a *Activity, ctx chasm.MutableContext, timeoutType enumspb.TimeoutType) error {
		store, err := a.Store.Get(ctx)
		if err != nil {
			return err
		}

		if store == nil {
			return a.RecordCompletion(ctx, func(ctx chasm.MutableContext) error {
				return a.recordActivityTimedOut(ctx, timeoutType)
			})
		}

		return store.RecordCompletion(ctx, func(ctx chasm.MutableContext) error {
			// Implement workflow timeout handling here, including logic to rebuild the workflow state from history if needed.
			return status.Errorf(codes.Unimplemented, "method StartActivityExecution not implemented")
		})
	},
)
