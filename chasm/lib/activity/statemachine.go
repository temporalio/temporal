package activity

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// TransitionRescheduled affects a transition to Scheduled from Started, which happens on retries. The event to pass in
// is the failure to be recorded from the previously failed attempt.
var TransitionRescheduled = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, // For retries the activity will be in started status
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	func(a *Activity, ctx chasm.MutableContext, failure *failurepb.Failure) error {
		attempt, err := a.Attempt.Get(ctx)
		if err != nil {
			return err
		}

		currentTime := ctx.Now(a)
		attempt.Count += 1

		retryInterval := failure.GetApplicationFailureInfo().GetNextRetryDelay().AsDuration()
		if retryInterval <= 0 {
			retryInterval = backoff.CalculateExponentialRetryInterval(a.GetRetryPolicy(), attempt.GetCount())
		}

		err = a.recordFailedAttempt(ctx, retryInterval, failure, false)
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
	func(a *Activity, ctx chasm.MutableContext, params RecordActivityCompletedParams) error {
		store, err := a.Store.Get(ctx)
		if err != nil {
			return err
		}

		if store == nil {
			err = a.RecordCompletion(ctx, func(ctx chasm.MutableContext) error {
				attempt, err := a.Attempt.Get(ctx)
				if err != nil {
					return err
				}

				attempt.LastAttemptCompleteTime = timestamppb.New(ctx.Now(a))
				if params.WorkerIdentity != "" {
					attempt.LastWorkerIdentity = params.WorkerIdentity
				}

				outcome, err := a.Outcome.Get(ctx)
				if err != nil {
					return err
				}

				outcome.Variant = &activitypb.ActivityOutcome_Successful_{
					Successful: &activitypb.ActivityOutcome_Successful{
						Output: params.Payload,
					},
				}

				return nil
			})
		} else {
			err = store.RecordCompletion(ctx, func(ctx chasm.MutableContext) error {
				// Implement workflow activity completion handling here, including logic to rebuild the workflow state from history if needed.
				return status.Errorf(codes.Unimplemented, "workflow activity completion handling is not implemented")
			})
		}

		return err
	},
)

// TransitionFailed affects a transition to Failed status
var TransitionFailed = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
	func(a *Activity, ctx chasm.MutableContext, params RecordActivityFailedParams) error {
		store, err := a.Store.Get(ctx)
		if err != nil {
			return err
		}

		if store == nil {
			return a.RecordCompletion(ctx, func(ctx chasm.MutableContext) error {
				if params.LastHeartbeatDetails != nil {
					heartbeatDetails, err := a.LastHeartbeat.Get(ctx)
					if err != nil {
						return err
					}

					heartbeatDetails.Details = params.LastHeartbeatDetails
					heartbeatDetails.RecordedTime = timestamppb.New(ctx.Now(a))
				}

				if params.WorkerIdentity != "" {
					attempt, err := a.Attempt.Get(ctx)
					if err != nil {
						return err
					}

					attempt.LastWorkerIdentity = params.WorkerIdentity
				}

				return a.recordFailedAttempt(ctx, 0, params.Failure, true)
			})
		}

		return store.RecordCompletion(ctx, func(ctx chasm.MutableContext) error {
			// Implement workflow failure handling here, including logic to rebuild the workflow state from history if needed.
			return status.Errorf(codes.Unimplemented, "workflow failure handling is not implemented")
		})
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
					return a.recordTimeoutFromScheduledStatus(ctx, timeoutType)
				case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
					failure := a.createStartToCloseTimeoutFailure()
					return a.recordFailedAttempt(ctx, 0, failure, true)
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
