package activity

import (
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
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

type rescheduleEvent struct {
	retryInterval time.Duration
	failure       *failurepb.Failure
}

// TransitionRescheduled affects a transition to Scheduled from Started, which happens on retries. The event to pass in
// is the failure to be recorded from the previously failed attempt.
var TransitionRescheduled = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, // For retries the activity will be in started status
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
	func(a *Activity, ctx chasm.MutableContext, event rescheduleEvent) error {
		attempt, err := a.Attempt.Get(ctx)
		if err != nil {
			return err
		}

		currentTime := ctx.Now(a)
		attempt.Count += 1

		err = a.recordFailedAttempt(ctx, event.retryInterval, event.failure, false)
		if err != nil {
			return err
		}

		if timeout := a.GetScheduleToStartTimeout().AsDuration(); timeout > 0 {
			ctx.AddTask(
				a,
				chasm.TaskAttributes{
					ScheduledTime: currentTime.Add(timeout).Add(event.retryInterval),
				},
				&activitypb.ScheduleToStartTimeoutTask{
					Attempt: attempt.GetCount(),
				})
		}

		ctx.AddTask(
			a,
			chasm.TaskAttributes{
				ScheduledTime: currentTime.Add(event.retryInterval),
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
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
	func(a *Activity, ctx chasm.MutableContext, request *historyservice.RespondActivityTaskCompletedRequest) error {
		// TODO: after rebase on main, don't need error and add a helper store := a.LoadStore(ctx)
		store, err := a.Store.Get(ctx)
		if err != nil {
			return err
		}

		if store == nil {
			store = a
		}

		return store.RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			attempt, err := a.Attempt.Get(ctx)
			if err != nil {
				return err
			}

			attempt.CompleteTime = timestamppb.New(ctx.Now(a))
			attempt.LastWorkerIdentity = request.GetCompleteRequest().GetIdentity()

			outcome, err := a.Outcome.Get(ctx)
			if err != nil {
				return err
			}

			outcome.Variant = &activitypb.ActivityOutcome_Successful_{
				Successful: &activitypb.ActivityOutcome_Successful{
					Output: request.GetCompleteRequest().GetResult(),
				},
			}

			return nil
		})
	},
)

// TransitionFailed affects a transition to Failed status
var TransitionFailed = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
	func(a *Activity, ctx chasm.MutableContext, req *historyservice.RespondActivityTaskFailedRequest) error {
		store, err := a.Store.Get(ctx)
		if err != nil {
			return err
		}

		if store == nil {
			store = a
		}

		return store.RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			if details := req.GetFailedRequest().GetLastHeartbeatDetails(); details != nil {
				heartbeat, err := a.getLastHeartbeat(ctx)
				if err != nil {
					return err
				}

				heartbeat.Details = details
				heartbeat.RecordedTime = timestamppb.New(ctx.Now(a))
			}

			attempt, err := a.Attempt.Get(ctx)
			if err != nil {
				return err
			}

			attempt.LastWorkerIdentity = req.GetFailedRequest().GetIdentity()

			return a.recordFailedAttempt(ctx, 0, req.GetFailedRequest().GetFailure(), true)
		})
	},
)

// TransitionTerminated affects a transition to Terminated status
var TransitionTerminated = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
	func(a *Activity, ctx chasm.MutableContext, req *activitypb.TerminateActivityExecutionRequest) error {
		store, err := a.Store.Get(ctx)
		if err != nil {
			return err
		}

		if store == nil {
			store = a
		}

		return store.RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			attempt, err := a.Attempt.Get(ctx)
			if err != nil {
				return err
			}

			if identity := req.GetFrontendRequest().GetIdentity(); identity != "" {
				attempt.LastWorkerIdentity = identity
			}

			outcome, err := a.Outcome.Get(ctx)
			if err != nil {
				return err
			}

			failure := &failurepb.Failure{
				Message:     req.GetFrontendRequest().GetReason(),
				FailureInfo: &failurepb.Failure_TerminatedFailureInfo{},
			}

			outcome.Variant = &activitypb.ActivityOutcome_Failed_{
				Failed: &activitypb.ActivityOutcome_Failed{
					Failure: failure,
				},
			}

			return nil
		})
	},
)

// TransitionCancelRequested affects a transition to CancelRequested status
var TransitionCancelRequested = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED, // Allow idempotent transition
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	func(a *Activity, ctx chasm.MutableContext, req *workflowservice.RequestCancelActivityExecutionRequest) error {

		a.CancelState = &activitypb.ActivityCancelState{
			Identity:    req.GetIdentity(),
			RequestId:   req.GetRequestId(),
			Reason:      req.GetReason(),
			RequestTime: timestamppb.New(ctx.Now(a)),
		}

		return nil
	},
)

// TransitionCanceled affects a transition to Canceled status
var TransitionCanceled = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
	func(a *Activity, ctx chasm.MutableContext, details *commonpb.Payloads) error {
		store, err := a.Store.Get(ctx)
		if err != nil {
			return err
		}

		if store == nil {
			store = a
		}

		return store.RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			outcome, err := a.Outcome.Get(ctx)
			if err != nil {
				return err
			}

			failure := &failurepb.Failure{
				FailureInfo: &failurepb.Failure_CanceledFailureInfo{
					CanceledFailureInfo: &failurepb.CanceledFailureInfo{
						Details: details,
					},
				},
			}

			outcome.Variant = &activitypb.ActivityOutcome_Failed_{
				Failed: &activitypb.ActivityOutcome_Failed{
					Failure: failure,
				},
			}

			return nil
		})
	},
)

// TransitionTimedOut affects a transition to TimedOut status
var TransitionTimedOut = chasm.NewTransition(
	[]activitypb.ActivityExecutionStatus{
		activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
	},
	activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
	func(a *Activity, ctx chasm.MutableContext, timeoutType enumspb.TimeoutType) error {
		store, err := a.Store.Get(ctx)
		if err != nil {
			return err
		}

		if store == nil {
			store = a
		}

		return store.RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
			switch timeoutType {
			case enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
				enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE:
				return a.recordScheduleToStartOrCloseTimeoutFailure(ctx, timeoutType)
			case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
				failure := createStartToCloseTimeoutFailure()
				return a.recordFailedAttempt(ctx, 0, failure, true)
			default:
				return fmt.Errorf("unhandled activity timeout: %v", timeoutType)
			}
		})
	},
)
