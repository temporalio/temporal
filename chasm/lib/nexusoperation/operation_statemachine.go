package nexusoperation

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EventScheduled is triggered when the operation is meant to be scheduled - immediately after initialization.
type EventScheduled struct {
}

var TransitionScheduled = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{nexusoperationpb.OPERATION_STATUS_UNSPECIFIED},
	nexusoperationpb.OPERATION_STATUS_SCHEDULED,
	func(o *Operation, ctx chasm.MutableContext, event EventScheduled) error {
		o.Attempt++
		// Emit an invocation task to start the operation.
		// The destination is the endpoint name, which routes the task to the correct outbound queue.
		ctx.AddTask(o, chasm.TaskAttributes{Destination: o.GetEndpoint()}, &nexusoperationpb.InvocationTask{
			Attempt: o.Attempt,
		})

		// Emit a schedule-to-start timeout task if configured
		if o.ScheduleToStartTimeout != nil && o.ScheduleToStartTimeout.AsDuration() != 0 {
			deadline := o.ScheduledTime.AsTime().Add(o.ScheduleToStartTimeout.AsDuration())
			ctx.AddTask(o, chasm.TaskAttributes{
				ScheduledTime: deadline,
			}, &nexusoperationpb.ScheduleToStartTimeoutTask{})
		}

		// Emit a schedule-to-close timeout task if configured
		if o.ScheduleToCloseTimeout != nil && o.ScheduleToCloseTimeout.AsDuration() != 0 {
			deadline := o.ScheduledTime.AsTime().Add(o.ScheduleToCloseTimeout.AsDuration())
			ctx.AddTask(o, chasm.TaskAttributes{
				ScheduledTime: deadline,
			}, &nexusoperationpb.ScheduleToCloseTimeoutTask{})
		}

		return nil
	},
)

// EventAttemptFailed is triggered when an invocation attempt is failed with a retryable error.
type EventAttemptFailed struct {
	Failure     *failurepb.Failure
	RetryPolicy backoff.RetryPolicy
}

var transitionAttemptFailed = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{nexusoperationpb.OPERATION_STATUS_SCHEDULED},
	nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	func(o *Operation, ctx chasm.MutableContext, event EventAttemptFailed) error {
		currentTime := ctx.Now(o)

		// Record the attempt
		o.LastAttemptCompleteTime = timestamppb.New(currentTime)
		o.LastAttemptFailure = event.Failure

		// Compute next retry delay
		// Use 0 for elapsed time as we don't limit the retry by time (for now)
		// The last argument (error) is ignored
		nextDelay := event.RetryPolicy.ComputeNextDelay(0, int(o.Attempt), nil)
		nextAttemptScheduleTime := currentTime.Add(nextDelay)
		o.NextAttemptScheduleTime = timestamppb.New(nextAttemptScheduleTime)

		// Emit a backoff task to retry after the delay
		ctx.AddTask(o, chasm.TaskAttributes{
			ScheduledTime: nextAttemptScheduleTime,
		}, &nexusoperationpb.InvocationBackoffTask{
			Attempt: o.Attempt,
		})

		return nil
	},
)

// EventRescheduled is triggered when the operation is meant to be rescheduled after backing off from a previous
// attempt.
type EventRescheduled struct {
}

var transitionRescheduled = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{nexusoperationpb.OPERATION_STATUS_BACKING_OFF},
	nexusoperationpb.OPERATION_STATUS_SCHEDULED,
	func(o *Operation, ctx chasm.MutableContext, event EventRescheduled) error {
		o.Attempt++
		// Clear the next attempt schedule time
		o.NextAttemptScheduleTime = nil

		// Emit a new invocation task for the retry attempt
		ctx.AddTask(o, chasm.TaskAttributes{Destination: o.GetEndpoint()}, &nexusoperationpb.InvocationTask{
			Attempt: o.Attempt,
		})

		return nil
	},
)

// EventStarted is triggered when an invocation attempt succeeds and the handler indicates that it started an
// asynchronous operation.
type EventStarted struct {
	OperationToken string
	// If not nil, uses the provided time instead of the current component time.
	// Used when a completion comes in before start is recorded (rare race).
	StartTime *time.Time
}

var TransitionStarted = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_STARTED,
	func(o *Operation, ctx chasm.MutableContext, event EventStarted) error {
		startTime := ctx.Now(o)
		if event.StartTime != nil {
			startTime = *event.StartTime
		}

		o.StartedTime = timestamppb.New(startTime)
		// Also consider this the completion of an attempt even if the task's saveResult call lost the race with
		// the async completion.
		o.LastAttemptCompleteTime = o.StartedTime
		o.LastAttemptFailure = nil
		// Clear the next attempt schedule time when leaving BACKING_OFF state.
		// This field is only valid in BACKING_OFF state.
		o.NextAttemptScheduleTime = nil

		// Store the operation token for async completion.
		o.OperationToken = event.OperationToken

		// Emit a start-to-close timeout task if configured.
		if o.StartToCloseTimeout != nil && o.StartToCloseTimeout.AsDuration() != 0 {
			deadline := startTime.Add(o.StartToCloseTimeout.AsDuration())
			ctx.AddTask(o, chasm.TaskAttributes{
				ScheduledTime: deadline,
			}, &nexusoperationpb.StartToCloseTimeoutTask{})
		}

		// If cancellation was already requested, schedule sending the cancellation request now that we have
		// an operation token.
		cancellation, ok := o.Cancellation.TryGet(ctx)
		if ok && cancellation.StateMachineState() == nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED {
			return TransitionCancellationScheduled.Apply(cancellation, ctx, EventCancellationScheduled{
				Destination: o.GetEndpoint(),
			})
		}

		return nil
	},
)

// EventSucceeded is triggered when an invocation attempt succeeds.
type EventSucceeded struct {
	// If not nil, uses the provided time instead of the current component time.
	// Used when a completion comes in before start is recorded (rare race).
	CompleteTime *time.Time
	Result       *commonpb.Payload
}

var TransitionSucceeded = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_STARTED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
	func(o *Operation, ctx chasm.MutableContext, event EventSucceeded) error {
		// Clear the next attempt schedule time when leaving BACKING_OFF state. This field is only valid in
		// BACKING_OFF state.
		closeTime := ctx.Now(o)
		if event.CompleteTime != nil {
			closeTime = *event.CompleteTime
		}
		o.NextAttemptScheduleTime = nil
		o.ClosedTime = timestamppb.New(closeTime)

		o.getOrCreateOutcome(ctx).Variant = &nexusoperationpb.OperationOutcome_Successful_{
			Successful: &nexusoperationpb.OperationOutcome_Successful{Result: event.Result},
		}

		// Terminal state - no tasks to emit.
		return nil
	},
)

// EventFailed is triggered when an invocation attempt is failed with a non retryable error.
type EventFailed struct {
	// If not nil, uses the provided time instead of the current component time.
	// Used when a completion comes in before start is recorded (rare race).
	CompleteTime *time.Time
	Failure      *failurepb.Failure
}

var TransitionFailed = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_STARTED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_FAILED,
	func(o *Operation, ctx chasm.MutableContext, event EventFailed) error {
		closeTime := ctx.Now(o)
		if event.CompleteTime != nil {
			closeTime = *event.CompleteTime
		}
		// Attempts only execute in SCHEDULED, so that status identifies attempt-originated failures.
		fromAttempt := o.GetStatus() == nexusoperationpb.OPERATION_STATUS_SCHEDULED
		return o.resolveUnsuccessfully(ctx, event.Failure, closeTime, fromAttempt)
	},
)

// EventCanceled is triggered when an operation is completed as canceled.
type EventCanceled struct {
	// If not nil, uses the provided time instead of the current component time.
	// Used when a completion comes in before start is recorded (rare race).
	CompleteTime *time.Time
	Failure      *failurepb.Failure
}

var TransitionCanceled = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_STARTED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_CANCELED,
	func(o *Operation, ctx chasm.MutableContext, event EventCanceled) error {
		closeTime := ctx.Now(o)
		if event.CompleteTime != nil {
			closeTime = *event.CompleteTime
		}
		// Attempts only execute in SCHEDULED, so that status identifies attempt-originated cancels.
		fromAttempt := o.GetStatus() == nexusoperationpb.OPERATION_STATUS_SCHEDULED
		return o.resolveUnsuccessfully(ctx, event.Failure, closeTime, fromAttempt)
	},
)

// EventTerminated is triggered when the operation is terminated by user request.
type EventTerminated struct {
	chasm.TerminateComponentRequest
}

var TransitionTerminated = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_STARTED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
		nexusoperationpb.OPERATION_STATUS_CANCELED,
	},
	nexusoperationpb.OPERATION_STATUS_TERMINATED,
	func(o *Operation, ctx chasm.MutableContext, event EventTerminated) error {
		closeTime := ctx.Now(o)
		o.TerminateState = &nexusoperationpb.NexusOperationTerminateState{
			RequestId: event.RequestID,
		}
		failure := &failurepb.Failure{
			Message: event.Reason,
			FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
				TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{
					Identity: event.Identity,
				},
			},
		}
		return o.resolveUnsuccessfully(ctx, failure, closeTime, false)
	},
)

// EventTimedOut is triggered when the schedule-to-close timeout is triggered for an operation.
type EventTimedOut struct {
	Failure *failurepb.Failure
	// FromAttempt is true when the failure came from an invocation attempt.
	FromAttempt bool
}

var TransitionTimedOut = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_STARTED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
	func(o *Operation, ctx chasm.MutableContext, event EventTimedOut) error {
		return o.resolveUnsuccessfully(ctx, event.Failure, ctx.Now(o), event.FromAttempt)
	},
)
