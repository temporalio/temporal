package nexusoperation

import (
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EventScheduled is triggered when the operation is meant to be scheduled - immediately after initialization.
type EventScheduled struct {
}

var transitionScheduled = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{nexusoperationpb.OPERATION_STATUS_UNSPECIFIED},
	nexusoperationpb.OPERATION_STATUS_SCHEDULED,
	func(o *Operation, ctx chasm.MutableContext, event EventScheduled) error {
		// Emit an invocation task to start the operation
		ctx.AddTask(o, chasm.TaskAttributes{}, &nexusoperationpb.InvocationTask{
			Attempt: o.Attempt,
		})

		// Emit a schedule-to-close timeout task if configured
		if o.ScheduleToCloseTimeout != nil && o.ScheduleToCloseTimeout.AsDuration() != 0 {
			deadline := o.ScheduledTime.AsTime().Add(o.ScheduleToCloseTimeout.AsDuration())
			ctx.AddTask(o, chasm.TaskAttributes{
				ScheduledTime: deadline,
			}, &nexusoperationpb.ScheduleToCloseTimeoutTask{
				Attempt: o.Attempt,
			})
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
		o.Attempt++
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
		// Emit a new invocation task for the retry attempt
		ctx.AddTask(o, chasm.TaskAttributes{}, &nexusoperationpb.InvocationTask{
			Attempt: o.Attempt,
		})

		return nil
	},
)

// EventStarted is triggered when an invocation attempt succeeds and the handler indicates that it started an
// asynchronous operation.
type EventStarted struct {
	OperationToken string
}

var transitionStarted = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_STARTED,
	func(o *Operation, ctx chasm.MutableContext, event EventStarted) error {
		currentTime := ctx.Now(o)

		// Record the attempt as successful
		o.Attempt++
		o.LastAttemptCompleteTime = timestamppb.New(currentTime)
		o.LastAttemptFailure = nil

		// Store the operation token for async completion
		o.OperationToken = event.OperationToken

		return nil
	},
)

// EventSucceeded is triggered when an invocation attempt succeeds.
type EventSucceeded struct {
}

var transitionSucceeded = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_STARTED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
	func(o *Operation, ctx chasm.MutableContext, event EventSucceeded) error {
		// Terminal state - no tasks to emit
		// The component will be deleted after this transition
		return nil
	},
)

// EventFailed is triggered when an invocation attempt is failed with a non retryable error.
type EventFailed struct {
}

var transitionFailed = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_STARTED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_FAILED,
	func(o *Operation, ctx chasm.MutableContext, event EventFailed) error {
		// Terminal state - no tasks to emit
		// Not recording the last attempt information here since the state machine
		// will be deleted immediately after this transition
		return nil
	},
)

// EventCanceled is triggered when an operation is completed as canceled.
type EventCanceled struct {
}

var transitionCanceled = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_STARTED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_CANCELED,
	func(o *Operation, ctx chasm.MutableContext, event EventCanceled) error {
		// Terminal state - no tasks to emit
		// The component will be deleted after this transition
		return nil
	},
)

// EventTimedOut is triggered when the schedule-to-close timeout is triggered for an operation.
type EventTimedOut struct {
}

var transitionTimedOut = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_STARTED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
	func(o *Operation, ctx chasm.MutableContext, event EventTimedOut) error {
		// Terminal state - no tasks to emit
		// The component will be deleted after this transition
		return nil
	},
)
