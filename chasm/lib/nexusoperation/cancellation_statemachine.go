package nexusoperation

import (
	"time"

	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EventCancellationScheduled is triggered when cancellation is meant to be scheduled for the first time - immediately
// after it has been requested.
type EventCancellationScheduled struct {
	Time time.Time
}

var transitionCancellationScheduled = chasm.NewTransition(
	[]nexusoperationpb.CancellationStatus{nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED},
	nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
	func(c *Cancellation, ctx chasm.MutableContext, event EventCancellationScheduled) error {
		// Record when cancellation was requested
		c.RequestedTime = timestamppb.New(event.Time)

		// Emit a cancellation task
		ctx.AddTask(c, chasm.TaskAttributes{}, &nexusoperationpb.CancellationTask{
			Attempt: c.Attempt,
		})

		return nil
	},
)

// EventCancellationRescheduled is triggered when cancellation is meant to be rescheduled after backing off from a
// previous attempt.
type EventCancellationRescheduled struct {
}

var transitionCancellationRescheduled = chasm.NewTransition(
	[]nexusoperationpb.CancellationStatus{nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF},
	nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
	func(c *Cancellation, ctx chasm.MutableContext, event EventCancellationRescheduled) error {
		// Clear the next attempt schedule time
		c.NextAttemptScheduleTime = nil

		// Emit a cancellation task for the retry
		ctx.AddTask(c, chasm.TaskAttributes{}, &nexusoperationpb.CancellationTask{
			Attempt: c.Attempt,
		})

		return nil
	},
)

// EventCancellationAttemptFailed is triggered when a cancellation attempt is failed with a retryable error.
type EventCancellationAttemptFailed struct {
	Time        time.Time
	Failure     *failurepb.Failure
	RetryPolicy backoff.RetryPolicy
}

var transitionCancellationAttemptFailed = chasm.NewTransition(
	[]nexusoperationpb.CancellationStatus{nexusoperationpb.CANCELLATION_STATUS_SCHEDULED},
	nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF,
	func(c *Cancellation, ctx chasm.MutableContext, event EventCancellationAttemptFailed) error {
		// Record the attempt - increments attempt, sets complete time, clears last failure
		c.Attempt++
		c.LastAttemptCompleteTime = timestamppb.New(event.Time)
		c.LastAttemptFailure = nil

		// Compute next retry delay
		// Use 0 for elapsed time as we don't limit the retry by time (for now)
		nextDelay := event.RetryPolicy.ComputeNextDelay(0, int(c.Attempt), nil)
		nextAttemptScheduleTime := event.Time.Add(nextDelay)
		c.NextAttemptScheduleTime = timestamppb.New(nextAttemptScheduleTime)

		// Store the failure
		c.LastAttemptFailure = event.Failure

		// Emit a backoff task
		ctx.AddTask(c, chasm.TaskAttributes{
			ScheduledTime: nextAttemptScheduleTime,
		}, &nexusoperationpb.CancellationBackoffTask{
			Attempt: c.Attempt,
		})

		return nil
	},
)

// EventCancellationFailed is triggered when a cancellation attempt is failed with a non retryable error.
type EventCancellationFailed struct {
	Time    time.Time
	Failure *failurepb.Failure
}

var transitionCancellationFailed = chasm.NewTransition(
	[]nexusoperationpb.CancellationStatus{
		// We can immediately transition to failed to since we don't know how to send a cancellation request for an
		// unstarted operation.
		nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED,
		nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
	},
	nexusoperationpb.CANCELLATION_STATUS_FAILED,
	func(c *Cancellation, ctx chasm.MutableContext, event EventCancellationFailed) error {
		// Record the attempt
		c.Attempt++
		c.LastAttemptCompleteTime = timestamppb.New(event.Time)
		c.LastAttemptFailure = nil

		// Store the failure
		c.LastAttemptFailure = event.Failure

		// Terminal state - no tasks to emit
		return nil
	},
)

// EventCancellationSucceeded is triggered when a cancellation attempt succeeds.
type EventCancellationSucceeded struct {
	Time time.Time
}

var transitionCancellationSucceeded = chasm.NewTransition(
	[]nexusoperationpb.CancellationStatus{nexusoperationpb.CANCELLATION_STATUS_SCHEDULED},
	nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED,
	func(c *Cancellation, ctx chasm.MutableContext, event EventCancellationSucceeded) error {
		// Record the attempt
		c.Attempt++
		c.LastAttemptCompleteTime = timestamppb.New(event.Time)
		c.LastAttemptFailure = nil

		// Terminal state - no tasks to emit
		return nil
	},
)
