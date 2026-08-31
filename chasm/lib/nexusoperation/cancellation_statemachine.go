package nexusoperation

import (
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EventCancellationScheduled is triggered when cancellation is meant to be scheduled for the first time - immediately
// after it has been requested.
type EventCancellationScheduled struct {
	// Destination is the endpoint name for the cancellation task.
	// Must be provided by the caller because ParentPtr is not available during inline creation.
	Destination string
}

var TransitionCancellationScheduled = chasm.NewTransition(
	[]nexusoperationpb.CancellationStatus{nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED},
	nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
	func(c *Cancellation, ctx chasm.MutableContext, event EventCancellationScheduled) error {
		c.Attempt++

		ctx.AddTask(c, chasm.TaskAttributes{
			Destination: event.Destination,
		}, &nexusoperationpb.CancellationTask{
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
		c.Attempt++
		c.NextAttemptScheduleTime = nil

		ctx.AddTask(c, chasm.TaskAttributes{
			Destination: c.Operation.Get(ctx).GetEndpoint(),
		}, &nexusoperationpb.CancellationTask{
			Attempt: c.Attempt,
		})

		return nil
	},
)

// EventCancellationAttemptFailed is triggered when a cancellation attempt is failed with a retryable error.
type EventCancellationAttemptFailed struct {
	Failure     *failurepb.Failure
	RetryPolicy backoff.RetryPolicy
}

var transitionCancellationAttemptFailed = chasm.NewTransition(
	[]nexusoperationpb.CancellationStatus{nexusoperationpb.CANCELLATION_STATUS_SCHEDULED},
	nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF,
	func(c *Cancellation, ctx chasm.MutableContext, event EventCancellationAttemptFailed) error {
		currentTime := ctx.Now(c)

		c.LastAttemptCompleteTime = timestamppb.New(currentTime)
		c.LastAttemptFailure = event.Failure

		nextDelay := event.RetryPolicy.ComputeNextDelay(0, int(c.Attempt), nil)
		nextAttemptScheduleTime := currentTime.Add(nextDelay)
		c.NextAttemptScheduleTime = timestamppb.New(nextAttemptScheduleTime)

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
	Failure *failurepb.Failure
}

var TransitionCancellationFailed = chasm.NewTransition(
	[]nexusoperationpb.CancellationStatus{
		// We can immediately transition to failed since we don't know how to send a cancellation request for an
		// unstarted operation.
		// TODO: This doesn't seem to happen in either the HSM or CHASM implementations.
		nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED,
		nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
	},
	nexusoperationpb.CANCELLATION_STATUS_FAILED,
	func(c *Cancellation, ctx chasm.MutableContext, event EventCancellationFailed) error {
		currentTime := ctx.Now(c)
		c.LastAttemptCompleteTime = timestamppb.New(currentTime)
		c.LastAttemptFailure = event.Failure
		// Terminal state - no tasks to emit.
		return nil
	},
)

// EventCancellationSucceeded is triggered when a cancellation attempt succeeds.
type EventCancellationSucceeded struct {
}

var TransitionCancellationSucceeded = chasm.NewTransition(
	[]nexusoperationpb.CancellationStatus{nexusoperationpb.CANCELLATION_STATUS_SCHEDULED},
	nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED,
	func(c *Cancellation, ctx chasm.MutableContext, event EventCancellationSucceeded) error {
		currentTime := ctx.Now(c)
		c.LastAttemptCompleteTime = timestamppb.New(currentTime)
		c.LastAttemptFailure = nil

		// Terminal state - no tasks to emit.
		return nil
	},
)

// EventCancellationAborted is triggered when a pending cancellation is called off before it
// resolved. Used by workflow reset: a system-initiated (auto-close) cancellation only exists
// because the caller was force-closed, so if the reset run adopts the operation the close is being
// undone and the cancellation must stop before it kills a handler the new run depends on.
type EventCancellationAborted struct {
	Failure *failurepb.Failure
}

// TransitionCancellationAborted moves a still-pending cancellation to a terminal state. FAILED is
// reused rather than adding a status: the outcome the caller cares about is "no further attempts",
// and the task validators gate on SCHEDULED/BACKING_OFF, so any terminal status invalidates the
// in-flight invocation and backoff tasks alike.
var TransitionCancellationAborted = chasm.NewTransition(
	[]nexusoperationpb.CancellationStatus{
		nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED,
		nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
		nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.CANCELLATION_STATUS_FAILED,
	func(c *Cancellation, ctx chasm.MutableContext, event EventCancellationAborted) error {
		currentTime := ctx.Now(c)
		c.LastAttemptCompleteTime = timestamppb.New(currentTime)
		c.LastAttemptFailure = event.Failure
		c.NextAttemptScheduleTime = nil
		// Terminal state - no tasks to emit.
		return nil
	},
)
