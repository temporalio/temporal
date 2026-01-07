package nexusoperation

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
)

// EventCancellationScheduled is triggered when cancellation is meant to be scheduled for the first time - immediately
// after it has been requested.
type EventCancellationScheduled struct {
}

var TransitionCancellationScheduled = chasm.NewTransition(
	[]nexusoperationpb.CancellationStatus{nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED},
	nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
	func(c *Cancellation, ctx chasm.MutableContext, event EventCancellationScheduled) error {
		panic("not implemented")
	},
)

// EventCancellationRescheduled is triggered when cancellation is meant to be rescheduled after backing off from a
// previous attempt.
type EventCancellationRescheduled struct {
}

var TransitionCancellationRescheduled = chasm.NewTransition(
	[]nexusoperationpb.CancellationStatus{nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF},
	nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
	func(c *Cancellation, ctx chasm.MutableContext, event EventCancellationRescheduled) error {
		panic("not implemented")
	},
)

// EventCancellationAttemptFailed is triggered when a cancellation attempt is failed with a retryable error.
type EventCancellationAttemptFailed struct {
}

var TransitionCancellationAttemptFailed = chasm.NewTransition(
	[]nexusoperationpb.CancellationStatus{nexusoperationpb.CANCELLATION_STATUS_SCHEDULED},
	nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF,
	func(c *Cancellation, ctx chasm.MutableContext, event EventCancellationAttemptFailed) error {
		panic("not implemented")
	},
)

// EventCancellationFailed is triggered when a cancellation attempt is failed with a non retryable error.
type EventCancellationFailed struct {
}

var TransitionCancellationFailed = chasm.NewTransition(
	[]nexusoperationpb.CancellationStatus{
		// We can immediately transition to failed to since we don't know how to send a cancellation request for an
		// unstarted operation.
		nexusoperationpb.CANCELLATION_STATUS_UNSPECIFIED,
		nexusoperationpb.CANCELLATION_STATUS_SCHEDULED,
	},
	nexusoperationpb.CANCELLATION_STATUS_FAILED,
	func(c *Cancellation, ctx chasm.MutableContext, event EventCancellationFailed) error {
		panic("not implemented")
	},
)

// EventCancellationSucceeded is triggered when a cancellation attempt succeeds.
type EventCancellationSucceeded struct {
}

var TransitionCancellationSucceeded = chasm.NewTransition(
	[]nexusoperationpb.CancellationStatus{nexusoperationpb.CANCELLATION_STATUS_SCHEDULED},
	nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED,
	func(c *Cancellation, ctx chasm.MutableContext, event EventCancellationSucceeded) error {
		panic("not implemented")
	},
)
