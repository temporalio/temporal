package nexusoperation

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
)

// EventScheduled is triggered when the operation is meant to be scheduled - immediately after initialization.
type EventScheduled struct {
}

var TransitionScheduled = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{nexusoperationpb.OPERATION_STATUS_UNSPECIFIED},
	nexusoperationpb.OPERATION_STATUS_SCHEDULED,
	func(o *Operation, ctx chasm.MutableContext, event EventScheduled) error {
		panic("not implemented")
	},
)

// EventAttemptFailed is triggered when an invocation attempt is failed with a retryable error.
type EventAttemptFailed struct {
}

var TransitionAttemptFailed = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{nexusoperationpb.OPERATION_STATUS_SCHEDULED},
	nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	func(o *Operation, ctx chasm.MutableContext, event EventAttemptFailed) error {
		panic("not implemented")
	},
)

// EventRescheduled is triggered when the operation is meant to be rescheduled after backing off from a previous
// attempt.
type EventRescheduled struct {
}

var TransitionRescheduled = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{nexusoperationpb.OPERATION_STATUS_BACKING_OFF},
	nexusoperationpb.OPERATION_STATUS_SCHEDULED,
	func(o *Operation, ctx chasm.MutableContext, event EventRescheduled) error {
		panic("not implemented")
	},
)

// EventStarted is triggered when an invocation attempt succeeds and the handler indicates that it started an
// asynchronous operation.
type EventStarted struct {
}

var TransitionStarted = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_STARTED,
	func(o *Operation, ctx chasm.MutableContext, event EventStarted) error {
		panic("not implemented")
	},
)

// EventSucceeded is triggered when an invocation attempt succeeds.
type EventSucceeded struct {
}

var TransitionSucceeded = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_STARTED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_SUCCEEDED,
	func(o *Operation, ctx chasm.MutableContext, event EventSucceeded) error {
		panic("not implemented")
	},
)

// EventFailed is triggered when an invocation attempt is failed with a non retryable error.
type EventFailed struct {
}

var TransitionFailed = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_STARTED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_FAILED,
	func(o *Operation, ctx chasm.MutableContext, event EventFailed) error {
		panic("not implemented")
	},
)

// EventCanceled is triggered when an operation is completed as canceled.
type EventCanceled struct {
}

var TransitionCanceled = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_STARTED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_CANCELED,
	func(o *Operation, ctx chasm.MutableContext, event EventCanceled) error {
		panic("not implemented")
	},
)

// EventTimedOut is triggered when the schedule-to-close timeout is triggered for an operation.
type EventTimedOut struct {
}

var TransitionTimedOut = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_STARTED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_TIMED_OUT,
	func(o *Operation, ctx chasm.MutableContext, event EventTimedOut) error {
		panic("not implemented")
	},
)
