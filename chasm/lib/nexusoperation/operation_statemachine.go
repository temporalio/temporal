package nexusoperation

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
)

// EventScheduled is triggered when the operation is meant to be scheduled - immediately after initialization.
type EventScheduled struct {
}

var transitionScheduled = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{nexusoperationpb.OPERATION_STATUS_UNSPECIFIED},
	nexusoperationpb.OPERATION_STATUS_SCHEDULED,
	func(o *Operation, ctx chasm.MutableContext, event EventScheduled) error {
		return serviceerror.NewUnimplemented("unimplemented")
	},
)

// EventAttemptFailed is triggered when an invocation attempt is failed with a retryable error.
type EventAttemptFailed struct {
}

var transitionAttemptFailed = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{nexusoperationpb.OPERATION_STATUS_SCHEDULED},
	nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	func(o *Operation, ctx chasm.MutableContext, event EventAttemptFailed) error {
		return serviceerror.NewUnimplemented("unimplemented")
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
		return serviceerror.NewUnimplemented("unimplemented")
	},
)

// EventStarted is triggered when an invocation attempt succeeds and the handler indicates that it started an
// asynchronous operation.
type EventStarted struct {
}

var transitionStarted = chasm.NewTransition(
	[]nexusoperationpb.OperationStatus{
		nexusoperationpb.OPERATION_STATUS_SCHEDULED,
		nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	},
	nexusoperationpb.OPERATION_STATUS_STARTED,
	func(o *Operation, ctx chasm.MutableContext, event EventStarted) error {
		return serviceerror.NewUnimplemented("unimplemented")
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
		return serviceerror.NewUnimplemented("unimplemented")
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
		return serviceerror.NewUnimplemented("unimplemented")
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
		return serviceerror.NewUnimplemented("unimplemented")
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
		return serviceerror.NewUnimplemented("unimplemented")
	},
)
