package nexusoperation

import (
	enumspb "go.temporal.io/api/enums/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
)

var (
	_ chasm.Component                                         = (*Cancellation)(nil)
	_ chasm.StateMachine[nexusoperationpb.CancellationStatus] = (*Cancellation)(nil)
)

// Cancellation is a CHASM component that represents a pending cancellation of a Nexus operation.
type Cancellation struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*nexusoperationpb.CancellationState
}

func newCancellation(state *nexusoperationpb.CancellationState) *Cancellation {
	return &Cancellation{CancellationState: state}
}

// LifecycleState maps the cancellation's status to a CHASM lifecycle state.
func (o *Cancellation) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch o.Status {
	case nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED:
		return chasm.LifecycleStateCompleted
	case nexusoperationpb.CANCELLATION_STATUS_FAILED,
		nexusoperationpb.CANCELLATION_STATUS_TIMED_OUT:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

// StateMachineState returns the current cancellation status.
func (o *Cancellation) StateMachineState() nexusoperationpb.CancellationStatus {
	return o.Status
}

// SetStateMachineState sets the cancellation status.
func (o *Cancellation) SetStateMachineState(status nexusoperationpb.CancellationStatus) {
	o.Status = status
}

func cancellationAPIState(status nexusoperationpb.CancellationStatus) enumspb.NexusOperationCancellationState {
	switch status {
	case nexusoperationpb.CANCELLATION_STATUS_SCHEDULED:
		return enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED
	case nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF:
		return enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF
	case nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED:
		return enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SUCCEEDED
	case nexusoperationpb.CANCELLATION_STATUS_FAILED:
		return enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED
	case nexusoperationpb.CANCELLATION_STATUS_TIMED_OUT:
		return enumspb.NEXUS_OPERATION_CANCELLATION_STATE_TIMED_OUT
	case nexusoperationpb.CANCELLATION_STATUS_BLOCKED:
		return enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BLOCKED
	default:
		return enumspb.NEXUS_OPERATION_CANCELLATION_STATE_UNSPECIFIED
	}
}

// ToCancellationInfo converts a CHASM Cancellation to the API NexusOperationCancellationInfo format.
func (o *Cancellation) ToCancellationInfo(circuitBreakerOpen func(endpoint string) bool, endpoint string) *workflowpb.NexusOperationCancellationInfo {
	state := cancellationAPIState(o.Status)
	blockedReason := ""
	switch {
	case state == enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BLOCKED:
		blockedReason = "The circuit breaker is open."
	case state == enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED && circuitBreakerOpen(endpoint):
		state = enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BLOCKED
		blockedReason = "The circuit breaker is open."
	}

	return &workflowpb.NexusOperationCancellationInfo{
		RequestedTime:           o.RequestedTime,
		State:                   state,
		Attempt:                 o.Attempt,
		LastAttemptCompleteTime: o.LastAttemptCompleteTime,
		LastAttemptFailure:      o.LastAttemptFailure,
		NextAttemptScheduleTime: o.NextAttemptScheduleTime,
		BlockedReason:           blockedReason,
	}
}
