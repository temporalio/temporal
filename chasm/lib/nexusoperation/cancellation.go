package nexusoperation

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
)

var _ chasm.Component = (*Cancellation)(nil)
var _ chasm.StateMachine[nexusoperationpb.CancellationStatus] = (*Cancellation)(nil)

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
