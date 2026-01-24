package nexusoperation

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
)

var _ chasm.Component = (*Cancellation)(nil)
var _ chasm.StateMachine[nexusoperationpb.CancellationStatus] = (*Cancellation)(nil)

type Cancellation struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*nexusoperationpb.CancellationState
}

func NewCancellation() *Cancellation {
	return &Cancellation{}
}

func (o *Cancellation) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch o.GetStatus() {
	case nexusoperationpb.CANCELLATION_STATUS_SUCCEEDED:
		return chasm.LifecycleStateCompleted
	case nexusoperationpb.CANCELLATION_STATUS_FAILED,
		nexusoperationpb.CANCELLATION_STATUS_TIMED_OUT:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

func (o *Cancellation) StateMachineState() nexusoperationpb.CancellationStatus {
	return o.GetStatus()
}

func (o *Cancellation) SetStateMachineState(status nexusoperationpb.CancellationStatus) {
	o.SetStatus(status)
}
