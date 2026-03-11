package nexusoperation

import (
	"errors"

	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
)

var (
	_ chasm.Component                                      = (*Operation)(nil)
	_ chasm.StateMachine[nexusoperationpb.OperationStatus] = (*Operation)(nil)

	// ErrCancellationAlreadyRequested is returned when a cancellation has already been requested for an operation.
	ErrCancellationAlreadyRequested = errors.New("cancellation already requested")

	// ErrOperationAlreadyCompleted is returned when trying to cancel an operation that has already completed.
	ErrOperationAlreadyCompleted = errors.New("operation already completed")
)

type OperationStore any

type Operation struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*nexusoperationpb.OperationState

	// Pointer to an implementation of the "store". For a workflow-based Nexus operation
	// this is a parent pointer back to the workflow. For a standalone Nexus operation this is nil.
	Store chasm.ParentPtr[OperationStore]

	// Cancellation is a child component that manages sending the cancel request to the Nexus endpoint.
	Cancellation chasm.Field[*Cancellation]
}

func NewOperation(state *nexusoperationpb.OperationState) *Operation {
	return &Operation{OperationState: state}
}

func (o *Operation) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch o.Status {
	case nexusoperationpb.OPERATION_STATUS_SUCCEEDED:
		return chasm.LifecycleStateCompleted
	case nexusoperationpb.OPERATION_STATUS_FAILED,
		nexusoperationpb.OPERATION_STATUS_CANCELED,
		nexusoperationpb.OPERATION_STATUS_TIMED_OUT:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

func (o *Operation) StateMachineState() nexusoperationpb.OperationStatus {
	return o.Status
}

func (o *Operation) SetStateMachineState(status nexusoperationpb.OperationStatus) {
	o.Status = status
}

// Cancel requests cancellation of the operation. It creates a Cancellation child component and, if the
// operation has already started, schedules the cancellation request to be sent to the Nexus endpoint.
func (o *Operation) Cancel(ctx chasm.MutableContext, requestedEventID int64) error {
	if !TransitionCanceled.Possible(o) {
		return ErrOperationAlreadyCompleted
	}
	if _, ok := o.Cancellation.TryGet(ctx); ok {
		return ErrCancellationAlreadyRequested
	}

	cancellation := NewCancellation(&nexusoperationpb.CancellationState{
		RequestedEventId: requestedEventID,
	})
	o.Cancellation = chasm.NewComponentField(ctx, cancellation)

	if o.Status == nexusoperationpb.OPERATION_STATUS_STARTED {
		return transitionCancellationScheduled.Apply(cancellation, ctx, EventCancellationScheduled{})
	}

	return nil
}
