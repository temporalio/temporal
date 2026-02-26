package nexusoperation

import (
	"errors"

	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
)

var _ chasm.Component = (*Operation)(nil)
var _ chasm.StateMachine[nexusoperationpb.OperationStatus] = (*Operation)(nil)

// ErrCancellationAlreadyRequested is returned when a cancellation has already been requested for an operation.
var ErrCancellationAlreadyRequested = errors.New("cancellation already requested")

// ErrOperationAlreadyCompleted is returned when trying to cancel an operation that has already completed.
var ErrOperationAlreadyCompleted = errors.New("operation already completed")

type OperationStore any

type Operation struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*nexusoperationpb.OperationState

	// Pointer to an implementation of the "store". For a workflow-based Nexus operation
	// this is a parent pointer back to the workflow. For a standalone Nexus operation this is nil.
	Store chasm.ParentPtr[OperationStore]

	// Cancellation is the child cancellation component (at most one per operation).
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

// Cancel marks the Operation as canceled by creating a child Cancellation component. If a
// cancellation was already requested, ErrCancellationAlreadyRequested is returned. If the
// operation is in STARTED state, the cancellation is immediately scheduled; otherwise it will
// be scheduled when the operation transitions to STARTED.
func (o *Operation) Cancel(ctx chasm.MutableContext, requestedEventID int64) error {
	// TODO: Remove this check when the operation auto-deletes itself on terminal state.
	// The operation may already be in a terminal state because it doesn't yet delete itself. We don't want to accept
	// cancellation in this case unless the terminal event is buffered (not yet handled in CHASM).
	if !transitionCanceled.Possible(o) {
		return ErrOperationAlreadyCompleted
	}
	// Cancel spawns a child Cancellation component; if one already exists we got a duplicate cancellation request.
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
