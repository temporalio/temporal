package nexusoperation

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"google.golang.org/protobuf/types/known/anypb"
)

var _ chasm.Component = (*Operation)(nil)
var _ chasm.StateMachine[nexusoperationpb.OperationStatus] = (*Operation)(nil)

// ErrCancellationAlreadyRequested is returned when a cancellation has already been requested for an operation.
var ErrCancellationAlreadyRequested = serviceerror.NewFailedPrecondition("cancellation already requested")

// ErrOperationAlreadyCompleted is returned when trying to cancel an operation that has already completed.
var ErrOperationAlreadyCompleted = serviceerror.NewFailedPrecondition("operation already completed")

// OperationStore defines the interface that must be implemented by any parent component that wants to manage Nexus operations.
// It's the responsibility of the parrent component to apply the appropriate state transitions to the operation.
type OperationStore interface {
	OnNexusOperationStarted(ctx chasm.MutableContext, operation *Operation) error
	OnNexusOperationCancelled(ctx chasm.MutableContext, operation *Operation) error
	OnNexusOperationFailed(ctx chasm.MutableContext, operation *Operation) error
	OnNexusOperationTimedOut(ctx chasm.MutableContext, operation *Operation) error
	OnNexusOperationCompleted(ctx chasm.MutableContext, operation *Operation) error
}

// Operation is a CHASM component that represents a Nexus operation.
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

// NewOperation creates a new Operation component with the given persisted state.
func NewOperation(state *nexusoperationpb.OperationState) *Operation {
	return &Operation{OperationState: state}
}

// LifecycleState maps the operation's status to a CHASM lifecycle state.
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

// StateMachineState returns the current operation status.
func (o *Operation) StateMachineState() nexusoperationpb.OperationStatus {
	return o.Status
}

// SetStateMachineState sets the operation status.
func (o *Operation) SetStateMachineState(status nexusoperationpb.OperationStatus) {
	o.Status = status
}

// Cancel requests cancellation of the operation. It creates a Cancellation child component and, if the
// operation has already started, schedules the cancellation request to be sent to the Nexus endpoint.
// parentData is opaque data injected by the parent (e.g. workflow) for its own bookkeeping.
func (o *Operation) Cancel(ctx chasm.MutableContext, parentData *anypb.Any) error {
	if !TransitionCanceled.Possible(o) {
		return ErrOperationAlreadyCompleted
	}
	if _, ok := o.Cancellation.TryGet(ctx); ok {
		return ErrCancellationAlreadyRequested
	}

	cancellation := newCancellation(&nexusoperationpb.CancellationState{
		ParentData: parentData,
	})
	o.Cancellation = chasm.NewComponentField(ctx, cancellation)

	// Once started, the handler returns a token that can be used in the cancelation request.
	// Until then, no need to schedule the cancelation.
	if o.Status == nexusoperationpb.OPERATION_STATUS_STARTED {
		return transitionCancellationScheduled.Apply(cancellation, ctx, EventCancellationScheduled{})
	}

	return nil
}

// StoreProcessor returns the OperationStore that should be used to process this operation's state transitions and commands.
// For a workflow-embedded Nexus operation, this will return the parent workflow as the store processor.
// For a standalone Nexus operation, this will return the operation itself as the store processor.
func (o *Operation) StoreProcessor(ctx chasm.Context) OperationStore {
	store, ok := o.Store.TryGet(ctx)
	if ok {
		return store
	}
	return o
}

func (o *Operation) OnNexusOperationStarted(ctx chasm.MutableContext, _ *Operation) error {
	return transitionStarted.Apply(o, ctx, EventStarted{
		OperationToken: o.GetOperationToken(),
		FromBackingOff: o.Status == nexusoperationpb.OPERATION_STATUS_BACKING_OFF,
	})
}

func (o *Operation) OnNexusOperationCompleted(ctx chasm.MutableContext, _ *Operation) error {
	return transitionSucceeded.Apply(o, ctx, EventSucceeded{})
}

func (o *Operation) OnNexusOperationFailed(ctx chasm.MutableContext, _ *Operation) error {
	return transitionFailed.Apply(o, ctx, EventFailed{})
}

func (o *Operation) OnNexusOperationCancelled(ctx chasm.MutableContext, _ *Operation) error {
	return TransitionCanceled.Apply(o, ctx, EventCanceled{})
}

func (o *Operation) OnNexusOperationTimedOut(ctx chasm.MutableContext, _ *Operation) error {
	return transitionTimedOut.Apply(o, ctx, EventTimedOut{})
}
