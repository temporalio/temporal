package nexusoperation

import (
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
)

var _ chasm.Component = (*Operation)(nil)
var _ chasm.StateMachine[nexusoperationpb.OperationStatus] = (*Operation)(nil)

type OperationStore interface {
	// TODO
}

type Operation struct {
	chasm.UnimplementedComponent

	// Persisted internal state
	*nexusoperationpb.OperationState

	// Pointer to an implementation of the "store". For a workflow-based Nexus operation
	// this is a parent pointer back to the workflow. For a standalone Nexus operation this is nil.
	Store chasm.ParentPtr[OperationStore]
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
