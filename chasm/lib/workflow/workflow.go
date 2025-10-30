package workflow

import (
	"go.temporal.io/server/chasm"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	// Archetype for today's workflow implementation.
	// This value is NOT persisted today, and ok to be changed.
	Archetype chasm.Archetype = "workflow.Workflow"
)

type Workflow struct {
	chasm.UnimplementedComponent

	// State of the workflow is managed by mutable_state_impl, not CHASM engine, so this will always be empty.
	State *emptypb.Empty

	// MSPointer is a special in-memory field for getting mutable state access.
	MSPointer chasm.MSPointer

	// TODO: populate with actual callback component type
	// Callbacks chasm.Map[string, *CallbackComponent]
}

func NewWorkflow(
	_ chasm.MutableContext,
	msPointer chasm.MSPointer,
) *Workflow {
	return &Workflow{
		MSPointer: msPointer,
	}
}

func (w *Workflow) LifecycleState(
	_ chasm.Context,
) chasm.LifecycleState {
	// NOTE: closeTransactionHandleRootLifecycleChange() is bypassed in tree.go
	//
	// NOTE: detached mode is not implemented yet, so always return Running here.
	// Otherwise, tasks for callback component can't be executed after workflow is closed.
	return chasm.LifecycleStateRunning
}
