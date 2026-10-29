package workflow

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
)

type WorkflowUpdate struct {
	chasm.UnimplementedComponent

	*workflowpb.UpdateState

	// MSPointer is a special in-memory field for accessing the underlying mutable state.
	chasm.MSPointer

	// Callbacks map is used to store the callbacks for the update.
	Callbacks chasm.Map[string, *callback.Callback]
}

func NewWorkflowUpdate(
	_ chasm.MutableContext, updateID string, msPointer chasm.MSPointer,
) *WorkflowUpdate {
	return &WorkflowUpdate{
		UpdateState: &workflowpb.UpdateState{
			UpdateId: updateID,
		},
		MSPointer: msPointer,
	}
}

func (u *WorkflowUpdate) LifecycleState(
	_ chasm.Context,
) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}
