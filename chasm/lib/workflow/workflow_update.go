package workflow

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
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

func (u *WorkflowUpdate) GetNexusCompletion(
	ctx chasm.Context,
	requestID string,
) (nexusrpc.CompleteOperationOptions, error) {
	// If the update was rejected, return the rejection failure directly instead
	// of looking up a completion event that doesn't exist.
	if rf := u.GetRejectionFailure(); rf != nil {
		f, err := commonnexus.TemporalFailureToNexusFailure(rf)
		if err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		opErr := &nexus.OperationError{
			Message: "update rejected",
			State:   nexus.OperationStateFailed,
			Cause:   &nexus.FailureError{Failure: f},
		}
		if err := nexusrpc.MarkAsWrapperError(nexusrpc.DefaultFailureConverter(), opErr); err != nil {
			return nexusrpc.CompleteOperationOptions{}, err
		}
		return nexusrpc.CompleteOperationOptions{
			Error: opErr,
		}, nil
	}

	// Retrieve the completion data from the underlying mutable state via MSPointer
	return u.GetNexusUpdateCompletion(ctx, u.UpdateId, requestID)
}
