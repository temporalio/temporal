package workflow

import (
	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

// WorkflowUpdate intentionally does not implement CompletionCallbackMetadata, because it needs
// the parent Workflow component. See below.

func (u *WorkflowUpdate) CompletionCallbacks() chasm.Map[string, *callback.Callback] {
	if u.Callbacks == nil {
		u.Callbacks = make(chasm.Map[string, *callback.Callback])
	}
	return u.Callbacks
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

// updateCallbackHost is the [callback.Host] for attaching completion callbacks to a WorkflowUpdate.
// It satisfies the interface's contract, but fetching the [callbackpb.CallbackMetadata] from the
// parent Workflow.
//
// WorkflowUpdate cannot implement [callback.Host] itself, because a chasm.ParentPtr to the parent
// Workflow component would not be initialized until the transaction that creates the WorkflowUpdate
// completes. So this type is used to essentially wire in the parent Workflow.
type updateCallbackHost struct {
	workflow *Workflow
	update   *WorkflowUpdate
}

var _ callback.Host = (*updateCallbackHost)(nil)

func (h updateCallbackHost) CompletionCallbacks() chasm.Map[string, *callback.Callback] {
	return h.update.CompletionCallbacks()
}

func (h updateCallbackHost) CompletionCallbackMetadata() *callbackpb.CallbackMetadata {
	return h.workflow.CompletionCallbackMetadata()
}

func (h updateCallbackHost) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	return h.update.LifecycleState(ctx)
}

func (h updateCallbackHost) GetNexusCompletion(
	ctx chasm.Context,
	requestID string,
) (nexusrpc.CompleteOperationOptions, error) {
	return h.update.GetNexusCompletion(ctx, requestID)
}
