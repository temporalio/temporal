package workflow

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

// CompletionCallbacks implements callback.Holder.
func (w *Workflow) CompletionCallbacks() chasm.Map[string, *callback.Callback] {
	if w.Callbacks == nil {
		w.Callbacks = make(chasm.Map[string, *callback.Callback])
	}
	return w.Callbacks
}

// CompletionCallbackMetadata implements callback.Host. The Workflow holds the
// metadata for every callback in the execution, including those attached to its WorkflowUpdates.
func (w *Workflow) CompletionCallbackMetadata() *callbackpb.CallbackMetadata {
	if w.CallbackMetadata == nil {
		w.CallbackMetadata = &callbackpb.CallbackMetadata{}
	}
	return w.CallbackMetadata
}

// GetNexusCompletion implements callback.CompletionSource.
func (w *Workflow) GetNexusCompletion(
	ctx chasm.Context,
	requestID string,
) (nexusrpc.CompleteOperationOptions, error) {
	// Retrieve the completion data from the underlying mutable state via MSPointer.
	return w.MSPointer.GetNexusCompletion(ctx, requestID)
}
