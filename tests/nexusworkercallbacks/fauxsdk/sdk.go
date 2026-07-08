package fauxsdk

import (
	"go.temporal.io/sdk/client"

	commonpb "go.temporal.io/api/common/v1"
)

// CallbackRef is a reference to a Nexus-based completion handler. (Which is a Nexus operation
// with a specific type signature.)
type CallbackRef struct {
	// TaskQueueName is the task queue the completion callback is dispatched to when the source
	// operation reaches a terminal state.
	TaskQueueName string
}

// AttachWorkerCallback attaches the target worker callback to the StartNexusOperationOptions.
func AttachWorkerCallback(
	inCallOpts *client.StartNexusOperationOptions,
	ref CallbackRef,
	context any) {

	// Build the Callback proto variant.
	cb := &commonpb.Callback_NexusWorker_{
		NexusWorker: &commonpb.Callback_NexusWorker{
			TaskqueueName: ref.TaskQueueName,
			SourceContext: mustBuildPayloads(context),
		},
	}
	newCB := &commonpb.Callback{
		Variant: cb,
		// TODO: What should go here? A link to the SANO operation we are creating in this same request?
		Links: nil,
	}

	// Attach it to the StartNexusOperationOptions. In the future, we will NOT expose the
	// raw completion callbacks. But instead have the type exposed from the SDK take some
	// SDK-defined interface. (And generate the Callback protobuf internally.)
	existing := inCallOpts.HackRawCompletionCallbacks
	inCallOpts.HackRawCompletionCallbacks = append(existing, newCB)
}
