package fauxsdk

import (
	"go.temporal.io/sdk/client"

	commonpb "go.temporal.io/api/common/v1"
)

// CallbackRef is a reference to a Nexus-based completion handler. (Which is a Nexus operation
// with a specific type signature.)
//
// This is intentionally opaque, to support different kinds of worker callbacks in the future.
// For now the only way to create a valid instance is via `func UseNotificationService(...)`.
type CallbackRef struct {
	taskqueueName string
	callContext   any
}

func UseNotificationService(targetTaskQueue string, sourceCallContext any) *CallbackRef {
	return &CallbackRef{
		taskqueueName: targetTaskQueue,
		callContext:   sourceCallContext,
	}
}

// AttachWorkerCallback attaches the target worker callback to the StartNexusOperationOptions.
func AttachWorkerCallback(
	inCallOpts *client.StartNexusOperationOptions,
	callback *CallbackRef) {

	// Build the Callback proto variant.
	cb := &commonpb.Callback_NexusWorker_{
		NexusWorker: &commonpb.Callback_NexusWorker{
			TaskqueueName: callback.taskqueueName,
			SourceContext: mustBuildPayloads(callback.callContext),
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
