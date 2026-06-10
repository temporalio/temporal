package callback

import (
	"context"
	"log"

	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/namespace"
)

// PROTOTYPE
type invokeWorkerCallback struct {
	callback *callbackspb.Callback_Nexus
}

func (c invokeWorkerCallback) WrapError(_ invocationResult, err error) error {
	return err
}

func (c invokeWorkerCallback) Invoke(
	ctx context.Context,
	ns *namespace.Namespace,
	h *invocationTaskHandler,
	task *callbackspb.InvocationTask,
	taskAttr chasm.TaskAttributes,
) invocationResult {

	// TODO: Tease out headers and things.
	// TODO: Craft the payload, etc.
	// TODO: Make this less jank.

	// TODO: How to create the SAA CHASM object?
	// - Fork the code from the activity frontend handler?
	// - Somehow inject the service?
	// - Just duplicate the logic and call

	log.Fatal("NYI: Got invokeWorkerCallback invocation, need to spawn an SAA")
	return invocationResultOK{}
}
